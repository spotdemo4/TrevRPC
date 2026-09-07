#include "lifecycle.hpp"

#include "callbacks.hpp"
#include "rpc_server_core.hpp"

#include <cerrno>
#include <condition_variable>
#include <deque>
#include <functional>
#include <thread>
#include <utility>

namespace trevrpc::detail {
namespace {

thread_local const void* current_executor;
thread_local bool in_server_callback;
thread_local bool in_channel_callback;

class LifecycleReaper final {
public:
  LifecycleReaper() : worker_([this] { run(); }) {}
  ~LifecycleReaper() = delete;
  LifecycleReaper(const LifecycleReaper&) = delete;
  LifecycleReaper& operator=(const LifecycleReaper&) = delete;

  void submit(std::function<void()> work) noexcept {
    try {
      std::lock_guard lock(mutex_);
      if (queue_.size() >= capacity_) {
        std::terminate();
      }
      queue_.push_back(std::move(work));
      condition_.notify_one();
    } catch (...) {
      std::terminate();
    }
  }

  [[nodiscard]] bool drain_until(std::chrono::steady_clock::time_point deadline) noexcept {
    std::unique_lock lock(mutex_);
    return condition_.wait_until(lock, deadline, [this] { return queue_.empty() && !working_; });
  }

private:
  void run() noexcept {
    for (;;) {
      std::function<void()> work;
      {
        std::unique_lock lock(mutex_);
        condition_.wait(lock, [this] { return !queue_.empty(); });
        work = std::move(queue_.front());
        queue_.pop_front();
        working_ = true;
      }
      try {
        work();
      } catch (...) {
        std::terminate();
      }
      {
        std::lock_guard lock(mutex_);
        working_ = false;
      }
      condition_.notify_all();
    }
  }

  static constexpr std::size_t capacity_ = 256;
  std::mutex mutex_;
  std::condition_variable condition_;
  std::deque<std::function<void()>> queue_;
  bool working_ = false;
  std::thread worker_;
};

[[nodiscard]] LifecycleReaper& lifecycle_reaper() {
  static LifecycleReaper* reaper = new LifecycleReaper();
  return *reaper;
}

} // namespace

ServerState::~ServerState() {
  std::lock_guard lock(mutex_);
  if (rpc_core_) {
    std::terminate();
  }
}

Result<std::uint16_t> ServerState::port() const {
  std::shared_ptr<RpcServerCore> rpc_core;
  {
    std::lock_guard lock(mutex_);
    rpc_core = rpc_core_;
  }
  if (!rpc_core) {
    return Error::runtime(-ESHUTDOWN, "server is released");
  }
  return rpc_core->port();
}

Result<ServerPhase> ServerState::phase() const {
  std::lock_guard lock(mutex_);
  if (!rpc_core_) {
    return ServerPhase::Released;
  }
  if (stop_requested_) {
    return ServerPhase::Stopping;
  }
  return rpc_frozen_ ? ServerPhase::Serving : ServerPhase::Configuring;
}

Result<void> ServerState::freeze() {
  std::shared_ptr<RpcServerCore> rpc_core;
  {
    std::lock_guard lock(mutex_);
    if (!rpc_core_ || stop_requested_) {
      return Error::runtime(-ESHUTDOWN, "server is stopping or released");
    }
    if (rpc_frozen_) {
      return {};
    }
    rpc_frozen_ = true;
    rpc_core = rpc_core_;
  }
  auto started = rpc_core->start();
  if (!started) {
    std::lock_guard lock(mutex_);
    rpc_frozen_ = false;
    return started.error();
  }
  return {};
}

Result<void> ServerState::request_stop() {
  std::shared_ptr<RpcServerCore> rpc_core;
  {
    std::lock_guard lock(mutex_);
    rpc_core = rpc_core_;
    if (!rpc_core) {
      return {};
    }
    stop_requested_ = true;
  }
  return rpc_core->request_stop();
}

Result<ShutdownReport> ServerState::shutdown(const ShutdownOptions& options) {
  if ((options.graceful_timeout.has_value() && options.graceful_timeout->count() < 0) ||
      options.cancellation_timeout.count() < 0) {
    return Error::runtime(-EINVAL, "shutdown durations must not be negative");
  }
  if (running_in_server_callback() || running_in_executor_context()) {
    return Error::runtime(-EDEADLK,
                          "blocking shutdown from a callback or executor worker is forbidden");
  }

  std::unique_lock shutdown_lock(shutdown_mutex_);
  std::shared_ptr<RpcServerCore> rpc_core;
  {
    std::lock_guard lock(mutex_);
    if (final_report_) {
      return *final_report_;
    }
    rpc_core = rpc_core_;
    if (!rpc_core) {
      return ShutdownReport{ShutdownOutcome::Graceful, ServerPhase::Released, true};
    }
    stop_requested_ = true;
  }

  auto report = rpc_core->shutdown(options);
  if (report && report.value().released) {
    std::lock_guard lock(mutex_);
    if (rpc_core_ == rpc_core) {
      rpc_core_.reset();
      stop_requested_ = true;
      final_report_ = report.value();
    }
  }
  return report;
}

void ServerState::cancel_for_abandonment() noexcept {
  std::shared_ptr<RpcServerCore> rpc_core;
  {
    std::lock_guard lock(mutex_);
    rpc_core = rpc_core_;
    stop_requested_ = true;
  }
  if (rpc_core) {
    rpc_core->cancel_for_abandonment();
  }
}

Result<void>
ServerState::register_rpc_route(std::string_view service, std::string_view method,
                                std::uint32_t kind,
                                std::function<void(std::shared_ptr<ServerCallState>)> callback,
                                const std::shared_ptr<void>& route,
                                const std::shared_ptr<AsyncServerScopeControl>& async_scope) {
  if (!route || !callback) {
    return Error::runtime(-EINVAL, "server route must not be null");
  }
  std::shared_ptr<RpcServerCore> rpc_core;
  {
    std::lock_guard lock(mutex_);
    if (stop_requested_) {
      return Error::runtime(-ESHUTDOWN, "server is stopping or released");
    }
    rpc_core = rpc_core_;
  }
  if (!rpc_core) {
    return Error::runtime(-ESHUTDOWN, "server is stopping or released");
  }
  return rpc_core->register_route(service, method, kind, std::move(callback), route, async_scope);
}

Result<void> ServerState::set_authorizer(std::shared_ptr<AuthorizerCallbackState> callback) {
  if (!callback || !callback->callback) {
    return Error::runtime(-EINVAL, "authorizer must not be null");
  }
  std::shared_ptr<AuthorizerCallbackState> previous;
  {
    std::lock_guard lock(mutex_);
    if (!rpc_core_ || stop_requested_) {
      return Error::runtime(-ESHUTDOWN, "server is stopping or released");
    }
    if (rpc_frozen_) {
      return Error::runtime(-EALREADY, "server configuration is frozen");
    }
    previous = std::exchange(authorizer_, callback);
    rpc_core_->set_authorizer(std::move(callback));
  }
  return {};
}

Result<void> ServerState::set_metrics(std::shared_ptr<MetricsCallbackState> callback) {
  if (!callback || !callback->callback) {
    return Error::runtime(-EINVAL, "metrics observer must not be null");
  }
  std::shared_ptr<MetricsCallbackState> previous;
  {
    std::lock_guard lock(mutex_);
    if (!rpc_core_ || stop_requested_) {
      return Error::runtime(-ESHUTDOWN, "server is stopping or released");
    }
    if (rpc_frozen_) {
      return Error::runtime(-EALREADY, "server configuration is frozen");
    }
    previous = std::exchange(metrics_, callback);
    rpc_core_->set_metrics(std::move(callback));
  }
  return {};
}

Result<void> ServerState::set_logger(std::shared_ptr<LoggerCallbackState> callback) {
  if (!callback || !callback->callback) {
    return Error::runtime(-EINVAL, "logger must not be null");
  }
  std::shared_ptr<LoggerCallbackState> previous;
  {
    std::lock_guard lock(mutex_);
    if (!rpc_core_ || stop_requested_) {
      return Error::runtime(-ESHUTDOWN, "server is stopping or released");
    }
    if (rpc_frozen_) {
      return Error::runtime(-EALREADY, "server configuration is frozen");
    }
    previous = std::exchange(logger_, callback);
    rpc_core_->set_logger(std::move(callback));
  }
  return {};
}

Result<void> ServerState::clear_authorizer() {
  std::shared_ptr<AuthorizerCallbackState> previous;
  {
    std::lock_guard lock(mutex_);
    if (!rpc_core_ || stop_requested_) {
      return Error::runtime(-ESHUTDOWN, "server is stopping or released");
    }
    if (rpc_frozen_) {
      return Error::runtime(-EALREADY, "server configuration is frozen");
    }
    previous = std::move(authorizer_);
    rpc_core_->set_authorizer({});
  }
  return {};
}

Result<void> ServerState::clear_metrics() {
  std::shared_ptr<MetricsCallbackState> previous;
  {
    std::lock_guard lock(mutex_);
    if (!rpc_core_ || stop_requested_) {
      return Error::runtime(-ESHUTDOWN, "server is stopping or released");
    }
    if (rpc_frozen_) {
      return Error::runtime(-EALREADY, "server configuration is frozen");
    }
    previous = std::move(metrics_);
    rpc_core_->set_metrics({});
  }
  return {};
}

Result<void> ServerState::clear_logger() {
  std::shared_ptr<LoggerCallbackState> previous;
  {
    std::lock_guard lock(mutex_);
    if (!rpc_core_ || stop_requested_) {
      return Error::runtime(-ESHUTDOWN, "server is stopping or released");
    }
    if (rpc_frozen_) {
      return Error::runtime(-EALREADY, "server configuration is frozen");
    }
    previous = std::move(logger_);
    rpc_core_->set_logger({});
  }
  return {};
}

bool drain_lifecycle_reaper_until(std::chrono::steady_clock::time_point deadline) noexcept {
  return lifecycle_reaper().drain_until(deadline);
}

void abandon_server(std::shared_ptr<ServerState> state) noexcept {
  if (!state) {
    return;
  }
  lifecycle_reaper().submit([state = std::move(state)] {
    ShutdownOptions options;
    options.graceful_timeout = std::chrono::nanoseconds(0);
    options.cancellation_timeout = std::chrono::nanoseconds::max();
    for (;;) {
      auto report = state->shutdown(options);
      if (report && report.value().released) {
        return;
      }
      const int error = report ? -ETIMEDOUT : report.error().code();
      if (error != -EAGAIN && error != -EBUSY && error != -EDEADLK &&
          error != -ETIMEDOUT && error != -ESHUTDOWN) {
        std::terminate();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });
}

ServerCallbackContextGuard::ServerCallbackContextGuard() noexcept : previous_(in_server_callback) {
  in_server_callback = true;
}

ServerCallbackContextGuard::~ServerCallbackContextGuard() { in_server_callback = previous_; }

bool running_in_server_callback() noexcept { return in_server_callback; }

ChannelCallbackContextGuard::ChannelCallbackContextGuard() noexcept
    : previous_(in_channel_callback) {
  in_channel_callback = true;
}

ChannelCallbackContextGuard::~ChannelCallbackContextGuard() {
  in_channel_callback = previous_;
}

bool running_in_channel_callback() noexcept { return in_channel_callback; }

ExecutorContextGuard::ExecutorContextGuard(const void* executor) noexcept
    : previous_(current_executor) {
  current_executor = executor;
}

ExecutorContextGuard::~ExecutorContextGuard() { current_executor = previous_; }

bool running_in_executor_context() noexcept { return current_executor != nullptr; }

bool running_in_executor_context(const void* executor) noexcept {
  return current_executor == executor;
}

} // namespace trevrpc::detail
