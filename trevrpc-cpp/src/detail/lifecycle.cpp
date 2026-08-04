#include "lifecycle.hpp"

#include "callbacks.hpp"

#include <cerrno>
#include <deque>
#include <functional>
#include <limits>
#include <thread>

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

[[nodiscard]] ServerPhase phase_from_native(std::uint32_t phase) {
  switch (phase) {
  case TREVRPC_SERVER_PHASE_CONFIGURING:
    return ServerPhase::Configuring;
  case TREVRPC_SERVER_PHASE_FROZEN:
    return ServerPhase::Frozen;
  case TREVRPC_SERVER_PHASE_SERVING:
    return ServerPhase::Serving;
  case TREVRPC_SERVER_PHASE_STOPPING:
    return ServerPhase::Stopping;
  case TREVRPC_SERVER_PHASE_CANCELLING:
    return ServerPhase::Cancelling;
  case TREVRPC_SERVER_PHASE_STOPPED:
  default:
    break;
  }
  return ServerPhase::Stopped;
}

[[nodiscard]] Result<std::chrono::steady_clock::time_point>
steady_deadline_from_native(std::uint64_t native_deadline) {
  if (native_deadline == TREVRPC_DEADLINE_INFINITE) {
    return std::chrono::steady_clock::time_point::max();
  }
  std::uint64_t native_now = 0;
  const int error = trevrpc_monotonic_now_nanos(&native_now);
  if (error != 0) {
    return Error::runtime(error);
  }
  if (native_deadline <= native_now) {
    return std::chrono::steady_clock::now();
  }
  const std::uint64_t remaining = native_deadline - native_now;
  const auto max_remaining =
      static_cast<std::uint64_t>(std::numeric_limits<std::chrono::nanoseconds::rep>::max());
  if (remaining >= max_remaining) {
    return std::chrono::steady_clock::time_point::max();
  }
  const auto duration =
      std::chrono::nanoseconds(static_cast<std::chrono::nanoseconds::rep>(remaining));
  const auto now = std::chrono::steady_clock::now();
  if (duration >= std::chrono::steady_clock::time_point::max() - now) {
    return std::chrono::steady_clock::time_point::max();
  }
  return now + duration;
}

} // namespace

ChannelState::Admission::~Admission() {
  if (state_) {
    state_->leave();
  }
}

ChannelState::Admission& ChannelState::Admission::operator=(Admission&& other) noexcept {
  if (this != &other) {
    if (state_) {
      state_->leave();
    }
    state_ = std::move(other.state_);
  }
  return *this;
}

trevrpc_channel* ChannelState::Admission::native_handle() const noexcept {
  return state_ ? state_->native_handle() : nullptr;
}

ChannelState::~ChannelState() { close(); }

Result<ChannelState::Admission> ChannelState::admit() {
  std::lock_guard lock(mutex_);
  if (closing_ || channel_ == nullptr) {
    return Error::runtime(-ESHUTDOWN, "channel is closing");
  }
  ++entered_;
  return Admission(shared_from_this());
}

void ChannelState::leave() noexcept {
  std::lock_guard lock(mutex_);
  if (entered_ > 0) {
    --entered_;
  }
  if (entered_ == 0) {
    condition_.notify_all();
  }
}

void ChannelState::close() noexcept {
  trevrpc_channel* channel = nullptr;
  {
    std::unique_lock lock(mutex_);
    if (released_) {
      return;
    }
    closing_ = true;
    if (running_in_channel_callback()) {
      if (!release_deferred_) {
        release_deferred_ = true;
        auto state = shared_from_this();
        lock.unlock();
        abandon_channel(std::move(state));
      }
      return;
    }
    channel = channel_;
  }
  if (channel != nullptr) {
    trevrpc_channel_close(channel);
  }
  {
    std::unique_lock lock(mutex_);
    condition_.wait(lock, [this] { return entered_ == 0; });
    if (released_) {
      return;
    }
    channel = channel_;
    channel_ = nullptr;
    released_ = true;
  }
  if (channel != nullptr) {
    trevrpc_channel_release(channel);
  }
  std::shared_ptr<ChannelLifecycleCallbackState> lifecycle_callback;
  {
    std::lock_guard lock(mutex_);
    lifecycle_callback = std::move(lifecycle_callback_);
  }
  (void)lifecycle_callback;
}

trevrpc_channel* ChannelState::native_handle() const noexcept {
  std::lock_guard lock(mutex_);
  return channel_;
}

ServerState::Admission::~Admission() {
  if (state_) {
    state_->leave();
  }
}

ServerState::Admission& ServerState::Admission::operator=(Admission&& other) noexcept {
  if (this != &other) {
    if (state_) {
      state_->leave();
    }
    state_ = std::move(other.state_);
  }
  return *this;
}

trevrpc_server* ServerState::Admission::native_handle() const noexcept {
  return state_ ? state_->native_handle() : nullptr;
}

ServerState::~ServerState() {
  std::lock_guard lock(mutex_);
  if (server_ != nullptr) {
    std::terminate();
  }
}

Result<ServerState::Admission> ServerState::admit() {
  std::lock_guard lock(mutex_);
  if (server_ == nullptr || releasing_) {
    return Error::runtime(-ESHUTDOWN, "server is stopping or released");
  }
  ++entered_;
  return Admission(shared_from_this());
}

void ServerState::leave() noexcept {
  std::lock_guard lock(mutex_);
  if (entered_ > 0) {
    --entered_;
  }
  if (entered_ == 0) {
    condition_.notify_all();
  }
}

trevrpc_server* ServerState::native_handle() const noexcept {
  std::lock_guard lock(mutex_);
  return server_;
}

Result<ServerPhase> ServerState::phase() const {
  trevrpc_server* server = native_handle();
  if (server == nullptr) {
    return ServerPhase::Released;
  }
  std::uint32_t phase = 0;
  const int error = trevrpc_server_get_phase(server, &phase);
  if (error != 0) {
    return Error::runtime(error);
  }
  return phase_from_native(phase);
}

Result<void> ServerState::request_stop() {
  {
    std::lock_guard lock(mutex_);
    if (server_ == nullptr) {
      return {};
    }
  }
  auto admission = admit();
  if (!admission) {
    return admission.error();
  }
  const int error = trevrpc_server_stop(admission.value().native_handle());
  return error == 0 ? Result<void>{} : Result<void>{Error::runtime(error)};
}

Result<std::uint64_t> ServerState::deadline_after(std::chrono::nanoseconds timeout) {
  if (timeout.count() < 0) {
    return Error::runtime(-EINVAL, "shutdown durations must not be negative");
  }
  if (timeout == std::chrono::nanoseconds::max()) {
    return TREVRPC_DEADLINE_INFINITE;
  }
  std::uint64_t now = 0;
  const int error = trevrpc_monotonic_now_nanos(&now);
  if (error != 0) {
    return Error::runtime(error);
  }
  const auto delta = static_cast<std::uint64_t>(timeout.count());
  if (delta > std::numeric_limits<std::uint64_t>::max() - now) {
    return TREVRPC_DEADLINE_INFINITE;
  }
  return now + delta;
}

Result<std::uint64_t>
ServerState::graceful_deadline(const std::optional<std::chrono::nanoseconds>& timeout) const {
  if (timeout.has_value()) {
    return deadline_after(*timeout);
  }
  trevrpc_server* server = native_handle();
  if (server == nullptr) {
    return TREVRPC_DEADLINE_INFINITE;
  }
  trevrpc_server_options_v1 options{};
  int error = trevrpc_server_options_v1_init(&options, sizeof(options));
  if (error == 0) {
    error = trevrpc_server_get_options_v1(server, &options);
  }
  if (error != 0) {
    return Error::runtime(error);
  }
  if (options.graceful_shutdown_timeout_nanos == 0) {
    return TREVRPC_DEADLINE_INFINITE;
  }
  if (options.graceful_shutdown_timeout_nanos >
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
    return TREVRPC_DEADLINE_INFINITE;
  }
  return deadline_after(std::chrono::nanoseconds(options.graceful_shutdown_timeout_nanos));
}

Result<ShutdownReport> ServerState::release(ShutdownOutcome outcome) {
  trevrpc_server* server = nullptr;
  {
    std::unique_lock lock(mutex_);
    if (server_ == nullptr) {
      if (final_report_.has_value()) {
        return *final_report_;
      }
      return ShutdownReport{outcome, ServerPhase::Released, true};
    }
    releasing_ = true;
    condition_.wait(lock, [this] { return entered_ == 0; });
    server = server_;
  }
  const int error = trevrpc_server_release(server);
  if (error != 0) {
    std::lock_guard lock(mutex_);
    releasing_ = false;
    condition_.notify_all();
    return Error::runtime(error);
  }

  std::vector<std::shared_ptr<void>> routes;
  std::vector<std::shared_ptr<AsyncServerScopeControl>> async_scopes;
  std::shared_ptr<WebTransportAdmissionState> webtransport_admission;
  std::shared_ptr<Http3AdmissionState> http3_admission;
  std::shared_ptr<AuthorizerCallbackState> authorizer;
  std::shared_ptr<MetricsCallbackState> metrics;
  std::shared_ptr<LoggerCallbackState> logger;
  std::shared_ptr<TransportCallbackState> transport_observer;
  ShutdownReport report{outcome, ServerPhase::Released, true};
  {
    std::lock_guard lock(mutex_);
    if (server_ == server) {
      server_ = nullptr;
      routes = std::move(routes_);
      async_scopes = std::move(async_scopes_);
      webtransport_admission = std::move(webtransport_admission_);
      http3_admission = std::move(http3_admission_);
      authorizer = std::move(authorizer_);
      metrics = std::move(metrics_);
      logger = std::move(logger_);
      transport_observer = std::move(transport_observer_);
      final_report_ = report;
    }
    releasing_ = false;
  }
  condition_.notify_all();
  return report;
}

void ServerState::cancel_async_scopes() noexcept {
  std::size_t index = 0;
  for (;;) {
    std::shared_ptr<AsyncServerScopeControl> scope;
    {
      std::lock_guard lock(mutex_);
      if (index >= async_scopes_.size()) {
        return;
      }
      scope = async_scopes_[index++];
    }
    if (scope) {
      scope->cancel();
    }
  }
}

Result<void>
ServerState::drain_async_scopes_until(std::chrono::steady_clock::time_point deadline) noexcept {
  std::size_t index = 0;
  for (;;) {
    std::shared_ptr<AsyncServerScopeControl> scope;
    {
      std::lock_guard lock(mutex_);
      if (index >= async_scopes_.size()) {
        return {};
      }
      scope = async_scopes_[index++];
    }
    if (!scope) {
      continue;
    }
    auto drained = scope->drain_until(deadline);
    if (!drained) {
      return drained.error();
    }
  }
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
  {
    std::lock_guard lock(mutex_);
    if (final_report_.has_value()) {
      return *final_report_;
    }
    if (server_ == nullptr) {
      return ShutdownReport{ShutdownOutcome::Graceful, ServerPhase::Released, true};
    }
  }

  trevrpc_server* server = native_handle();
  std::uint32_t native_phase = TREVRPC_SERVER_PHASE_STOPPED;
  int error = trevrpc_server_get_phase(server, &native_phase);
  if (error != 0) {
    return Error::runtime(error);
  }

  bool cancellation_requested = false;
  {
    std::lock_guard lock(mutex_);
    cancellation_requested = cancellation_requested_;
  }

  if (native_phase < TREVRPC_SERVER_PHASE_CANCELLING) {
    auto graceful = graceful_deadline(options.graceful_timeout);
    if (!graceful) {
      return graceful.error();
    }
    auto graceful_cpp = steady_deadline_from_native(graceful.value());
    if (!graceful_cpp) {
      return graceful_cpp.error();
    }
    error = trevrpc_server_stop(server);
    if (error != 0) {
      return Error::runtime(error);
    }
    error = trevrpc_server_wait_until(server, graceful.value());
    if (error == 0) {
      auto drained = drain_async_scopes_until(graceful_cpp.value());
      if (drained) {
        return release(ShutdownOutcome::Graceful);
      }
      if (drained.error().code() != -ETIMEDOUT) {
        return drained.error();
      }
    } else if (error != -ETIMEDOUT) {
      return Error::runtime(error);
    }

    error = trevrpc_server_cancel(server);
    if (error != 0) {
      return Error::runtime(error);
    }
    cancel_async_scopes();
    {
      std::lock_guard lock(mutex_);
      cancellation_requested_ = true;
    }
    cancellation_requested = true;
  } else if (native_phase == TREVRPC_SERVER_PHASE_CANCELLING) {
    cancellation_requested = true;
    cancel_async_scopes();
    std::lock_guard lock(mutex_);
    cancellation_requested_ = true;
  }

  auto cancellation = deadline_after(options.cancellation_timeout);
  if (!cancellation) {
    return cancellation.error();
  }
  auto cancellation_cpp = steady_deadline_from_native(cancellation.value());
  if (!cancellation_cpp) {
    return cancellation_cpp.error();
  }
  if (native_phase != TREVRPC_SERVER_PHASE_STOPPED) {
    error = trevrpc_server_wait_until(server, cancellation.value());
    if (error == -ETIMEDOUT) {
      std::uint32_t current = TREVRPC_SERVER_PHASE_STOPPED;
      const int phase_error = trevrpc_server_get_phase(server, &current);
      if (phase_error != 0) {
        return Error::runtime(phase_error);
      }
      return ShutdownReport{ShutdownOutcome::TimedOut, phase_from_native(current), false};
    }
    if (error != 0) {
      return Error::runtime(error);
    }
  }
  auto drained = drain_async_scopes_until(cancellation_cpp.value());
  if (!drained) {
    if (drained.error().code() != -ETIMEDOUT) {
      return drained.error();
    }
    std::uint32_t current = TREVRPC_SERVER_PHASE_STOPPED;
    const int phase_error = trevrpc_server_get_phase(server, &current);
    if (phase_error != 0) {
      return Error::runtime(phase_error);
    }
    return ShutdownReport{ShutdownOutcome::TimedOut, phase_from_native(current), false};
  }
  return release(cancellation_requested ? ShutdownOutcome::Cancelled : ShutdownOutcome::Graceful);
}

void ServerState::cancel_for_abandonment() noexcept {
  trevrpc_server* server = native_handle();
  if (server != nullptr) {
    const int error = trevrpc_server_cancel(server);
    cancel_async_scopes();
    if (error == 0) {
      std::lock_guard lock(mutex_);
      cancellation_requested_ = true;
    }
  }
}

Result<void>
ServerState::register_route(std::string_view service, std::string_view method, std::uint32_t kind,
                            trevrpc_call_handler callback, const std::shared_ptr<void>& route,
                            void* user_data,
                            const std::shared_ptr<AsyncServerScopeControl>& async_scope) {
  if (!route || callback == nullptr) {
    return Error::runtime(-EINVAL, "server route must not be null");
  }
  std::string service_string;
  std::string method_string;
  try {
    service_string = service;
    method_string = method;
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to own server route name");
  }
  std::lock_guard lock(mutex_);
  if (server_ == nullptr || releasing_) {
    return Error::runtime(-ESHUTDOWN, "server is stopping or released");
  }
  const std::size_t route_count = routes_.size();
  const std::size_t scope_count = async_scopes_.size();
  try {
    routes_.push_back(route);
    if (async_scope) {
      async_scopes_.push_back(async_scope);
    }
  } catch (...) {
    routes_.resize(route_count);
    async_scopes_.resize(scope_count);
    return Error::runtime(-ENOMEM, "failed to retain server route");
  }
  const int error = trevrpc_server_register_call(server_, service_string.c_str(),
                                                 method_string.c_str(), kind, callback, user_data);
  if (error != 0) {
    routes_.pop_back();
    if (async_scope) {
      async_scopes_.pop_back();
    }
    return Error::runtime(error);
  }
  return {};
}

Result<void> ServerState::set_authorizer(std::shared_ptr<AuthorizerCallbackState> callback) {
  if (!callback || !callback->callback) {
    return Error::runtime(-EINVAL, "authorizer must not be null");
  }
  std::shared_ptr<AuthorizerCallbackState> previous;
  {
    std::lock_guard lock(mutex_);
    if (server_ == nullptr) {
      return Error::runtime(-EINVAL, "server is released");
    }
    const int error = trevrpc_server_set_authorizer(server_, authorizer_trampoline, callback.get());
    if (error != 0) {
      return Error::runtime(error);
    }
    previous = std::exchange(authorizer_, std::move(callback));
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
    if (server_ == nullptr) {
      return Error::runtime(-EINVAL, "server is released");
    }
    const int error = trevrpc_server_set_metrics(server_, &callback->native);
    if (error != 0) {
      return Error::runtime(error);
    }
    previous = std::exchange(metrics_, std::move(callback));
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
    if (server_ == nullptr) {
      return Error::runtime(-EINVAL, "server is released");
    }
    const int error = trevrpc_server_set_logger(server_, &callback->native);
    if (error != 0) {
      return Error::runtime(error);
    }
    previous = std::exchange(logger_, std::move(callback));
  }
  return {};
}

Result<void> ServerState::set_transport_observer(std::shared_ptr<TransportCallbackState> callback) {
  if (!callback || !callback->callback) {
    return Error::runtime(-EINVAL, "transport observer must not be null");
  }
  std::shared_ptr<TransportCallbackState> previous;
  {
    std::lock_guard lock(mutex_);
    if (server_ == nullptr) {
      return Error::runtime(-EINVAL, "server is released");
    }
    const int error = trevrpc_server_set_transport_observer(server_, &callback->native);
    if (error != 0) {
      return Error::runtime(error);
    }
    previous = std::exchange(transport_observer_, std::move(callback));
  }
  return {};
}

Result<void> ServerState::clear_authorizer() {
  std::shared_ptr<AuthorizerCallbackState> previous;
  {
    std::lock_guard lock(mutex_);
    if (server_ == nullptr) {
      return Error::runtime(-EINVAL, "server is released");
    }
    std::uint32_t phase = 0;
    const int error = trevrpc_server_get_phase(server_, &phase);
    if (error != 0) {
      return Error::runtime(error);
    }
    if (phase != TREVRPC_SERVER_PHASE_CONFIGURING) {
      return Error::runtime(-EALREADY, "server configuration is frozen");
    }
    trevrpc_server_clear_authorizer(server_);
    previous = std::move(authorizer_);
  }
  return {};
}

Result<void> ServerState::clear_metrics() {
  std::shared_ptr<MetricsCallbackState> previous;
  {
    std::lock_guard lock(mutex_);
    if (server_ == nullptr) {
      return Error::runtime(-EINVAL, "server is released");
    }
    std::uint32_t phase = 0;
    const int error = trevrpc_server_get_phase(server_, &phase);
    if (error != 0) {
      return Error::runtime(error);
    }
    if (phase != TREVRPC_SERVER_PHASE_CONFIGURING) {
      return Error::runtime(-EALREADY, "server configuration is frozen");
    }
    trevrpc_server_clear_metrics(server_);
    previous = std::move(metrics_);
  }
  return {};
}

Result<void> ServerState::clear_logger() {
  std::shared_ptr<LoggerCallbackState> previous;
  {
    std::lock_guard lock(mutex_);
    if (server_ == nullptr) {
      return Error::runtime(-EINVAL, "server is released");
    }
    std::uint32_t phase = 0;
    const int error = trevrpc_server_get_phase(server_, &phase);
    if (error != 0) {
      return Error::runtime(error);
    }
    if (phase != TREVRPC_SERVER_PHASE_CONFIGURING) {
      return Error::runtime(-EALREADY, "server configuration is frozen");
    }
    trevrpc_server_clear_logger(server_);
    previous = std::move(logger_);
  }
  return {};
}

Result<void> ServerState::clear_transport_observer() {
  std::shared_ptr<TransportCallbackState> previous;
  {
    std::lock_guard lock(mutex_);
    if (server_ == nullptr) {
      return Error::runtime(-EINVAL, "server is released");
    }
    std::uint32_t phase = 0;
    const int error = trevrpc_server_get_phase(server_, &phase);
    if (error != 0) {
      return Error::runtime(error);
    }
    if (phase != TREVRPC_SERVER_PHASE_CONFIGURING) {
      return Error::runtime(-EALREADY, "server configuration is frozen");
    }
    trevrpc_server_clear_transport_observer(server_);
    previous = std::move(transport_observer_);
  }
  return {};
}

void abandon_channel(std::shared_ptr<ChannelState> state) noexcept {
  if (state && state->native_handle() != nullptr) {
    lifecycle_reaper().submit([state = std::move(state)] { state->close(); });
  }
}

bool drain_lifecycle_reaper_until(std::chrono::steady_clock::time_point deadline) noexcept {
  return lifecycle_reaper().drain_until(deadline);
}

void abandon_server(std::shared_ptr<ServerState> state) noexcept {
  if (state && state->native_handle() != nullptr) {
    lifecycle_reaper().submit([state = std::move(state)] {
      state->cancel_for_abandonment();
      ShutdownOptions options;
      options.graceful_timeout = std::chrono::nanoseconds(0);
      options.cancellation_timeout = std::chrono::nanoseconds::max();
      (void)state->shutdown(options);
    });
  }
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

ChannelCallbackContextGuard::~ChannelCallbackContextGuard() { in_channel_callback = previous_; }

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
