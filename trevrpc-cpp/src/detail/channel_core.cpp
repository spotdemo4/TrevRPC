#include "channel_core.hpp"

#include "lifecycle.hpp"

#include <trevrpc_rpc_msquic.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cmath>
#include <condition_variable>
#include <deque>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace trevrpc::detail {
namespace {

std::atomic<int> injected_channel_start_error{0};

[[nodiscard]] bool fits_u32(std::size_t value) noexcept {
  return value <= std::numeric_limits<std::uint32_t>::max();
}

[[nodiscard]] bool null_handle(trevrpc_rpc_cancellation_v1 handle) noexcept {
  return handle.owner == 0 && handle.slot == 0 && handle.generation == 0;
}

[[nodiscard]] std::chrono::milliseconds bounded_backoff(const ChannelCoreConfig& config,
                                                        std::size_t attempt) noexcept {
  const auto initial = std::max(config.reconnect_initial_delay, std::chrono::milliseconds::zero());
  const auto maximum = std::max(config.reconnect_max_delay, initial);
  if (attempt == 0 || initial == std::chrono::milliseconds::zero()) {
    return initial;
  }
  const double multiplier = config.reconnect_multiplier < 1.0 ? 1.0 : config.reconnect_multiplier;
  const double scaled =
      static_cast<double>(initial.count()) * std::pow(multiplier, static_cast<double>(attempt));
  if (!std::isfinite(scaled) || scaled >= static_cast<double>(maximum.count())) {
    return maximum;
  }
  return std::chrono::milliseconds(static_cast<std::int64_t>(scaled));
}

} // namespace

struct RpcCancellation::Attachment final
    : public std::enable_shared_from_this<RpcCancellation::Attachment> {
  explicit Attachment(std::shared_ptr<RpcEventRuntime> runtime_value,
                      trevrpc_rpc_cancellation_v1 handle_value) noexcept
      : runtime(std::move(runtime_value)), cancellation(handle_value) {}

  ~Attachment() {
    if (!runtime || null_handle(cancellation)) {
      return;
    }
    (void)trevrpc_rpc_cancellation_release(runtime->native_handle(), cancellation);
  }

  void request_cancel() noexcept {
    bool expected = false;
    if (!cancel_started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      return;
    }
    auto operation = runtime->reserve_operation();
    if (!operation) {
      cancel_error.store(operation.error().code(), std::memory_order_release);
      cancel_started.store(false, std::memory_order_release);
      return;
    }
    const std::uint64_t operation_id = operation.value();
    auto self = shared_from_this();
    Result<void> subscribed = Error::runtime(-ENOMEM);
    try {
      subscribed = runtime->subscribe_operation(
          operation_id, [self = std::move(self)](Result<RpcEvent> result) noexcept {
            self->cancel_error.store(result ? result.value().status : result.error().code(),
                                     std::memory_order_release);
            self->cancel_completed.store(true, std::memory_order_release);
          });
    } catch (...) {
      subscribed = Error::runtime(-ENOMEM, "failed to allocate cancellation callback");
    }
    if (!subscribed) {
      runtime->reject_operation(operation_id);
      cancel_error.store(subscribed.error().code(), std::memory_order_release);
      cancel_started.store(false, std::memory_order_release);
      return;
    }
    const int error =
        trevrpc_rpc_cancellation_cancel(runtime->native_handle(), cancellation, operation_id);
    if (error != 0) {
      runtime->reject_operation(operation_id);
      cancel_error.store(error, std::memory_order_release);
      if (error != -EALREADY) {
        cancel_started.store(false, std::memory_order_release);
      }
    }
  }

  std::shared_ptr<RpcEventRuntime> runtime;
  trevrpc_rpc_cancellation_v1 cancellation{};
  std::atomic<bool> cancel_started{false};
  std::atomic<bool> cancel_completed{false};
  std::atomic<int> cancel_error{0};
};

struct RpcCancellation::SharedState final {
  mutable std::mutex mutex;
  bool cancelled = false;
  std::uint64_t next_subscription = 1;
  std::unordered_map<std::uint64_t, std::function<void()>> subscriptions;
  std::unordered_map<RpcEventRuntime*, std::weak_ptr<Attachment>> attachments;
};

trevrpc_rpc_cancellation_v1 RpcCancellation::AttachmentLease::handle() const noexcept {
  return attachment_ ? attachment_->cancellation : trevrpc_rpc_cancellation_v1{};
}

RpcCancellation::Subscription::~Subscription() { reset(); }

RpcCancellation::Subscription::Subscription(Subscription&& other) noexcept
    : state_(std::move(other.state_)), id_(std::exchange(other.id_, 0)) {}

RpcCancellation::Subscription&
RpcCancellation::Subscription::operator=(Subscription&& other) noexcept {
  if (this != &other) {
    reset();
    state_ = std::move(other.state_);
    id_ = std::exchange(other.id_, 0);
  }
  return *this;
}

void RpcCancellation::Subscription::reset() noexcept {
  if (!state_ || id_ == 0) {
    state_.reset();
    id_ = 0;
    return;
  }
  {
    std::lock_guard lock(state_->mutex);
    state_->subscriptions.erase(id_);
  }
  state_.reset();
  id_ = 0;
}

RpcCancellation::RpcCancellation() : state_(std::make_shared<SharedState>()) {}

void RpcCancellation::cancel() noexcept {
  std::vector<std::function<void()>> callbacks;
  std::vector<std::shared_ptr<Attachment>> attachments;
  {
    std::lock_guard lock(state_->mutex);
    const bool first_cancel = !state_->cancelled;
    state_->cancelled = true;
    try {
      if (first_cancel) {
        callbacks.reserve(state_->subscriptions.size());
        for (auto& entry : state_->subscriptions) {
          callbacks.push_back(entry.second);
        }
      }
      attachments.reserve(state_->attachments.size());
      for (auto iterator = state_->attachments.begin(); iterator != state_->attachments.end();) {
        if (auto attachment = iterator->second.lock()) {
          attachments.push_back(std::move(attachment));
          ++iterator;
        } else {
          iterator = state_->attachments.erase(iterator);
        }
      }
    } catch (...) {
      std::terminate();
    }
  }
  for (auto& callback : callbacks) {
    try {
      callback();
    } catch (...) {
      // Cancellation notification is best effort and must remain noexcept.
      (void)std::current_exception();
    }
  }
  for (const auto& attachment : attachments) {
    attachment->request_cancel();
  }
}

bool RpcCancellation::cancelled() const noexcept {
  std::lock_guard lock(state_->mutex);
  return state_->cancelled;
}

Result<RpcCancellation::AttachmentLease>
RpcCancellation::attach(const std::shared_ptr<RpcEventRuntime>& runtime) const {
  if (!runtime) {
    return Error::runtime(-EINVAL, "RPC cancellation runtime must not be null");
  }
  std::shared_ptr<Attachment> attachment;
  bool request_cancel = false;
  {
    std::lock_guard lock(state_->mutex);
    const auto found = state_->attachments.find(runtime.get());
    if (found != state_->attachments.end()) {
      attachment = found->second.lock();
    }
    if (!attachment) {
      trevrpc_rpc_cancellation_v1 handle{};
      const int error = trevrpc_rpc_cancellation_create(runtime->native_handle(), &handle);
      if (error != 0) {
        return Error::runtime(error, "failed to create RPC cancellation attachment");
      }
      try {
        attachment = std::make_shared<Attachment>(runtime, handle);
        state_->attachments[runtime.get()] = attachment;
      } catch (...) {
        if (!attachment) {
          (void)trevrpc_rpc_cancellation_release(runtime->native_handle(), handle);
        }
        return Error::runtime(-ENOMEM, "failed to retain RPC cancellation attachment");
      }
    }
    request_cancel = state_->cancelled;
  }
  if (request_cancel) {
    attachment->request_cancel();
  }
  return AttachmentLease(std::move(attachment));
}

Result<RpcCancellation::Subscription>
RpcCancellation::subscribe(std::function<void()> callback) const {
  if (!callback) {
    return Error::runtime(-EINVAL, "cancellation callback must not be empty");
  }
  bool notify = false;
  std::uint64_t id = 0;
  {
    std::lock_guard lock(state_->mutex);
    if (state_->cancelled) {
      notify = true;
    } else {
      try {
        id = state_->next_subscription++;
        if (id == 0) {
          id = state_->next_subscription++;
        }
        state_->subscriptions.emplace(id, callback);
      } catch (...) {
        return Error::runtime(-ENOMEM, "failed to register cancellation callback");
      }
    }
  }
  if (notify) {
    try {
      callback();
    } catch (...) {
      // Readiness waits only use a noexcept notification callback.
      (void)std::current_exception();
    }
    return Subscription{};
  }
  return Subscription(state_, id);
}

struct ChannelCore::Generation final {
  std::weak_ptr<SharedState> owner;
  std::shared_ptr<RpcEventRuntime> runtime;
  trevrpc_rpc_endpoint_v1 endpoint{};
  std::uint64_t logical_generation = 0;
  std::size_t pins = 0;
  bool ready = false;
  bool startup_failed = false;
  bool terminal = false;
  bool retry_scheduled = false;
  bool subject_armed = false;
  bool subject_arm_needed = false;
  bool close_submitted = false;
  // The C runtime and provider both report -EALREADY, but only the former
  // guarantees that a terminal subject event is still in flight.
  bool close_observation_pending = false;
  int close_error = 0;
  std::size_t close_attempt = 0;
  std::chrono::steady_clock::time_point close_retry_at{};
  bool unregistered_cleanup_needed = false;
  int registration_error = 0;
  int cleanup_error = 0;
  std::size_t cleanup_attempt = 0;
  std::chrono::steady_clock::time_point cleanup_retry_at{};
  bool releasing = false;
  int release_error = 0;
  std::size_t release_attempt = 0;
  std::chrono::steady_clock::time_point release_retry_at{};
  bool released = false;
};

struct ChannelCore::SharedState final
    : public std::enable_shared_from_this<ChannelCore::SharedState> {
  SharedState(std::shared_ptr<RpcEventRuntime> runtime_value, ChannelCoreConfig config_value,
              EndpointStarter endpoint_starter_value, Backoff backoff_value)
      : runtime(std::move(runtime_value)), config(std::move(config_value)),
        endpoint_starter(std::move(endpoint_starter_value)), backoff(std::move(backoff_value)) {}

  [[nodiscard]] Result<void> start() {
    const int injected_error = injected_channel_start_error.exchange(0, std::memory_order_acq_rel);
    if (injected_error != 0) {
      {
        std::lock_guard lock(mutex);
        close_requested = true;
        phase = ChannelCorePhase::Closed;
        close_error = injected_error;
        worker_exited = true;
      }
      condition.notify_all();
      return Error::runtime(injected_error, "injected RPC channel coordinator start failure");
    }
    try {
      std::thread([state = shared_from_this()] { state->worker_loop(); }).detach();
    } catch (...) {
      {
        std::lock_guard lock(mutex);
        close_requested = true;
        phase = ChannelCorePhase::Closed;
        close_error = -EAGAIN;
        worker_exited = true;
      }
      condition.notify_all();
      return Error::runtime(-EAGAIN, "failed to start RPC channel coordinator");
    }
    {
      std::lock_guard lock(mutex);
      retry_pending = true;
      retry_at = std::chrono::steady_clock::now();
    }
    condition.notify_all();
    return {};
  }

  [[nodiscard]] std::chrono::milliseconds retry_delay(std::size_t attempt) const noexcept {
    if (backoff) {
      try {
        return std::max(backoff(attempt), std::chrono::milliseconds::zero());
      } catch (...) {
        return std::chrono::milliseconds::zero();
      }
    }
    return bounded_backoff(config, attempt);
  }

  [[nodiscard]] static std::chrono::milliseconds cleanup_delay(std::size_t attempt) noexcept {
    constexpr auto initial = std::chrono::milliseconds(10);
    constexpr auto maximum = std::chrono::seconds(1);
    const std::size_t shift = std::min<std::size_t>(attempt, 7);
    return std::min(initial * static_cast<int>(std::size_t{1} << shift),
                    std::chrono::duration_cast<std::chrono::milliseconds>(maximum));
  }

  [[nodiscard]] static std::chrono::steady_clock::time_point
  cleanup_retry_deadline(std::size_t attempt) noexcept {
    constexpr std::size_t automatic_retries = 2;
    if (attempt >= automatic_retries) {
      return std::chrono::steady_clock::time_point::max();
    }
    return std::chrono::steady_clock::now() + cleanup_delay(attempt);
  }

  void record_close_failure_locked(int error) noexcept {
    if (!close_requested || error == 0 || close_failure_generation >= close_request_generation) {
      return;
    }
    close_failure_error = error;
    close_failure_generation = close_request_generation;
  }

  void enqueue_lifecycle_locked(ChannelCoreEvent event) noexcept {
    if (!config.lifecycle_observer || lifecycle_capacity == 0) {
      return;
    }
    try {
      if (lifecycle_events.size() < lifecycle_capacity) {
        lifecycle_events.push_back(event);
        return;
      }
      const auto found =
          std::find_if(lifecycle_events.rbegin(), lifecycle_events.rend(),
                       [&](const ChannelCoreEvent& queued) { return queued.kind == event.kind; });
      if (found != lifecycle_events.rend()) {
        *found = event;
      }
    } catch (...) {
      // Lifecycle detail is lossy by contract; authoritative state is retained.
      (void)std::current_exception();
    }
  }

  void set_phase_locked(ChannelCorePhase next, int error = 0) noexcept {
    if (phase == next) {
      return;
    }
    phase = next;
    enqueue_lifecycle_locked(
        ChannelCoreEvent{ChannelCoreEventKind::StateChanged, phase, committed_generation, error});
  }

  void schedule_retry(const std::shared_ptr<Generation>& generation, int error,
                      bool connection_shutdown) noexcept {
    std::size_t attempt = 0;
    {
      std::lock_guard lock(mutex);
      if (close_requested || generation->retry_scheduled) {
        return;
      }
      generation->retry_scheduled = true;
      if (connection_shutdown) {
        active_generation.reset();
        set_phase_locked(ChannelCorePhase::Reconnecting, error);
        enqueue_lifecycle_locked(ChannelCoreEvent{ChannelCoreEventKind::ConnectionShutdown, phase,
                                                  generation->logical_generation, error});
      } else {
        enqueue_lifecycle_locked(ChannelCoreEvent{ChannelCoreEventKind::ConnectFailed, phase,
                                                  committed_generation, error});
      }
      // Reserve the retry attempt before invoking user code. The worker is the only
      // scheduler, while request_close() may concurrently cancel this reservation.
      attempt = retry_attempt++;
    }

    // Backoff is user code and must never run while SharedState::mutex is held.
    const auto delay = retry_delay(attempt);
    {
      std::lock_guard lock(mutex);
      if (!close_requested) {
        retry_pending = true;
        retry_at = std::chrono::steady_clock::now() + delay;
      }
    }
    condition.notify_all();
  }

  void schedule_start_failure(int error) noexcept {
    std::size_t attempt = 0;
    {
      std::lock_guard lock(mutex);
      if (close_requested) {
        return;
      }
      enqueue_lifecycle_locked(ChannelCoreEvent{ChannelCoreEventKind::ConnectFailed, phase,
                                                committed_generation, error});
      // Reserve the retry attempt before invoking user code. request_close() can
      // cancel the pending retry while the callback is running.
      attempt = retry_attempt++;
    }

    // Backoff is user code and must never run while SharedState::mutex is held.
    const auto delay = retry_delay(attempt);
    {
      std::lock_guard lock(mutex);
      if (!close_requested) {
        retry_pending = true;
        retry_at = std::chrono::steady_clock::now() + delay;
      }
    }
    condition.notify_all();
  }

  void handle_start_event(const std::shared_ptr<Generation>& generation,
                          Result<RpcEvent> result) noexcept {
    bool retry = false;
    int retry_error = 0;
    {
      std::lock_guard lock(mutex);
      if (generation->released) {
        return;
      }
      if (!result) {
        generation->startup_failed = true;
        retry = true;
        retry_error = result.error().code();
      } else if (result.value().kind == TREVRPC_RPC_EVENT_ENDPOINT_READY &&
                 result.value().status == 0) {
        if (!close_requested && !generation->terminal && !generation->ready) {
          generation->ready = true;
          generation->logical_generation = ++committed_generation;
          active_generation = generation;
          retry_attempt = 0;
          set_phase_locked(ChannelCorePhase::Ready);
        }
      } else {
        generation->startup_failed = true;
        retry = true;
        retry_error = result.value().status == 0 ? -EIO : result.value().status;
      }
      generation->subject_arm_needed = !generation->terminal;
    }
    if (retry) {
      schedule_retry(generation, retry_error, false);
    }
    condition.notify_all();
  }

  void handle_subject_event(const std::shared_ptr<Generation>& generation,
                            Result<RpcEvent> result) noexcept {
    bool retry = false;
    bool connection_shutdown = false;
    int retry_error = 0;
    {
      std::lock_guard lock(mutex);
      generation->subject_armed = false;
      if (generation->released) {
        return;
      }
      if (!result) {
        generation->terminal = true;
        retry_error = result.error().code();
        if (close_requested &&
            (generation->close_submitted || generation->close_observation_pending) &&
            !generation->retry_scheduled) {
          record_close_failure_locked(retry_error);
        }
        const bool was_ready = generation->ready;
        if (was_ready && active_generation == generation) {
          retry = true;
          connection_shutdown = true;
        } else if (!generation->retry_scheduled) {
          retry = true;
        }
      } else {
        const RpcEvent& event = result.value();
        if (event.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED) {
          if (close_requested &&
              (generation->close_submitted || generation->close_observation_pending) &&
              !generation->retry_scheduled && event.status != 0) {
            record_close_failure_locked(event.status);
          }
          generation->terminal = true;
          const bool was_ready = generation->ready;
          if (was_ready && active_generation == generation) {
            retry = true;
            connection_shutdown = true;
            retry_error = event.status;
          } else if (!generation->retry_scheduled && !close_requested) {
            retry = true;
            retry_error = event.status == 0 ? -ECONNRESET : event.status;
          }
        } else {
          generation->subject_arm_needed = true;
        }
      }
    }
    if (retry) {
      schedule_retry(generation, retry_error, connection_shutdown);
    }
    condition.notify_all();
  }

  void arm_subject(const std::shared_ptr<Generation>& generation) noexcept {
    {
      std::lock_guard lock(mutex);
      if (generation->terminal || generation->released || generation->subject_armed ||
          !generation->subject_arm_needed) {
        return;
      }
      generation->subject_armed = true;
      generation->subject_arm_needed = false;
    }
    auto weak = weak_from_this();
    Result<void> subscribed = Error::runtime(-ENOMEM);
    try {
      subscribed = runtime->subscribe_endpoint(
          generation->endpoint, [weak, generation](Result<RpcEvent> result) noexcept {
            if (auto state = weak.lock()) {
              state->handle_subject_event(generation, std::move(result));
            }
          });
    } catch (...) {
      subscribed = Error::runtime(-ENOMEM, "failed to allocate RPC endpoint callback");
    }
    if (!subscribed) {
      handle_subject_event(generation, subscribed.error());
    }
  }

  void start_endpoint() noexcept {
    auto startup = runtime->reserve_operation();
    if (!startup) {
      schedule_start_failure(startup.error().code());
      return;
    }
    std::shared_ptr<Generation> generation;
    try {
      generation = std::make_shared<Generation>();
      generation->owner = weak_from_this();
      generation->runtime = runtime;
    } catch (...) {
      runtime->reject_operation(startup.value());
      schedule_start_failure(-ENOMEM);
      return;
    }
    try {
      std::lock_guard lock(mutex);
      if (close_requested) {
        runtime->reject_operation(startup.value());
        return;
      }
      generations.push_back(generation);
      ++start_attempts;
    } catch (...) {
      runtime->reject_operation(startup.value());
      schedule_start_failure(-ENOMEM);
      return;
    }
    condition.notify_all();

    trevrpc_rpc_endpoint_v1 endpoint{};
    int error = 0;
    try {
      error = endpoint_starter(runtime->native_handle(), startup.value(), &endpoint);
    } catch (...) {
      error = -EIO;
    }
    if (error != 0) {
      runtime->reject_operation(startup.value());
      if (endpoint.owner != 0) {
        generation->endpoint = endpoint;
        {
          std::lock_guard lock(mutex);
          generation->registration_error = error;
          generation->unregistered_cleanup_needed = true;
        }
      } else {
        {
          std::lock_guard lock(mutex);
          generation->released = true;
          generations.erase(std::remove(generations.begin(), generations.end(), generation),
                            generations.end());
        }
        schedule_start_failure(error);
      }
      condition.notify_all();
      return;
    }
    generation->endpoint = endpoint;
    auto registered = runtime->register_endpoint(endpoint);
    if (!registered) {
      runtime->reject_operation(startup.value());
      {
        std::lock_guard lock(mutex);
        generation->registration_error = registered.error().code();
        generation->unregistered_cleanup_needed = true;
      }
      condition.notify_all();
      return;
    }
    {
      std::lock_guard lock(mutex);
      generation->subject_arm_needed = true;
    }
    arm_subject(generation);

    auto weak = weak_from_this();
    Result<void> subscribed = Error::runtime(-ENOMEM);
    try {
      subscribed = runtime->subscribe_operation(
          startup.value(), [weak, generation](Result<RpcEvent> result) noexcept {
            if (auto state = weak.lock()) {
              state->handle_start_event(generation, std::move(result));
            }
          });
    } catch (...) {
      subscribed = Error::runtime(-ENOMEM, "failed to allocate RPC startup callback");
    }
    if (!subscribed) {
      handle_start_event(generation, subscribed.error());
    }
  }

  void submit_endpoint_close(const std::shared_ptr<Generation>& generation) noexcept {
    auto operation = runtime->reserve_operation();
    if (!operation) {
      {
        std::lock_guard lock(mutex);
        generation->close_submitted = false;
        generation->close_observation_pending = false;
        generation->close_error = operation.error().code();
        generation->close_retry_at = cleanup_retry_deadline(generation->close_attempt++);
        record_close_failure_locked(generation->close_error);
      }
      condition.notify_all();
      return;
    }
    const int error = trevrpc_rpc_endpoint_close(runtime->native_handle(), generation->endpoint,
                                                 operation.value());
    if (error != 0) {
      runtime->reject_operation(operation.value());
      {
        std::lock_guard lock(mutex);
        generation->close_submitted = false;
        generation->close_observation_pending = error == -EALREADY;
        generation->close_error = error;
        generation->close_retry_at = cleanup_retry_deadline(generation->close_attempt++);
        // -EALREADY is ambiguous at this layer: the C runtime may have an
        // existing close in flight, or the provider may have rejected this
        // close without publishing a terminal event. Give the existing close
        // a bounded opportunity to settle before reporting a failure.
        if (error != -EALREADY ||
            generation->close_retry_at == std::chrono::steady_clock::time_point::max()) {
          record_close_failure_locked(error);
        }
      }
      condition.notify_all();
      return;
    }
    {
      std::lock_guard lock(mutex);
      generation->close_observation_pending = false;
      generation->close_error = 0;
      generation->close_attempt = 0;
    }
    Result<void> consumed = Error::runtime(-ENOMEM);
    try {
      auto weak = weak_from_this();
      consumed =
          runtime->subscribe_operation(operation.value(), [weak](Result<RpcEvent> result) noexcept {
            if (auto state = weak.lock()) {
              std::lock_guard lock(state->mutex);
              if (!result) {
                state->record_close_failure_locked(result.error().code());
              } else if (result.value().status != 0) {
                state->record_close_failure_locked(result.value().status);
              }
              state->condition.notify_all();
            }
          });
    } catch (...) {
      consumed = Error::runtime(-ENOMEM, "failed to allocate RPC close callback");
    }
    if (!consumed) {
      runtime->reject_operation(operation.value());
      // Driver settlement will fail any installed endpoint subject callback.
      condition.notify_all();
    }
  }

  void settle_unregistered_endpoint(const std::shared_ptr<Generation>& generation) noexcept {
    auto settled = runtime->settle_unregistered_endpoint(generation->endpoint);
    if (!settled) {
      {
        std::lock_guard lock(mutex);
        generation->cleanup_error = settled.error().code();
        generation->cleanup_retry_at = cleanup_retry_deadline(generation->cleanup_attempt++);
        record_close_failure_locked(generation->cleanup_error);
      }
      condition.notify_all();
      return;
    }
    int registration_error = 0;
    {
      std::lock_guard lock(mutex);
      generation->unregistered_cleanup_needed = false;
      generation->cleanup_error = 0;
      generation->cleanup_attempt = 0;
      generation->terminal = true;
      registration_error = generation->registration_error;
    }
    condition.notify_all();
    schedule_start_failure(registration_error);
  }

  void release_endpoint(const std::shared_ptr<Generation>& generation) noexcept {
    runtime->unregister_endpoint(generation->endpoint);
    const int error = trevrpc_rpc_endpoint_release(runtime->native_handle(), generation->endpoint);
    {
      std::lock_guard lock(mutex);
      generation->releasing = false;
      if (error == 0 || error == -ESTALE) {
        generation->release_error = 0;
        generation->released = true;
        generations.erase(std::remove(generations.begin(), generations.end(), generation),
                          generations.end());
      } else {
        generation->release_error = error;
        generation->release_retry_at = cleanup_retry_deadline(generation->release_attempt++);
        record_close_failure_locked(error);
      }
    }
    condition.notify_all();
  }

  void dispatch_lifecycle(ChannelCoreEvent event) noexcept {
    if (!config.lifecycle_observer) {
      return;
    }
    ChannelCallbackContextGuard guard;
    try {
      config.lifecycle_observer(event);
    } catch (...) {
      if (config.callback_exception_sink) {
        try {
          config.callback_exception_sink(std::current_exception());
        } catch (...) {
          // Exception sinks must not terminate the coordinator.
          (void)std::current_exception();
        }
      }
    }
  }

  void worker_loop() noexcept {
    {
      std::lock_guard lock(mutex);
      worker_thread_id = std::this_thread::get_id();
    }
    for (;;) {
      enum class Action { None, Start, Arm, Close, Cleanup, Release, Dispatch, Shutdown };
      Action action = Action::None;
      std::shared_ptr<Generation> generation;
      std::optional<ChannelCoreEvent> lifecycle_event;
      {
        std::unique_lock lock(mutex);
        const auto now = std::chrono::steady_clock::now();
        if (!lifecycle_events.empty()) {
          lifecycle_event = lifecycle_events.front();
          lifecycle_events.pop_front();
          lifecycle_callback_active = true;
          action = Action::Dispatch;
        } else if (auto found = std::find_if(generations.begin(), generations.end(),
                                             [&](const auto& candidate) {
                                               return candidate->unregistered_cleanup_needed &&
                                                      (candidate->cleanup_error == 0 ||
                                                       now >= candidate->cleanup_retry_at);
                                             });
                   found != generations.end()) {
          generation = *found;
          action = Action::Cleanup;
        } else if (auto found = std::find_if(generations.begin(), generations.end(),
                                             [&](const auto& candidate) {
                                               return candidate->terminal && candidate->pins == 0 &&
                                                      !candidate->released &&
                                                      !candidate->releasing &&
                                                      (candidate->release_error == 0 ||
                                                       now >= candidate->release_retry_at);
                                             });
                   found != generations.end()) {
          generation = *found;
          generation->releasing = true;
          action = Action::Release;
        } else if (auto found = std::find_if(generations.begin(), generations.end(),
                                             [&](const auto& candidate) {
                                               return close_requested && candidate->pins == 0 &&
                                                      !candidate->terminal &&
                                                      !candidate->close_submitted &&
                                                      (candidate->close_error == 0 ||
                                                       now >= candidate->close_retry_at);
                                             });
                   found != generations.end()) {
          generation = *found;
          generation->close_submitted = true;
          action = Action::Close;
        } else if (auto found = std::find_if(generations.begin(), generations.end(),
                                             [](const auto& candidate) {
                                               return candidate->subject_arm_needed &&
                                                      !candidate->subject_armed &&
                                                      !candidate->terminal;
                                             });
                   found != generations.end()) {
          generation = *found;
          action = Action::Arm;
        } else if (!close_requested && retry_pending && now >= retry_at) {
          retry_pending = false;
          action = Action::Start;
        } else if (close_requested && generations.empty() &&
                   (shutdown_retry_error == 0 || now >= shutdown_retry_at)) {
          action = Action::Shutdown;
        } else {
          auto deadline = retry_pending ? retry_at : std::chrono::steady_clock::time_point::max();
          if (shutdown_retry_error != 0) {
            deadline = std::min(deadline, shutdown_retry_at);
          }
          for (const auto& candidate : generations) {
            if (candidate->unregistered_cleanup_needed && candidate->cleanup_error != 0) {
              deadline = std::min(deadline, candidate->cleanup_retry_at);
            }
            if (candidate->terminal && candidate->pins == 0 && candidate->release_error != 0) {
              deadline = std::min(deadline, candidate->release_retry_at);
            }
            if (close_requested && !candidate->terminal && !candidate->close_submitted &&
                candidate->close_error != 0) {
              deadline = std::min(deadline, candidate->close_retry_at);
            }
          }
          if (deadline == std::chrono::steady_clock::time_point::max()) {
            condition.wait(lock);
          } else {
            condition.wait_until(lock, deadline);
          }
          continue;
        }
      }

      switch (action) {
      case Action::Start:
        start_endpoint();
        break;
      case Action::Arm:
        arm_subject(generation);
        break;
      case Action::Close:
        submit_endpoint_close(generation);
        break;
      case Action::Cleanup:
        settle_unregistered_endpoint(generation);
        break;
      case Action::Release:
        release_endpoint(generation);
        break;
      case Action::Dispatch:
        dispatch_lifecycle(*lifecycle_event);
        {
          std::lock_guard lock(mutex);
          lifecycle_callback_active = false;
        }
        condition.notify_all();
        break;
      case Action::Shutdown: {
        auto result = runtime->shutdown();
        if (!result && result.error().code() == -EBUSY) {
          {
            std::lock_guard lock(mutex);
            shutdown_retry_error = result.error().code();
            shutdown_retry_at = cleanup_retry_deadline(shutdown_attempt++);
            record_close_failure_locked(shutdown_retry_error);
          }
          condition.notify_all();
          continue;
        }
        // A transport close can transiently report that its event queue is
        // full while deferred stream cleanup is still being drained. Retry
        // those submission errors without publishing a premature channel
        // failure; unlike -EBUSY, they are not a strict-shutdown result.
        if (!result && (result.error().code() == -EAGAIN || result.error().code() == -EINPROGRESS)) {
          std::lock_guard lock(mutex);
          if (shutdown_attempt++ < 2) {
            shutdown_retry_error = result.error().code();
            shutdown_retry_at = cleanup_retry_deadline(shutdown_attempt - 1);
            condition.notify_all();
            continue;
          }
          shutdown_retry_error = 0;
          close_error = result.error().code();
          worker_exited = true;
          worker_thread_id = {};
          condition.notify_all();
          return;
        }
        {
          std::lock_guard lock(mutex);
          shutdown_retry_error = 0;
          close_error = result ? 0 : result.error().code();
          worker_exited = true;
          worker_thread_id = {};
        }
        condition.notify_all();
        return;
      }
      case Action::None:
        break;
      }
    }
  }

  void unpin(const std::shared_ptr<Generation>& generation) noexcept {
    {
      std::lock_guard lock(mutex);
      assert(generation->pins != 0);
      --generation->pins;
    }
    condition.notify_all();
  }

  std::shared_ptr<RpcEventRuntime> runtime;
  ChannelCoreConfig config;
  EndpointStarter endpoint_starter;
  Backoff backoff;

  mutable std::mutex mutex;
  std::condition_variable condition;
  ChannelCorePhase phase = ChannelCorePhase::Connecting;
  std::uint64_t committed_generation = 0;
  std::shared_ptr<Generation> active_generation;
  std::vector<std::shared_ptr<Generation>> generations;
  bool close_requested = false;
  bool retry_pending = false;
  std::chrono::steady_clock::time_point retry_at{};
  std::size_t retry_attempt = 0;
  std::size_t start_attempts = 0;
  std::size_t lifecycle_capacity = std::max<std::size_t>(config.lifecycle_capacity, 1);
  std::deque<ChannelCoreEvent> lifecycle_events;
  bool lifecycle_callback_active = false;
  bool worker_exited = false;
  std::uint64_t close_request_generation = 0;
  std::uint64_t close_failure_generation = 0;
  int close_failure_error = 0;
  int shutdown_retry_error = 0;
  std::size_t shutdown_attempt = 0;
  std::chrono::steady_clock::time_point shutdown_retry_at{};
  int close_error = 0;
  std::thread::id worker_thread_id{};
};

ChannelCore::GenerationLease::GenerationLease(std::shared_ptr<Generation> generation) noexcept
    : generation_(std::move(generation)) {}

ChannelCore::GenerationLease::~GenerationLease() { reset(); }

ChannelCore::GenerationLease::GenerationLease(GenerationLease&& other) noexcept
    : generation_(std::move(other.generation_)) {}

ChannelCore::GenerationLease&
ChannelCore::GenerationLease::operator=(GenerationLease&& other) noexcept {
  if (this != &other) {
    reset();
    generation_ = std::move(other.generation_);
  }
  return *this;
}

void ChannelCore::GenerationLease::reset() noexcept {
  if (!generation_) {
    return;
  }
  auto generation = std::move(generation_);
  if (auto owner = generation->owner.lock()) {
    owner->unpin(generation);
  }
}

std::uint64_t ChannelCore::GenerationLease::generation() const noexcept {
  return generation_ ? generation_->logical_generation : 0;
}

trevrpc_rpc_endpoint_v1 ChannelCore::GenerationLease::endpoint() const noexcept {
  return generation_ ? generation_->endpoint : trevrpc_rpc_endpoint_v1{};
}

std::shared_ptr<RpcEventRuntime> ChannelCore::GenerationLease::runtime() const noexcept {
  return generation_ ? generation_->runtime : nullptr;
}

Result<std::shared_ptr<ChannelCore>> ChannelCore::create(std::shared_ptr<RpcEventRuntime> runtime,
                                                         ChannelCoreConfig config,
                                                         EndpointStarter endpoint_starter,
                                                         Backoff backoff) {
  if (!runtime || !endpoint_starter || config.host.empty() || !fits_u32(config.host.size()) ||
      !fits_u32(config.cert_file.size()) || !fits_u32(config.key_file.size()) ||
      !fits_u32(config.ca_cert_file.size()) || config.max_idle_timeout.count() < 0 ||
      config.keep_alive.count() < 0 ||
      static_cast<std::uint64_t>(config.keep_alive.count()) >
          std::numeric_limits<std::uint32_t>::max() ||
      config.reconnect_initial_delay.count() < 0 || config.reconnect_max_delay.count() < 0 ||
      config.reconnect_multiplier < 1.0 || config.lifecycle_capacity == 0) {
    return Error::runtime(-EINVAL, "invalid RPC channel configuration");
  }
  std::shared_ptr<SharedState> state;
  std::shared_ptr<ChannelCore> core;
  try {
    state = std::make_shared<SharedState>(std::move(runtime), std::move(config),
                                          std::move(endpoint_starter), std::move(backoff));
    core = std::shared_ptr<ChannelCore>(new ChannelCore(state));
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to allocate RPC channel state");
  }
  auto started = state->start();
  if (!started) {
    return started.error();
  }
  return core;
}

Result<std::shared_ptr<RpcEventRuntime>>
ChannelCore::adopt_created_runtime(trevrpc_rpc_runtime* runtime,
                                   const trevrpc_rpc_runtime_config_v1& config) {
  auto adopted = RpcEventRuntime::adopt(runtime, config);
  if (!adopted) {
    RpcEventRuntime::reap_unadopted(runtime);
    return adopted.error();
  }
  RpcEventRuntime::cancel_unadopted_cleanup();
  return std::move(adopted).value();
}

Result<std::shared_ptr<ChannelCore>> ChannelCore::connect(ChannelCoreConfig config) {
  if (config.host.empty()) {
    return Error::runtime(-EINVAL, "RPC channel host must not be empty");
  }
  trevrpc_rpc_runtime_config_v1 runtime_config{};
  trevrpc_rpc_msquic_config_v1 provider_config{};
  int error = trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config));
  if (error == 0) {
    error = trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config));
  }
  if (error != 0) {
    return Error::runtime(error);
  }
  auto cleanup_reserved = RpcEventRuntime::reserve_unadopted_cleanup();
  if (!cleanup_reserved) {
    return cleanup_reserved.error();
  }
  trevrpc_rpc_runtime* raw_runtime = nullptr;
  error = trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &raw_runtime);
  if (error != 0) {
    RpcEventRuntime::cancel_unadopted_cleanup();
    return Error::runtime(error);
  }
  auto adopted = adopt_created_runtime(raw_runtime, runtime_config);
  if (!adopted) {
    return adopted.error();
  }
  auto runtime = std::move(adopted).value();
  ChannelCoreConfig starter_config = config;
  EndpointStarter starter = [config = std::move(starter_config)](
                                trevrpc_rpc_runtime* native, std::uint64_t operation_id,
                                trevrpc_rpc_endpoint_v1* endpoint) {
    trevrpc_rpc_msquic_endpoint_config_v1 native_config{};
    int init_error =
        trevrpc_rpc_msquic_endpoint_config_v1_init(&native_config, sizeof(native_config));
    if (init_error != 0) {
      return init_error;
    }
    native_config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_CLIENT;
    native_config.transport = TREVRPC_RPC_MSQUIC_TRANSPORT_NATIVE;
    native_config.host = config.host.data();
    native_config.host_len = static_cast<std::uint32_t>(config.host.size());
    native_config.port = config.port;
    native_config.server_name = config.host.data();
    native_config.server_name_len = static_cast<std::uint32_t>(config.host.size());
    native_config.flags = config.skip_certificate_validation ? 0 : TREVRPC_RPC_MSQUIC_VERIFY_PEER;
    native_config.cert_file = config.cert_file.data();
    native_config.cert_file_len = static_cast<std::uint32_t>(config.cert_file.size());
    native_config.key_file = config.key_file.data();
    native_config.key_file_len = static_cast<std::uint32_t>(config.key_file.size());
    native_config.ca_cert_file = config.ca_cert_file.data();
    native_config.ca_cert_file_len = static_cast<std::uint32_t>(config.ca_cert_file.size());
    if (config.peer_bidi_stream_count != 0) {
      native_config.peer_bidi_stream_count = config.peer_bidi_stream_count;
    }
    if (config.max_pending_send_bytes != 0) {
      native_config.max_pending_send_bytes = config.max_pending_send_bytes;
    }
    if (config.max_pending_send_count != 0) {
      native_config.max_pending_send_count = static_cast<std::uint32_t>(std::min<std::size_t>(
          config.max_pending_send_count, std::numeric_limits<std::uint32_t>::max()));
    }
    if (config.max_frame_size != 0) {
      native_config.max_frame_size = config.max_frame_size;
    }
    native_config.max_idle_timeout_ms = static_cast<std::uint64_t>(config.max_idle_timeout.count());
    native_config.keep_alive_ms = static_cast<std::uint32_t>(config.keep_alive.count());
    native_config.stream_recv_window = config.stream_recv_window;
    native_config.conn_flow_control_window = config.conn_flow_control_window;
    return trevrpc_rpc_msquic_endpoint_start_v1(native, &native_config, operation_id, endpoint);
  };
  auto core = create(runtime, std::move(config), std::move(starter));
  if (!core) {
    (void)runtime->shutdown();
  }
  return core;
}

ChannelCore::~ChannelCore() { (void)request_close(); }

ChannelCorePhase ChannelCore::phase() const noexcept {
  auto state = state_;
  if (!state) {
    return ChannelCorePhase::Closed;
  }
  std::lock_guard lock(state->mutex);
  return state->phase;
}

std::uint64_t ChannelCore::generation() const noexcept {
  auto state = state_;
  if (!state) {
    return 0;
  }
  std::lock_guard lock(state->mutex);
  return state->committed_generation;
}

Result<std::uint64_t> ChannelCore::wait_ready(std::chrono::nanoseconds timeout,
                                              const RpcCancellation* cancellation) const {
  if (timeout.count() < 0) {
    return Error::runtime(-EINVAL, "readiness timeout must not be negative");
  }
  auto state = state_;
  if (!state) {
    return Error::runtime(-ESHUTDOWN, "RPC channel is closed");
  }
  if (cancellation && cancellation->cancelled()) {
    return Error::runtime(-ECANCELED, "RPC channel readiness wait was cancelled");
  }
  RpcCancellation::Subscription subscription;
  if (cancellation) {
    auto subscribed = cancellation->subscribe([weak = std::weak_ptr(state)]() noexcept {
      if (auto current = weak.lock()) {
        current->condition.notify_all();
      }
    });
    if (!subscribed) {
      return subscribed.error();
    }
    subscription = std::move(subscribed).value();
  }

  std::unique_lock lock(state->mutex);
  const auto ready = [&] {
    return state->phase == ChannelCorePhase::Ready || state->close_requested ||
           (cancellation && cancellation->cancelled());
  };
  bool awakened = true;
  if (!ready()) {
    if (timeout == std::chrono::nanoseconds::zero()) {
      state->condition.wait(lock, ready);
    } else {
      awakened = state->condition.wait_for(lock, timeout, ready);
    }
  }
  if (cancellation && cancellation->cancelled()) {
    return Error::runtime(-ECANCELED, "RPC channel readiness wait was cancelled");
  }
  if (state->phase == ChannelCorePhase::Ready && state->active_generation) {
    return state->committed_generation;
  }
  if (state->close_requested) {
    return Error::runtime(-ESHUTDOWN, "RPC channel is closed");
  }
  return Error::runtime(awakened ? -ENOTCONN : -ETIMEDOUT,
                        awakened ? "RPC channel is not ready"
                                 : "RPC channel readiness wait timed out");
}

Result<ChannelCore::GenerationLease> ChannelCore::acquire_generation() {
  auto state = state_;
  if (!state) {
    return Error::runtime(-ESHUTDOWN, "RPC channel is closed");
  }
  std::lock_guard lock(state->mutex);
  if (state->close_requested || state->phase == ChannelCorePhase::Closed) {
    return Error::runtime(-ESHUTDOWN, "RPC channel is closed");
  }
  if (state->phase != ChannelCorePhase::Ready || !state->active_generation) {
    return Error::runtime(-ENOTCONN, "RPC channel is not ready");
  }
  ++state->active_generation->pins;
  return GenerationLease(state->active_generation);
}

Result<void> ChannelCore::request_close() noexcept {
  auto state = state_;
  if (!state) {
    return {};
  }
  {
    std::lock_guard lock(state->mutex);
    if (state->worker_exited) {
      return state->close_error == 0
                 ? Result<void>{}
                 : Result<void>{Error::runtime(state->close_error, "RPC channel shutdown failed")};
    }
    if (!state->close_requested) {
      state->close_requested = true;
      state->retry_pending = false;
      state->active_generation.reset();
      state->set_phase_locked(ChannelCorePhase::Closed);
    }
    ++state->close_request_generation;
    const auto now = std::chrono::steady_clock::now();
    if (state->shutdown_retry_error != 0) {
      state->shutdown_attempt = 0;
      state->shutdown_retry_at = now;
    }
    for (const auto& generation : state->generations) {
      if (generation->close_error != 0) {
        generation->close_attempt = 0;
        generation->close_retry_at = now;
      }
      if (generation->cleanup_error != 0) {
        generation->cleanup_attempt = 0;
        generation->cleanup_retry_at = now;
      }
      if (generation->release_error != 0) {
        generation->release_attempt = 0;
        generation->release_retry_at = now;
      }
    }
  }
  state->condition.notify_all();
  return {};
}

Result<void> ChannelCore::close() {
  auto state = state_;
  if (!state) {
    return {};
  }
  auto requested = request_close();
  if (!requested) {
    return requested.error();
  }
  bool wait = true;
  std::uint64_t request_generation = 0;
  {
    std::lock_guard lock(state->mutex);
    request_generation = state->close_request_generation;
    wait = state->worker_thread_id != std::this_thread::get_id() &&
           !state->runtime->blocking_wait_forbidden();
  }
  if (!wait) {
    return {};
  }
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock, [&] {
    return state->worker_exited || state->close_failure_generation >= request_generation;
  });
  if (!state->worker_exited) {
    return Error::runtime(state->close_failure_error, "RPC channel endpoint cleanup failed");
  }
  return state->close_error == 0
             ? Result<void>{}
             : Result<void>{Error::runtime(state->close_error, "RPC channel shutdown failed")};
}

void ChannelCore::test_fail_next_start(int error) noexcept {
  injected_channel_start_error.store(error, std::memory_order_release);
}

void ChannelCore::test_wait_for_attempts(std::size_t count) {
  auto state = state_;
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock,
                        [&] { return state->start_attempts >= count || state->worker_exited; });
}

void ChannelCore::test_wait_for_retired(std::size_t count) {
  auto state = state_;
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock, [&] {
    return static_cast<std::size_t>(std::count_if(
               state->generations.begin(), state->generations.end(),
               [](const auto& generation) { return generation->terminal; })) >= count ||
           state->worker_exited;
  });
}

void ChannelCore::test_wait_for_lifecycle_idle() {
  auto state = state_;
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock, [&] {
    return (state->lifecycle_events.empty() && !state->lifecycle_callback_active) ||
           state->worker_exited;
  });
}

void ChannelCore::test_enqueue_lifecycle(ChannelCoreEvent event) {
  auto state = state_;
  {
    std::lock_guard lock(state->mutex);
    state->enqueue_lifecycle_locked(event);
  }
  state->condition.notify_all();
}

std::size_t ChannelCore::test_live_generations() const noexcept {
  auto state = state_;
  std::lock_guard lock(state->mutex);
  return state->generations.size();
}

} // namespace trevrpc::detail
