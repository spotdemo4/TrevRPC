#include "rpc_event_runtime.hpp"

#include "rpc_client.hpp"
#include "rpc_metadata.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <exception>
#include <limits>
#include <map>
#include <mutex>
#include <new>
#include <optional>
#include <stop_token>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#ifndef _WIN32
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#endif

namespace trevrpc::detail {
namespace {

[[nodiscard]] std::size_t positive_capacity(std::uint32_t value) noexcept {
  return std::max<std::size_t>(value, 1);
}

[[nodiscard]] std::size_t saturating_add(std::size_t left, std::size_t right) noexcept {
  return right > std::numeric_limits<std::size_t>::max() - left
             ? std::numeric_limits<std::size_t>::max()
             : left + right;
}

[[nodiscard]] std::size_t saturating_mul(std::size_t left, std::size_t right) noexcept {
  return left != 0 && right > std::numeric_limits<std::size_t>::max() / left
             ? std::numeric_limits<std::size_t>::max()
             : left * right;
}

std::atomic<int> injected_adopt_start_error{0};

[[nodiscard]] bool retryable_cleanup_error(int error) noexcept {
  return error == -EAGAIN || error == -EBUSY || error == -EINPROGRESS || error == -EIO ||
         error == -EDEADLK;
}

void abandon_cleanup_owner(const std::shared_ptr<RpcCleanupWork>& work) noexcept {
  if (!work) {
    return;
  }
  work->interrupt_cleanup();
  work->abandon_cleanup();
}

class UnadoptedRuntimeReaper final {
public:
  UnadoptedRuntimeReaper()
      : worker_([this](std::stop_token stop) { run(std::move(stop)); }) {}

  ~UnadoptedRuntimeReaper() {
    worker_.request_stop();
    condition_.notify_all();
  }

  [[nodiscard]] Result<void> reserve() {
    std::lock_guard lock(mutex_);
    try {
      entries_.reserve(entries_.size() + reserved_ + 1);
    } catch (...) {
      return Error::runtime(-ENOMEM, "failed to reserve unadopted RPC runtime cleanup");
    }
    ++reserved_;
    return {};
  }

  void cancel() noexcept {
    std::lock_guard lock(mutex_);
    if (reserved_ != 0) {
      --reserved_;
    }
  }

  void submit(trevrpc_rpc_runtime* runtime) noexcept {
    {
      std::lock_guard lock(mutex_);
      if (reserved_ != 0) {
        --reserved_;
      }
      for (auto& entry : entries_) {
        if (entry.runtime == nullptr) {
          entry.runtime = runtime;
          entry.stage = Stage::Close;
          ++active_;
          ++generation_;
          condition_.notify_one();
          return;
        }
      }
      entries_.push_back(Entry{runtime, Stage::Close});
      ++active_;
      ++generation_;
      condition_.notify_one();
    }
  }

  void wait_until_idle() noexcept {
    std::unique_lock lock(mutex_);
    condition_.wait(lock, [&] { return active_ == 0; });
  }

private:
  enum class Stage {
    Close,
    Events,
    Drain,
    Release,
  };

  struct Entry {
    trevrpc_rpc_runtime* runtime = nullptr;
    Stage stage = Stage::Close;
  };

  void run(std::stop_token stop) noexcept {
    std::size_t cursor = 0;
    for (;;) {
      trevrpc_rpc_runtime* runtime = nullptr;
      Stage stage = Stage::Close;
      std::size_t selected = 0;
      {
        std::unique_lock lock(mutex_);
        condition_.wait(lock, [&] { return stop.stop_requested() || active_ != 0; });
        if (stop.stop_requested()) {
          return;
        }
        for (std::size_t offset = 0; offset < entries_.size(); ++offset) {
          const std::size_t index = (cursor + offset) % entries_.size();
          if (entries_[index].runtime != nullptr) {
            selected = index;
            runtime = entries_[index].runtime;
            stage = entries_[index].stage;
            cursor = (index + 1) % entries_.size();
            break;
          }
        }
      }

      Stage next = stage;
      bool complete = false;
      switch (stage) {
      case Stage::Close: {
        const int error =
            trevrpc_rpc_runtime_close(runtime, std::numeric_limits<std::uint64_t>::max());
        if (error == 0 || error == -EALREADY) {
          next = Stage::Events;
        }
        break;
      }
      case Stage::Events: {
        trevrpc_rpc_event* event = nullptr;
        const int error = trevrpc_rpc_runtime_next_event(runtime, &event);
        if (error == 0 && event != nullptr) {
          trevrpc_rpc_event_info_v1 info{};
          int info_error = trevrpc_rpc_event_info_v1_init(&info, sizeof(info));
          if (info_error == 0) {
            info_error = trevrpc_rpc_event_get_info_v1(event, &info);
          }
          if (info_error == 0 && info.kind == TREVRPC_RPC_EVENT_STOPPED) {
            next = Stage::Drain;
          }
          trevrpc_rpc_event_release(event);
        }
        break;
      }
      case Stage::Drain:
        if (trevrpc_rpc_runtime_drain(runtime) == 0) {
          next = Stage::Release;
        }
        break;
      case Stage::Release:
        complete = trevrpc_rpc_runtime_release(runtime) == 0;
        break;
      }

      {
        std::unique_lock lock(mutex_);
        if (complete) {
          entries_[selected] = {};
          --active_;
          condition_.notify_all();
        } else {
          entries_[selected].stage = next;
        }
        const auto observed = generation_;
        condition_.wait_for(lock, std::chrono::milliseconds(10),
                            [&] { return stop.stop_requested() || generation_ != observed; });
        if (stop.stop_requested()) {
          return;
        }
      }
    }
  }

  std::mutex mutex_;
  std::condition_variable condition_;
  std::vector<Entry> entries_;
  std::size_t active_ = 0;
  std::size_t reserved_ = 0;
  std::size_t generation_ = 0;
  std::jthread worker_;
};

[[nodiscard]] UnadoptedRuntimeReaper& unadopted_runtime_reaper() {
  static UnadoptedRuntimeReaper reaper;
  return reaper;
}

} // namespace

struct RpcEventRuntime::SharedState final {
  struct HandleKey {
    std::uint64_t owner = 0;
    std::uint32_t slot = 0;
    std::uint32_t generation = 0;

    [[nodiscard]] bool operator==(const HandleKey&) const noexcept = default;
  };

  struct HandleKeyHash {
    [[nodiscard]] std::size_t operator()(const HandleKey& key) const noexcept {
      std::size_t value = std::hash<std::uint64_t>{}(key.owner);
      value ^= std::hash<std::uint32_t>{}(key.slot) + 0x9e3779b9U + (value << 6U) + (value >> 2U);
      value ^=
          std::hash<std::uint32_t>{}(key.generation) + 0x9e3779b9U + (value << 6U) + (value >> 2U);
      return value;
    }
  };

  struct EventDelivery {
    EventCallback callback;
    std::optional<RpcEvent> event;
    int error = 0;
  };

  struct IncomingDelivery {
    IncomingCallback callback;
    std::optional<RpcIncomingCall> incoming;
    int error = 0;
  };

  struct OperationSlot {
    std::optional<RpcEvent> event;
    EventCallback callback;
    bool blocking_waiter = false;
  };

  struct ShutdownCompletionSlot {
    std::uint64_t operation_id = 1;
    std::optional<RpcEvent> event;
    EventCallback callback;
    bool blocking_waiter = false;
    bool consumed = false;
  };

  struct StrictAttempt {
    std::uint64_t generation = 0;
    int status = 0;
    bool completed = false;
    bool close_submission_claimed = false;
  };

  struct SubjectSlot {
    std::deque<RpcEvent> events;
    EventCallback callback;
    bool blocking_waiter = false;
    bool readable_pending = false;
    bool discard_until_call_closed = false;
  };

  struct CleanupTask {
    std::shared_ptr<RpcCleanupWork> work;
    std::chrono::steady_clock::time_point due{};
    unsigned int attempts = 0;
    bool retryable = true;
  };

  struct SubjectRegistry {
    std::unordered_map<HandleKey, SubjectSlot, HandleKeyHash> registered;
    std::unordered_map<HandleKey, SubjectSlot, HandleKeyHash> pending;
    std::unordered_set<HandleKey, HandleKeyHash> discarded;
    std::size_t registered_limit = 1;
    std::size_t pending_limit = 1;
    enum class ReadablePolicy {
      QueueAll,
      Coalesce,
    };

    ReadablePolicy readable_policy = ReadablePolicy::QueueAll;
    bool injected_registration_commit_failure = false;

    void reserve() {
      registered.reserve(registered_limit);
      pending.reserve(pending_limit);
      discarded.reserve(pending_limit);
    }

    [[nodiscard]] bool is_discarded(const HandleKey& subject_key) const noexcept {
      return discarded.contains(subject_key);
    }

    [[nodiscard]] int discard_subject(const HandleKey& subject_key) noexcept {
      try {
        discarded.insert(subject_key);
        return 0;
      } catch (...) {
        return -ENOMEM;
      }
    }

    void consume_discarded(const HandleKey& subject_key) noexcept { discarded.erase(subject_key); }

    [[nodiscard]] Result<void> register_subject(const HandleKey& subject_key) {
      if (discarded.contains(subject_key)) {
        return Error::runtime(-ECANCELED, "RPC stream was rejected under incoming-call overload");
      }
      if (registered.contains(subject_key)) {
        return Error::runtime(-EALREADY, "RPC subject is already registered");
      }
      const auto pending_subject = pending.find(subject_key);
      if (pending_subject != pending.end() && pending_subject->second.discard_until_call_closed) {
        return Error::runtime(-ECANCELED, "RPC stream was rejected under incoming-call overload");
      }
      if (registered.size() >= registered_limit) {
        return Error::runtime(-ENOBUFS, "RPC subject capacity is exhausted");
      }
      if (pending_subject == pending.end()) {
        try {
          registered.emplace(subject_key, SubjectSlot{});
        } catch (...) {
          return Error::runtime(-ENOMEM, "failed to register RPC subject");
        }
        return {};
      }
      auto node = pending.extract(pending_subject);
      try {
        if (std::exchange(injected_registration_commit_failure, false)) {
          throw std::bad_alloc();
        }
        auto inserted = registered.insert(std::move(node));
        if (!inserted.inserted) {
          pending.insert(std::move(inserted.node));
          return Error::runtime(-EALREADY, "RPC subject is already registered");
        }
      } catch (...) {
        if (!node.empty()) {
          try {
            pending.insert(std::move(node));
          } catch (...) {
            std::terminate();
          }
        }
        return Error::runtime(-ENOMEM, "failed to register RPC subject");
      }
      return {};
    }

    [[nodiscard]] EventDelivery unregister_subject(const HandleKey& subject_key,
                                                   std::size_t& buffered_count) noexcept {
      EventDelivery delivery;
      const auto registered_subject = registered.find(subject_key);
      if (registered_subject != registered.end()) {
        buffered_count -= std::min(buffered_count, registered_subject->second.events.size());
        if (registered_subject->second.callback) {
          delivery.callback = std::move(registered_subject->second.callback);
          delivery.error = -ECANCELED;
        }
        registered.erase(registered_subject);
      }
      const auto pending_subject = pending.find(subject_key);
      if (pending_subject != pending.end() && !pending_subject->second.discard_until_call_closed) {
        buffered_count -= std::min(buffered_count, pending_subject->second.events.size());
        pending.erase(pending_subject);
      }
      discarded.erase(subject_key);
      return delivery;
    }

    [[nodiscard]] SubjectSlot* find_registered(const HandleKey& subject_key) noexcept {
      const auto found = registered.find(subject_key);
      return found == registered.end() ? nullptr : &found->second;
    }

    [[nodiscard]] int prepare_route(const HandleKey& subject_key, SubjectSlot*& out_slot) noexcept {
      const auto registered_subject = registered.find(subject_key);
      if (registered_subject != registered.end()) {
        out_slot = &registered_subject->second;
        return 0;
      }
      const auto existing_pending = pending.find(subject_key);
      if (existing_pending != pending.end()) {
        out_slot = &existing_pending->second;
        return 0;
      }
      if (pending.size() >= pending_limit) {
        return -ENOBUFS;
      }
      try {
        out_slot = &pending.emplace(subject_key, SubjectSlot{}).first->second;
        return 0;
      } catch (...) {
        return -ENOMEM;
      }
    }

    [[nodiscard]] std::optional<RpcEvent> claim_event(SubjectSlot& slot,
                                                      std::size_t& buffered_count) {
      if (slot.events.empty()) {
        return std::nullopt;
      }
      RpcEvent event = std::move(slot.events.front());
      slot.events.pop_front();
      --buffered_count;
      if (readable_policy == ReadablePolicy::Coalesce &&
          event.kind == TREVRPC_RPC_EVENT_STREAM_READABLE) {
        slot.readable_pending = false;
      }
      return event;
    }

    [[nodiscard]] int route(SubjectSlot& slot, RpcEvent event, std::size_t& buffered_count,
                            std::size_t buffered_limit, EventDelivery& delivery) noexcept {
      if (slot.discard_until_call_closed) {
        if (event.kind == TREVRPC_RPC_EVENT_CALL_CLOSED) {
          slot.discard_until_call_closed = false;
        }
        return 0;
      }
      if (readable_policy == ReadablePolicy::Coalesce &&
          event.kind == TREVRPC_RPC_EVENT_STREAM_READABLE && slot.readable_pending) {
        return 0;
      }
      if (slot.callback) {
        delivery.callback = std::move(slot.callback);
        delivery.event = std::move(event);
        return 0;
      }
      if (buffered_count >= buffered_limit) {
        return -ENOBUFS;
      }
      try {
        slot.events.push_back(std::move(event));
        ++buffered_count;
        if (readable_policy == ReadablePolicy::Coalesce &&
            slot.events.back().kind == TREVRPC_RPC_EVENT_STREAM_READABLE) {
          slot.readable_pending = true;
        }
        return 0;
      } catch (...) {
        return -ENOMEM;
      }
    }

    void clear_queued(std::size_t& buffered_count) noexcept {
      for (auto& [unused, slot] : registered) {
        (void)unused;
        buffered_count -= std::min(buffered_count, slot.events.size());
        slot.events.clear();
        slot.readable_pending = false;
      }
      for (auto& [unused, slot] : pending) {
        (void)unused;
        buffered_count -= std::min(buffered_count, slot.events.size());
        slot.events.clear();
        slot.readable_pending = false;
      }
    }

    void clear_pending(std::size_t& buffered_count) noexcept {
      for (auto& [unused, slot] : pending) {
        (void)unused;
        buffered_count -= std::min(buffered_count, slot.events.size());
      }
      pending.clear();
      discarded.clear();
    }
  };

  enum class Lifecycle {
    Open,
    CloseSubmitting,
    CloseAdmitted,
    Stopped,
    Releasing,
    Released,
  };

  struct CallbackScope {
    explicit CallbackScope(SharedState* current) noexcept
        : state(current), previous(active_callback) {
      active_callback = this;
    }
    ~CallbackScope() { active_callback = previous; }

    SharedState* state;
    CallbackScope* previous;
  };

  explicit SharedState(trevrpc_rpc_runtime* raw_runtime,
                       const trevrpc_rpc_runtime_config_v1& config)
      : runtime(raw_runtime), endpoint_limit(positive_capacity(config.endpoint_capacity)),
        stream_limit(positive_capacity(config.stream_capacity)),
        pending_stream_limit(
            std::max<std::size_t>(1, std::min(config.call_capacity, config.stream_capacity))),
        pending_endpoint_limit(positive_capacity(config.endpoint_capacity)),
        incoming_limit(std::max<std::size_t>(
            1, std::min({config.event_capacity, config.call_capacity, config.stream_capacity}))) {
    const std::size_t event_capacity = positive_capacity(config.event_capacity);
    const std::size_t call_capacity = positive_capacity(config.call_capacity);
    buffered_event_limit = saturating_add(
        event_capacity, saturating_add(saturating_mul(endpoint_limit, 2),
                                       saturating_add(saturating_mul(call_capacity, 6),
                                                      saturating_mul(stream_limit, 4))));
    streams.registered_limit = stream_limit;
    streams.pending_limit = pending_stream_limit;
    streams.readable_policy = SubjectRegistry::ReadablePolicy::Coalesce;
    endpoints.registered_limit = endpoint_limit;
    endpoints.pending_limit = pending_endpoint_limit;
    streams.reserve();
    endpoints.reserve();
    incoming_events.resize(incoming_limit, nullptr);
  }

  ~SharedState() {
#ifndef _WIN32
    if (control_read >= 0) {
      ::close(control_read);
    }
    if (control_write >= 0) {
      ::close(control_write);
    }
#endif
  }

  [[nodiscard]] static HandleKey key(trevrpc_rpc_stream_v1 stream) noexcept {
    return HandleKey{stream.owner, stream.slot, stream.generation};
  }

  [[nodiscard]] static HandleKey key(trevrpc_rpc_endpoint_v1 endpoint) noexcept {
    return HandleKey{endpoint.owner, endpoint.slot, endpoint.generation};
  }

  [[nodiscard]] bool callback_active() const noexcept {
    for (const CallbackScope* scope = active_callback; scope != nullptr; scope = scope->previous) {
      if (scope->state == this) {
        return true;
      }
    }
    return false;
  }

  [[nodiscard]] bool blocking_wait_forbidden_locked() const noexcept {
    return callback_active() || driver_thread_id == std::this_thread::get_id();
  }

  [[nodiscard]] trevrpc_rpc_runtime* acquire_native_call() noexcept {
    std::lock_guard lock(mutex);
    if (runtime == nullptr || lifecycle == Lifecycle::Releasing ||
        lifecycle == Lifecycle::Released) {
      return nullptr;
    }
    ++native_calls_in_flight;
    return runtime;
  }

  void release_native_call() noexcept {
    {
      std::lock_guard lock(mutex);
      assert(native_calls_in_flight != 0);
      --native_calls_in_flight;
    }
    condition.notify_all();
    wake_driver();
  }

  [[nodiscard]] Result<void> initialize() {
#ifdef _WIN32
    return Error::runtime(-ENOTSUP, "RPC wake integration is not available on Windows");
#else
    int error = trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake));
    if (error == 0) {
      error = trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake);
    }
    if (error == 0 && wake.kind != TREVRPC_RPC_WAKE_SOURCE_POSIX_FD) {
      error = -ENOTSUP;
    }
    if (error != 0) {
      return Error::runtime(error);
    }
    int descriptors[2];
    if (::pipe(descriptors) != 0) {
      return Error::runtime(-errno, "failed to create RPC driver control pipe");
    }
    const int read_flags = ::fcntl(descriptors[0], F_GETFL, 0);
    const int write_flags = ::fcntl(descriptors[1], F_GETFL, 0);
    if (read_flags < 0 || write_flags < 0 ||
        ::fcntl(descriptors[0], F_SETFL, read_flags | O_NONBLOCK) != 0 ||
        ::fcntl(descriptors[1], F_SETFL, write_flags | O_NONBLOCK) != 0 ||
        ::fcntl(descriptors[0], F_SETFD, FD_CLOEXEC) != 0 ||
        ::fcntl(descriptors[1], F_SETFD, FD_CLOEXEC) != 0) {
      const int pipe_error = errno;
      ::close(descriptors[0]);
      ::close(descriptors[1]);
      return Error::runtime(-pipe_error, "failed to configure RPC driver control pipe");
    }
    control_read = descriptors[0];
    control_write = descriptors[1];
    return {};
#endif
  }

  void wake_driver() noexcept {
#ifndef _WIN32
    {
      std::lock_guard lock(mutex);
      if (control_write >= 0) {
        const std::uint8_t byte = 1;
        ssize_t written = -1;
        int write_error = 0;
        do {
          const int injected_error = std::exchange(injected_control_write_error, 0);
          if (injected_error != 0) {
            write_error = injected_error;
          } else {
            written = ::write(control_write, &byte, sizeof(byte));
            write_error = written < 0 ? errno : 0;
          }
        } while (written < 0 && write_error == EINTR);
        if (written < 0 && write_error != EAGAIN && write_error != EWOULDBLOCK) {
          if (driver_error == 0) {
            driver_error = write_error == 0 ? -EIO : -write_error;
          }
          terminal_settlement_needed = true;
          const int descriptor = std::exchange(control_write, -1);
          if (descriptor >= 0) {
            ::close(descriptor);
          }
        }
      }
    }
#endif
    condition.notify_all();
  }

  [[nodiscard]] Result<std::uint64_t> reserve_operation() {
    std::lock_guard lock(mutex);
    const int injected_error = std::exchange(injected_operation_reservation_error, 0);
    if (injected_error != 0) {
      return Error::runtime(injected_error, "injected RPC operation reservation failure");
    }
    if (driver_error != 0) {
      return Error::runtime(driver_error, "RPC event driver failed");
    }
    if (lifecycle != Lifecycle::Open) {
      return Error::runtime(-ESHUTDOWN, "RPC runtime is stopping");
    }
    try {
      for (;;) {
        const std::uint64_t operation_id = next_operation_id++;
        if (next_operation_id == 0) {
          next_operation_id = 1;
        }
        if (operation_id == 0 || operation_id == shutdown_completion.operation_id) {
          continue;
        }
        if (operations.emplace(operation_id, OperationSlot{}).second) {
          return operation_id;
        }
      }
    } catch (...) {
      return Error::runtime(-ENOMEM, "failed to reserve RPC operation completion");
    }
  }

  void reject_operation(std::uint64_t operation_id) noexcept {
    EventDelivery delivery;
    {
      std::lock_guard lock(mutex);
      if (operation_id == shutdown_completion.operation_id) {
        return;
      }
      const auto found = operations.find(operation_id);
      if (found == operations.end()) {
        return;
      }
      if (found->second.callback) {
        delivery.callback = std::move(found->second.callback);
        delivery.error = -ECANCELED;
        ++callbacks_in_flight;
      }
      operations.erase(found);
    }
    condition.notify_all();
    invoke(std::move(delivery));
  }

  [[nodiscard]] Result<RpcEvent> wait_operation(std::uint64_t operation_id) {
    std::unique_lock lock(mutex);
    if (operation_id == shutdown_completion.operation_id) {
      if (shutdown_completion.consumed) {
        return Error::runtime(-EALREADY, "RPC shutdown completion was already consumed");
      }
      if (shutdown_completion.callback || shutdown_completion.blocking_waiter) {
        return Error::runtime(-EBUSY, "RPC shutdown completion already has a consumer");
      }
      if (!shutdown_completion.event && blocking_wait_forbidden_locked()) {
        return Error::runtime(-EDEADLK,
                              "cannot block for RPC shutdown completion on the event driver");
      }
      shutdown_completion.blocking_waiter = true;
      condition.notify_all();
      condition.wait(lock, [&] {
        return shutdown_completion.event.has_value() || shutdown_completion.consumed ||
               driver_error != 0 || lifecycle == Lifecycle::Released;
      });
      shutdown_completion.blocking_waiter = false;
      if (shutdown_completion.event) {
        RpcEvent event = std::move(shutdown_completion.event).value();
        shutdown_completion.event.reset();
        shutdown_completion.consumed = true;
        return event;
      }
      if (shutdown_completion.consumed) {
        return Error::runtime(-EALREADY, "RPC shutdown completion was already consumed");
      }
      shutdown_completion.consumed = true;
      return Error::runtime(driver_error != 0 ? driver_error : -ESHUTDOWN,
                            "RPC runtime stopped before shutdown completion");
    }

    auto found = operations.find(operation_id);
    if (found == operations.end()) {
      return Error::runtime(driver_error != 0 ? driver_error : -EINVAL,
                            driver_error != 0 ? "RPC event driver failed"
                                              : "RPC operation was not reserved");
    }
    if (found->second.callback || found->second.blocking_waiter) {
      return Error::runtime(-EBUSY, "RPC operation already has a consumer");
    }
    if (!found->second.event && blocking_wait_forbidden_locked()) {
      return Error::runtime(-EDEADLK, "cannot block for an RPC operation on the event driver");
    }
    found->second.blocking_waiter = true;
    condition.notify_all();
    condition.wait(lock, [&] {
      const auto current = operations.find(operation_id);
      return driver_error != 0 || lifecycle == Lifecycle::Stopped ||
             lifecycle == Lifecycle::Released || current == operations.end() ||
             current->second.event.has_value();
    });
    found = operations.find(operation_id);
    if (found == operations.end()) {
      return Error::runtime(driver_error != 0 ? driver_error : -ECANCELED,
                            driver_error != 0 ? "RPC event driver failed"
                                              : "RPC operation was cancelled");
    }
    found->second.blocking_waiter = false;
    if (found->second.event) {
      RpcEvent event = std::move(found->second.event).value();
      operations.erase(found);
      return event;
    }
    operations.erase(found);
    return Error::runtime(driver_error != 0 ? driver_error : -ESHUTDOWN,
                          driver_error != 0 ? "RPC event driver failed"
                                            : "RPC runtime stopped before operation completion");
  }

  [[nodiscard]] Result<std::optional<RpcEvent>> try_wait_operation(std::uint64_t operation_id) {
    std::lock_guard lock(mutex);
    if (operation_id == shutdown_completion.operation_id) {
      return Error::runtime(-EINVAL, "RPC shutdown completion is not a call operation");
    }
    const auto found = operations.find(operation_id);
    if (found == operations.end()) {
      return Error::runtime(driver_error != 0 ? driver_error : -EINVAL,
                            driver_error != 0 ? "RPC event driver failed"
                                              : "RPC operation was not reserved");
    }
    if (found->second.callback || found->second.blocking_waiter) {
      return Error::runtime(-EBUSY, "RPC operation already has a consumer");
    }
    if (!found->second.event) {
      if (driver_error != 0) {
        operations.erase(found);
        return Error::runtime(driver_error, "RPC event driver failed");
      }
      if (lifecycle == Lifecycle::Stopped || lifecycle == Lifecycle::Released) {
        operations.erase(found);
        return Error::runtime(-ESHUTDOWN, "RPC runtime stopped before operation completion");
      }
      return std::optional<RpcEvent>{};
    }
    RpcEvent event = std::move(found->second.event).value();
    operations.erase(found);
    return std::optional<RpcEvent>(std::move(event));
  }

  [[nodiscard]] Result<void> subscribe_operation(std::uint64_t operation_id,
                                                 EventCallback callback) {
    if (!callback) {
      return Error::runtime(-EINVAL, "RPC operation callback must not be empty");
    }
    EventDelivery delivery;
    {
      std::lock_guard lock(mutex);
      if (operation_id == shutdown_completion.operation_id) {
        if (shutdown_completion.consumed) {
          return Error::runtime(-EALREADY, "RPC shutdown completion was already consumed");
        }
        if (shutdown_completion.callback || shutdown_completion.blocking_waiter) {
          return Error::runtime(-EBUSY, "RPC shutdown completion already has a consumer");
        }
        if (shutdown_completion.event) {
          delivery.callback = std::move(callback);
          delivery.event = std::move(shutdown_completion.event);
          shutdown_completion.event.reset();
          shutdown_completion.consumed = true;
          ++callbacks_in_flight;
        } else if (driver_error != 0 || lifecycle == Lifecycle::Released) {
          return Error::runtime(driver_error != 0 ? driver_error : -ESHUTDOWN,
                                driver_error != 0 ? "RPC event driver failed"
                                                  : "RPC runtime is released");
        } else {
          shutdown_completion.callback = std::move(callback);
          return {};
        }
      } else {
        const auto found = operations.find(operation_id);
        if (found == operations.end()) {
          return Error::runtime(driver_error != 0 ? driver_error : -EINVAL,
                                driver_error != 0 ? "RPC event driver failed"
                                                  : "RPC operation was not reserved");
        }
        if (found->second.callback || found->second.blocking_waiter) {
          return Error::runtime(-EBUSY, "RPC operation already has a consumer");
        }
        if (found->second.event) {
          delivery.callback = std::move(callback);
          delivery.event = std::move(found->second.event);
          operations.erase(found);
          ++callbacks_in_flight;
        } else if (driver_error != 0) {
          return Error::runtime(driver_error, "RPC event driver failed");
        } else if (lifecycle == Lifecycle::Stopped || lifecycle == Lifecycle::Released) {
          delivery.callback = std::move(callback);
          delivery.error = -ESHUTDOWN;
          operations.erase(found);
          ++callbacks_in_flight;
        } else {
          found->second.callback = std::move(callback);
          return {};
        }
      }
    }
    invoke(std::move(delivery));
    return {};
  }

  [[nodiscard]] Result<void> register_subject(SubjectRegistry& registry,
                                              const HandleKey& subject_key) {
    std::lock_guard lock(mutex);
    if (driver_error != 0) {
      return Error::runtime(driver_error, "RPC event driver failed");
    }
    if (lifecycle != Lifecycle::Open) {
      return Error::runtime(-ESHUTDOWN, "RPC runtime is stopping");
    }
    auto result = registry.register_subject(subject_key);
    condition.notify_all();
    return result;
  }

  void unregister_subject(SubjectRegistry& registry, const HandleKey& subject_key) noexcept {
    EventDelivery delivery;
    {
      std::lock_guard lock(mutex);
      delivery = registry.unregister_subject(subject_key, buffered_event_count);
      if (delivery.callback) {
        ++callbacks_in_flight;
      }
    }
    condition.notify_all();
    invoke(std::move(delivery));
  }

  [[nodiscard]] Result<void> settle_unregistered_stream(const HandleKey& stream_key) {
    std::unique_lock lock(mutex);
    if (streams.registered.contains(stream_key)) {
      return Error::runtime(-EALREADY, "RPC stream is registered");
    }
    condition.wait(lock, [&] {
      const auto pending = streams.pending.find(stream_key);
      if (pending != streams.pending.end()) {
        return std::any_of(
            pending->second.events.begin(), pending->second.events.end(),
            [](const RpcEvent& event) { return event.kind == TREVRPC_RPC_EVENT_CALL_CLOSED; });
      }
      return driver_error != 0 || lifecycle == Lifecycle::Stopped ||
             lifecycle == Lifecycle::Released;
    });
    const auto pending = streams.pending.find(stream_key);
    if (pending == streams.pending.end()) {
      return Error::runtime(driver_error != 0 ? driver_error : -ESHUTDOWN,
                            driver_error != 0 ? "RPC event driver failed"
                                              : "RPC runtime stopped before call cleanup");
    }
    buffered_event_count -= std::min(buffered_event_count, pending->second.events.size());
    streams.pending.erase(pending);
    return {};
  }

  [[nodiscard]] Result<bool> try_settle_unregistered_stream(const HandleKey& stream_key) {
    std::lock_guard lock(mutex);
    if (streams.registered.contains(stream_key)) {
      return Error::runtime(-EALREADY, "RPC stream is registered");
    }
    const auto pending = streams.pending.find(stream_key);
    if (pending == streams.pending.end()) {
      if (driver_error != 0) {
        return Error::runtime(driver_error, "RPC event driver failed");
      }
      if (lifecycle == Lifecycle::Stopped || lifecycle == Lifecycle::Released) {
        return Error::runtime(-ESHUTDOWN, "RPC runtime stopped before call cleanup");
      }
      return false;
    }
    const bool closed = std::any_of(
        pending->second.events.begin(), pending->second.events.end(),
        [](const RpcEvent& event) { return event.kind == TREVRPC_RPC_EVENT_CALL_CLOSED; });
    if (!closed) {
      return false;
    }
    buffered_event_count -= std::min(buffered_event_count, pending->second.events.size());
    streams.pending.erase(pending);
    return true;
  }

  [[nodiscard]] Result<bool> probe_unregistered_endpoint(const HandleKey& endpoint_key) {
    std::lock_guard lock(mutex);
    if (endpoints.registered.contains(endpoint_key)) {
      return Error::runtime(-EALREADY, "RPC endpoint is registered");
    }
    const auto pending = endpoints.pending.find(endpoint_key);
    if (pending != endpoints.pending.end() &&
        std::any_of(pending->second.events.begin(), pending->second.events.end(),
                    [](const RpcEvent& event) {
                      return event.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED;
                    })) {
      return true;
    }
    if (driver_error != 0) {
      return Error::runtime(driver_error, "RPC event driver failed");
    }
    if (lifecycle == Lifecycle::Stopped || lifecycle == Lifecycle::Released) {
      return Error::runtime(-ESHUTDOWN, "RPC runtime stopped before endpoint cleanup");
    }
    return false;
  }

  [[nodiscard]] Result<void> settle_unregistered_endpoint(const HandleKey& endpoint_key) {
    std::unique_lock lock(mutex);
    if (endpoints.registered.contains(endpoint_key)) {
      return Error::runtime(-EALREADY, "RPC endpoint is registered");
    }
    const auto closed = [&] {
      const auto pending = endpoints.pending.find(endpoint_key);
      return pending != endpoints.pending.end() &&
             std::any_of(pending->second.events.begin(), pending->second.events.end(),
                         [](const RpcEvent& event) {
                           return event.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED;
                         });
    };
    condition.wait(lock, [&] {
      return closed() || driver_error != 0 || lifecycle == Lifecycle::Stopped ||
             lifecycle == Lifecycle::Released;
    });
    const auto pending = endpoints.pending.find(endpoint_key);
    if (!closed() || pending == endpoints.pending.end()) {
      return Error::runtime(driver_error != 0 ? driver_error : -ESHUTDOWN,
                            driver_error != 0 ? "RPC event driver failed"
                                              : "RPC runtime stopped before endpoint cleanup");
    }
    buffered_event_count -= std::min(buffered_event_count, pending->second.events.size());
    endpoints.pending.erase(pending);
    return {};
  }

  [[nodiscard]] Result<RpcEvent> wait_subject(SubjectRegistry& registry,
                                              const HandleKey& subject_key) {
    std::unique_lock lock(mutex);
    SubjectSlot* slot = registry.find_registered(subject_key);
    if (slot == nullptr) {
      return Error::runtime(lifecycle == Lifecycle::Stopped || lifecycle == Lifecycle::Released
                                ? -ESHUTDOWN
                                : -EINVAL,
                            "RPC subject is not registered");
    }
    if (slot->callback || slot->blocking_waiter) {
      return Error::runtime(-EBUSY, "RPC subject already has a consumer");
    }
    if (slot->events.empty() && blocking_wait_forbidden_locked()) {
      return Error::runtime(-EDEADLK, "cannot block for an RPC subject on the event driver");
    }
    slot->blocking_waiter = true;
    condition.notify_all();
    condition.wait(lock, [&] {
      const SubjectSlot* current = registry.find_registered(subject_key);
      return driver_error != 0 || lifecycle == Lifecycle::Stopped ||
             lifecycle == Lifecycle::Released || current == nullptr || !current->events.empty();
    });
    slot = registry.find_registered(subject_key);
    if (slot == nullptr) {
      return Error::runtime(driver_error != 0 ? driver_error : -ECANCELED,
                            driver_error != 0 ? "RPC event driver failed"
                                              : "RPC subject was unregistered");
    }
    slot->blocking_waiter = false;
    if (auto event = registry.claim_event(*slot, buffered_event_count)) {
      return std::move(event).value();
    }
    return Error::runtime(driver_error != 0 ? driver_error : -ESHUTDOWN,
                          driver_error != 0
                              ? "RPC event driver failed"
                              : "RPC runtime stopped before the subject event arrived");
  }

  [[nodiscard]] Result<std::optional<RpcEvent>> try_wait_subject(SubjectRegistry& registry,
                                                                 const HandleKey& subject_key) {
    std::lock_guard lock(mutex);
    SubjectSlot* slot = registry.find_registered(subject_key);
    if (slot == nullptr) {
      return Error::runtime(lifecycle == Lifecycle::Stopped || lifecycle == Lifecycle::Released
                                ? -ESHUTDOWN
                                : -EINVAL,
                            "RPC subject is not registered");
    }
    if (slot->callback || slot->blocking_waiter) {
      return Error::runtime(-EBUSY, "RPC subject already has a consumer");
    }
    if (auto event = registry.claim_event(*slot, buffered_event_count)) {
      return event;
    }
    if (driver_error != 0) {
      return Error::runtime(driver_error, "RPC event driver failed");
    }
    if (lifecycle == Lifecycle::Stopped || lifecycle == Lifecycle::Released) {
      return Error::runtime(-ESHUTDOWN, "RPC runtime stopped before the subject event arrived");
    }
    return std::optional<RpcEvent>{};
  }

  [[nodiscard]] Result<void> subscribe_subject(SubjectRegistry& registry,
                                               const HandleKey& subject_key,
                                               EventCallback callback) {
    if (!callback) {
      return Error::runtime(-EINVAL, "RPC subject callback must not be empty");
    }
    EventDelivery delivery;
    {
      std::lock_guard lock(mutex);
      SubjectSlot* slot = registry.find_registered(subject_key);
      if (slot == nullptr) {
        return Error::runtime(driver_error != 0 ? driver_error : -EINVAL,
                              driver_error != 0 ? "RPC event driver failed"
                                                : "RPC subject is not registered");
      }
      if (slot->callback || slot->blocking_waiter) {
        return Error::runtime(-EBUSY, "RPC subject already has a consumer");
      }
      if (auto event = registry.claim_event(*slot, buffered_event_count)) {
        delivery.callback = std::move(callback);
        delivery.event = std::move(event);
        ++callbacks_in_flight;
      } else if (driver_error != 0) {
        return Error::runtime(driver_error, "RPC event driver failed");
      } else if (lifecycle == Lifecycle::Stopped || lifecycle == Lifecycle::Released) {
        delivery.callback = std::move(callback);
        delivery.error = -ESHUTDOWN;
        ++callbacks_in_flight;
      } else {
        slot->callback = std::move(callback);
        return {};
      }
    }
    invoke(std::move(delivery));
    return {};
  }

  [[nodiscard]] Result<RpcIncomingCall> wait_incoming() {
    trevrpc_rpc_event* raw_event = nullptr;
    {
      std::unique_lock lock(mutex);
      if (incoming_callback || incoming_waiter) {
        return Error::runtime(-EBUSY, "RPC incoming-call queue already has a consumer");
      }
      if (incoming_count == 0 && driver_error != 0) {
        return Error::runtime(driver_error, "RPC event driver failed");
      }
      if (incoming_count == 0 &&
          (incoming_stopped || lifecycle == Lifecycle::Stopped ||
           lifecycle == Lifecycle::Released)) {
        return Error::runtime(-ESHUTDOWN, "RPC incoming-call delivery is stopped");
      }
      if (incoming_count == 0 && blocking_wait_forbidden_locked()) {
        return Error::runtime(-EDEADLK,
                              "cannot block for an incoming RPC call on the event driver");
      }
      incoming_waiter = true;
      condition.notify_all();
      raw_event = pop_incoming_locked();
      if (raw_event == nullptr) {
        condition.wait(lock, [&] {
          return incoming_handoff != nullptr || !incoming_waiter || incoming_stopped ||
                 driver_error != 0 || lifecycle == Lifecycle::Stopped ||
                 lifecycle == Lifecycle::Released;
        });
        raw_event = std::exchange(incoming_handoff, nullptr);
      }
      if (raw_event == nullptr) {
        incoming_waiter = false;
        condition.notify_all();
        const int error = driver_error != 0 ? driver_error : -ESHUTDOWN;
        return Error::runtime(error, driver_error != 0
                                         ? "RPC event driver failed"
                                         : "RPC runtime stopped before an incoming call arrived");
      }
      ++incoming_materializations;
    }

    RpcIncomingCall incoming;
    const int error = materialize_incoming(raw_event, incoming);
    {
      std::lock_guard lock(mutex);
      incoming_waiter = false;
      --incoming_materializations;
    }
    condition.notify_all();
    wake_driver();
    return error == 0 ? Result<RpcIncomingCall>{std::move(incoming)}
                      : Result<RpcIncomingCall>{Error::runtime(error)};
  }

  void stop_incoming() noexcept {
    IncomingDelivery delivery;
    {
      std::lock_guard lock(mutex);
      incoming_stopped = true;
      if (incoming_count == 0) {
        incoming_waiter = false;
        if (incoming_callback) {
          delivery.callback = std::move(incoming_callback);
          delivery.error = -ESHUTDOWN;
          ++callbacks_in_flight;
        }
      }
    }
    condition.notify_all();
    invoke(std::move(delivery));
    wake_driver();
  }

  [[nodiscard]] Result<void> reject_incoming(const RpcIncomingCall& incoming) {
    if (incoming.call.owner == 0 || incoming.stream.owner == 0) {
      return Error::runtime(-EINVAL, "invalid incoming RPC call");
    }
    trevrpc_rpc_runtime* native = acquire_native_call();
    if (native == nullptr) {
      return Error::runtime(-ESHUTDOWN, "RPC runtime is released");
    }
    struct NativeCallGuard final {
      SharedState* state;
      ~NativeCallGuard() { state->release_native_call(); }
    } guard{this};

    auto operation = reserve_operation();
    if (!operation) {
      return operation.error();
    }
    const int close_error = trevrpc_rpc_call_close(
        native, incoming.call, operation.value(), TREVRPC_RPC_CLOSE_FLAG_ABORT,
        TREVRPC_RPC_STATUS_RESOURCE_EXHAUSTED);
    if (close_error != 0) {
      reject_operation(operation.value());
      return Error::runtime(close_error, "failed to close rejected incoming RPC call");
    }
    auto closed = wait_operation(operation.value());
    if (!closed) {
      return closed.error();
    }
    auto settled = settle_unregistered_stream(key(incoming.stream));
    if (!settled) {
      return settled.error();
    }
    const int stream_error = trevrpc_rpc_stream_release(native, incoming.stream);
    const int call_error = trevrpc_rpc_call_release(native, incoming.call);
    if (stream_error != 0 && stream_error != -ESTALE) {
      return Error::runtime(stream_error, "failed to release rejected RPC stream");
    }
    if (call_error != 0 && call_error != -ESTALE) {
      return Error::runtime(call_error, "failed to release rejected RPC call");
    }
    return {};
  }

  [[nodiscard]] Result<void> subscribe_incoming(IncomingCallback callback) {
    if (!callback) {
      return Error::runtime(-EINVAL, "RPC incoming callback must not be empty");
    }
    {
      std::lock_guard lock(mutex);
      if (incoming_callback || incoming_waiter) {
        return Error::runtime(-EBUSY, "RPC incoming-call queue already has a consumer");
      }
      if (driver_error != 0) {
        return Error::runtime(driver_error, "RPC event driver failed");
      }
      if (incoming_count == 0 &&
          (incoming_stopped || lifecycle == Lifecycle::Stopped ||
           lifecycle == Lifecycle::Released)) {
        return Error::runtime(-ESHUTDOWN, "RPC incoming-call delivery is stopped");
      }
      incoming_callback = std::move(callback);
    }
    wake_driver();
    return {};
  }

  [[nodiscard]] trevrpc_rpc_event* pop_incoming_locked() noexcept {
    if (incoming_count == 0) {
      return nullptr;
    }
    trevrpc_rpc_event* event = incoming_events[incoming_head];
    incoming_events[incoming_head] = nullptr;
    incoming_head = (incoming_head + 1) % incoming_events.size();
    --incoming_count;
    return event;
  }

  [[nodiscard]] int materialize_incoming(trevrpc_rpc_event* raw_event,
                                         RpcIncomingCall& incoming) noexcept {
    trevrpc_rpc_event_info_v1 info{};
    int error = trevrpc_rpc_event_info_v1_init(&info, sizeof(info));
    if (error == 0) {
      error = trevrpc_rpc_event_get_info_v1(raw_event, &info);
    }
    if (error != 0) {
      trevrpc_rpc_event_release(raw_event);
      return error;
    }

    if ((info.service_len != 0 && info.service == nullptr) ||
        (info.method_len != 0 && info.method == nullptr)) {
      trevrpc_rpc_event_release(raw_event);
      return -EINVAL;
    }

    try {
      incoming.rpc_kind = info.rpc_kind;
      if (info.service_len != 0) {
        incoming.service.assign(info.service, info.service_len);
      }
      if (info.method_len != 0) {
        incoming.method.assign(info.method, info.method_len);
      }
    } catch (...) {
      trevrpc_rpc_event_release(raw_event);
      return -ENOMEM;
    }

    trevrpc_rpc_receive* initial = nullptr;
    error =
        trevrpc_rpc_event_take_incoming_call(raw_event, &incoming.call, &incoming.stream, &initial);
    if (error != 0) {
      trevrpc_rpc_event_release(raw_event);
      return error;
    }

    trevrpc_rpc_receive_info_v1 receive_info{};
    int preparation_error = trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info));
    if (preparation_error == 0) {
      preparation_error = trevrpc_rpc_receive_get_info_v1(initial, &receive_info);
    }
    if (preparation_error == 0 &&
        ((receive_info.data_len != 0 && receive_info.data == nullptr) ||
         (receive_info.metadata_count != 0 && receive_info.metadata == nullptr))) {
      preparation_error = -EINVAL;
    }
    if (incoming.preparation_status == 0) {
      incoming.preparation_status = preparation_error;
    }
    if (preparation_error == 0) {
      try {
        incoming.initial_message.resize(receive_info.data_len);
        if (receive_info.data_len != 0) {
          std::memcpy(incoming.initial_message.data(), receive_info.data, receive_info.data_len);
        }
        incoming.metadata = copy_rpc_metadata(receive_info.metadata, receive_info.metadata_count);
      } catch (...) {
        incoming.preparation_status = -ENOMEM;
      }
    }
    trevrpc_rpc_receive_release(initial);

    int context_error =
        trevrpc_rpc_call_context_info_v1_init(&incoming.context, sizeof(incoming.context));
    if (context_error == 0) {
      context_error = trevrpc_rpc_call_get_context_v1(runtime, incoming.call, &incoming.context);
    }
    if (incoming.preparation_status == 0 && context_error != 0) {
      incoming.preparation_status = context_error;
    }
    trevrpc_rpc_event_release(raw_event);
    return 0;
  }

  [[nodiscard]] bool service_incoming() noexcept {
    trevrpc_rpc_event* raw_event = nullptr;
    IncomingCallback callback;
    {
      std::lock_guard lock(mutex);
      if (incoming_callback && incoming_count != 0) {
        callback = std::move(incoming_callback);
        raw_event = pop_incoming_locked();
        ++callbacks_in_flight;
      }
    }
    if (raw_event == nullptr) {
      return false;
    }

    RpcIncomingCall incoming;
    const int error = materialize_incoming(raw_event, incoming);
    IncomingDelivery delivery;
    delivery.callback = std::move(callback);
    if (error == 0) {
      delivery.incoming = std::move(incoming);
    } else {
      delivery.error = error;
    }
    invoke(std::move(delivery));
    return true;
  }

  [[nodiscard]] std::uint64_t next_rejected_operation_id_locked() noexcept {
    for (;;) {
      const std::uint64_t operation_id = next_rejected_operation_id--;
      if (next_rejected_operation_id == 0) {
        next_rejected_operation_id = UINT64_MAX;
      }
      if (operation_id != 0 && operation_id != shutdown_completion.operation_id &&
          !operations.contains(operation_id)) {
        return operation_id;
      }
    }
  }

  [[nodiscard]] int close_rejected_incoming(const trevrpc_rpc_event_info_v1& info) noexcept {
    if (info.call.owner == 0) {
      return 0;
    }
    std::uint64_t operation_id;
    {
      std::lock_guard lock(mutex);
      operation_id = next_rejected_operation_id_locked();
    }
    const int error = trevrpc_rpc_call_close(runtime,
                                             info.call,
                                             operation_id,
                                             TREVRPC_RPC_CLOSE_FLAG_ABORT,
                                             TREVRPC_RPC_STATUS_RESOURCE_EXHAUSTED);
    if (error == 0 || error == -EALREADY || error == -ESTALE || error == -ESHUTDOWN) {
      return 0;
    }
    return error;
  }

  [[nodiscard]] int mark_rejected_incoming(trevrpc_rpc_event* raw_event) noexcept {
    trevrpc_rpc_event_info_v1 info{};
    int error = trevrpc_rpc_event_info_v1_init(&info, sizeof(info));
    if (error == 0) {
      error = trevrpc_rpc_event_get_info_v1(raw_event, &info);
    }
    if (error != 0) {
      return error;
    }
    {
      std::lock_guard lock(mutex);
      error = reject_overloaded_incoming_locked(info);
    }
    if (error != 0) {
      return error;
    }
    return close_rejected_incoming(info);
  }

  void discard_queued_incoming(bool include_handoff) noexcept {
    int settlement_error = 0;
    for (;;) {
      trevrpc_rpc_event* raw_event = nullptr;
      {
        std::lock_guard lock(mutex);
        raw_event = pop_incoming_locked();
        if (raw_event == nullptr && include_handoff) {
          raw_event = std::exchange(incoming_handoff, nullptr);
        }
      }
      if (raw_event == nullptr) {
        break;
      }
      const int marker_error = mark_rejected_incoming(raw_event);
      if (settlement_error == 0 && marker_error != 0) {
        settlement_error = marker_error;
      }
      trevrpc_rpc_event_release(raw_event);
    }
    if (settlement_error != 0) {
      fail_driver(settlement_error);
    }
    condition.notify_all();
  }

  void prepare_strict_close() noexcept {
    std::uint64_t generation = 0;
    {
      std::lock_guard lock(mutex);
      if (!strict_attempt || strict_attempt->completed || lifecycle != Lifecycle::Open ||
          strict_preclose_generation >= strict_attempt->generation) {
        return;
      }
      generation = strict_attempt->generation;
    }
    discard_queued_incoming(true);
    {
      std::lock_guard lock(mutex);
      strict_preclose_generation = std::max(strict_preclose_generation, generation);
    }
    condition.notify_all();
  }

  [[nodiscard]] bool is_rejected_followup_locked(const trevrpc_rpc_event_info_v1& info) noexcept {
    if (info.stream.owner == 0) {
      return false;
    }
    const auto stream_key = key(info.stream);
    if (streams.is_discarded(stream_key)) {
      if (info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED) {
        streams.consume_discarded(stream_key);
      }
      return true;
    }
    const auto pending = streams.pending.find(stream_key);
    if (pending == streams.pending.end() || !pending->second.discard_until_call_closed) {
      return false;
    }
    switch (info.kind) {
    case TREVRPC_RPC_EVENT_STREAM_READABLE:
    case TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN:
    case TREVRPC_RPC_EVENT_STREAM_CLOSED:
      return true;
    case TREVRPC_RPC_EVENT_CALL_CLOSED:
      streams.pending.erase(pending);
      return true;
    default:
      return false;
    }
  }

  [[nodiscard]] int
  reject_overloaded_incoming_locked(const trevrpc_rpc_event_info_v1& info) noexcept {
    if (info.stream.owner == 0) {
      return 0;
    }
    const auto stream_key = key(info.stream);
    if (streams.is_discarded(stream_key)) {
      ++rejected_incoming_count;
      return 0;
    }
    SubjectSlot* slot = nullptr;
    const int error = streams.prepare_route(stream_key, slot);
    if (error == -ENOBUFS) {
      const int discard_error = streams.discard_subject(stream_key);
      if (discard_error != 0) {
        return discard_error;
      }
      ++rejected_incoming_count;
      return 0;
    }
    if (error != 0) {
      return error;
    }
    buffered_event_count -= std::min(buffered_event_count, slot->events.size());
    slot->events.clear();
    slot->readable_pending = false;
    slot->discard_until_call_closed = true;
    ++rejected_incoming_count;
    return 0;
  }

  [[nodiscard]] int accept_raw_incoming(trevrpc_rpc_event* raw_event,
                                        const trevrpc_rpc_event_info_v1& info) noexcept {
    IncomingCallback callback;
    bool waiter_handoff = false;
    bool reject = false;
    int error = 0;
    {
      std::lock_guard lock(mutex);
      if (!incoming_stopped) {
        if (incoming_callback) {
          callback = std::move(incoming_callback);
          ++callbacks_in_flight;
        } else if (incoming_waiter && incoming_handoff == nullptr) {
          incoming_handoff = raw_event;
          waiter_handoff = true;
        } else if (incoming_count != incoming_events.size()) {
          incoming_events[incoming_tail] = raw_event;
          incoming_tail = (incoming_tail + 1) % incoming_events.size();
          ++incoming_count;
        } else {
          reject = true;
        }
      } else {
        reject = true;
      }
      if (reject) {
        error = reject_overloaded_incoming_locked(info);
      }
    }
    if (reject && error == 0) {
      error = close_rejected_incoming(info);
    }
    condition.notify_all();
    if (callback) {
      RpcIncomingCall incoming;
      const int materialize_error = materialize_incoming(raw_event, incoming);
      IncomingDelivery delivery;
      delivery.callback = std::move(callback);
      if (materialize_error == 0) {
        delivery.incoming = std::move(incoming);
      } else {
        delivery.error = materialize_error;
      }
      invoke(std::move(delivery));
    } else if (waiter_handoff) {
      condition.notify_all();
    } else if (reject) {
      trevrpc_rpc_event_release(raw_event);
    }
    return error;
  }

  [[nodiscard]] static bool stream_event_kind(std::uint32_t kind) noexcept {
    return kind == TREVRPC_RPC_EVENT_CALL_FAILED || kind == TREVRPC_RPC_EVENT_STREAM_READABLE ||
           kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN ||
           kind == TREVRPC_RPC_EVENT_STREAM_CLOSED || kind == TREVRPC_RPC_EVENT_CALL_CLOSED;
  }

  [[nodiscard]] static bool endpoint_event_kind(std::uint32_t kind) noexcept {
    return kind == TREVRPC_RPC_EVENT_ENDPOINT_READY || kind == TREVRPC_RPC_EVENT_ENDPOINT_FAILED ||
           kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED;
  }

  [[nodiscard]] int dispatch(RpcEvent event) noexcept {
    EventDelivery operation_delivery;
    EventDelivery subject_delivery;
    const bool stream_candidate = event.stream.owner != 0 && stream_event_kind(event.kind);
    const bool endpoint_candidate = event.endpoint.owner != 0 && endpoint_event_kind(event.kind);
    const bool subject_candidate = stream_candidate || endpoint_candidate;
    std::optional<RpcEvent> operation_copy;
    if (event.operation_id != 0 && subject_candidate) {
      try {
        operation_copy = event;
      } catch (...) {
        return -ENOMEM;
      }
    }

    int result = 0;
    {
      std::lock_guard lock(mutex);
      if (event.kind == TREVRPC_RPC_EVENT_STOPPED) {
        lifecycle = Lifecycle::Stopped;
        terminal_status = event.status;
        if (shutdown_completion.callback) {
          operation_delivery.callback = std::move(shutdown_completion.callback);
          operation_delivery.event = std::move(event);
          shutdown_completion.consumed = true;
          ++callbacks_in_flight;
        } else if (!shutdown_completion.event && !shutdown_completion.consumed) {
          shutdown_completion.event = std::move(event);
        }
        terminal_settlement_needed = true;
      } else {
        OperationSlot* operation = nullptr;
        if (event.operation_id != 0) {
          const auto found = operations.find(event.operation_id);
          if (found != operations.end() && !found->second.event) {
            operation = &found->second;
          }
        }

        SubjectRegistry* registry = nullptr;
        SubjectSlot* subject = nullptr;
        if (stream_candidate) {
          registry = &streams;
          result = registry->prepare_route(key(event.stream), subject);
        } else if (endpoint_candidate) {
          registry = &endpoints;
          result = registry->prepare_route(key(event.endpoint), subject);
        }

        const bool subject_route = result == 0 && subject != nullptr;
        auto route_operation = [&](RpcEvent routed) {
          const std::uint64_t operation_id = routed.operation_id;
          if (operation->callback) {
            operation_delivery.callback = std::move(operation->callback);
            operation_delivery.event = std::move(routed);
            operations.erase(operation_id);
            ++callbacks_in_flight;
          } else {
            operation->event = std::move(routed);
          }
        };
        if (subject_route) {
          if (operation != nullptr) {
            route_operation(std::move(operation_copy).value());
          }
          result = registry->route(*subject, std::move(event), buffered_event_count,
                                   buffered_event_limit, subject_delivery);
          if (subject_delivery.callback) {
            ++callbacks_in_flight;
          }
        } else if (operation != nullptr) {
          route_operation(std::move(event));
        }
      }
    }
    condition.notify_all();
    invoke(std::move(operation_delivery));
    invoke(std::move(subject_delivery));
    return result;
  }

  [[nodiscard]] int drain_events() noexcept {
    for (;;) {
      trevrpc_rpc_event* raw_event = nullptr;
      const int next_error = trevrpc_rpc_runtime_next_event(runtime, &raw_event);
      if (next_error != 0) {
        return next_error;
      }
      if (raw_event == nullptr) {
        return -EIO;
      }
      trevrpc_rpc_event_info_v1 info{};
      int error = trevrpc_rpc_event_info_v1_init(&info, sizeof(info));
      if (error == 0) {
        error = trevrpc_rpc_event_get_info_v1(raw_event, &info);
      }
      if (error != 0) {
        trevrpc_rpc_event_release(raw_event);
        return error;
      }
      bool rejected_followup = false;
      {
        std::lock_guard lock(mutex);
        rejected_followup = is_rejected_followup_locked(info);
      }
      if (rejected_followup) {
        trevrpc_rpc_event_release(raw_event);
        condition.notify_all();
        continue;
      }
      if (info.kind == TREVRPC_RPC_EVENT_CALL_INCOMING) {
        error = accept_raw_incoming(raw_event, info);
        if (error != 0) {
          return error;
        }
        continue;
      }

      try {
        RpcEvent event;
        event.kind = info.kind;
        event.flags = info.flags;
        event.status = info.status;
        event.subject_kind = info.subject_kind;
        event.sequence = info.sequence;
        event.endpoint = info.endpoint;
        event.call = info.call;
        event.stream = info.stream;
        event.cancellation = info.cancellation;
        event.operation_id = info.operation_id;
        event.rpc_status = info.rpc_status;
        event.rpc_kind = info.rpc_kind;
        event.application_error_code = info.application_error_code;
        event.provider_error_code = info.provider_error_code;
        if (info.service != nullptr) {
          event.service.assign(info.service, info.service_len);
        }
        if (info.method != nullptr) {
          event.method.assign(info.method, info.method_len);
        }
        error = dispatch(std::move(event));
      } catch (...) {
        error = -ENOMEM;
      }
      trevrpc_rpc_event_release(raw_event);
      if (error != 0) {
        return error;
      }
    }
  }

  void invoke(EventDelivery delivery) noexcept {
    if (!delivery.callback) {
      return;
    }
    CallbackScope scope(this);
    try {
      if (delivery.event) {
        delivery.callback(Result<RpcEvent>(std::move(delivery.event).value()));
      } else {
        delivery.callback(Result<RpcEvent>(Error::runtime(delivery.error)));
      }
    } catch (...) {
      // Binding callbacks are scheduling stubs. Keep failures contained.
      (void)std::current_exception();
    }
    {
      std::lock_guard lock(mutex);
      --callbacks_in_flight;
    }
    condition.notify_all();
    wake_driver();
  }

  void invoke(IncomingDelivery delivery) noexcept {
    if (!delivery.callback) {
      return;
    }
    CallbackScope scope(this);
    try {
      if (delivery.incoming) {
        delivery.callback(Result<RpcIncomingCall>(std::move(delivery.incoming).value()));
      } else {
        delivery.callback(Result<RpcIncomingCall>(Error::runtime(delivery.error)));
      }
    } catch (...) {
      // Binding callbacks are scheduling stubs. Keep failures contained.
      (void)std::current_exception();
    }
    {
      std::lock_guard lock(mutex);
      --callbacks_in_flight;
    }
    condition.notify_all();
    wake_driver();
  }

  void settle_consumers(int error, bool abandon) noexcept {
    std::vector<trevrpc_rpc_event*> raw_events;
    trevrpc_rpc_event* handoff = nullptr;
    bool mark_followups = false;
    {
      std::lock_guard lock(mutex);
      mark_followups = lifecycle != Lifecycle::Stopped;
      raw_events = std::move(incoming_events);
      incoming_head = 0;
      incoming_tail = 0;
      incoming_count = 0;
      handoff = std::exchange(incoming_handoff, nullptr);
      incoming_waiter = false;
      streams.clear_pending(buffered_event_count);
      endpoints.clear_pending(buffered_event_count);
      timers.clear();
      if (abandon) {
        streams.clear_queued(buffered_event_count);
        endpoints.clear_queued(buffered_event_count);
        streams.registered.clear();
        endpoints.registered.clear();
      }
      terminal_settlement_needed = false;
    }
    int raw_settlement_error = 0;
    for (trevrpc_rpc_event* raw_event : raw_events) {
      if (raw_event != nullptr) {
        const int marker_error = mark_followups ? mark_rejected_incoming(raw_event) : 0;
        if (raw_settlement_error == 0 && marker_error != 0) {
          raw_settlement_error = marker_error;
        }
        trevrpc_rpc_event_release(raw_event);
      }
    }
    if (handoff != nullptr) {
      const int marker_error = mark_followups ? mark_rejected_incoming(handoff) : 0;
      if (raw_settlement_error == 0 && marker_error != 0) {
        raw_settlement_error = marker_error;
      }
      trevrpc_rpc_event_release(handoff);
    }
    if (raw_settlement_error != 0) {
      std::lock_guard lock(mutex);
      if (driver_error == 0) {
        driver_error = raw_settlement_error;
      }
      terminal_settlement_needed = true;
    }

    EventDelivery shutdown_delivery;
    {
      std::lock_guard lock(mutex);
      if (shutdown_completion.callback && !shutdown_completion.consumed) {
        shutdown_delivery.callback = std::move(shutdown_completion.callback);
        shutdown_delivery.error = automatic_close_error != 0 ? automatic_close_error : error;
        shutdown_completion.consumed = true;
        ++callbacks_in_flight;
      }
    }
    invoke(std::move(shutdown_delivery));

    for (;;) {
      EventDelivery delivery;
      {
        std::lock_guard lock(mutex);
        auto found = std::find_if(operations.begin(), operations.end(),
                                  [](const auto& entry) { return bool(entry.second.callback); });
        if (found == operations.end()) {
          break;
        }
        delivery.callback = std::move(found->second.callback);
        delivery.error = error;
        operations.erase(found);
        ++callbacks_in_flight;
      }
      invoke(std::move(delivery));
    }
    {
      std::lock_guard lock(mutex);
      for (auto iterator = operations.begin(); iterator != operations.end();) {
        if (!iterator->second.blocking_waiter && !iterator->second.event) {
          iterator = operations.erase(iterator);
        } else {
          ++iterator;
        }
      }
    }

    auto settle_registry = [&](SubjectRegistry& registry) {
      for (;;) {
        EventDelivery delivery;
        {
          std::lock_guard lock(mutex);
          auto found = std::find_if(registry.registered.begin(), registry.registered.end(),
                                    [](const auto& entry) { return bool(entry.second.callback); });
          if (found == registry.registered.end()) {
            break;
          }
          delivery.callback = std::move(found->second.callback);
          delivery.error = error;
          ++callbacks_in_flight;
        }
        invoke(std::move(delivery));
      }
    };
    settle_registry(streams);
    settle_registry(endpoints);

    IncomingDelivery incoming_delivery;
    {
      std::lock_guard lock(mutex);
      if (incoming_callback) {
        incoming_delivery.callback = std::move(incoming_callback);
        incoming_delivery.error = error;
        ++callbacks_in_flight;
      }
    }
    invoke(std::move(incoming_delivery));
    condition.notify_all();
  }

  void fail_driver(int error) noexcept {
    {
      std::lock_guard lock(mutex);
      if (driver_error == 0) {
        driver_error = error == 0 ? -EIO : error;
      }
      terminal_settlement_needed = true;
    }
    condition.notify_all();
    wake_driver();
  }

  [[nodiscard]] Result<std::uint64_t> request_close(bool* joined = nullptr) {
    std::uint64_t submission_generation = 0;
    {
      std::unique_lock lock(mutex);
      if (joined != nullptr) {
        *joined = false;
      }
      if (lifecycle == Lifecycle::Released || lifecycle == Lifecycle::Releasing) {
        return Error::runtime(-ESHUTDOWN, "RPC runtime is released");
      }
      if (lifecycle == Lifecycle::Stopped) {
        return shutdown_completion.consumed
                   ? Result<std::uint64_t>{Error::runtime(
                         -EALREADY, "RPC shutdown completion was already consumed")}
                   : Result<std::uint64_t>{shutdown_completion.operation_id};
      }
      if (lifecycle == Lifecycle::CloseAdmitted) {
        return shutdown_completion.operation_id;
      }
      if (lifecycle == Lifecycle::CloseSubmitting) {
        if (blocking_wait_forbidden_locked()) {
          return Error::runtime(-EDEADLK,
                                "cannot wait for RPC close submission on the event driver");
        }
        submission_generation = close_submission_generation;
        ++close_submission_waiters;
        if (joined != nullptr) {
          *joined = true;
        }
        condition.wait(
            lock, [&] { return completed_close_submission_generation >= submission_generation; });
        const int status = close_submission_status;
        --close_submission_waiters;
        condition.notify_all();
        if (status == 0 || lifecycle == Lifecycle::Stopped ||
            lifecycle == Lifecycle::CloseAdmitted) {
          return shutdown_completion.operation_id;
        }
        return Error::runtime(status);
      }
      if (close_submission_waiters != 0) {
        if (blocking_wait_forbidden_locked()) {
          return Error::runtime(-EDEADLK,
                                "cannot wait for an RPC close submitter on the event driver");
        }
        condition.wait(lock, [&] { return close_submission_waiters == 0; });
      }
      if (lifecycle != Lifecycle::Open) {
        lock.unlock();
        return request_close(joined);
      }
      lifecycle = Lifecycle::CloseSubmitting;
      submission_generation = ++close_submission_generation;
    }

    const int error = trevrpc_rpc_runtime_close(runtime, shutdown_completion.operation_id);
    bool stopped = false;
    bool completion_consumed = false;
    {
      std::lock_guard lock(mutex);
      if (lifecycle == Lifecycle::CloseSubmitting) {
        lifecycle = error == 0 ? Lifecycle::CloseAdmitted : Lifecycle::Open;
      }
      close_submission_status = error;
      completed_close_submission_generation = submission_generation;
      stopped = lifecycle == Lifecycle::Stopped;
      completion_consumed = shutdown_completion.consumed;
    }
    condition.notify_all();
    if (stopped && completion_consumed) {
      return Error::runtime(-EALREADY, "RPC shutdown completion was already consumed");
    }
    if (error == 0 || stopped) {
      return shutdown_completion.operation_id;
    }
    return Error::runtime(error);
  }

  [[nodiscard]] std::shared_ptr<StrictAttempt> begin_strict(bool require_stopped,
                                                            int& immediate_error) noexcept {
    std::lock_guard lock(mutex);
    immediate_error = 0;
    if (lifecycle == Lifecycle::Released) {
      return {};
    }
    if (require_stopped && lifecycle != Lifecycle::Stopped) {
      immediate_error = -EAGAIN;
      return {};
    }
    if (strict_attempt && !strict_attempt->completed) {
      ++shutdown_waiters;
      return strict_attempt;
    }
    try {
      strict_attempt = std::make_shared<StrictAttempt>();
    } catch (...) {
      immediate_error = -ENOMEM;
      return {};
    }
    strict_attempt->generation = ++next_strict_generation;
    return strict_attempt;
  }

  void cancel_strict(const std::shared_ptr<StrictAttempt>& attempt, int status) noexcept {
    std::lock_guard lock(mutex);
    if (strict_attempt == attempt && attempt && !attempt->completed) {
      attempt->status = status;
      attempt->completed = true;
    }
    condition.notify_all();
  }

  [[nodiscard]] Result<void> wait_strict(const std::shared_ptr<StrictAttempt>& attempt) {
    if (!attempt) {
      return {};
    }
    std::unique_lock lock(mutex);
    condition.wait(lock, [&] { return attempt->completed; });
    if (block_next_completed_strict_waiter) {
      block_next_completed_strict_waiter = false;
      completed_strict_waiter_blocked = true;
      condition.notify_all();
      condition.wait(lock, [&] { return release_completed_strict_waiter; });
      release_completed_strict_waiter = false;
      completed_strict_waiter_blocked = false;
    }
    const int status = attempt->status;
    if (shutdown_waiters != 0) {
      --shutdown_waiters;
    }
    return status == 0 ? Result<void>{}
                       : Result<void>{Error::runtime(status, "RPC runtime shutdown failed")};
  }

  void complete_strict(int status) noexcept {
    std::lock_guard lock(mutex);
    complete_strict_unlocked(status);
  }

  void abandon() noexcept {
    {
      std::lock_guard lock(mutex);
      if (!ownership_transferred || lifecycle == Lifecycle::Released) {
        return;
      }
      abandon_requested = true;
    }
    wake_driver();
  }

  [[nodiscard]] Result<void> schedule_at(std::chrono::steady_clock::time_point deadline,
                                         std::function<void()> callback) {
    if (!callback) {
      return Error::runtime(-EINVAL, "RPC timer callback must not be empty");
    }
    {
      std::lock_guard lock(mutex);
      if (driver_error != 0) {
        return Error::runtime(driver_error, "RPC event driver failed");
      }
      if (lifecycle != Lifecycle::Open) {
        return Error::runtime(-ESHUTDOWN, "RPC runtime is stopping");
      }
      try {
        timers.emplace(deadline, std::move(callback));
      } catch (...) {
        return Error::runtime(-ENOMEM, "failed to schedule RPC timer callback");
      }
    }
    wake_driver();
    return {};
  }

  [[nodiscard]] bool process_timer_callbacks() noexcept {
    std::function<void()> callback;
    {
      std::lock_guard lock(mutex);
      if (timers.empty() || timers.begin()->first > std::chrono::steady_clock::now()) {
        return false;
      }
      callback = std::move(timers.begin()->second);
      timers.erase(timers.begin());
    }
    CallbackScope scope(this);
    try {
      callback();
    } catch (...) {
      // Runtime timer callbacks are scheduling stubs. Keep failures contained.
      (void)std::current_exception();
    }
    return true;
  }

  void schedule_cleanup(const std::shared_ptr<RpcCleanupWork>& work) noexcept {
    if (!work) {
      return;
    }
    bool retain = false;
    {
      std::lock_guard lock(mutex);
      if (!ownership_transferred || lifecycle == Lifecycle::Stopped ||
          lifecycle == Lifecycle::Releasing || lifecycle == Lifecycle::Released ||
          abandon_requested) {
        retain = true;
      } else {
        try {
          if (RpcClientStream::test_consume_cleanup_task_allocation_failure()) {
            throw std::bad_alloc();
          }
          cleanup_tasks.push_back(CleanupTask{work, std::chrono::steady_clock::now(), 0, true});
        } catch (...) {
          retain = true;
        }
      }
    }
    if (retain) {
      abandon_cleanup_owner(work);
      return;
    }
    wake_driver();
  }

  void drain_cleanup() noexcept {
    for (;;) {
      std::shared_ptr<RpcCleanupWork> failed;
      {
        std::lock_guard lock(mutex);
        if (cleanup_retained.empty()) {
          break;
        }
        const auto now = std::chrono::steady_clock::now();
        auto owner = cleanup_retained.front().work;
        cleanup_retained.front().retryable = true;
        cleanup_retained.front().due = now;
        try {
          cleanup_tasks.push_back(std::move(cleanup_retained.front()));
          cleanup_retained.pop_front();
        } catch (...) {
          cleanup_retained.pop_front();
          failed = std::move(owner);
        }
      }
      if (failed) {
        abandon_cleanup_owner(failed);
      }
    }
    wake_driver();
  }

  [[nodiscard]] bool process_cleanup_tasks() noexcept {
    CleanupTask task;
    {
      std::lock_guard lock(mutex);
      const auto now = std::chrono::steady_clock::now();
      auto found = std::find_if(cleanup_tasks.begin(), cleanup_tasks.end(),
                                [&](const CleanupTask& candidate) { return candidate.due <= now; });
      if (found != cleanup_tasks.end()) {
        task = std::move(*found);
        cleanup_tasks.erase(found);
      } else {
        auto retained = std::find_if(cleanup_retained.begin(), cleanup_retained.end(),
                                     [&](const CleanupTask& candidate) {
                                       return candidate.retryable && candidate.due <= now;
                                     });
        if (retained == cleanup_retained.end()) {
          return false;
        }
        task = std::move(*retained);
        cleanup_retained.erase(retained);
      }
    }
    if (!task.work) {
      return true;
    }
    int step_error = 0;
    try {
      auto result = task.work->cleanup_step();
      if (result) {
        return true;
      }
      step_error = result.error().code();
    } catch (...) {
      step_error = -ENOMEM;
    }
    ++task.attempts;
    if (retryable_cleanup_error(step_error) && task.attempts < 8) {
      task.due = std::chrono::steady_clock::now() +
                 std::chrono::milliseconds(std::min<unsigned int>(1U << task.attempts, 1000U));
      auto owner = task.work;
      try {
        if (RpcClientStream::test_consume_cleanup_requeue_allocation_failure()) {
          throw std::bad_alloc();
        }
        std::lock_guard lock(mutex);
        cleanup_tasks.push_back(std::move(task));
      } catch (...) {
        abandon_cleanup_owner(owner);
      }
      return true;
    }
    const int terminal_error = step_error;
    // A retryable cleanup failure is only retryable for the bounded attempt
    // window above. Once that window is exhausted, make it terminal just like
    // any other cleanup failure: publish the error and abandon the owner so
    // the runtime/stream cycle cannot keep strict shutdown alive forever.
    auto owner = std::move(task.work);
    {
      std::lock_guard lock(mutex);
      if (cleanup_error == 0) {
        cleanup_error = terminal_error == 0 ? -EIO : terminal_error;
      }
      terminal_status = cleanup_error;
      terminal_settlement_needed = true;
      // The stream owner must be abandoned, but an active ChannelCore still
      // needs the runtime alive long enough to close and release its endpoint.
      // Mark this separately from an explicit runtime abandonment so the
      // driver does not tear down the runtime underneath that endpoint.
      cleanup_abandon_requested = true;
      abandon_requested = true;
    }
    if (owner) {
      owner->interrupt_cleanup();
      owner->abandon_cleanup();
    }
    wake_driver();
    return true;
  }

  [[nodiscard]] int cleanup_timeout_ms_locked() const noexcept {
    if (cleanup_tasks.empty() && cleanup_retained.empty() && timers.empty()) {
      return -1;
    }
    const auto now = std::chrono::steady_clock::now();
    auto due = std::chrono::steady_clock::time_point::max();
    bool has_due = false;
    if (!timers.empty()) {
      due = timers.begin()->first;
      has_due = true;
    }
    for (const auto& task : cleanup_tasks) {
      due = std::min(due, task.due);
      has_due = true;
    }
    for (const auto& task : cleanup_retained) {
      if (task.retryable) {
        due = std::min(due, task.due);
        has_due = true;
      }
    }
    if (!has_due) {
      return -1;
    }
    if (due <= now) {
      return 0;
    }
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(due - now);
    return static_cast<int>(std::min<std::int64_t>(remaining.count() + 1, 1000));
  }

  void abandon_cleanup_tasks() noexcept {
    for (;;) {
      std::shared_ptr<RpcCleanupWork> owner;
      {
        std::lock_guard lock(mutex);
        if (!cleanup_tasks.empty()) {
          owner = std::move(cleanup_tasks.front().work);
          cleanup_tasks.pop_front();
        } else if (!cleanup_retained.empty()) {
          owner = std::move(cleanup_retained.front().work);
          cleanup_retained.pop_front();
        } else {
          return;
        }
      }
      if (owner) {
        owner->interrupt_cleanup();
        owner->abandon_cleanup();
      }
    }
  }

  [[nodiscard]] bool strict_busy_locked() const noexcept {
    const bool buffered_operation =
        std::any_of(operations.begin(), operations.end(),
                    [](const auto& entry) { return entry.second.event.has_value(); });
    return buffered_operation || !streams.registered.empty() || !endpoints.registered.empty() ||
           callbacks_in_flight != 0 || incoming_materializations != 0 ||
           native_calls_in_flight != 0;
  }

  [[nodiscard]] bool process_finalization() noexcept {
    bool abandon = false;
    bool strict = false;
    bool stopped = false;
    bool settle = false;
    int settlement_error = -ESHUTDOWN;
    {
      std::lock_guard lock(mutex);
      abandon = abandon_requested;
      strict = strict_attempt && !strict_attempt->completed;
      stopped = lifecycle == Lifecycle::Stopped;
      settle = terminal_settlement_needed;
      settlement_error =
          cleanup_error != 0 ? cleanup_error : (driver_error != 0 ? driver_error : -ESHUTDOWN);
    }

    // Terminal cleanup errors can be discovered while the runtime is still
    // serving other endpoint owners. Do not clear their registries or settle
    // their callbacks until the native runtime has actually stopped.
    if (settle && stopped) {
      settle_consumers(settlement_error, abandon);
    }
    if (!stopped || (!abandon && !strict)) {
      return false;
    }
    {
      std::unique_lock lock(mutex);
      if (pause_finalization) {
        finalization_paused = true;
        condition.notify_all();
        condition.wait(lock, [&] { return release_finalization || !pause_finalization; });
        finalization_paused = false;
        release_finalization = false;
      }
    }
    if (abandon) {
      // Cleanup owners retain their native handles until the runtime has
      // stopped; once abandonment is requested, preserve them without
      // attempting blocking release from the driver teardown path.
      abandon_cleanup_tasks();
    }
    {
      std::lock_guard lock(mutex);
      // Cleanup tasks own the stream handles that make strict shutdown appear
      // busy. Give their bounded retry window a chance to finish (or publish
      // a terminal cleanup error) before treating the still-registered stream
      // as an application-owned EBUSY condition.
      if (!abandon && (!cleanup_tasks.empty() || !cleanup_retained.empty())) {
        return false;
      }
      if (!abandon && strict_busy_locked()) {
        complete_strict_unlocked(-EBUSY);
        return false;
      }
      if (callbacks_in_flight != 0 || incoming_materializations != 0 ||
          native_calls_in_flight != 0) {
        return false;
      }
      lifecycle = Lifecycle::Releasing;
    }

    const int drain_error = drain_events();
    if (drain_error != -EAGAIN) {
      std::lock_guard lock(mutex);
      lifecycle = Lifecycle::Stopped;
      if (driver_error == 0) {
        driver_error = drain_error == 0 ? -EIO : drain_error;
      }
      terminal_status = driver_error;
      terminal_settlement_needed = true;
      abandon_requested = true;
      return false;
    }
    settle_consumers(settlement_error, abandon);
    int error = trevrpc_rpc_runtime_drain(runtime);
    if (error == 0) {
      error = trevrpc_rpc_runtime_release(runtime);
    }
    int completion_status = 0;
    {
      std::lock_guard lock(mutex);
      if (error == 0) {
        runtime = nullptr;
        lifecycle = Lifecycle::Released;
        completion_status = cleanup_error != 0 ? cleanup_error : terminal_status;
        if (strict_attempt && !strict_attempt->completed) {
          complete_strict_unlocked(completion_status);
        }
      } else {
        lifecycle = Lifecycle::Stopped;
        if (!abandon) {
          complete_strict_unlocked(error);
        }
      }
    }
    condition.notify_all();
    return error == 0;
  }

  void complete_strict_unlocked(int status) noexcept {
    if (!strict_attempt || strict_attempt->completed) {
      return;
    }
    strict_attempt->status = status;
    strict_attempt->completed = true;
    condition.notify_all();
  }

  void driver_loop() noexcept {
    {
      std::unique_lock lock(mutex);
      driver_thread_id = std::this_thread::get_id();
      condition.wait(lock, [&] { return driver_start || driver_cancel_start; });
      if (driver_cancel_start) {
        driver_exited = true;
        driver_thread_id = {};
        condition.notify_all();
        return;
      }
    }

    bool retry_abandon_close = false;
    bool retry_failure_close = false;
    for (;;) {
      {
        std::unique_lock lock(mutex);
        if (pause_driver) {
          driver_paused = true;
          condition.notify_all();
          condition.wait(lock, [&] { return !pause_driver; });
          driver_paused = false;
        }
      }
      while (service_incoming()) {
      }

      const int drain_error = drain_events();
      if (drain_error != -EAGAIN) {
        fail_driver(drain_error);
      }

      prepare_strict_close();
      while (process_timer_callbacks()) {
      }
      (void)process_cleanup_tasks();

      bool need_abandon_close = false;
      bool need_failure_close = false;
      {
        std::lock_guard lock(mutex);
        // A cleanup-terminal abandonment breaks the stream ownership cycle,
        // but must not close the shared runtime: ChannelCore still has to
        // close/release its endpoint and will then perform runtime shutdown.
        // Only an explicit runtime abandonment may initiate an automatic
        // close while the runtime is still open.
        need_abandon_close = abandon_requested && lifecycle == Lifecycle::Open &&
                             !cleanup_abandon_requested;
        need_failure_close = !need_abandon_close && driver_error != 0 &&
                             lifecycle == Lifecycle::Open &&
                             (!strict_attempt || strict_attempt->completed);
      }
      if (need_abandon_close) {
        discard_queued_incoming(true);
        settle_consumers(-ESHUTDOWN, true);
        auto close = request_close();
        retry_abandon_close = !close;
        retry_failure_close = false;
      } else if (need_failure_close) {
        auto close = request_close();
        {
          std::lock_guard lock(mutex);
          automatic_close_error = close ? 0 : close.error().code();
        }
        retry_failure_close = !close;
        retry_abandon_close = false;
      } else {
        retry_abandon_close = false;
        retry_failure_close = false;
      }

      if (process_finalization()) {
        break;
      }

#ifndef _WIN32
      {
        std::unique_lock lock(mutex);
        if (control_write < 0 && lifecycle == Lifecycle::Stopped && !abandon_requested &&
            (!strict_attempt || strict_attempt->completed)) {
          condition.wait(lock);
          continue;
        }
      }
      pollfd descriptors[2] = {
          {static_cast<int>(wake.native_handle), POLLIN, 0},
          {control_read, POLLIN, 0},
      };
      int cleanup_timeout = -1;
      {
        std::lock_guard lock(mutex);
        cleanup_timeout = cleanup_timeout_ms_locked();
      }
      int poll_error;
      do {
        const int retry_timeout = retry_abandon_close || retry_failure_close ? 10 : -1;
        const int timeout =
            retry_timeout < 0
                ? cleanup_timeout
                : (cleanup_timeout < 0 ? retry_timeout : std::min(retry_timeout, cleanup_timeout));
        poll_error = ::poll(descriptors, 2, timeout);
      } while (poll_error < 0 && errno == EINTR);
      if (poll_error < 0) {
        fail_driver(-errno);
      }
      if (poll_error > 0 && (descriptors[1].revents & POLLIN) != 0) {
        std::uint8_t bytes[64];
        while (::read(control_read, bytes, sizeof(bytes)) > 0) {
        }
      }
#endif
    }

    {
      std::lock_guard lock(mutex);
      driver_exited = true;
      driver_thread_id = {};
#ifndef _WIN32
      if (control_read >= 0) {
        ::close(control_read);
        control_read = -1;
      }
      if (control_write >= 0) {
        ::close(control_write);
        control_write = -1;
      }
#endif
    }
    condition.notify_all();
  }

  trevrpc_rpc_runtime* runtime = nullptr;
  trevrpc_rpc_wake_source_v1 wake{};
  int control_read = -1;
  int control_write = -1;

  mutable std::mutex mutex;
  std::condition_variable condition;
  std::unordered_map<std::uint64_t, OperationSlot> operations;
  std::multimap<std::chrono::steady_clock::time_point, std::function<void()>> timers;
  std::unordered_map<HandleKey, std::uint64_t, HandleKeyHash> deferred_endpoint_close_operations;
  ShutdownCompletionSlot shutdown_completion;
  SubjectRegistry streams;
  SubjectRegistry endpoints;
  std::vector<trevrpc_rpc_event*> incoming_events;
  std::size_t incoming_head = 0;
  std::size_t incoming_tail = 0;
  std::size_t incoming_count = 0;
  trevrpc_rpc_event* incoming_handoff = nullptr;
  IncomingCallback incoming_callback;
  bool incoming_waiter = false;
  bool incoming_stopped = false;
  std::size_t incoming_materializations = 0;

  std::uint64_t next_operation_id = 2;
  std::uint64_t next_rejected_operation_id = UINT64_MAX;
  std::size_t endpoint_limit = 1;
  std::size_t stream_limit = 1;
  std::size_t pending_stream_limit = 1;
  std::size_t pending_endpoint_limit = 1;
  std::size_t incoming_limit = 1;
  std::size_t buffered_event_limit = 1;
  std::size_t buffered_event_count = 0;

  Lifecycle lifecycle = Lifecycle::Open;
  int driver_error = 0;
  int cleanup_error = 0;
  int terminal_status = 0;
  bool terminal_settlement_needed = false;
  bool abandon_requested = false;
  bool cleanup_abandon_requested = false;
  std::shared_ptr<StrictAttempt> strict_attempt;
  std::uint64_t next_strict_generation = 0;
  std::uint64_t strict_preclose_generation = 0;
  std::size_t shutdown_waiters = 0;
  std::size_t callbacks_in_flight = 0;
  std::size_t native_calls_in_flight = 0;
  std::size_t rejected_incoming_count = 0;
  int injected_operation_reservation_error = 0;
  int injected_endpoint_registration_error = 0;
  int injected_stream_registration_error = 0;
  int injected_control_write_error = 0;
  int automatic_close_error = 0;
  std::uint64_t close_submission_generation = 0;
  std::uint64_t completed_close_submission_generation = 0;
  std::size_t close_submission_waiters = 0;
  int close_submission_status = 0;
  std::deque<CleanupTask> cleanup_tasks;
  std::deque<CleanupTask> cleanup_retained;
  bool block_next_completed_strict_waiter = false;
  bool completed_strict_waiter_blocked = false;
  bool release_completed_strict_waiter = false;
  bool pause_driver = false;
  bool driver_paused = false;
  bool pause_finalization = false;
  bool finalization_paused = false;
  bool release_finalization = false;

  bool ownership_transferred = false;
  bool driver_start = false;
  bool driver_cancel_start = false;
  bool driver_exited = false;
  std::thread::id driver_thread_id{};

  static thread_local CallbackScope* active_callback;
};

thread_local RpcEventRuntime::SharedState::CallbackScope*
    RpcEventRuntime::SharedState::active_callback = nullptr;

RpcEventRuntime::NativeRuntimeLease::~NativeRuntimeLease() { reset(); }

RpcEventRuntime::NativeRuntimeLease::NativeRuntimeLease(NativeRuntimeLease&& other) noexcept
    : state_(std::move(other.state_)), runtime_(std::exchange(other.runtime_, nullptr)) {}

RpcEventRuntime::NativeRuntimeLease&
RpcEventRuntime::NativeRuntimeLease::operator=(NativeRuntimeLease&& other) noexcept {
  if (this != &other) {
    reset();
    state_ = std::move(other.state_);
    runtime_ = std::exchange(other.runtime_, nullptr);
  }
  return *this;
}

void RpcEventRuntime::NativeRuntimeLease::reset() noexcept {
  if (runtime_ == nullptr) {
    state_.reset();
    return;
  }
  runtime_ = nullptr;
  auto state = std::move(state_);
  state->release_native_call();
}

Result<void> RpcEventRuntime::reserve_unadopted_cleanup() {
  try {
    return unadopted_runtime_reaper().reserve();
  } catch (...) {
    return Error::runtime(-EAGAIN, "failed to start the unadopted RPC runtime reaper");
  }
}

void RpcEventRuntime::cancel_unadopted_cleanup() noexcept { unadopted_runtime_reaper().cancel(); }

void RpcEventRuntime::reap_unadopted(trevrpc_rpc_runtime* runtime) noexcept {
  unadopted_runtime_reaper().submit(runtime);
}

Result<std::shared_ptr<RpcEventRuntime>>
RpcEventRuntime::adopt(trevrpc_rpc_runtime* runtime, const trevrpc_rpc_runtime_config_v1& config) {
  if (runtime == nullptr) {
    return Error::runtime(-EINVAL, "RPC runtime must not be null");
  }
  if (config.struct_version != TREVRPC_RPC_ABI_VERSION ||
      config.struct_size < sizeof(trevrpc_rpc_runtime_config_v1)) {
    return Error::runtime(-EINVAL, "RPC runtime configuration is not ABI 1");
  }

  std::shared_ptr<SharedState> state;
  try {
    state = std::make_shared<SharedState>(runtime, config);
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to allocate C++ RPC event runtime");
  }
  auto initialized = state->initialize();
  if (!initialized) {
    return initialized.error();
  }

  std::shared_ptr<RpcEventRuntime> wrapper;
  try {
    wrapper = std::shared_ptr<RpcEventRuntime>(new RpcEventRuntime(state));
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to allocate C++ RPC event runtime wrapper");
  }

  const int injected_error = injected_adopt_start_error.exchange(0, std::memory_order_acq_rel);
  if (injected_error != 0) {
    return Error::runtime(injected_error, "failed to start C++ RPC wake-drain thread");
  }

  std::thread driver;
  try {
    driver = std::thread([state] { state->driver_loop(); });
    driver.detach();
  } catch (...) {
    {
      std::lock_guard lock(state->mutex);
      state->driver_cancel_start = true;
    }
    state->condition.notify_all();
    if (driver.joinable()) {
      driver.join();
    }
    return Error::runtime(-EAGAIN, "failed to start C++ RPC wake-drain thread");
  }

  {
    std::lock_guard lock(state->mutex);
    state->ownership_transferred = true;
    state->driver_start = true;
  }
  state->condition.notify_all();
  return wrapper;
}

RpcEventRuntime::~RpcEventRuntime() {
  auto state = std::move(state_);
  if (state) {
    state->abandon();
  }
}

RpcEventRuntime::NativeRuntimeLease RpcEventRuntime::native_handle() const noexcept {
  auto state = state_;
  if (!state) {
    return {};
  }
  trevrpc_rpc_runtime* runtime = state->acquire_native_call();
  return runtime == nullptr ? NativeRuntimeLease{} : NativeRuntimeLease(std::move(state), runtime);
}

bool RpcEventRuntime::stopped() const noexcept {
  auto state = state_;
  if (!state) {
    return true;
  }
  std::lock_guard lock(state->mutex);
  return state->lifecycle == SharedState::Lifecycle::Stopped ||
         state->lifecycle == SharedState::Lifecycle::Releasing ||
         state->lifecycle == SharedState::Lifecycle::Released;
}

bool RpcEventRuntime::callback_active() const noexcept {
  auto state = state_;
  return state != nullptr && state->callback_active();
}

bool RpcEventRuntime::blocking_wait_forbidden() const noexcept {
  auto state = state_;
  if (!state) {
    return false;
  }
  std::lock_guard lock(state->mutex);
  return state->blocking_wait_forbidden_locked();
}

Result<void> RpcEventRuntime::schedule_at(std::chrono::steady_clock::time_point deadline,
                                          std::function<void()> callback) {
  auto state = state_;
  return state ? state->schedule_at(deadline, std::move(callback))
               : Result<void>{Error::runtime(-ESHUTDOWN, "RPC runtime is released")};
}

void RpcEventRuntime::schedule_cleanup(const std::shared_ptr<RpcCleanupWork>& work) noexcept {
  auto state = state_;
  if (state) {
    state->schedule_cleanup(work);
  } else if (work) {
    work->interrupt_cleanup();
    work->abandon_cleanup();
  }
}

void RpcEventRuntime::drain_cleanup() noexcept {
  auto state = state_;
  if (state) {
    state->drain_cleanup();
  }
}

Result<std::uint64_t> RpcEventRuntime::reserve_operation() {
  auto state = state_;
  return state ? state->reserve_operation()
               : Result<std::uint64_t>{Error::runtime(-ESHUTDOWN, "RPC runtime is released")};
}

void RpcEventRuntime::reject_operation(std::uint64_t operation_id) noexcept {
  auto state = state_;
  if (state) {
    state->reject_operation(operation_id);
  }
}

Result<RpcEvent> RpcEventRuntime::wait_operation(std::uint64_t operation_id) {
  auto state = state_;
  return state ? state->wait_operation(operation_id)
               : Result<RpcEvent>{Error::runtime(-ESHUTDOWN, "RPC runtime is released")};
}

Result<std::optional<RpcEvent>> RpcEventRuntime::try_wait_operation(std::uint64_t operation_id) {
  auto state = state_;
  return state ? state->try_wait_operation(operation_id)
               : Result<std::optional<RpcEvent>>{
                     Error::runtime(-ESHUTDOWN, "RPC runtime is released")};
}

Result<void> RpcEventRuntime::subscribe_operation(std::uint64_t operation_id,
                                                  EventCallback callback) {
  auto state = state_;
  return state ? state->subscribe_operation(operation_id, std::move(callback))
               : Result<void>{Error::runtime(-ESHUTDOWN, "RPC runtime is released")};
}

Result<void> RpcEventRuntime::register_stream(trevrpc_rpc_stream_v1 stream) {
  if (stream.owner == 0) {
    return Error::runtime(-EINVAL, "RPC stream handle must not be null");
  }
  auto state = state_;
  if (!state) {
    return Error::runtime(-ESHUTDOWN, "RPC runtime is released");
  }
  {
    std::lock_guard lock(state->mutex);
    const int injected_error = std::exchange(state->injected_stream_registration_error, 0);
    if (injected_error != 0) {
      return Error::runtime(injected_error, "injected RPC stream registration failure");
    }
  }
  return state->register_subject(state->streams, SharedState::key(stream));
}

void RpcEventRuntime::unregister_stream(trevrpc_rpc_stream_v1 stream) noexcept {
  auto state = state_;
  if (state) {
    state->unregister_subject(state->streams, SharedState::key(stream));
  }
}

Result<void> RpcEventRuntime::settle_unregistered_stream(trevrpc_rpc_stream_v1 stream) {
  auto state = state_;
  return state ? state->settle_unregistered_stream(SharedState::key(stream))
               : Result<void>{Error::runtime(-ESHUTDOWN, "RPC runtime is released")};
}

Result<bool> RpcEventRuntime::try_settle_unregistered_stream(trevrpc_rpc_stream_v1 stream) {
  auto state = state_;
  return state ? state->try_settle_unregistered_stream(SharedState::key(stream))
               : Result<bool>{Error::runtime(-ESHUTDOWN, "RPC runtime is released")};
}

Result<RpcEvent> RpcEventRuntime::wait_stream(trevrpc_rpc_stream_v1 stream) {
  auto state = state_;
  return state ? state->wait_subject(state->streams, SharedState::key(stream))
               : Result<RpcEvent>{Error::runtime(-ESHUTDOWN, "RPC runtime is released")};
}

Result<std::optional<RpcEvent>> RpcEventRuntime::try_wait_stream(trevrpc_rpc_stream_v1 stream) {
  auto state = state_;
  return state ? state->try_wait_subject(state->streams, SharedState::key(stream))
               : Result<std::optional<RpcEvent>>{
                     Error::runtime(-ESHUTDOWN, "RPC runtime is released")};
}

Result<void> RpcEventRuntime::subscribe_stream(trevrpc_rpc_stream_v1 stream,
                                               EventCallback callback) {
  auto state = state_;
  return state ? state->subscribe_subject(state->streams, SharedState::key(stream),
                                          std::move(callback))
               : Result<void>{Error::runtime(-ESHUTDOWN, "RPC runtime is released")};
}

Result<void> RpcEventRuntime::register_endpoint(trevrpc_rpc_endpoint_v1 endpoint) {
  if (endpoint.owner == 0) {
    return Error::runtime(-EINVAL, "RPC endpoint handle must not be null");
  }
  auto state = state_;
  if (!state) {
    return Error::runtime(-ESHUTDOWN, "RPC runtime is released");
  }
  {
    std::lock_guard lock(state->mutex);
    const int injected_error = std::exchange(state->injected_endpoint_registration_error, 0);
    if (injected_error != 0) {
      return Error::runtime(injected_error, "injected RPC endpoint registration failure");
    }
  }
  return state->register_subject(state->endpoints, SharedState::key(endpoint));
}

void RpcEventRuntime::unregister_endpoint(trevrpc_rpc_endpoint_v1 endpoint) noexcept {
  auto state = state_;
  if (state) {
    state->unregister_subject(state->endpoints, SharedState::key(endpoint));
  }
}

Result<void> RpcEventRuntime::settle_unregistered_endpoint(trevrpc_rpc_endpoint_v1 endpoint) {
  auto state = state_;
  if (!state) {
    return Error::runtime(-ESHUTDOWN, "RPC runtime is released");
  }
  const auto endpoint_key = SharedState::key(endpoint);
  auto probe = state->probe_unregistered_endpoint(endpoint_key);
  if (!probe) {
    return probe.error();
  }
  if (probe.value()) {
    std::uint64_t deferred_operation = 0;
    {
      std::lock_guard lock(state->mutex);
      if (const auto found = state->deferred_endpoint_close_operations.find(endpoint_key);
          found != state->deferred_endpoint_close_operations.end()) {
        deferred_operation = found->second;
        state->deferred_endpoint_close_operations.erase(found);
      }
    }
    if (deferred_operation != 0) {
      state->reject_operation(deferred_operation);
    }
    return state->settle_unregistered_endpoint(endpoint_key);
  }

  std::uint64_t operation_id = 0;
  bool newly_reserved = false;
  {
    std::lock_guard lock(state->mutex);
    if (const auto found = state->deferred_endpoint_close_operations.find(endpoint_key);
        found != state->deferred_endpoint_close_operations.end()) {
      operation_id = found->second;
    }
  }
  if (operation_id == 0) {
    auto operation = reserve_operation();
    if (!operation) {
      return operation.error();
    }
    operation_id = operation.value();
    newly_reserved = true;
    try {
      std::lock_guard lock(state->mutex);
      const auto [found, inserted] =
          state->deferred_endpoint_close_operations.emplace(endpoint_key, operation_id);
      if (!inserted) {
        operation_id = found->second;
        newly_reserved = false;
      }
    } catch (...) {
      reject_operation(operation_id);
      return Error::runtime(-ENOMEM, "failed to retain deferred RPC endpoint close");
    }
    if (!newly_reserved) {
      // Another cleanup caller won the reservation while this one was allocating.
      state->reject_operation(operation.value());
    }
  }

  const int close_error = trevrpc_rpc_endpoint_close(native_handle(), endpoint, operation_id);
  if (close_error == -EDEADLK) {
    return Error::runtime(close_error, "deferred RPC endpoint close cannot run here");
  }
  {
    std::lock_guard lock(state->mutex);
    state->deferred_endpoint_close_operations.erase(endpoint_key);
  }
  state->reject_operation(operation_id);
  if (close_error != 0 && close_error != -EALREADY) {
    return Error::runtime(close_error, "failed to close unregistered RPC endpoint");
  }
  return state->settle_unregistered_endpoint(endpoint_key);
}

Result<RpcEvent> RpcEventRuntime::wait_endpoint(trevrpc_rpc_endpoint_v1 endpoint) {
  auto state = state_;
  return state ? state->wait_subject(state->endpoints, SharedState::key(endpoint))
               : Result<RpcEvent>{Error::runtime(-ESHUTDOWN, "RPC runtime is released")};
}

Result<void> RpcEventRuntime::subscribe_endpoint(trevrpc_rpc_endpoint_v1 endpoint,
                                                 EventCallback callback) {
  auto state = state_;
  return state ? state->subscribe_subject(state->endpoints, SharedState::key(endpoint),
                                          std::move(callback))
               : Result<void>{Error::runtime(-ESHUTDOWN, "RPC runtime is released")};
}

Result<RpcIncomingCall> RpcEventRuntime::wait_incoming() {
  auto state = state_;
  return state ? state->wait_incoming()
               : Result<RpcIncomingCall>{Error::runtime(-ESHUTDOWN, "RPC runtime is released")};
}

void RpcEventRuntime::stop_incoming() noexcept {
  auto state = state_;
  if (state) {
    state->stop_incoming();
  }
}

Result<void> RpcEventRuntime::reject_incoming(const RpcIncomingCall& incoming) {
  auto state = state_;
  if (!state) {
    return Error::runtime(-ESHUTDOWN, "RPC runtime is released");
  }
  auto rejected = state->reject_incoming(incoming);
  if (!rejected) {
    // Once ownership transfers from the incoming event, a failed rejection
    // cannot leave the call orphaned. Runtime abandonment is the bounded
    // fallback when close, settlement, or native release cannot complete.
    state->abandon();
  }
  return rejected;
}

Result<void> RpcEventRuntime::subscribe_incoming_impl(IncomingCallback callback) {
  auto state = state_;
  return state ? state->subscribe_incoming(std::move(callback))
               : Result<void>{Error::runtime(-ESHUTDOWN, "RPC runtime is released")};
}

Result<std::uint64_t> RpcEventRuntime::request_close() {
  auto state = state_;
  return state ? state->request_close()
               : Result<std::uint64_t>{Error::runtime(-ESHUTDOWN, "RPC runtime is released")};
}

void RpcEventRuntime::request_abandon() noexcept {
  auto state = state_;
  if (state) {
    state->abandon();
  }
}

Result<void> RpcEventRuntime::drain_and_release() {
  auto state = state_;
  if (!state) {
    return {};
  }
  if (state->callback_active()) {
    return Error::runtime(-EDEADLK, "cannot release RPC runtime from one of its callbacks");
  }
  {
    std::lock_guard lock(state->mutex);
    if (state->driver_thread_id == std::this_thread::get_id()) {
      return Error::runtime(-EDEADLK, "cannot release RPC runtime on its event-driver thread");
    }
  }
  int immediate_error = 0;
  auto attempt = state->begin_strict(true, immediate_error);
  if (immediate_error != 0) {
    return Error::runtime(immediate_error, "RPC runtime has not stopped");
  }
  state->wake_driver();
  return state->wait_strict(attempt);
}

Result<void> RpcEventRuntime::shutdown() {
  auto state = state_;
  if (!state) {
    return {};
  }
  if (state->callback_active()) {
    return Error::runtime(-EDEADLK, "cannot synchronously shut down from an RPC callback");
  }
  {
    std::lock_guard lock(state->mutex);
    if (state->driver_thread_id == std::this_thread::get_id()) {
      return Error::runtime(-EDEADLK, "cannot synchronously shut down from the event driver");
    }
    if (state->lifecycle == SharedState::Lifecycle::Released) {
      return {};
    }
  }

  int immediate_error = 0;
  auto attempt = state->begin_strict(false, immediate_error);
  if (immediate_error != 0) {
    return Error::runtime(immediate_error, "failed to start RPC runtime shutdown");
  }
  if (!attempt) {
    return {};
  }
  state->wake_driver();
  bool coordinate_close = false;
  {
    std::lock_guard lock(state->mutex);
    coordinate_close = state->strict_attempt == attempt && !attempt->completed &&
                       !attempt->close_submission_claimed;
    if (coordinate_close) {
      attempt->close_submission_claimed = true;
    }
  }
  while (coordinate_close) {
    bool submit_or_join = false;
    {
      std::unique_lock lock(state->mutex);
      state->condition.wait(lock, [&] {
        return attempt->completed || state->lifecycle != SharedState::Lifecycle::Open ||
               state->strict_preclose_generation >= attempt->generation;
      });
      if (attempt->completed || state->lifecycle == SharedState::Lifecycle::Stopped ||
          state->lifecycle == SharedState::Lifecycle::CloseAdmitted ||
          state->lifecycle == SharedState::Lifecycle::Releasing ||
          state->lifecycle == SharedState::Lifecycle::Released) {
        break;
      }
      submit_or_join = state->lifecycle == SharedState::Lifecycle::CloseSubmitting ||
                       (state->lifecycle == SharedState::Lifecycle::Open &&
                        state->strict_preclose_generation >= attempt->generation);
    }
    if (!submit_or_join) {
      continue;
    }
    bool joined = false;
    auto requested = state->request_close(&joined);
    if (requested) {
      break;
    }
    if (!joined) {
      state->cancel_strict(attempt, requested.error().code());
      break;
    }
    state->wake_driver();
  }
  state->wake_driver();
  return state->wait_strict(attempt);
}

void RpcEventRuntime::test_wait_for_operation_waiters(std::size_t count) {
  auto state = state_;
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock, [&] {
    const std::size_t operation_waiters = static_cast<std::size_t>(
        std::count_if(state->operations.begin(), state->operations.end(),
                      [](const auto& entry) { return entry.second.blocking_waiter; }));
    return operation_waiters + (state->shutdown_completion.blocking_waiter ? 1 : 0) >= count;
  });
}

void RpcEventRuntime::test_wait_for_stream_waiters(std::size_t count) {
  auto state = state_;
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock, [&] {
    return static_cast<std::size_t>(std::count_if(
               state->streams.registered.begin(), state->streams.registered.end(),
               [](const auto& entry) { return entry.second.blocking_waiter; })) >= count;
  });
}

void RpcEventRuntime::test_wait_for_pending_stream_events(trevrpc_rpc_stream_v1 stream,
                                                          std::size_t count) {
  auto state = state_;
  const auto stream_key = SharedState::key(stream);
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock, [&] {
    const auto found = state->streams.pending.find(stream_key);
    return found != state->streams.pending.end() && found->second.events.size() >= count;
  });
}

void RpcEventRuntime::test_wait_for_endpoint_waiters(std::size_t count) {
  auto state = state_;
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock, [&] {
    return static_cast<std::size_t>(std::count_if(
               state->endpoints.registered.begin(), state->endpoints.registered.end(),
               [](const auto& entry) { return entry.second.blocking_waiter; })) >= count;
  });
}

void RpcEventRuntime::test_wait_for_shutdown_waiters(std::size_t count) {
  auto state = state_;
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock, [&] { return state->shutdown_waiters >= count; });
}

bool RpcEventRuntime::test_wait_for_close_submission_waiters(std::size_t count,
                                                             std::chrono::milliseconds timeout) {
  auto state = state_;
  std::unique_lock lock(state->mutex);
  return state->condition.wait_for(lock, timeout,
                                   [&] { return state->close_submission_waiters >= count; });
}

void RpcEventRuntime::test_block_next_completed_strict_waiter() {
  auto state = state_;
  std::lock_guard lock(state->mutex);
  state->block_next_completed_strict_waiter = true;
}

void RpcEventRuntime::test_wait_for_blocked_strict_waiter() {
  auto state = state_;
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock, [&] { return state->completed_strict_waiter_blocked; });
}

void RpcEventRuntime::test_release_blocked_strict_waiter() {
  auto state = state_;
  {
    std::lock_guard lock(state->mutex);
    state->release_completed_strict_waiter = true;
  }
  state->condition.notify_all();
}

void RpcEventRuntime::test_wait_for_stopped() {
  auto state = state_;
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock, [&] {
    return state->lifecycle == SharedState::Lifecycle::Stopped ||
           state->lifecycle == SharedState::Lifecycle::Releasing ||
           state->lifecycle == SharedState::Lifecycle::Released;
  });
}

void RpcEventRuntime::test_pause_finalization() {
  auto state = state_;
  {
    std::lock_guard lock(state->mutex);
    state->pause_finalization = true;
  }
  state->wake_driver();
}

void RpcEventRuntime::test_wait_for_finalization_pause() {
  auto state = state_;
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock, [&] { return state->finalization_paused; });
}

void RpcEventRuntime::test_release_finalization() {
  auto state = state_;
  {
    std::lock_guard lock(state->mutex);
    state->release_finalization = true;
    state->pause_finalization = false;
  }
  state->condition.notify_all();
}

void RpcEventRuntime::test_wait_for_incoming_backlog(std::size_t count) {
  auto state = state_;
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock, [&] { return state->incoming_count >= count; });
}

void RpcEventRuntime::test_wait_for_incoming_waiter() {
  auto state = state_;
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock, [&] { return state->incoming_waiter; });
}

int RpcEventRuntime::test_dispatch(RpcEvent event) {
  auto state = state_;
  return state ? state->dispatch(std::move(event)) : -ESHUTDOWN;
}

void RpcEventRuntime::test_wait_for_rejected_streams(std::size_t count) {
  auto state = state_;
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock, [&] {
    if (count != 0) {
      return state->rejected_incoming_count >= count;
    }
    return std::none_of(state->streams.pending.begin(), state->streams.pending.end(),
                        [](const auto& entry) { return entry.second.discard_until_call_closed; });
  });
}

void RpcEventRuntime::test_fail_driver(int error) {
  auto state = state_;
  state->fail_driver(error);
}

void RpcEventRuntime::test_pause_driver() {
  auto state = state_;
  {
    std::lock_guard lock(state->mutex);
    state->pause_driver = true;
  }
  state->wake_driver();
  std::unique_lock lock(state->mutex);
  state->condition.wait(lock, [&] { return state->driver_paused; });
}

void RpcEventRuntime::test_resume_driver() {
  auto state = state_;
  {
    std::lock_guard lock(state->mutex);
    state->pause_driver = false;
  }
  state->condition.notify_all();
}

void RpcEventRuntime::test_wait_for_terminal_settlement() {
  auto state = state_;
  std::unique_lock lock(state->mutex);
  state->condition.wait(
      lock, [&] { return state->driver_error != 0 && !state->terminal_settlement_needed; });
}

void RpcEventRuntime::test_settle_driver_failure(int error) {
  auto state = state_;
  state->settle_consumers(error, false);
}

void RpcEventRuntime::test_fail_next_operation_reservation(int error) {
  auto state = state_;
  std::lock_guard lock(state->mutex);
  state->injected_operation_reservation_error = error;
}

void RpcEventRuntime::test_fail_next_endpoint_registration(int error) {
  auto state = state_;
  std::lock_guard lock(state->mutex);
  state->injected_endpoint_registration_error = error;
}

void RpcEventRuntime::test_fail_next_stream_registration(int error) {
  auto state = state_;
  std::lock_guard lock(state->mutex);
  state->injected_stream_registration_error = error;
}

void RpcEventRuntime::test_fail_next_endpoint_registration_commit() {
  auto state = state_;
  std::lock_guard lock(state->mutex);
  state->endpoints.injected_registration_commit_failure = true;
}

void RpcEventRuntime::test_fail_next_control_write(int error) {
  auto state = state_;
  std::lock_guard lock(state->mutex);
  state->injected_control_write_error = error;
}

void RpcEventRuntime::test_fail_next_adopt_start(int error) noexcept {
  injected_adopt_start_error.store(error, std::memory_order_release);
}

void RpcEventRuntime::test_wait_for_unadopted_cleanup() noexcept {
  unadopted_runtime_reaper().wait_until_idle();
}

std::weak_ptr<void> RpcEventRuntime::test_state_observer() const noexcept { return state_; }

std::thread::id RpcEventRuntime::test_driver_thread_id() const noexcept {
  auto state = state_;
  if (!state) {
    return {};
  }
  std::lock_guard lock(state->mutex);
  return state->driver_thread_id;
}

} // namespace trevrpc::detail
