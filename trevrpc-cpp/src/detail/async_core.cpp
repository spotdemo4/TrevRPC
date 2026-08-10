#include "async_core.hpp"

#include <algorithm>
#include <cerrno>

namespace trevrpc::detail {
namespace {

class Abi6NativeOps final : public NativeOps {
public:
  int send(trevrpc_stream* stream, std::span<const std::byte> body) noexcept override {
    return trevrpc_stream_send_message_borrowed_wait(
        stream, reinterpret_cast<const std::uint8_t*>(body.data()), body.size());
  }

  int finish_send(trevrpc_stream* stream) noexcept override {
    return trevrpc_stream_finish_send(stream);
  }

  Result<std::optional<StreamFrame>>
  receive_ready_since(trevrpc_stream* stream, std::uint64_t wait_started) noexcept override {
    trevrpc_inbound_stream_frame* frame = nullptr;
    int ready = 0;
    const int error = trevrpc_stream_recv_inbound_ready_since(stream, &frame, &ready, wait_started);
    if (error != 0) {
      trevrpc_inbound_stream_frame_release(frame);
      return Error::runtime(error);
    }
    if (ready == 0) {
      trevrpc_inbound_stream_frame_release(frame);
      return std::optional<StreamFrame>{};
    }
    auto decoded = decode_inbound_frame(stream, frame);
    if (!decoded) {
      return decoded.error();
    }
    return std::optional<StreamFrame>(std::move(decoded).value());
  }

  void cancel(trevrpc_stream* stream) noexcept override { trevrpc_stream_cancel(stream); }

  void close(trevrpc_stream* stream) noexcept override { trevrpc_stream_close(stream); }
};

class Abi6ServerCallOps final : public ServerCallOps {
public:
  int defer(trevrpc_call* call) noexcept override { return trevrpc_call_defer(call); }
  int retain(trevrpc_call* call) noexcept override { return trevrpc_call_retain(call); }
  void release(trevrpc_call* call) noexcept override { trevrpc_call_release(call); }
  trevrpc_stream* stream(trevrpc_call* call) noexcept override { return trevrpc_call_stream(call); }
  int respond(trevrpc_call* call, const Status& status, std::span<const std::byte> body,
              const Metadata& metadata) noexcept override {
    const trevrpc_request* request = trevrpc_call_request(call);
    if (request != nullptr && request->kind == TREVRPC_RPC_KIND_CLIENT_STREAMING) {
      trevrpc_stream* stream = trevrpc_call_stream(call);
      const int send_error = trevrpc_stream_send_message_borrowed_wait(
          stream, reinterpret_cast<const std::uint8_t*>(body.data()), body.size());
      if (send_error != 0) {
        return send_error;
      }
      return finish(call, Status(status.code(), status.message(), metadata));
    }
    NativeMetadata native_metadata;
    const int metadata_error = native_metadata.assign(metadata);
    if (metadata_error != 0) {
      return metadata_error;
    }
    return respond_borrowed(call, static_cast<std::uint32_t>(status.code()), status.message(), body,
                            native_metadata.get());
  }
  int finish(trevrpc_call* call, const Status& status) noexcept override {
    NativeMetadata native_metadata;
    const int metadata_error = native_metadata.assign(status.metadata());
    if (metadata_error != 0) {
      return metadata_error;
    }
    return finish_borrowed(call, static_cast<std::uint32_t>(status.code()), status.message(),
                           native_metadata.get());
  }
  void cancel(trevrpc_call* call) noexcept override { trevrpc_call_cancel(call); }
  void close(trevrpc_call* call) noexcept override { trevrpc_call_close(call); }
};

class ServerStreamNativeOps final : public NativeOps {
public:
  ServerStreamNativeOps(trevrpc_call* call, std::shared_ptr<ServerCallOps> call_ops)
      : call_(call), call_ops_(std::move(call_ops)) {}

  int send(trevrpc_stream* stream, std::span<const std::byte> body) noexcept override {
    return trevrpc_stream_send_message_borrowed_wait(
        stream, reinterpret_cast<const std::uint8_t*>(body.data()), body.size());
  }
  int finish_send(trevrpc_stream* stream) noexcept override {
    return trevrpc_stream_finish_send(stream);
  }
  Result<std::optional<StreamFrame>>
  receive_ready_since(trevrpc_stream* stream, std::uint64_t wait_started) noexcept override {
    trevrpc_inbound_stream_frame* frame = nullptr;
    int ready = 0;
    const int error = trevrpc_stream_recv_inbound_ready_since(stream, &frame, &ready, wait_started);
    if (error != 0) {
      trevrpc_inbound_stream_frame_release(frame);
      return Error::runtime(error);
    }
    if (ready == 0) {
      trevrpc_inbound_stream_frame_release(frame);
      return std::optional<StreamFrame>{};
    }
    if (frame == nullptr) {
      StreamFrame end;
      end.terminal = true;
      end.status = Status::ok();
      return std::optional<StreamFrame>(std::move(end));
    }
    auto decoded = decode_inbound_frame(stream, frame);
    if (!decoded) {
      return decoded.error();
    }
    return std::optional<StreamFrame>(std::move(decoded).value());
  }
  void cancel(trevrpc_stream*) noexcept override { call_ops_->cancel(call_); }
  void close(trevrpc_stream*) noexcept override {}

private:
  trevrpc_call* call_;
  std::shared_ptr<ServerCallOps> call_ops_;
};

[[nodiscard]] std::optional<Deadline> earliest_deadline(std::optional<Deadline> first,
                                                        std::optional<Deadline> second) {
  if (!first) {
    return second;
  }
  if (!second) {
    return first;
  }
  return std::min(*first, *second);
}

} // namespace

bool CancellationState::cancelled() const noexcept {
  std::lock_guard lock(mutex_);
  return cancelled_;
}

void CancellationState::cancel() noexcept {
  std::unordered_map<std::uint64_t, Work> callbacks;
  {
    std::lock_guard lock(mutex_);
    if (cancelled_) {
      return;
    }
    cancelled_ = true;
    callbacks.swap(callbacks_);
  }
  for (auto& [id, callback] : callbacks) {
    (void)id;
    try {
      callback();
    } catch (...) {
      (void)std::current_exception();
    }
  }
}

std::uint64_t CancellationState::register_callback(Work callback) {
  {
    std::lock_guard lock(mutex_);
    if (!cancelled_) {
      const std::uint64_t id = next_id_++;
      callbacks_.emplace(id, std::move(callback));
      return id;
    }
  }
  callback();
  return 0;
}

void CancellationState::unregister_callback(std::uint64_t id) noexcept {
  if (id == 0) {
    return;
  }
  std::lock_guard lock(mutex_);
  callbacks_.erase(id);
}

AsyncRuntimeState::AsyncRuntimeState(std::shared_ptr<Executor> continuation,
                                     std::shared_ptr<ThreadPoolExecutor> native_io,
                                     AsyncRuntimeOptions options)
    : continuation_(std::move(continuation)), native_io_(std::move(native_io)), options_(options),
      timer_thread_([this] { timer_loop(); }) {}

AsyncRuntimeState::~AsyncRuntimeState() {
  {
    std::lock_guard lock(timer_mutex_);
    timer_stop_ = true;
    timers_.clear();
  }
  timer_condition_.notify_all();
  if (timer_thread_.joinable()) {
    timer_thread_.join();
  }
  native_io_->request_stop();
  if (!native_io_->running_in_this_executor()) {
    (void)native_io_->drain_until(Deadline::max());
  }
}

Result<void> AsyncRuntimeState::submit_native(Work work) noexcept {
  return native_io_->execute(std::move(work));
}

Result<void> AsyncRuntimeState::schedule_at(Deadline deadline, Work work) noexcept {
  try {
    std::lock_guard lock(timer_mutex_);
    if (timer_stop_) {
      return Error::runtime(-ESHUTDOWN, "async runtime timer is stopping");
    }
    timers_.emplace(deadline, std::move(work));
    timer_condition_.notify_all();
    return {};
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to schedule async timer");
  }
}

void AsyncRuntimeState::timer_loop() noexcept {
  for (;;) {
    Work work;
    {
      std::unique_lock lock(timer_mutex_);
      if (timers_.empty() && !timer_stop_) {
        timer_condition_.wait(lock, [this] { return timer_stop_ || !timers_.empty(); });
      }
      if (timer_stop_) {
        return;
      }
      auto next = timers_.begin();
      const Deadline next_deadline = next->first;
      if (timer_condition_.wait_until(lock, next_deadline) != std::cv_status::timeout) {
        continue;
      }
      next = timers_.begin();
      if (next == timers_.end() || next->first > Deadline::clock::now()) {
        continue;
      }
      work = std::move(next->second);
      timers_.erase(next);
    }
    try {
      work();
    } catch (...) {
      (void)std::current_exception();
    }
  }
}

std::shared_ptr<NativeOps> production_native_ops() {
  static auto instance = std::make_shared<Abi6NativeOps>();
  return instance;
}

std::shared_ptr<ServerCallOps> production_server_call_ops() {
  static auto instance = std::make_shared<Abi6ServerCallOps>();
  return instance;
}

std::shared_ptr<OperationState>
OperationState::create(trevrpc_stream* stream, const std::shared_ptr<AsyncRuntime>& runtime,
                       std::shared_ptr<NativeOps> native_ops, std::optional<Deadline> deadline,
                       std::shared_ptr<Cancellation> cancellation_bridge,
                       std::shared_ptr<CancellationState> cancellation_state,
                       std::uint64_t cancellation_registration, bool terminal_stops_send) {
  if (!runtime || !runtime->state_) {
    return nullptr;
  }
  return std::make_shared<OperationState>(
      stream, runtime->state_, std::move(native_ops), deadline, std::move(cancellation_bridge),
      std::move(cancellation_state), cancellation_registration, terminal_stops_send);
}

OperationState::OperationState(trevrpc_stream* stream, std::shared_ptr<AsyncRuntimeState> runtime,
                               std::shared_ptr<NativeOps> native_ops,
                               std::optional<Deadline> deadline,
                               std::shared_ptr<Cancellation> cancellation_bridge,
                               std::shared_ptr<CancellationState> cancellation_state,
                               std::uint64_t cancellation_registration, bool terminal_stops_send)
    : stream_(stream), runtime_(std::move(runtime)), native_ops_(std::move(native_ops)),
      deadline_(deadline), cancellation_bridge_(std::move(cancellation_bridge)),
      cancellation_state_(std::move(cancellation_state)),
      cancellation_registration_(cancellation_registration),
      terminal_stops_send_(terminal_stops_send) {}

OperationState::~OperationState() { close(); }

std::shared_ptr<Executor> OperationState::continuation_executor() const noexcept {
  return runtime_->continuation();
}

void OperationState::set_cancellation_registration(std::uint64_t registration) noexcept {
  std::lock_guard lock(mutex_);
  cancellation_registration_ = registration;
}

bool OperationState::has_capacity_locked(std::size_t bytes) const noexcept {
  const auto& options = runtime_->options();
  return pending_items_ < options.max_pending_sends_per_stream &&
         bytes <= options.max_pending_send_bytes_per_stream - pending_bytes_;
}

Task<Result<void>> OperationState::send(std::size_t encoded_size,
                                        std::function<Result<std::vector<std::byte>>()> serializer,
                                        SendOptions options, bool finish) {
  NativeSendAction action;
  if (finish) {
    action = [native_ops = native_ops_](trevrpc_stream* stream,
                                        std::span<const std::byte>) noexcept {
      return native_ops->finish_send(stream);
    };
  } else {
    action = [native_ops = native_ops_](trevrpc_stream* stream,
                                        std::span<const std::byte> body) noexcept {
      return native_ops->send(stream, body);
    };
  }
  co_return co_await send_action(encoded_size, std::move(serializer), options, finish,
                                 std::move(action));
}

Task<Result<void>>
OperationState::send_action(std::size_t encoded_size,
                            std::function<Result<std::vector<std::byte>>()> serializer,
                            SendOptions options, bool seal_send, NativeSendAction action) {
  std::shared_ptr<OperationState> self;
  std::shared_ptr<AsyncCompletion<Result<void>>> completion;
  std::shared_ptr<SendItem> item;
  std::shared_ptr<SendWaiter> waiter;
  try {
    self = shared_from_this();
    auto created_completion = make_completion<Result<void>>(runtime_->continuation());
    if (!created_completion) {
      co_return created_completion.error();
    }
    completion = std::move(created_completion).value();
    const auto effective_deadline = earliest_deadline(deadline_, options.deadline);
    {
      std::lock_guard lock(mutex_);
      if (closed_ || cancelled_) {
        co_return Error::runtime(-ECANCELED, "async stream is closed");
      }
      if (send_cancelled_) {
        co_return Error::runtime(-EPIPE, "async stream send side was terminated by the peer");
      }
      if (send_sealed_) {
        co_return Error::runtime(-EPIPE, "async stream send side is closed");
      }
      if (!action) {
        co_return Error::runtime(-EINVAL, "async send action is empty");
      }
      if (encoded_size > runtime_->options().max_pending_send_bytes_per_stream) {
        co_return Error::runtime(-ENOBUFS, "async stream message exceeds send byte capacity");
      }
      if (effective_deadline && Deadline::clock::now() >= *effective_deadline) {
        co_return Error::runtime(-ETIMEDOUT, "async send deadline exceeded");
      }
      if (has_capacity_locked(encoded_size)) {
        item = std::make_shared<SendItem>();
        item->bytes_reserved = encoded_size;
        item->seal_send = seal_send;
        item->action = std::move(action);
        item->completion = completion;
        send_queue_.push_back(item);
        ++pending_items_;
        pending_bytes_ += encoded_size;
      } else if (options.backpressure == BackpressureMode::FailFast) {
        co_return Error::runtime(-ENOBUFS, "async stream send queue is full");
      } else {
        if (send_waiters_.size() >= runtime_->options().max_waiting_senders_per_stream) {
          co_return Error::runtime(-ENOBUFS, "async stream sender wait queue is full");
        }
        waiter = std::make_shared<SendWaiter>();
        waiter->item = std::make_shared<SendItem>();
        waiter->bytes = encoded_size;
        waiter->seal_send = seal_send;
        waiter->deadline = effective_deadline;
        waiter->serializer = serializer;
        waiter->action = std::move(action);
        waiter->completion = completion;
        send_waiters_.push_back(waiter);
      }
      if (seal_send) {
        send_sealed_ = true;
      }
    }
    if (item) {
      prepare_admitted_item(item, serializer);
    } else if (waiter->deadline) {
      std::weak_ptr<OperationState> weak = self;
      auto scheduled = runtime_->schedule_at(*waiter->deadline, [weak, waiter] {
        if (auto operation = weak.lock()) {
          operation->timeout_waiter(waiter);
        }
      });
      if (!scheduled) {
        timeout_waiter(waiter);
      }
    }
  } catch (...) {
    if (seal_send) {
      std::lock_guard lock(mutex_);
      send_sealed_ = false;
    }
    co_return Error::runtime(-ENOMEM, "failed to enqueue async stream send");
  }
  co_return co_await *completion;
}

void OperationState::prepare_admitted_item(
    const std::shared_ptr<SendItem>& item,
    const std::function<Result<std::vector<std::byte>>()>& serializer) noexcept {
  Result<std::vector<std::byte>> encoded(
      Error::runtime(-ENOMEM, "async send serialization failed"));
  try {
    encoded = serializer();
  } catch (...) {
    encoded = Error::runtime(-ENOMEM, "async send serialization threw");
  }
  {
    std::lock_guard lock(mutex_);
    if (encoded && encoded.value().size() != item->bytes_reserved) {
      item->preparation_error =
          Error::protobuf("serialized message size changed after capacity admission");
    } else if (encoded) {
      item->bytes = std::move(encoded).value();
    } else {
      item->preparation_error = encoded.error();
    }
    item->ready = true;
  }
  start_send_if_ready();
}

void OperationState::start_send_if_ready() noexcept {
  std::shared_ptr<SendItem> item;
  bool native_work = false;
  {
    std::lock_guard lock(mutex_);
    if (send_running_ || send_queue_.empty() || !send_queue_.front()->ready || closed_ ||
        send_cancelled_) {
      return;
    }
    send_running_ = true;
    item = send_queue_.front();
    if (!item->preparation_error) {
      ++native_work_;
      native_work = true;
    }
  }
  if (!native_work) {
    finish_send_item(item, item->preparation_error->code());
    return;
  }
  auto self = shared_from_this();
  auto submitted = runtime_->submit_native([self = std::move(self), item] {
    const int error = item->action(self->stream_, item->bytes);
    self->finish_send_item(item, error);
    self->finish_native_work();
  });
  if (!submitted) {
    finish_send_item(item, submitted.error().code());
    finish_native_work();
  }
}

void OperationState::admit_waiters_locked(std::vector<std::shared_ptr<SendWaiter>>& admitted) {
  while (!send_waiters_.empty()) {
    auto waiter = send_waiters_.front();
    if (waiter->settled) {
      send_waiters_.pop_front();
      continue;
    }
    if (waiter->deadline && Deadline::clock::now() >= *waiter->deadline) {
      waiter->settled = true;
      if (waiter->seal_send) {
        send_sealed_ = false;
      }
      waiter->item.reset();
      send_waiters_.pop_front();
      admitted.push_back(waiter);
      continue;
    }
    if (!has_capacity_locked(waiter->bytes)) {
      break;
    }
    send_waiters_.pop_front();
    waiter->settled = true;
    waiter->item->bytes_reserved = waiter->bytes;
    waiter->item->seal_send = waiter->seal_send;
    waiter->item->action = std::move(waiter->action);
    waiter->item->completion = waiter->completion;
    ++pending_items_;
    pending_bytes_ += waiter->bytes;
    send_queue_.push_back(waiter->item);
    admitted.push_back(waiter);
  }
}

void OperationState::finish_send_item(const std::shared_ptr<SendItem>& item, int error) noexcept {
  std::vector<std::shared_ptr<SendWaiter>> admitted;
  std::vector<Work> idle_callbacks;
  bool cancelled = false;
  {
    std::lock_guard lock(mutex_);
    if (send_queue_.empty() || send_queue_.front() != item) {
      return;
    }
    send_queue_.pop_front();
    send_running_ = false;
    if (pending_items_ > 0) {
      --pending_items_;
    }
    pending_bytes_ -= std::min(pending_bytes_, item->bytes_reserved);
    cancelled = cancelled_ || closed_ || send_cancelled_;
    if (!cancelled) {
      admit_waiters_locked(admitted);
    }
    collect_idle_callbacks_locked(idle_callbacks);
  }
  if (item->preparation_error) {
    item->completion->complete(Result<void>(*item->preparation_error));
  } else if (cancelled) {
    item->completion->complete(
        Result<void>(Error::runtime(-ECANCELED, "async stream was cancelled")));
  } else if (error != 0) {
    item->completion->complete(Result<void>(Error::runtime(error)));
  } else {
    item->completion->complete(Result<void>{});
  }
  for (const auto& waiter : admitted) {
    if (!waiter->item) {
      waiter->completion->complete(
          Result<void>(Error::runtime(-ETIMEDOUT, "async send capacity wait timed out")));
    } else {
      prepare_admitted_item(waiter->item, waiter->serializer);
    }
  }
  for (Work& callback : idle_callbacks) {
    try {
      callback();
    } catch (...) {
      (void)std::current_exception();
    }
  }
  start_send_if_ready();
}

void OperationState::collect_idle_callbacks_locked(std::vector<Work>& callbacks) {
  if (!send_running_ && send_queue_.empty() && native_work_ == 0) {
    callbacks = std::move(send_idle_callbacks_);
  }
}

void OperationState::finish_native_work() noexcept {
  trevrpc_stream* stream = nullptr;
  std::vector<Work> idle_callbacks;
  {
    std::lock_guard lock(mutex_);
    if (native_work_ == 0) {
      std::terminate();
    }
    --native_work_;
    if (native_work_ == 0 && close_requested_) {
      stream = std::exchange(stream_, nullptr);
      close_requested_ = false;
    }
    collect_idle_callbacks_locked(idle_callbacks);
  }
  if (stream != nullptr) {
    native_ops_->close(stream);
  }
  for (Work& callback : idle_callbacks) {
    try {
      callback();
    } catch (...) {
      (void)std::current_exception();
    }
  }
}

void OperationState::timeout_waiter(const std::shared_ptr<SendWaiter>& waiter) noexcept {
  bool timed_out = false;
  {
    std::lock_guard lock(mutex_);
    if (!waiter->settled) {
      waiter->settled = true;
      if (waiter->seal_send) {
        send_sealed_ = false;
      }
      waiter->item.reset();
      const auto found = std::find(send_waiters_.begin(), send_waiters_.end(), waiter);
      if (found != send_waiters_.end()) {
        send_waiters_.erase(found);
      }
      timed_out = true;
    }
  }
  if (timed_out) {
    waiter->completion->complete(
        Result<void>(Error::runtime(-ETIMEDOUT, "async send capacity wait timed out")));
  }
}

Task<Result<StreamFrame>> OperationState::receive() {
  auto created_completion = make_completion<Result<StreamFrame>>(runtime_->continuation());
  if (!created_completion) {
    co_return created_completion.error();
  }
  auto completion = std::move(created_completion).value();
  std::uint64_t wait_started = 0;
  {
    std::lock_guard lock(mutex_);
    if (closed_ || cancelled_) {
      co_return Error::runtime(-ECANCELED, "async stream is closed");
    }
    if (receive_terminal_) {
      co_return Error::runtime(-EALREADY, "async stream terminal result was already received");
    }
    if (receive_pending_) {
      co_return Error::runtime(-EBUSY, "an async receive is already pending");
    }
    receive_pending_ = true;
    receive_completion_ = completion;
  }
  const int clock_error = trevrpc_monotonic_now_nanos(&wait_started);
  if (clock_error != 0) {
    settle_receive(completion, Error::runtime(clock_error));
  } else {
    start_receive_attempt(completion, wait_started, runtime_->options().receive_poll_min);
  }
  co_return co_await *completion;
}

void OperationState::start_receive_attempt(
    std::shared_ptr<AsyncCompletion<Result<StreamFrame>>> completion, std::uint64_t wait_started,
    std::chrono::nanoseconds delay) noexcept {
  bool active = false;
  bool stopped = false;
  {
    std::lock_guard lock(mutex_);
    active = receive_pending_ && receive_completion_ == completion;
    stopped = closed_ || cancelled_;
    if (active && !stopped) {
      ++native_work_;
    }
  }
  if (!active) {
    return;
  }
  if (stopped) {
    settle_receive(completion, Error::runtime(-ECANCELED, "async stream was cancelled"));
    return;
  }

  auto self = shared_from_this();
  std::weak_ptr<OperationState> weak = self;
  auto submitted = runtime_->submit_native([self = std::move(self), weak,
                                            completion = std::move(completion), wait_started,
                                            delay] {
    auto& operation = self;
    bool attempt_active = false;
    bool attempt_stopped = false;
    {
      std::lock_guard lock(operation->mutex_);
      attempt_active = operation->receive_pending_ && operation->receive_completion_ == completion;
      attempt_stopped = operation->closed_ || operation->cancelled_;
    }
    if (attempt_active && attempt_stopped) {
      operation->settle_receive(completion,
                                Error::runtime(-ECANCELED, "async stream was cancelled"));
    } else if (attempt_active && operation->deadline_ &&
               Deadline::clock::now() >= *operation->deadline_) {
      operation->settle_receive(completion,
                                Error::runtime(-ETIMEDOUT, "async call deadline exceeded"));
      operation->cancel();
    } else if (attempt_active) {
      auto ready = operation->native_ops_->receive_ready_since(operation->stream_, wait_started);
      if (!ready) {
        operation->settle_receive(completion, ready.error());
      } else if (ready.value().has_value()) {
        operation->settle_receive(completion, std::move(ready.value()).value());
      } else {
        const auto next_delay =
            std::min(delay * 2, operation->runtime_->options().receive_poll_max);
        auto scheduled = operation->runtime_->schedule_at(
            Deadline::clock::now() + delay, [weak, completion, wait_started, next_delay] {
              if (auto current = weak.lock()) {
                current->start_receive_attempt(completion, wait_started, next_delay);
              } else {
                completion->complete(Result<StreamFrame>(
                    Error::runtime(-ECANCELED, "async operation was released")));
              }
            });
        if (!scheduled) {
          operation->settle_receive(completion, scheduled.error());
        }
      }
    }
    operation->finish_native_work();
  });
  if (!submitted) {
    settle_receive(completion, submitted.error());
    finish_native_work();
  }
}

void OperationState::settle_receive(
    const std::shared_ptr<AsyncCompletion<Result<StreamFrame>>>& completion,
    Result<StreamFrame> result) noexcept {
  std::vector<std::shared_ptr<AsyncCompletion<Result<void>>>> send_completions;
  std::vector<Work> idle_callbacks;
  trevrpc_stream* stream_to_cancel = nullptr;
  bool settled = false;
  const bool terminal = result && result.value().terminal;
  {
    std::lock_guard lock(mutex_);
    if (receive_pending_ && receive_completion_ == completion) {
      receive_pending_ = false;
      receive_completion_.reset();
      receive_terminal_ = terminal;
      settled = true;
      if (terminal && terminal_stops_send_ && !send_cancelled_) {
        send_cancelled_ = true;
        send_sealed_ = true;
        const std::size_t first_queued = send_running_ && !send_queue_.empty() ? 1 : 0;
        while (send_queue_.size() > first_queued) {
          auto item = send_queue_[first_queued];
          pending_bytes_ -= std::min(pending_bytes_, item->bytes_reserved);
          if (pending_items_ > 0) {
            --pending_items_;
          }
          send_completions.push_back(item->completion);
          send_queue_.erase(send_queue_.begin() + static_cast<std::ptrdiff_t>(first_queued));
        }
        for (const auto& waiter : send_waiters_) {
          if (!waiter->settled) {
            waiter->settled = true;
            send_completions.push_back(waiter->completion);
          }
        }
        send_waiters_.clear();
        if (send_running_) {
          stream_to_cancel = stream_;
        }
        collect_idle_callbacks_locked(idle_callbacks);
      }
    }
  }
  if (stream_to_cancel != nullptr) {
    native_ops_->cancel(stream_to_cancel);
  }
  for (const auto& send_completion : send_completions) {
    send_completion->complete(Result<void>(
        Error::runtime(-ECANCELED, "async request send was cancelled by the terminal response")));
  }
  for (Work& callback : idle_callbacks) {
    try {
      callback();
    } catch (...) {
      (void)std::current_exception();
    }
  }
  if (settled) {
    completion->complete(std::move(result));
  }
}

void OperationState::cancel() noexcept {
  std::shared_ptr<AsyncCompletion<Result<StreamFrame>>> receive_completion;
  trevrpc_stream* stream = nullptr;
  {
    std::lock_guard lock(mutex_);
    if (cancelled_) {
      return;
    }
    cancelled_ = true;
    if (receive_pending_) {
      receive_pending_ = false;
      receive_completion = std::move(receive_completion_);
    }
    stream = stream_;
  }
  native_ops_->cancel(stream);

  for (;;) {
    std::shared_ptr<AsyncCompletion<Result<void>>> completion;
    {
      std::lock_guard lock(mutex_);
      const std::size_t first_queued = send_running_ && !send_queue_.empty() ? 1 : 0;
      if (send_queue_.size() > first_queued) {
        auto item = send_queue_[first_queued];
        pending_bytes_ -= std::min(pending_bytes_, item->bytes_reserved);
        if (pending_items_ > 0) {
          --pending_items_;
        }
        completion = item->completion;
        send_queue_.erase(send_queue_.begin() + static_cast<std::ptrdiff_t>(first_queued));
      } else {
        while (!send_waiters_.empty() && send_waiters_.front()->settled) {
          send_waiters_.pop_front();
        }
        if (send_waiters_.empty()) {
          break;
        }
        auto waiter = send_waiters_.front();
        send_waiters_.pop_front();
        waiter->settled = true;
        completion = waiter->completion;
      }
    }
    completion->complete(Result<void>(Error::runtime(-ECANCELED, "async stream was cancelled")));
  }

  if (receive_completion) {
    receive_completion->complete(
        Result<StreamFrame>(Error::runtime(-ECANCELED, "async stream was cancelled")));
  }
}

void OperationState::close() noexcept {
  {
    std::lock_guard lock(mutex_);
    if (closed_) {
      return;
    }
  }
  cancel();
  trevrpc_stream* stream = nullptr;
  std::shared_ptr<CancellationState> cancellation_state;
  std::uint64_t cancellation_registration = 0;
  {
    std::lock_guard lock(mutex_);
    if (closed_) {
      return;
    }
    closed_ = true;
    cancellation_state = std::move(cancellation_state_);
    cancellation_registration = std::exchange(cancellation_registration_, 0);
    if (native_work_ == 0) {
      stream = std::exchange(stream_, nullptr);
    } else {
      close_requested_ = true;
    }
  }
  if (cancellation_state) {
    cancellation_state->unregister_callback(cancellation_registration);
  }
  if (stream != nullptr) {
    native_ops_->close(stream);
  }
}

void OperationState::retire(const Error& reason) noexcept {
  std::vector<std::shared_ptr<AsyncCompletion<Result<void>>>> send_completions;
  std::shared_ptr<AsyncCompletion<Result<StreamFrame>>> receive_completion;
  std::shared_ptr<CancellationState> cancellation_state;
  std::uint64_t cancellation_registration = 0;
  {
    std::lock_guard lock(mutex_);
    if (closed_) {
      return;
    }
    closed_ = true;
    cancelled_ = true;
    stream_ = nullptr;
    close_requested_ = false;
    cancellation_state = std::move(cancellation_state_);
    cancellation_registration = std::exchange(cancellation_registration_, 0);
    for (const auto& item : send_queue_) {
      send_completions.push_back(item->completion);
    }
    for (const auto& waiter : send_waiters_) {
      if (!waiter->settled) {
        waiter->settled = true;
        send_completions.push_back(waiter->completion);
      }
    }
    send_queue_.clear();
    send_waiters_.clear();
    pending_items_ = 0;
    pending_bytes_ = 0;
    send_running_ = false;
    if (receive_pending_) {
      receive_pending_ = false;
      receive_completion = std::move(receive_completion_);
    }
  }
  if (cancellation_state) {
    cancellation_state->unregister_callback(cancellation_registration);
  }
  for (const auto& completion : send_completions) {
    completion->complete(Result<void>(reason));
  }
  if (receive_completion) {
    receive_completion->complete(Result<StreamFrame>(reason));
  }
}

void OperationState::when_send_idle(Work callback) noexcept {
  {
    std::lock_guard lock(mutex_);
    if (send_running_ || !send_queue_.empty() || native_work_ != 0) {
      try {
        send_idle_callbacks_.push_back(std::move(callback));
      } catch (...) {
        std::terminate();
      }
      return;
    }
  }
  try {
    callback();
  } catch (...) {
    (void)std::current_exception();
  }
}

Task<Result<ByteResponse>> OperationState::run_unary(std::shared_ptr<Channel> channel,
                                                     std::shared_ptr<AsyncRuntime> runtime,
                                                     std::string service, std::string method,
                                                     std::vector<std::byte> request,
                                                     OwnedAsyncCallOptions options) {
  if (!channel || !runtime || !runtime->state_) {
    co_return Error::runtime(-EINVAL, "async client channel and runtime must not be null");
  }
  auto created_completion = make_completion<Result<ByteResponse>>(runtime->state_->continuation());
  if (!created_completion) {
    co_return created_completion.error();
  }
  auto completion = std::move(created_completion).value();
  std::shared_ptr<Cancellation> bridge;
  std::shared_ptr<CancellationState> cancellation_state = options.cancellation.state_;
  std::uint64_t registration = 0;
  if (cancellation_state) {
    try {
      bridge = options.retained_cancellation ? options.retained_cancellation
                                             : std::make_shared<Cancellation>();
      registration = cancellation_state->register_callback([bridge] { bridge->cancel(); });
      options.call_options.cancellation = bridge.get();
    } catch (...) {
      co_return Error::runtime(-ENOMEM, "failed to create async cancellation bridge");
    }
  } else if (options.retained_cancellation) {
    bridge = options.retained_cancellation;
    options.call_options.cancellation = bridge.get();
  }
  auto runtime_state = runtime->state_;
  auto submitted = runtime_state->submit_native(
      [channel = std::move(channel), service = std::move(service), method = std::move(method),
       request = std::move(request), options = std::move(options), completion, cancellation_state,
       registration]() mutable {
        if (options.deadline) {
          const auto now = Deadline::clock::now();
          if (now >= *options.deadline) {
            if (cancellation_state) {
              cancellation_state->unregister_callback(registration);
            }
            completion->complete(
                Result<ByteResponse>(Error::runtime(-ETIMEDOUT, "async call deadline exceeded")));
            return;
          }
          options.call_options.timeout =
              std::chrono::duration_cast<std::chrono::nanoseconds>(*options.deadline - now);
        }
        auto result = channel->call_unary(service, method, request, options.call_options);
        if (cancellation_state) {
          cancellation_state->unregister_callback(registration);
        }
        completion->complete(std::move(result));
      });
  if (!submitted) {
    if (cancellation_state) {
      cancellation_state->unregister_callback(registration);
    }
    co_return submitted.error();
  }
  co_return co_await *completion;
}

Task<Result<std::shared_ptr<OperationState>>>
OperationState::start_stream(std::shared_ptr<Channel> channel,
                             std::shared_ptr<AsyncRuntime> runtime, std::string service,
                             std::string method, std::uint32_t kind, std::vector<std::byte> request,
                             OwnedAsyncCallOptions options) {
  if (!channel || !runtime || !runtime->state_) {
    co_return Error::runtime(-EINVAL, "async client channel and runtime must not be null");
  }
  auto created_completion =
      make_completion<Result<std::shared_ptr<OperationState>>>(runtime->state_->continuation());
  if (!created_completion) {
    co_return created_completion.error();
  }
  auto completion = std::move(created_completion).value();
  std::shared_ptr<CancellationState> cancellation_state = options.cancellation.state_;
  std::shared_ptr<Cancellation> bridge;
  std::uint64_t registration = 0;
  if (cancellation_state) {
    try {
      bridge = options.retained_cancellation ? options.retained_cancellation
                                             : std::make_shared<Cancellation>();
      registration = cancellation_state->register_callback([bridge] { bridge->cancel(); });
      options.call_options.cancellation = bridge.get();
    } catch (...) {
      co_return Error::runtime(-ENOMEM, "failed to create async cancellation bridge");
    }
  } else if (options.retained_cancellation) {
    bridge = options.retained_cancellation;
    options.call_options.cancellation = bridge.get();
  }
  auto runtime_state = runtime->state_;
  auto submitted = runtime_state->submit_native(
      [channel = std::move(channel), runtime = std::move(runtime), service = std::move(service),
       method = std::move(method), kind, request = std::move(request), options = std::move(options),
       completion, cancellation_state, bridge, registration]() mutable {
        if (options.deadline) {
          const auto now = Deadline::clock::now();
          if (now >= *options.deadline) {
            if (cancellation_state) {
              cancellation_state->unregister_callback(registration);
            }
            completion->complete(Result<std::shared_ptr<OperationState>>(
                Error::runtime(-ETIMEDOUT, "async call deadline exceeded")));
            return;
          }
          options.call_options.timeout =
              std::chrono::duration_cast<std::chrono::nanoseconds>(*options.deadline - now);
        }
        auto stream = channel->start_stream(service, method, kind, request, options.call_options);
        if (!stream) {
          if (cancellation_state) {
            cancellation_state->unregister_callback(registration);
          }
          completion->complete(Result<std::shared_ptr<OperationState>>(stream.error()));
          return;
        }
        auto operation = OperationState::create(stream.value().release_native_handle(), runtime,
                                                production_native_ops(), options.deadline, bridge,
                                                cancellation_state, 0, true);
        if (!operation) {
          if (cancellation_state) {
            cancellation_state->unregister_callback(registration);
          }
          completion->complete(Result<std::shared_ptr<OperationState>>(
              Error::runtime(-ENOMEM, "failed to create async stream state")));
          return;
        }
        if (cancellation_state) {
          cancellation_state->unregister_callback(registration);
          try {
            std::weak_ptr<OperationState> weak = operation;
            registration = cancellation_state->register_callback([bridge, weak] {
              bridge->cancel();
              if (auto current = weak.lock()) {
                current->cancel();
              }
            });
            operation->set_cancellation_registration(registration);
          } catch (...) {
            operation->close();
            completion->complete(Result<std::shared_ptr<OperationState>>(
                Error::runtime(-ENOMEM, "failed to attach async stream cancellation")));
            return;
          }
        }
        completion->complete(std::move(operation));
      });
  if (!submitted) {
    if (cancellation_state) {
      cancellation_state->unregister_callback(registration);
    }
    co_return submitted.error();
  }
  co_return co_await *completion;
}

ServerCallState::ServerCallState(trevrpc_call* call, std::uint32_t kind,
                                 std::shared_ptr<ServerCallOps> call_ops) noexcept
    : call_(call), kind_(kind), call_ops_(std::move(call_ops)) {}

Result<std::shared_ptr<ServerCallState>>
ServerCallState::create(trevrpc_call* call, std::uint32_t kind,
                        const std::shared_ptr<AsyncRuntime>& runtime,
                        const std::shared_ptr<ServerCallOps>& call_ops,
                        std::shared_ptr<NativeOps> stream_ops, bool* deferred) {
  if (deferred != nullptr) {
    *deferred = false;
  }
  if (call == nullptr || !runtime || !call_ops) {
    return Error::runtime(-EINVAL, "async server call inputs must not be null");
  }
  if (kind != TREVRPC_RPC_KIND_UNARY && kind != TREVRPC_RPC_KIND_CLIENT_STREAMING &&
      kind != TREVRPC_RPC_KIND_SERVER_STREAMING &&
      kind != TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
    return Error::runtime(TREVRPC_ERR_UNSUPPORTED_RPC_KIND);
  }
  const int defer_error = call_ops->defer(call);
  if (defer_error != 0) {
    return Error::runtime(defer_error, "failed to defer async server call");
  }
  if (deferred != nullptr) {
    *deferred = true;
  }
  const int retain_error = call_ops->retain(call);
  if (retain_error != 0) {
    call_ops->close(call);
    return Error::runtime(retain_error, "failed to retain async server call");
  }
  try {
    if (!stream_ops) {
      stream_ops = std::make_shared<ServerStreamNativeOps>(call, call_ops);
    }
    trevrpc_stream* stream = call_ops->stream(call);
    if (kind != TREVRPC_RPC_KIND_UNARY && stream == nullptr) {
      call_ops->close(call);
      call_ops->release(call);
      return Error::runtime(-EINVAL, "streaming server call has no native stream");
    }
    auto operation = OperationState::create(stream, runtime, std::move(stream_ops), std::nullopt);
    if (!operation) {
      call_ops->close(call);
      call_ops->release(call);
      return Error::runtime(-ENOMEM, "failed to create async server operation");
    }
    auto state = std::shared_ptr<ServerCallState>(new ServerCallState(call, kind, call_ops));
    state->operation_ = std::move(operation);
    return state;
  } catch (...) {
    call_ops->close(call);
    call_ops->release(call);
    return Error::runtime(-ENOMEM, "failed to allocate async server call state");
  }
}

ServerCallState::~ServerCallState() {
  trevrpc_call* call = nullptr;
  std::shared_ptr<OperationState> operation;
  std::shared_ptr<ServerCallOps> call_ops;
  {
    std::lock_guard lock(mutex_);
    if (pin_released_ || call_ == nullptr) {
      return;
    }
    pin_released_ = true;
    call = std::exchange(call_, nullptr);
    operation = operation_;
    call_ops = call_ops_;
  }
  if (operation) {
    operation->cancel();
  }
  call_ops->close(call);
  if (operation) {
    operation->when_send_idle(
        [operation = std::move(operation), call_ops = std::move(call_ops), call] {
          operation->retire(Error::runtime(-ECANCELED, "async server call was abandoned"));
          call_ops->release(call);
        });
  } else {
    call_ops->release(call);
  }
}

Task<Result<void>> ServerCallState::respond(std::vector<std::byte> body, Status status,
                                            Metadata metadata) {
  return respond_owned(shared_from_this(), std::move(body), std::move(status), std::move(metadata));
}

Task<Result<void>> ServerCallState::respond_owned(std::shared_ptr<ServerCallState> self,
                                                  std::vector<std::byte> body, Status status,
                                                  Metadata metadata) {
  if (self->kind_ != TREVRPC_RPC_KIND_UNARY && self->kind_ != TREVRPC_RPC_KIND_CLIENT_STREAMING) {
    co_return Error::runtime(TREVRPC_ERR_UNSUPPORTED_RPC_KIND,
                             "server call does not use a unary response");
  }
  trevrpc_call* call = nullptr;
  std::shared_ptr<ServerCallOps> call_ops;
  {
    std::lock_guard lock(self->mutex_);
    call = self->call_;
    call_ops = self->call_ops_;
  }
  std::weak_ptr<ServerCallState> weak = self;
  NativeSendAction action = [call, call_ops = std::move(call_ops), status = std::move(status),
                             metadata = std::move(metadata),
                             weak](trevrpc_stream*, std::span<const std::byte> encoded) noexcept {
    if (call == nullptr) {
      return -EALREADY;
    }
    const int error = call_ops->respond(call, status, encoded, metadata);
    if (auto state = weak.lock()) {
      state->record_native_terminal_result(error);
    }
    return error;
  };
  const std::size_t encoded_size = body.size();
  co_return co_await self->terminal(
      encoded_size,
      [body = std::move(body)]() mutable {
        return Result<std::vector<std::byte>>(std::move(body));
      },
      std::move(action));
}

Task<Result<void>> ServerCallState::finish(Status status) {
  return finish_owned(shared_from_this(), std::move(status));
}

Task<Result<void>> ServerCallState::finish_owned(std::shared_ptr<ServerCallState> self,
                                                 Status status) {
  if (self->kind_ == TREVRPC_RPC_KIND_UNARY || self->kind_ == TREVRPC_RPC_KIND_CLIENT_STREAMING) {
    co_return Error::runtime(TREVRPC_ERR_UNSUPPORTED_RPC_KIND,
                             "server call requires a unary response");
  }
  trevrpc_call* call = nullptr;
  std::shared_ptr<ServerCallOps> call_ops;
  {
    std::lock_guard lock(self->mutex_);
    call = self->call_;
    call_ops = self->call_ops_;
  }
  std::weak_ptr<ServerCallState> weak = self;
  NativeSendAction action = [call, call_ops = std::move(call_ops), status = std::move(status),
                             weak](trevrpc_stream*, std::span<const std::byte>) noexcept {
    if (call == nullptr) {
      return -EALREADY;
    }
    const int error = call_ops->finish(call, status);
    if (auto state = weak.lock()) {
      state->record_native_terminal_result(error);
    }
    return error;
  };
  co_return co_await self->terminal(
      0, [] { return Result<std::vector<std::byte>>(std::vector<std::byte>{}); },
      std::move(action));
}

Task<Result<void>>
ServerCallState::terminal(std::size_t encoded_size,
                          std::function<Result<std::vector<std::byte>>()> serializer,
                          NativeSendAction action) {
  auto created_completion = make_completion<Result<void>>(operation_->continuation_executor());
  if (!created_completion) {
    co_return created_completion.error();
  }
  auto completion = std::move(created_completion).value();
  bool winner = false;
  std::optional<Error> immediate_error;
  bool immediate = false;
  {
    std::lock_guard lock(mutex_);
    terminal_waiters_.push_back(completion);
    if (terminal_phase_ == ServerTerminalPhase::Open && !final_selected_) {
      terminal_phase_ = ServerTerminalPhase::ApplicationPending;
      winner = true;
    } else if (terminal_phase_ == ServerTerminalPhase::Settled) {
      immediate_error = final_error_;
      immediate = true;
      terminal_waiters_.pop_back();
    }
  }
  if (immediate) {
    completion->complete(immediate_error ? Result<void>(*immediate_error) : Result<void>{});
  }
  if (!winner) {
    co_return co_await *completion;
  }

  auto result = co_await operation_->send_action(encoded_size, std::move(serializer), {}, true,
                                                 std::move(action));
  settle_application(std::move(result));
  co_return co_await *completion;
}

void ServerCallState::record_native_terminal_result(int error) noexcept {
  std::lock_guard lock(mutex_);
  if (terminal_phase_ != ServerTerminalPhase::ApplicationPending || final_selected_) {
    return;
  }
  final_selected_ = true;
  if (error == -EALREADY) {
    final_error_ = Error::runtime(-ECANCELED, "native server completed the async call first");
  } else if (error != 0) {
    final_error_ = Error::runtime(error);
  }
}

void ServerCallState::settle_application(Result<void> result) noexcept {
  std::vector<std::shared_ptr<AsyncCompletion<Result<void>>>> waiters;
  std::optional<Error> final_error;
  bool close_call = false;
  Error retire_reason = Error::runtime(-ECANCELED, "async server call completed");
  {
    std::lock_guard lock(mutex_);
    close_call = !external_stop_ && !result && result.error().code() != -EALREADY;
    terminal_phase_ = ServerTerminalPhase::Settled;
    if (!final_selected_) {
      final_selected_ = true;
      if (!result) {
        if (result.error().code() == -EALREADY) {
          final_error_ = Error::runtime(-ECANCELED, "native server completed the async call first");
        } else {
          final_error_ = result.error();
        }
      }
    }
    publish_final_locked(waiters, final_error);
    if (final_error_) {
      retire_reason = *final_error_;
    }
  }
  cleanup_after_settlement(close_call, retire_reason);
  for (const auto& waiter : waiters) {
    waiter->complete(final_error ? Result<void>(*final_error) : Result<void>{});
  }
}

void ServerCallState::settle_without_application(const Error& result) noexcept {
  std::vector<std::shared_ptr<AsyncCompletion<Result<void>>>> waiters;
  std::optional<Error> final_error;
  {
    std::lock_guard lock(mutex_);
    terminal_phase_ = ServerTerminalPhase::Settled;
    if (!final_selected_) {
      final_selected_ = true;
      final_error_ = result;
    }
    publish_final_locked(waiters, final_error);
  }
  cleanup_after_settlement(false, result);
  for (const auto& waiter : waiters) {
    waiter->complete(Result<void>(final_error ? *final_error : result));
  }
}

void ServerCallState::publish_final_locked(
    std::vector<std::shared_ptr<AsyncCompletion<Result<void>>>>& waiters,
    std::optional<Error>& error) noexcept {
  waiters = std::move(terminal_waiters_);
  error = final_error_;
}

void ServerCallState::cleanup_after_settlement(bool close_call, Error retire_reason) noexcept {
  trevrpc_call* call = nullptr;
  std::shared_ptr<OperationState> operation;
  std::shared_ptr<ServerCallOps> call_ops;
  {
    std::lock_guard lock(mutex_);
    if (pin_released_ || call_ == nullptr) {
      return;
    }
    pin_released_ = true;
    call = std::exchange(call_, nullptr);
    operation = operation_;
    call_ops = call_ops_;
  }
  if (close_call) {
    call_ops->close(call);
  }
  if (operation) {
    operation->when_send_idle([operation = std::move(operation), call_ops = std::move(call_ops),
                               call, retire_reason = std::move(retire_reason)]() mutable {
      operation->retire(retire_reason);
      call_ops->release(call);
    });
  } else {
    call_ops->release(call);
  }
}

Error ServerCallState::stop_error(ServerStopReason reason) {
  switch (reason) {
  case ServerStopReason::Deadline:
    return Error::runtime(-ETIMEDOUT, "async server call deadline exceeded");
  case ServerStopReason::PeerCancellation:
    return Error::runtime(-ECANCELED, "peer cancelled async server call");
  case ServerStopReason::LocalClose:
    return Error::runtime(-ECANCELED, "async server call was closed");
  case ServerStopReason::ServerCancellation:
    return Error::runtime(-ECANCELED, "server cancelled async call");
  }
  return Error::runtime(-ECANCELED, "async server call was cancelled");
}

void ServerCallState::stop(ServerStopReason reason) noexcept {
  bool pending_application = false;
  bool settle_now = false;
  trevrpc_call* call = nullptr;
  const Error selected = stop_error(reason);
  {
    std::lock_guard lock(mutex_);
    if (terminal_phase_ == ServerTerminalPhase::Settled || final_selected_) {
      return;
    }
    external_stop_ = reason;
    final_selected_ = true;
    final_error_ = selected;
    pending_application = terminal_phase_ == ServerTerminalPhase::ApplicationPending;
    settle_now = terminal_phase_ == ServerTerminalPhase::Open;
    call = call_;
  }
  operation_->cancel();
  call_ops_->close(call);
  if (settle_now) {
    settle_without_application(selected);
  } else if (!pending_application) {
    cleanup_after_settlement(false, selected);
  }
}

ServerCallSnapshot ServerCallState::snapshot() const noexcept {
  std::lock_guard lock(mutex_);
  return {terminal_phase_, external_stop_, final_selected_, pin_released_};
}

Result<std::uint64_t> ServerScope::add(std::shared_ptr<ServerCallState> call) {
  if (!call) {
    return Error::runtime(-EINVAL, "server scope call must not be null");
  }
  std::lock_guard lock(mutex_);
  if (stopping_) {
    return Error::runtime(-ESHUTDOWN, "server scope is stopping");
  }
  if (next_id_ == 0) {
    return Error::runtime(-EOVERFLOW, "server scope identifier overflow");
  }
  const std::uint64_t id = next_id_++;
  try {
    calls_.emplace(id, std::move(call));
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to register async server call");
  }
  return id;
}

void ServerScope::complete(std::uint64_t id) noexcept {
  {
    std::lock_guard lock(mutex_);
    calls_.erase(id);
  }
  condition_.notify_all();
}

void ServerScope::request_stop(ServerStopReason reason) noexcept {
  {
    std::lock_guard lock(mutex_);
    if (stopping_) {
      return;
    }
    stopping_ = true;
  }

  std::uint64_t last_id = 0;
  for (;;) {
    std::shared_ptr<ServerCallState> call;
    {
      std::lock_guard lock(mutex_);
      const auto next = calls_.upper_bound(last_id);
      if (next == calls_.end()) {
        break;
      }
      last_id = next->first;
      call = next->second;
    }
    call->stop(reason);
  }
  condition_.notify_all();
}

Result<void> ServerScope::drain_until(Deadline deadline) noexcept {
  std::unique_lock lock(mutex_);
  const auto drained = [this] { return calls_.empty(); };
  if (deadline == Deadline::max()) {
    condition_.wait(lock, drained);
    return {};
  }
  if (!condition_.wait_until(lock, deadline, drained)) {
    return Error::runtime(-ETIMEDOUT, "async server scope drain timed out");
  }
  return {};
}

std::size_t ServerScope::active() const noexcept {
  std::lock_guard lock(mutex_);
  return calls_.size();
}

std::shared_ptr<OperationState> create_operation(trevrpc_stream* stream,
                                                 const std::shared_ptr<AsyncRuntime>& runtime,
                                                 std::shared_ptr<NativeOps> native_ops,
                                                 std::optional<Deadline> deadline) {
  return OperationState::create(stream, runtime, std::move(native_ops), deadline);
}

} // namespace trevrpc::detail
