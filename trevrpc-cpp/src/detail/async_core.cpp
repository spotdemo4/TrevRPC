#include "async_core.hpp"

#include "rpc_client.hpp"
#include "rpc_metadata.hpp"

#include <trevrpc/trevrpc.hpp>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <new>

namespace trevrpc::detail {

struct AsyncChannelAccess {
  static std::shared_ptr<ChannelCore> core(const std::shared_ptr<Channel>& channel) noexcept {
    if (!channel) {
      return nullptr;
    }
    std::lock_guard lock(channel->mutex_);
    return channel->core_;
  }
};

struct AsyncStreamAccess {
  static std::shared_ptr<RpcClientStream> take(ClientStream&& stream) noexcept {
    return std::move(stream.stream_);
  }
};

namespace {

std::atomic_bool injected_server_call_allocation_failure{false};

class Abi1ClientNativeOps final : public NativeOps {
public:
  explicit Abi1ClientNativeOps(std::shared_ptr<RpcClientStream> stream) noexcept
      : stream_(std::move(stream)) {}

  Result<void> start_send(std::span<const std::byte> body,
                          NativeCompletion completion) noexcept override {
    auto stream = acquire_stream();
    if (!stream) {
      return Error::runtime(-ESHUTDOWN, "async stream is closed");
    }
    return stream->start_send(body, [completion = std::move(completion)](Result<void> result) {
      completion(result ? 0 : result.error().code());
    });
  }

  Result<void> start_finish_send(NativeCompletion completion) noexcept override {
    auto stream = acquire_stream();
    if (!stream) {
      return Error::runtime(-ESHUTDOWN, "async stream is closed");
    }
    return stream->start_finish_send([completion = std::move(completion)](Result<void> result) {
      completion(result ? 0 : result.error().code());
    });
  }

  Result<void> start_receive(NativeReceiveCompletion completion) noexcept override {
    auto stream = acquire_stream();
    if (!stream) {
      return Error::runtime(-ESHUTDOWN, "async stream is closed");
    }
    return stream->start_receive(std::move(completion));
  }

  Result<void> schedule_at(Deadline deadline, Work work) noexcept override {
    auto stream = acquire_stream();
    if (!stream) {
      return Error::runtime(-ESHUTDOWN, "async stream is closed");
    }
    return stream->schedule_at(deadline, std::move(work));
  }

  void cancel() noexcept override {
    if (auto stream = acquire_stream()) {
      stream->cancel();
    }
  }

  Result<void> close() noexcept override {
    std::shared_ptr<RpcClientStream> stream;
    {
      std::lock_guard lock(mutex_);
      stream = std::move(stream_);
    }
    if (!stream) {
      return {};
    }
    auto closed = stream->close_result();
    if (!closed) {
      RpcClientStream::schedule_cleanup(std::move(stream));
    }
    return {};
  }

private:
  [[nodiscard]] std::shared_ptr<RpcClientStream> acquire_stream() noexcept {
    std::lock_guard lock(mutex_);
    return stream_;
  }

  std::mutex mutex_;
  std::shared_ptr<RpcClientStream> stream_;
};

[[nodiscard]] int wait_rpc_operation(const std::shared_ptr<RpcEventRuntime>& runtime,
                                     std::uint64_t operation_id) noexcept;
[[nodiscard]] int reserve_rpc_operation(const std::shared_ptr<RpcEventRuntime>& runtime,
                                        std::uint64_t* operation_id) noexcept;

struct Abi1ServerCallHandle final {
  std::shared_ptr<RpcEventRuntime> runtime;
  trevrpc_rpc_call_v1 call{};
  trevrpc_rpc_stream_v1 stream{};
  std::atomic<bool> cleanup_started{false};
  std::atomic<bool> stream_released{false};
  std::atomic<bool> terminal_success{false};

  struct CleanupState final : RpcCleanupWork {
    explicit CleanupState(std::shared_ptr<Abi1ServerCallHandle> handle_value,
                          bool force_close_value, Work completion_value) noexcept
        : handle(std::move(handle_value)), force_close(force_close_value),
          completion(std::move(completion_value)) {}

    [[nodiscard]] Result<void> cleanup_step() override {
      if (!handle || abandoned) {
        complete();
        return {};
      }
      auto& native = *handle;

      if (force_close && !close_completion_consumed) {
        if (close_operation == 0) {
          auto operation = native.runtime->reserve_operation();
          if (!operation) {
            return operation.error();
          }
          close_operation = operation.value();
        }
        if (!close_submitted) {
          const int close_error =
              trevrpc_rpc_stream_close(native.runtime->native_handle(), native.stream,
                                       close_operation, TREVRPC_RPC_CLOSE_FLAG_ABORT, 0);
          if (close_error != 0) {
            native.runtime->reject_operation(close_operation);
            close_operation = 0;
            if (terminal_close_error(close_error)) {
              close_completion_consumed = true;
            } else {
              return Error::runtime(close_error);
            }
          } else {
            close_submitted = true;
          }
        }
        if (close_submitted) {
          auto completion = native.runtime->try_wait_operation(close_operation);
          if (!completion) {
            return completion.error();
          }
          if (!completion.value()) {
            return Error::runtime(-EAGAIN);
          }
          note_event(completion.value().value());
          if (completion.value()->status != 0 &&
              !terminal_close_error(completion.value()->status)) {
            return Error::runtime(completion.value()->status);
          }
          close_completion_consumed = true;
        }
      }

      constexpr std::size_t max_receives_per_step = 32;
      constexpr std::size_t max_events_per_step = 16;
      std::size_t receive_count = 0;
      for (std::size_t event_count = 0;
           event_count < max_events_per_step && !(stream_closed && call_closed); ++event_count) {
        for (;;) {
          trevrpc_rpc_receive* receive = nullptr;
          const int receive_error =
              trevrpc_rpc_stream_receive(native.runtime->native_handle(), native.stream, &receive);
          if (receive_error == -EAGAIN) {
            break;
          }
          if ((receive_error == -ESTALE && stream_closed && call_closed) ||
              terminal_transport_error(receive_error)) {
            break;
          }
          if (receive_error != 0) {
            return Error::runtime(receive_error);
          }
          if (receive != nullptr) {
            trevrpc_rpc_receive_release(receive);
          }
          if (++receive_count == max_receives_per_step) {
            return Error::runtime(-EAGAIN);
          }
        }

        auto event = native.runtime->try_wait_stream(native.stream);
        if (!event) {
          return event.error();
        }
        if (!event.value()) {
          return Error::runtime(-EAGAIN);
        }
        note_event(event.value().value());
      }
      if (!stream_closed || !call_closed) {
        return Error::runtime(-EAGAIN);
      }

      if (!stream_unregistered) {
        native.runtime->unregister_stream(native.stream);
        stream_unregistered = true;
      }
      if (!stream_released) {
        const int stream_error =
            trevrpc_rpc_stream_release(native.runtime->native_handle(), native.stream);
        if (stream_error != 0 && stream_error != -ESTALE) {
          return Error::runtime(stream_error);
        }
        stream_released = true;
        native.stream_released.store(true, std::memory_order_release);
      }
      if (!call_released) {
        const int call_error =
            trevrpc_rpc_call_release(native.runtime->native_handle(), native.call);
        if (call_error != 0 && call_error != -ESTALE) {
          return Error::runtime(call_error);
        }
        call_released = true;
      }
      handle.reset();
      complete();
      return {};
    }

    void interrupt_cleanup() noexcept override {
      if (!handle || abandoned) {
        return;
      }
      if (close_operation != 0 && !close_submitted) {
        handle->runtime->reject_operation(close_operation);
        close_operation = 0;
      }
      handle->runtime->request_abandon();
    }

    void abandon_cleanup() noexcept override {
      if (!handle || abandoned) {
        return;
      }
      abandoned = true;
      if (!stream_unregistered) {
        handle->runtime->unregister_stream(handle->stream);
        stream_unregistered = true;
      }
      handle.reset();
      complete();
    }

  private:
    void complete() noexcept {
      Work notify = std::move(completion);
      if (!notify) {
        return;
      }
      try {
        notify();
      } catch (...) {
        (void)std::current_exception();
      }
    }

    [[nodiscard]] static bool terminal_transport_error(int error) noexcept {
      return error == -ECANCELED || error == -EPIPE || error == -ESHUTDOWN;
    }

    [[nodiscard]] static bool terminal_close_error(int error) noexcept {
      return terminal_transport_error(error) || error == -EALREADY || error == -ESTALE;
    }

    void note_event(const RpcEvent& event) noexcept {
      if (event.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED) {
        stream_closed = true;
      } else if (event.kind == TREVRPC_RPC_EVENT_CALL_CLOSED) {
        call_closed = true;
      }
    }

    std::shared_ptr<Abi1ServerCallHandle> handle;
    std::uint64_t close_operation = 0;
    bool force_close = false;
    bool close_submitted = false;
    bool close_completion_consumed = false;
    bool stream_closed = false;
    bool call_closed = false;
    bool stream_unregistered = false;
    bool stream_released = false;
    bool call_released = false;
    bool abandoned = false;
    Work completion;
  };

  static void begin_cleanup(const std::shared_ptr<Abi1ServerCallHandle>& handle, bool force_close,
                            Work completion) noexcept {
    if (!handle || handle->cleanup_started.exchange(true)) {
      return;
    }
    try {
      handle->runtime->schedule_cleanup(
          std::make_shared<CleanupState>(handle, force_close, std::move(completion)));
    } catch (...) {
      handle->runtime->request_abandon();
      handle->runtime->unregister_stream(handle->stream);
      if (completion) {
        try {
          completion();
        } catch (...) {
          (void)std::current_exception();
        }
      }
    }
  }
};

[[nodiscard]] int wait_rpc_operation(const std::shared_ptr<RpcEventRuntime>& runtime,
                                     std::uint64_t operation_id) noexcept {
  if (!runtime) {
    return -ESHUTDOWN;
  }
  auto event = runtime->wait_operation(operation_id);
  if (!event) {
    return event.error().code();
  }
  return event.value().status;
}

[[nodiscard]] int reserve_rpc_operation(const std::shared_ptr<RpcEventRuntime>& runtime,
                                        std::uint64_t* operation_id) noexcept {
  if (!runtime || operation_id == nullptr) {
    return -EINVAL;
  }
  auto reserved = runtime->reserve_operation();
  if (!reserved) {
    return reserved.error().code();
  }
  *operation_id = reserved.value();
  return 0;
}

class Abi1ServerStreamNativeOps final
    : public NativeOps,
      public std::enable_shared_from_this<Abi1ServerStreamNativeOps> {
public:
  explicit Abi1ServerStreamNativeOps(std::shared_ptr<Abi1ServerCallHandle> handle)
      : handle_(std::move(handle)) {}

  Result<void> start_send(std::span<const std::byte> body,
                          NativeCompletion completion) noexcept override {
    return start_operation(TREVRPC_RPC_EVENT_SEND_COMPLETE, std::move(completion),
                           [this, body](std::uint64_t operation) {
                             return trevrpc_rpc_stream_send_copy_v1(
                                 handle_->runtime->native_handle(), handle_->stream, operation,
                                 reinterpret_cast<const std::uint8_t*>(body.data()), body.size(),
                                 TREVRPC_RPC_SEND_FLAG_NONE);
                           });
  }

  Result<void> start_finish_send(NativeCompletion completion) noexcept override {
    return start_operation(TREVRPC_RPC_EVENT_SEND_FINISHED, std::move(completion),
                           [this](std::uint64_t operation) {
                             return trevrpc_rpc_stream_finish_send(
                                 handle_->runtime->native_handle(), handle_->stream, operation);
                           });
  }

  Result<void> start_receive(NativeReceiveCompletion completion) noexcept override {
    if (!completion) {
      return Error::runtime(-EINVAL, "async receive callback must not be empty");
    }
    try {
      receive_step(completion);
      return {};
    } catch (...) {
      return Error::runtime(-ENOMEM, "failed to start async server receive");
    }
  }

  Result<void> schedule_at(Deadline deadline, Work work) noexcept override {
    return handle_->runtime->schedule_at(deadline, std::move(work));
  }

  void cancel() noexcept override {
    std::uint64_t operation = 0;
    if (reserve_rpc_operation(handle_->runtime, &operation) != 0) {
      return;
    }
    const int error =
        trevrpc_rpc_call_cancel(handle_->runtime->native_handle(), handle_->call, operation, 0);
    if (error != 0) {
      handle_->runtime->reject_operation(operation);
      return;
    }
    auto subscribed =
        handle_->runtime->subscribe_operation(operation, [](const Result<RpcEvent>&) noexcept {});
    if (!subscribed) {
      handle_->runtime->reject_operation(operation);
    }
  }

  Result<void> close() noexcept override { return {}; }

private:
  template <typename Start>
  Result<void> start_operation(std::uint32_t expected_kind, NativeCompletion completion,
                               Start&& start) noexcept {
    if (!completion) {
      return Error::runtime(-EINVAL, "async native completion must not be empty");
    }
    std::uint64_t operation = 0;
    const int reserve_error = reserve_rpc_operation(handle_->runtime, &operation);
    if (reserve_error != 0) {
      return Error::runtime(reserve_error);
    }
    const int start_error = std::invoke(std::forward<Start>(start), operation);
    if (start_error != 0) {
      handle_->runtime->reject_operation(operation);
      return Error::runtime(start_error);
    }
    auto subscribed = handle_->runtime->subscribe_operation(
        operation, [expected_kind, completion = std::move(completion)](Result<RpcEvent> result) {
          if (!result) {
            completion(result.error().code());
          } else if (result.value().kind != expected_kind) {
            completion(-EIO);
          } else {
            completion(result.value().status);
          }
        });
    if (!subscribed) {
      handle_->runtime->reject_operation(operation);
      return subscribed.error();
    }
    return {};
  }

  Result<std::optional<StreamFrame>> try_receive_frame() noexcept {
    constexpr std::size_t max_receives_per_attempt = 32;
    for (std::size_t count = 0; count < max_receives_per_attempt; ++count) {
      trevrpc_rpc_receive* receive = nullptr;
      const int error =
          trevrpc_rpc_stream_receive(handle_->runtime->native_handle(), handle_->stream, &receive);
      if (error == -EAGAIN) {
        return std::optional<StreamFrame>{};
      }
      if (error != 0) {
        return Error::runtime(error);
      }
      if (receive == nullptr) {
        continue;
      }
      trevrpc_rpc_receive_info_v1 info{};
      int info_error = trevrpc_rpc_receive_info_v1_init(&info, sizeof(info));
      if (info_error == 0) {
        info_error = trevrpc_rpc_receive_get_info_v1(receive, &info);
      }
      if (info_error != 0) {
        trevrpc_rpc_receive_release(receive);
        return Error::runtime(info_error);
      }
      StreamFrame frame;
      if (info.kind == TREVRPC_RPC_RECEIVE_STATUS) {
        frame.terminal = true;
        frame.status = Status(static_cast<StatusCode>(info.rpc_status),
                              info.message == nullptr ? std::string{}
                                                      : std::string(info.message, info.message_len),
                              copy_rpc_metadata(info.metadata, info.metadata_count));
      } else if (info.kind == TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE ||
                 info.kind == TREVRPC_RPC_RECEIVE_MESSAGE) {
        if (info.data == nullptr && info.data_len != 0) {
          trevrpc_rpc_receive_release(receive);
          return Error::runtime(-EINVAL, "RPC receive returned an invalid body view");
        }
        frame.body.resize(info.data_len);
        if (info.data_len != 0) {
          std::memcpy(frame.body.data(), info.data, info.data_len);
        }
        frame.message = true;
      } else {
        trevrpc_rpc_receive_release(receive);
        return Error::runtime(-EPROTO, "RPC receive returned an unknown frame kind");
      }
      trevrpc_rpc_receive_release(receive);
      return std::optional<StreamFrame>(std::move(frame));
    }
    return Error::runtime(-ENOBUFS, "RPC receive burst exceeded the async drain bound");
  }

  void receive_step(const NativeReceiveCompletion& completion) noexcept {
    auto ready = try_receive_frame();
    if (!ready) {
      completion(ready.error());
      return;
    }
    if (ready.value()) {
      completion(std::move(ready.value()).value());
      return;
    }
    for (;;) {
      auto event = handle_->runtime->try_wait_stream(handle_->stream);
      if (!event) {
        completion(event.error());
        return;
      }
      if (!event.value()) {
        break;
      }
      const RpcEvent& stream_event = event.value().value();
      if (stream_event.kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN) {
        StreamFrame terminal;
        terminal.terminal = true;
        terminal.status = Status::ok();
        completion(std::move(terminal));
        return;
      }
      if (stream_event.kind == TREVRPC_RPC_EVENT_CALL_CLOSED ||
          stream_event.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED ||
          stream_event.kind == TREVRPC_RPC_EVENT_CALL_FAILED) {
        completion(Error::runtime(stream_event.status == 0 ? -ECONNRESET : stream_event.status));
        return;
      }
      if (stream_event.kind == TREVRPC_RPC_EVENT_STREAM_READABLE) {
        ready = try_receive_frame();
        if (!ready) {
          completion(ready.error());
          return;
        }
        if (ready.value()) {
          completion(std::move(ready.value()).value());
          return;
        }
      }
    }
    std::shared_ptr<NativeReceiveCompletion> retained_completion;
    try {
      retained_completion = std::make_shared<NativeReceiveCompletion>(completion);
    } catch (...) {
      completion(Error::runtime(-ENOMEM, "failed to retain async receive completion"));
      return;
    }
    auto self = shared_from_this();
    auto subscribed = handle_->runtime->subscribe_stream(
        handle_->stream,
        [self = std::move(self), retained_completion](Result<RpcEvent> event) mutable {
          if (!event) {
            (*retained_completion)(event.error());
            return;
          }
          const RpcEvent& stream_event = event.value();
          if (stream_event.kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN) {
            StreamFrame terminal;
            terminal.terminal = true;
            terminal.status = Status::ok();
            (*retained_completion)(std::move(terminal));
            return;
          }
          if (stream_event.kind == TREVRPC_RPC_EVENT_CALL_CLOSED ||
              stream_event.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED ||
              stream_event.kind == TREVRPC_RPC_EVENT_CALL_FAILED) {
            (*retained_completion)(
                Error::runtime(stream_event.status == 0 ? -ECONNRESET : stream_event.status));
            return;
          }
          self->receive_step(*retained_completion);
        });
    if (!subscribed) {
      (*retained_completion)(subscribed.error());
    }
  }

  std::shared_ptr<Abi1ServerCallHandle> handle_;
};

class Abi1ServerCallOps final : public RpcServerCallOps {
public:
  explicit Abi1ServerCallOps(std::shared_ptr<Abi1ServerCallHandle> handle)
      : handle_(std::move(handle)) {}

  void release(Work completion) noexcept override {
    if (released_) {
      return;
    }
    released_ = true;
    const auto handle = handle_;
    Abi1ServerCallHandle::begin_cleanup(
        handle, !handle->terminal_success.load(std::memory_order_acquire), std::move(completion));
  }
  Result<void> start_respond(const Status& status, std::span<const std::byte> body,
                             const Metadata& metadata,
                             NativeCompletion completion) noexcept override {
    trevrpc_rpc_status_v1 native_status{};
    int error = trevrpc_rpc_status_v1_init(&native_status, sizeof(native_status));
    if (error != 0) {
      return Error::runtime(error);
    }
    const bool metadata_valid = valid_rpc_metadata(metadata);
    const Metadata empty_metadata;
    const Metadata& effective_metadata = metadata_valid ? metadata : empty_metadata;
    const StatusCode effective_code = metadata_valid ? status.code() : StatusCode::Internal;
    const std::string_view effective_message = metadata_valid
                                                   ? std::string_view(status.message())
                                                   : std::string_view("invalid response metadata");
    std::vector<trevrpc_rpc_metadata_entry_v1> entries;
    entries.reserve(effective_metadata.entries().size());
    for (const auto& entry : effective_metadata.entries()) {
      entries.push_back({entry.key.data(), static_cast<std::uint32_t>(entry.key.size()), 0,
                         reinterpret_cast<const std::uint8_t*>(entry.value.data()),
                         entry.value.size()});
    }
    native_status.code = static_cast<std::uint32_t>(effective_code);
    native_status.message = effective_message.empty() ? nullptr : effective_message.data();
    native_status.message_len = effective_message.size();
    native_status.metadata_count = static_cast<std::uint32_t>(entries.size());
    native_status.metadata = entries.empty() ? nullptr : entries.data();
    std::uint64_t operation = 0;
    error = reserve_rpc_operation(handle_->runtime, &operation);
    if (error != 0) {
      return Error::runtime(error);
    }
    const std::byte empty{};
    const auto* message = body.empty() && effective_code != StatusCode::Ok
                              ? nullptr
                              : (body.empty() ? reinterpret_cast<const std::uint8_t*>(&empty)
                                              : reinterpret_cast<const std::uint8_t*>(body.data()));
    error = trevrpc_rpc_call_respond_copy_v1(handle_->runtime->native_handle(), handle_->call,
                                             operation, &native_status, message, body.size());
    if (error != 0) {
      handle_->runtime->reject_operation(operation);
      return Error::runtime(error);
    }
    return consume_terminal(operation, std::move(completion));
  }

  Result<void> start_finish(const Status& status, NativeCompletion completion) noexcept override {
    trevrpc_rpc_status_v1 native_status{};
    int error = trevrpc_rpc_status_v1_init(&native_status, sizeof(native_status));
    if (error != 0) {
      return Error::runtime(error);
    }
    const bool metadata_valid = valid_rpc_metadata(status.metadata());
    const Metadata empty_metadata;
    const Metadata& effective_metadata = metadata_valid ? status.metadata() : empty_metadata;
    const StatusCode effective_code = metadata_valid ? status.code() : StatusCode::Internal;
    const std::string_view effective_message = metadata_valid
                                                   ? std::string_view(status.message())
                                                   : std::string_view("invalid response metadata");
    std::vector<trevrpc_rpc_metadata_entry_v1> entries;
    entries.reserve(effective_metadata.entries().size());
    for (const auto& entry : effective_metadata.entries()) {
      entries.push_back({entry.key.data(), static_cast<std::uint32_t>(entry.key.size()), 0,
                         reinterpret_cast<const std::uint8_t*>(entry.value.data()),
                         entry.value.size()});
    }
    native_status.code = static_cast<std::uint32_t>(effective_code);
    native_status.message = effective_message.empty() ? nullptr : effective_message.data();
    native_status.message_len = effective_message.size();
    native_status.metadata_count = static_cast<std::uint32_t>(entries.size());
    native_status.metadata = entries.empty() ? nullptr : entries.data();
    std::uint64_t operation = 0;
    error = reserve_rpc_operation(handle_->runtime, &operation);
    if (error != 0) {
      return Error::runtime(error);
    }
    error = trevrpc_rpc_call_finish_v1(handle_->runtime->native_handle(), handle_->call, operation,
                                       &native_status);
    if (error != 0) {
      handle_->runtime->reject_operation(operation);
      return Error::runtime(error);
    }
    return consume_terminal(operation, std::move(completion));
  }

  void cancel() noexcept override {
    std::uint64_t operation = 0;
    if (reserve_rpc_operation(handle_->runtime, &operation) != 0) {
      return;
    }
    const int error =
        trevrpc_rpc_call_cancel(handle_->runtime->native_handle(), handle_->call, operation, 0);
    if (error != 0) {
      handle_->runtime->reject_operation(operation);
      return;
    }
    auto subscribed =
        handle_->runtime->subscribe_operation(operation, [](const Result<RpcEvent>&) noexcept {});
    if (!subscribed) {
      handle_->runtime->reject_operation(operation);
    }
  }

  void close() noexcept override {
    std::uint64_t operation = 0;
    if (reserve_rpc_operation(handle_->runtime, &operation) != 0) {
      return;
    }
    const int error = trevrpc_rpc_call_close(handle_->runtime->native_handle(), handle_->call,
                                             operation, TREVRPC_RPC_CLOSE_FLAG_NONE, 0);
    if (error != 0) {
      handle_->runtime->reject_operation(operation);
      return;
    }
    auto subscribed =
        handle_->runtime->subscribe_operation(operation, [](const Result<RpcEvent>&) noexcept {});
    if (!subscribed) {
      handle_->runtime->reject_operation(operation);
    }
  }

private:
  Result<void> consume_terminal(std::uint64_t operation, NativeCompletion completion) noexcept {
    auto handle = handle_;
    auto subscribed = handle_->runtime->subscribe_operation(
        operation,
        [handle = std::move(handle), completion = std::move(completion)](Result<RpcEvent> result) {
          const int error = !result ? result.error().code() : result.value().status;
          if (error == 0) {
            handle->terminal_success.store(true, std::memory_order_release);
          }
          completion(error);
        });
    if (!subscribed) {
      handle_->runtime->reject_operation(operation);
      return subscribed.error();
    }
    return {};
  }

  std::shared_ptr<Abi1ServerCallHandle> handle_;
  bool released_ = false;
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
                                     AsyncRuntimeOptions options) noexcept
    : continuation_(std::move(continuation)), options_(options) {}

std::shared_ptr<OperationState>
OperationState::create(const std::shared_ptr<AsyncRuntime>& runtime,
                       std::shared_ptr<NativeOps> native_ops, std::optional<Deadline> deadline,
                       std::shared_ptr<Cancellation> cancellation_bridge,
                       std::shared_ptr<CancellationState> cancellation_state,
                       std::uint64_t cancellation_registration, bool terminal_stops_send) {
  if (!runtime || !runtime->state_ || !native_ops) {
    return nullptr;
  }
  try {
    return std::make_shared<OperationState>(
        runtime->state_, std::move(native_ops), deadline, std::move(cancellation_bridge),
        std::move(cancellation_state), cancellation_registration, terminal_stops_send);
  } catch (...) {
    return nullptr;
  }
}

OperationState::OperationState(std::shared_ptr<AsyncRuntimeState> runtime,
                               std::shared_ptr<NativeOps> native_ops,
                               std::optional<Deadline> deadline,
                               std::shared_ptr<Cancellation> cancellation_bridge,
                               std::shared_ptr<CancellationState> cancellation_state,
                               std::uint64_t cancellation_registration, bool terminal_stops_send)
    : runtime_(std::move(runtime)), native_ops_(std::move(native_ops)), deadline_(deadline),
      cancellation_bridge_(std::move(cancellation_bridge)),
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
    action = [native_ops = native_ops_](std::span<const std::byte>,
                                        NativeCompletion completion) noexcept {
      return native_ops->start_finish_send(std::move(completion));
    };
  } else {
    action = [native_ops = native_ops_](std::span<const std::byte> body,
                                        NativeCompletion completion) noexcept {
      return native_ops->start_send(body, std::move(completion));
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
      auto scheduled = native_ops_->schedule_at(*waiter->deadline, [weak, waiter] {
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
  auto started = item->action(item->bytes, [self = std::move(self), item](int error) {
    self->finish_send_item(item, error);
    self->finish_native_work();
  });
  if (!started) {
    finish_send_item(item, started.error().code());
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

void OperationState::close_native_once() noexcept {
  {
    std::lock_guard lock(mutex_);
    if (native_closed_ || native_close_in_progress_) {
      return;
    }
    native_close_in_progress_ = true;
    ++native_close_attempts_;
  }

  const auto result = native_ops_->close();
  bool retry = false;
  {
    std::lock_guard lock(mutex_);
    native_close_in_progress_ = false;
    if (result) {
      native_closed_ = true;
    } else {
      const int error = result.error().code();
      constexpr std::size_t max_close_attempts = 32;
      retry = (error == -EAGAIN || error == -EBUSY || error == -EDEADLK) &&
              native_close_attempts_ < max_close_attempts;
      if (!retry) {
        native_closed_ = true;
      }
    }
  }
  if (!retry) {
    return;
  }

  auto self = weak_from_this().lock();
  if (!self) {
    std::lock_guard lock(mutex_);
    native_closed_ = true;
    return;
  }
  const auto scheduled =
      native_ops_->schedule_at(Deadline::clock::now() + std::chrono::milliseconds(1),
                               [self = std::move(self)] { self->close_native_once(); });
  if (!scheduled) {
    std::lock_guard lock(mutex_);
    native_closed_ = true;
  }
}

void OperationState::finish_native_work() noexcept {
  bool close_requested = false;
  std::vector<Work> idle_callbacks;
  {
    std::lock_guard lock(mutex_);
    if (native_work_ == 0) {
      std::terminate();
    }
    --native_work_;
    if (native_work_ == 0 && close_requested_) {
      close_requested_ = false;
      close_requested = true;
    }
    collect_idle_callbacks_locked(idle_callbacks);
  }
  if (close_requested) {
    close_native_once();
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

  if (deadline_) {
    if (Deadline::clock::now() >= *deadline_) {
      settle_receive(completion, Error::runtime(-ETIMEDOUT, "async call deadline exceeded"));
      cancel();
      co_return co_await *completion;
    }
    std::weak_ptr<OperationState> weak = shared_from_this();
    auto scheduled = native_ops_->schedule_at(*deadline_, [weak, completion] {
      if (auto operation = weak.lock()) {
        operation->settle_receive(completion,
                                  Error::runtime(-ETIMEDOUT, "async call deadline exceeded"));
        operation->cancel();
      }
    });
    if (!scheduled) {
      settle_receive(completion, scheduled.error());
      co_return co_await *completion;
    }
  }

  auto self = shared_from_this();
  auto started =
      native_ops_->start_receive([self = std::move(self), completion](Result<StreamFrame> result) {
        self->settle_receive(completion, std::move(result));
      });
  if (!started) {
    settle_receive(completion, started.error());
  }
  co_return co_await *completion;
}

void OperationState::settle_receive(
    const std::shared_ptr<AsyncCompletion<Result<StreamFrame>>>& completion,
    Result<StreamFrame> result) noexcept {
  std::vector<std::shared_ptr<AsyncCompletion<Result<void>>>> send_completions;
  std::vector<Work> idle_callbacks;
  bool cancel_native = false;
  bool close_native = false;
  bool settled = false;
  const bool terminal = result && result.value().terminal;
  {
    std::lock_guard lock(mutex_);
    if (receive_pending_ && receive_completion_ == completion) {
      receive_pending_ = false;
      receive_completion_.reset();
      receive_terminal_ = terminal;
      settled = true;
      if (terminal) {
        close_requested_ = true;
        if (native_work_ == 0) {
          close_native = true;
          close_requested_ = false;
        }
      }
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
          cancel_native = true;
        }
        collect_idle_callbacks_locked(idle_callbacks);
      }
    }
  }
  if (cancel_native) {
    native_ops_->cancel();
  }
  if (close_native) {
    close_native_once();
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
  }
  native_ops_->cancel();

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
  bool close_now = false;
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
      close_now = true;
    } else {
      close_requested_ = true;
    }
  }
  if (cancellation_state) {
    cancellation_state->unregister_callback(cancellation_registration);
  }
  if (close_now) {
    close_native_once();
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
    native_closed_ = true;
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
  if (options.deadline) {
    const auto now = Deadline::clock::now();
    if (now >= *options.deadline) {
      if (cancellation_state) {
        cancellation_state->unregister_callback(registration);
      }
      co_return Error::runtime(-ETIMEDOUT, "async call deadline exceeded");
    }
    options.call_options.timeout =
        std::chrono::duration_cast<std::chrono::nanoseconds>(*options.deadline - now);
  }
  auto core = AsyncChannelAccess::core(channel);
  if (!core) {
    if (cancellation_state) {
      cancellation_state->unregister_callback(registration);
    }
    co_return Error::runtime(-EINVAL, "async client channel is closed");
  }
  auto started = RpcClientStream::open_async(
      core, std::move(service), std::move(method), TREVRPC_RPC_KIND_UNARY, std::move(request),
      std::move(options.call_options),
      [completion, cancellation_state, registration,
       bridge](Result<std::shared_ptr<RpcClientStream>> stream_result) mutable noexcept {
        try {
          if (!stream_result) {
            if (cancellation_state) {
              cancellation_state->unregister_callback(registration);
            }
            completion->complete(stream_result.error());
            return;
          }
          auto stream = std::move(stream_result).value();
          auto receive_started =
              stream->start_receive([stream, completion, cancellation_state, registration,
                                     bridge](Result<StreamFrame> frame) mutable noexcept {
                try {
                  if (cancellation_state) {
                    cancellation_state->unregister_callback(registration);
                  }
                  Result<ByteResponse> result =
                      Error::protobuf("unary RPC did not return exactly one response message");
                  if (!frame) {
                    result = frame.error();
                  } else if (frame.value().terminal &&
                             (!frame.value().status.is_ok() || frame.value().message)) {
                    ByteResponse response;
                    response.status = std::move(frame.value().status);
                    response.metadata = response.status.metadata();
                    response.body = std::move(frame.value().body);
                    result = std::move(response);
                  }
                  stream->close();
                  completion->complete(std::move(result));
                } catch (...) {
                  if (cancellation_state) {
                    cancellation_state->unregister_callback(registration);
                  }
                  stream->close();
                  completion->complete(
                      Error::runtime(-ENOMEM, "failed to complete asynchronous unary RPC"));
                }
              });
          if (!receive_started) {
            if (cancellation_state) {
              cancellation_state->unregister_callback(registration);
            }
            stream->close();
            completion->complete(receive_started.error());
          }
        } catch (...) {
          if (cancellation_state) {
            cancellation_state->unregister_callback(registration);
          }
          completion->complete(
              Error::runtime(-ENOMEM, "failed to start asynchronous unary receive"));
        }
      });
  if (!started) {
    if (cancellation_state) {
      cancellation_state->unregister_callback(registration);
    }
    co_return started.error();
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
  if (options.deadline) {
    const auto now = Deadline::clock::now();
    if (now >= *options.deadline) {
      if (cancellation_state) {
        cancellation_state->unregister_callback(registration);
      }
      co_return Error::runtime(-ETIMEDOUT, "async call deadline exceeded");
    }
    options.call_options.timeout =
        std::chrono::duration_cast<std::chrono::nanoseconds>(*options.deadline - now);
  }
  auto core = AsyncChannelAccess::core(channel);
  if (!core) {
    if (cancellation_state) {
      cancellation_state->unregister_callback(registration);
    }
    co_return Error::runtime(-EINVAL, "async client channel is closed");
  }
  const auto deadline = options.deadline;
  auto started = RpcClientStream::open_async(
      core, std::move(service), std::move(method), kind, std::move(request),
      std::move(options.call_options),
      [runtime = std::move(runtime), completion, cancellation_state, bridge, registration,
       deadline](Result<std::shared_ptr<RpcClientStream>> stream_result) mutable noexcept {
        try {
          if (!stream_result) {
            if (cancellation_state) {
              cancellation_state->unregister_callback(registration);
            }
            completion->complete(stream_result.error());
            return;
          }
          auto native_ops = std::make_shared<Abi1ClientNativeOps>(std::move(stream_result).value());
          auto operation = OperationState::create(runtime, std::move(native_ops), deadline, bridge,
                                                  cancellation_state, 0, true);
          if (!operation) {
            if (cancellation_state) {
              cancellation_state->unregister_callback(registration);
            }
            completion->complete(Error::runtime(-ENOMEM, "failed to create async stream state"));
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
              completion->complete(
                  Error::runtime(-ENOMEM, "failed to attach async stream cancellation"));
              return;
            }
          }
          completion->complete(std::move(operation));
        } catch (...) {
          if (cancellation_state) {
            cancellation_state->unregister_callback(registration);
          }
          completion->complete(Error::runtime(-ENOMEM, "failed to create async stream state"));
        }
      });
  if (!started) {
    if (cancellation_state) {
      cancellation_state->unregister_callback(registration);
    }
    co_return started.error();
  }
  co_return co_await *completion;
}

CallContext ServerCallState::context() const {
  return incoming_.has_value() ? CallContext(incoming_->context, incoming_->metadata)
                               : CallContext{};
}

CallContext server_call_context(const std::shared_ptr<ServerCallState>& state) {
  return state ? state->context() : CallContext{};
}

std::span<const std::byte>
server_initial_message(const std::shared_ptr<ServerCallState>& state) noexcept {
  return state ? state->initial_message() : std::span<const std::byte>{};
}

Result<std::optional<std::vector<std::byte>>> ServerCallState::receive_message() {
  std::vector<std::byte> initial;
  {
    std::lock_guard lock(mutex_);
    if (!initial_message_consumed_ && incoming_.has_value()) {
      initial_message_consumed_ = true;
      initial = incoming_->initial_message;
      const bool streaming_request = kind_ == TREVRPC_RPC_KIND_CLIENT_STREAMING ||
                                     kind_ == TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING;
      if (!streaming_request || !initial.empty()) {
        return std::optional<std::vector<std::byte>>(std::move(initial));
      }
    }
  }
  if (!operation_) {
    return Error::runtime(-EINVAL, "server call state is empty");
  }
  auto frame = sync_wait(operation_->receive());
  if (!frame) {
    return frame.error();
  }
  if (frame.value().terminal) {
    if (!frame.value().status.is_ok()) {
      return Error::rpc(std::move(frame.value().status));
    }
    return std::optional<std::vector<std::byte>>{};
  }
  return std::optional<std::vector<std::byte>>(std::move(frame.value().body));
}

Result<void> send_server_message(const std::shared_ptr<ServerCallState>& state,
                                 std::span<const std::byte> body) {
  if (!state || !state->operation()) {
    return Error::runtime(-EINVAL, "server call state is empty");
  }
  std::vector<std::byte> owned(body.begin(), body.end());
  const std::size_t encoded_size = owned.size();
  auto result = sync_wait(state->operation()->send(
      encoded_size,
      [owned = std::move(owned)]() mutable {
        return Result<std::vector<std::byte>>(std::move(owned));
      },
      {}, false));
  return result;
}

Result<std::optional<std::vector<std::byte>>>
receive_server_message(const std::shared_ptr<ServerCallState>& state) {
  if (!state) {
    return Error::runtime(-EINVAL, "server call state is empty");
  }
  return state->receive_message();
}

Result<std::shared_ptr<ServerCallState>>
ServerCallState::create_for_test(std::uint32_t kind, const std::shared_ptr<AsyncRuntime>& runtime,
                                 std::shared_ptr<RpcServerCallOps> call_ops,
                                 std::shared_ptr<NativeOps> stream_ops) {
  if (!runtime || !call_ops || !stream_ops) {
    return Error::runtime(-EINVAL, "invalid test server call state");
  }
  auto operation = OperationState::create(runtime, std::move(stream_ops), std::nullopt);
  if (!operation) {
    return Error::runtime(-ENOMEM, "failed to create test server operation");
  }
  try {
    auto state = std::shared_ptr<ServerCallState>(new ServerCallState(kind));
    state->operation_ = std::move(operation);
    state->rpc_call_ops_ = std::move(call_ops);
    return state;
  } catch (...) {
    call_ops->close();
    call_ops->release({});
    return Error::runtime(-ENOMEM, "failed to allocate test server call state");
  }
}

void ServerCallState::test_fail_next_rpc_allocation() noexcept {
  injected_server_call_allocation_failure.store(true, std::memory_order_release);
}

Result<std::shared_ptr<ServerCallState>>
ServerCallState::create_rpc(RpcIncomingCall incoming, std::shared_ptr<RpcEventRuntime> rpc_runtime,
                            const std::shared_ptr<AsyncRuntime>& runtime, bool* accepted) {
  if (accepted != nullptr) {
    *accepted = false;
  }
  if (!rpc_runtime || !runtime || incoming.call.owner == 0 || incoming.stream.owner == 0) {
    return Error::runtime(-EINVAL, "invalid ABI1 incoming server call");
  }

  std::shared_ptr<Abi1ServerCallHandle> handle;
  std::shared_ptr<Abi1ServerCallOps> call_ops;
  std::shared_ptr<Abi1ServerStreamNativeOps> stream_ops;
  std::shared_ptr<ServerCallState> state;
  try {
    if (injected_server_call_allocation_failure.exchange(false, std::memory_order_acq_rel)) {
      throw std::bad_alloc();
    }
    handle = std::make_shared<Abi1ServerCallHandle>();
    handle->runtime = rpc_runtime;
    handle->call = incoming.call;
    handle->stream = incoming.stream;
    call_ops = std::make_shared<Abi1ServerCallOps>(handle);
    stream_ops = std::make_shared<Abi1ServerStreamNativeOps>(handle);
    state = std::shared_ptr<ServerCallState>(new ServerCallState(incoming.rpc_kind));
  } catch (...) {
    (void)rpc_runtime->reject_incoming(incoming);
    return Error::runtime(-ENOMEM, "failed to allocate ABI1 incoming call state");
  }

  auto registered = rpc_runtime->register_stream(incoming.stream);
  if (!registered) {
    call_ops->close();
    call_ops->release({});
    return registered.error();
  }
  if (incoming.preparation_status != 0) {
    call_ops->close();
    call_ops->release({});
    return Error::runtime(incoming.preparation_status, "failed to prepare ABI1 incoming call");
  }
  std::uint64_t accept_operation = 0;
  int error = reserve_rpc_operation(rpc_runtime, &accept_operation);
  if (error == 0) {
    error = trevrpc_rpc_call_accept(rpc_runtime->native_handle(), incoming.call, accept_operation);
    if (error == 0) {
      error = wait_rpc_operation(rpc_runtime, accept_operation);
    } else {
      rpc_runtime->reject_operation(accept_operation);
    }
  }
  if (error != 0) {
    call_ops->close();
    call_ops->release({});
    return Error::runtime(error, "failed to accept ABI1 incoming call");
  }

  auto operation = OperationState::create(runtime, std::move(stream_ops), std::nullopt);
  if (!operation) {
    call_ops->close();
    call_ops->release({});
    return Error::runtime(-ENOMEM, "failed to create ABI1 server operation");
  }
  try {
    state->incoming_.emplace(std::move(incoming));
  } catch (...) {
    call_ops->close();
    call_ops->release({});
    return Error::runtime(-ENOMEM, "failed to allocate ABI1 server call state");
  }
  state->operation_ = std::move(operation);
  state->rpc_call_ops_ = std::move(call_ops);
  state->rpc_runtime_ = std::move(rpc_runtime);
  state->rpc_call_owner_ = std::move(handle);
  if (accepted != nullptr) {
    *accepted = true;
  }
  return state;
}

ServerCallState::~ServerCallState() {
  std::shared_ptr<OperationState> operation;
  std::shared_ptr<RpcServerCallOps> rpc_call_ops;
  ServerCallTerminalObserver observer;
  std::optional<ServerCallTerminal> terminal;
  {
    std::lock_guard lock(mutex_);
    if (pin_released_ || !rpc_call_ops_) {
      return;
    }
    if (!terminal_result_) {
      terminal_phase_ = ServerTerminalPhase::Settled;
      final_selected_ = true;
      final_error_ = Error::runtime(-ECANCELED, "async server call was abandoned");
      terminal_result_ = ServerCallTerminal{StatusCode::Cancelled, 0};
    }
    claim_terminal_observer_locked(observer, terminal);
    pin_released_ = true;
    operation = operation_;
    rpc_call_ops = rpc_call_ops_;
  }
  if (terminal) {
    notify_terminal(observer, *terminal);
  }
  if (operation) {
    operation->cancel();
  }
  rpc_call_ops->close();
  if (operation) {
    operation->when_send_idle(
        [operation = std::move(operation), rpc_call_ops = std::move(rpc_call_ops)] {
          operation->retire(Error::runtime(-ECANCELED, "async server call was abandoned"));
          rpc_call_ops->release({});
        });
  } else {
    rpc_call_ops->release({});
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
    co_return Error::runtime(-ENOTSUP, "server call does not use a unary response");
  }
  std::shared_ptr<RpcServerCallOps> rpc_call_ops;
  {
    std::lock_guard lock(self->mutex_);
    rpc_call_ops = self->rpc_call_ops_;
  }
  std::weak_ptr<ServerCallState> weak = self;
  const StatusCode status_code = status.code();
  NativeSendAction action = [rpc_call_ops = std::move(rpc_call_ops), status = std::move(status),
                             metadata = std::move(metadata),
                             weak](std::span<const std::byte> encoded,
                                   NativeCompletion completion) noexcept -> Result<void> {
    if (!rpc_call_ops) {
      return Error::runtime(-EALREADY);
    }
    return rpc_call_ops->start_respond(status, encoded, metadata,
                                       [weak, completion = std::move(completion)](int error) {
                                         if (auto state = weak.lock()) {
                                           state->record_native_terminal_result(error);
                                         }
                                         completion(error);
                                       });
  };
  const std::size_t encoded_size = body.size();
  co_return co_await self->terminal(
      status_code, encoded_size,
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
    co_return Error::runtime(-ENOTSUP, "server call requires a unary response");
  }
  std::shared_ptr<RpcServerCallOps> rpc_call_ops;
  {
    std::lock_guard lock(self->mutex_);
    rpc_call_ops = self->rpc_call_ops_;
  }
  std::weak_ptr<ServerCallState> weak = self;
  const StatusCode status_code = status.code();
  NativeSendAction action = [rpc_call_ops = std::move(rpc_call_ops), status = std::move(status),
                             weak](std::span<const std::byte>,
                                   NativeCompletion completion) noexcept -> Result<void> {
    if (!rpc_call_ops) {
      return Error::runtime(-EALREADY);
    }
    return rpc_call_ops->start_finish(status,
                                      [weak, completion = std::move(completion)](int error) {
                                        if (auto state = weak.lock()) {
                                          state->record_native_terminal_result(error);
                                        }
                                        completion(error);
                                      });
  };
  co_return co_await self->terminal(
      status_code, 0, [] { return Result<std::vector<std::byte>>(std::vector<std::byte>{}); },
      std::move(action));
}

void ServerCallState::set_terminal_observer(ServerCallTerminalObserver observer) noexcept {
  if (!observer) {
    return;
  }
  ServerCallTerminalObserver notify;
  std::optional<ServerCallTerminal> terminal;
  {
    std::lock_guard lock(mutex_);
    if (terminal_observer_called_ || terminal_observer_) {
      return;
    }
    terminal_observer_ = std::move(observer);
    claim_terminal_observer_locked(notify, terminal);
  }
  if (terminal) {
    notify_terminal(notify, *terminal);
  }
}

void ServerCallState::set_cleanup_observer(Work observer) noexcept {
  if (!observer) {
    return;
  }
  Work notify;
  {
    std::lock_guard lock(mutex_);
    if (cleanup_observer_called_ || cleanup_observer_) {
      return;
    }
    cleanup_observer_ = std::move(observer);
    if (cleanup_completed_) {
      cleanup_observer_called_ = true;
      notify = std::move(cleanup_observer_);
    }
  }
  if (notify) {
    try {
      notify();
    } catch (...) {
      (void)std::current_exception();
    }
  }
}

Task<Result<void>>
ServerCallState::terminal(StatusCode status, std::size_t encoded_size,
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
      selected_status_ = status;
      selected_response_body_size_ = encoded_size;
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

  auto result = co_await operation_->send_action(encoded_size, std::move(serializer), {}, false,
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
  ServerCallTerminalObserver observer;
  std::optional<ServerCallTerminal> terminal;
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
    terminal_result_ =
        external_stop_
            ? ServerCallTerminal{stop_status(*external_stop_), 0}
            : ServerCallTerminal{selected_status_, result ? selected_response_body_size_ : 0};
    claim_terminal_observer_locked(observer, terminal);
    publish_final_locked(waiters, final_error);
    if (final_error_) {
      retire_reason = *final_error_;
    }
  }
  if (terminal) {
    notify_terminal(observer, *terminal);
  }
  cleanup_after_settlement(close_call, retire_reason);
  for (const auto& waiter : waiters) {
    waiter->complete(final_error ? Result<void>(*final_error) : Result<void>{});
  }
}

void ServerCallState::settle_without_application(const Error& result) noexcept {
  std::vector<std::shared_ptr<AsyncCompletion<Result<void>>>> waiters;
  std::optional<Error> final_error;
  ServerCallTerminalObserver observer;
  std::optional<ServerCallTerminal> terminal;
  {
    std::lock_guard lock(mutex_);
    terminal_phase_ = ServerTerminalPhase::Settled;
    if (!final_selected_) {
      final_selected_ = true;
      final_error_ = result;
    }
    terminal_result_ =
        ServerCallTerminal{external_stop_ ? stop_status(*external_stop_) : StatusCode::Unknown, 0};
    claim_terminal_observer_locked(observer, terminal);
    publish_final_locked(waiters, final_error);
  }
  if (terminal) {
    notify_terminal(observer, *terminal);
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

void ServerCallState::claim_terminal_observer_locked(
    ServerCallTerminalObserver& observer, std::optional<ServerCallTerminal>& terminal) noexcept {
  if (terminal_observer_called_ || !terminal_result_ || !terminal_observer_) {
    return;
  }
  terminal_observer_called_ = true;
  observer = std::move(terminal_observer_);
  terminal = terminal_result_;
}

void ServerCallState::notify_terminal(const ServerCallTerminalObserver& observer,
                                      ServerCallTerminal terminal) noexcept {
  if (!observer) {
    return;
  }
  try {
    observer(terminal);
  } catch (...) {
    (void)std::current_exception();
  }
}

void ServerCallState::finish_cleanup() noexcept {
  Work observer;
  {
    std::lock_guard lock(mutex_);
    cleanup_completed_ = true;
    if (!cleanup_observer_called_ && cleanup_observer_) {
      cleanup_observer_called_ = true;
      observer = std::move(cleanup_observer_);
    }
  }
  if (observer) {
    try {
      observer();
    } catch (...) {
      (void)std::current_exception();
    }
  }
}

void ServerCallState::cleanup_after_settlement(bool close_call, Error retire_reason) noexcept {
  std::shared_ptr<OperationState> operation;
  std::shared_ptr<RpcServerCallOps> rpc_call_ops;
  {
    std::lock_guard lock(mutex_);
    if (pin_released_ || !rpc_call_ops_) {
      return;
    }
    pin_released_ = true;
    operation = operation_;
    rpc_call_ops = rpc_call_ops_;
  }
  if (close_call) {
    rpc_call_ops->close();
  }
  std::weak_ptr<ServerCallState> weak = weak_from_this();
  Work completion = [weak] {
    if (auto state = weak.lock()) {
      state->finish_cleanup();
    }
  };
  if (operation) {
    operation->when_send_idle(
        [operation = std::move(operation), rpc_call_ops = std::move(rpc_call_ops),
         retire_reason = std::move(retire_reason), completion = std::move(completion)]() mutable {
          operation->retire(retire_reason);
          rpc_call_ops->release(std::move(completion));
        });
  } else {
    rpc_call_ops->release(std::move(completion));
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

StatusCode ServerCallState::stop_status(ServerStopReason reason) noexcept {
  switch (reason) {
  case ServerStopReason::Deadline:
    return StatusCode::DeadlineExceeded;
  case ServerStopReason::PeerCancellation:
  case ServerStopReason::LocalClose:
  case ServerStopReason::ServerCancellation:
    return StatusCode::Cancelled;
  }
  return StatusCode::Cancelled;
}

void ServerCallState::stop(ServerStopReason reason) noexcept {
  bool pending_application = false;
  bool settle_now = false;
  std::shared_ptr<OperationState> operation;
  std::shared_ptr<RpcServerCallOps> rpc_call_ops;
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
    operation = operation_;
    rpc_call_ops = rpc_call_ops_;
    pin_released_ = true;
  }
  if (operation) {
    operation->cancel();
  }
  if (rpc_call_ops) {
    rpc_call_ops->close();
    std::weak_ptr<ServerCallState> weak = weak_from_this();
    Work completion = [weak] {
      if (auto state = weak.lock()) {
        state->finish_cleanup();
      }
    };
    if (operation) {
      operation->when_send_idle([operation = std::move(operation),
                                 rpc_call_ops = std::move(rpc_call_ops), selected,
                                 completion = std::move(completion)]() mutable {
        operation->retire(selected);
        rpc_call_ops->release(std::move(completion));
      });
    } else {
      rpc_call_ops->release(std::move(completion));
    }
  }
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

std::shared_ptr<OperationState> create_operation(const std::shared_ptr<AsyncRuntime>& runtime,
                                                 std::shared_ptr<NativeOps> native_ops,
                                                 std::optional<Deadline> deadline) {
  return OperationState::create(runtime, std::move(native_ops), deadline);
}

} // namespace trevrpc::detail
