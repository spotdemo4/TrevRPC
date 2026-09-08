#include "rpc_client.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace trevrpc::detail {
namespace {

[[nodiscard]] bool fits_u32(std::size_t value) noexcept {
  return value <= std::numeric_limits<std::uint32_t>::max();
}

[[nodiscard]] StatusCode status_code(std::uint32_t code) noexcept {
  return code <= TREVRPC_RPC_STATUS_UNAUTHENTICATED ? static_cast<StatusCode>(code)
                                                    : StatusCode::Unknown;
}

[[nodiscard]] bool null_handle(trevrpc_rpc_stream_v1 value) noexcept {
  return value.owner == 0 && value.slot == 0 && value.generation == 0;
}

[[nodiscard]] bool null_handle(trevrpc_rpc_call_v1 value) noexcept {
  return value.owner == 0 && value.slot == 0 && value.generation == 0;
}

[[nodiscard]] Result<Metadata> copy_rpc_metadata(const trevrpc_rpc_receive_info_v1& info) {
  Metadata result;
  for (std::uint32_t index = 0; index < info.metadata_count; ++index) {
    const auto& entry = info.metadata[index];
    if ((entry.key == nullptr && entry.key_len != 0) ||
        (entry.value == nullptr && entry.value_len != 0)) {
      return Error::runtime(-EINVAL, "runtime returned invalid RPC metadata");
    }
    result.set(std::string(entry.key == nullptr ? "" : entry.key, entry.key_len),
               std::span(reinterpret_cast<const std::byte*>(entry.value),
                         static_cast<std::size_t>(entry.value_len)));
  }
  return result;
}

[[nodiscard]] Status status_from_info(const trevrpc_rpc_receive_info_v1& info, Metadata metadata) {
  const std::string message =
      info.message == nullptr ? std::string{} : std::string(info.message, info.message_len);
  return Status(status_code(info.rpc_status), message, std::move(metadata));
}

std::atomic_bool& receive_allocation_failure() noexcept {
  static std::atomic_bool failure{false};
  return failure;
}

std::atomic_bool& cleanup_owner_allocation_failure() noexcept {
  static std::atomic_bool failure{false};
  return failure;
}

std::atomic_bool& cleanup_task_allocation_failure() noexcept {
  static std::atomic_bool failure{false};
  return failure;
}

std::atomic_bool& cleanup_requeue_allocation_failure() noexcept {
  static std::atomic_bool failure{false};
  return failure;
}

class ReceiveGuard final {
public:
  explicit ReceiveGuard(trevrpc_rpc_receive* receive) noexcept : receive_(receive) {}
  ~ReceiveGuard() {
    if (receive_ != nullptr) {
      trevrpc_rpc_receive_release(receive_);
    }
  }
  ReceiveGuard(const ReceiveGuard&) = delete;
  ReceiveGuard& operator=(const ReceiveGuard&) = delete;
  void release() noexcept { receive_ = nullptr; }

private:
  trevrpc_rpc_receive* receive_;
};

[[nodiscard]] bool runtime_terminal_cleanup_error(int error) noexcept {
  return error == -ESHUTDOWN || error == -ECANCELED || error == -ESTALE || error == -EPIPE ||
         error == -EIO;
}

void abandon_cleanup_owner(const std::shared_ptr<RpcClientStream>& stream) noexcept {
  if (!stream) {
    return;
  }
  stream->interrupt_cleanup();
  stream->abandon_cleanup();
}

class AsyncClientOpen final : public std::enable_shared_from_this<AsyncClientOpen> {
public:
  AsyncClientOpen(std::shared_ptr<RpcEventRuntime> runtime, std::shared_ptr<RpcClientStream> owner,
                  trevrpc_rpc_endpoint_v1 endpoint, trevrpc_rpc_call_config_v1 config,
                  Metadata metadata_owner, std::string service, std::string method,
                  std::vector<std::byte> initial_message, RpcClientStream::OpenCallback callback)
      : runtime_(std::move(runtime)), owner_(std::move(owner)), endpoint_(endpoint),
        config_(config), metadata_owner_(std::move(metadata_owner)), service_(std::move(service)),
        method_(std::move(method)), initial_message_(std::move(initial_message)),
        callback_(std::move(callback)) {
    metadata_.reserve(metadata_owner_.entries().size());
    for (const auto& entry : metadata_owner_.entries()) {
      metadata_.push_back(trevrpc_rpc_metadata_entry_v1{
          entry.key.data(), static_cast<std::uint32_t>(entry.key.size()), 0,
          reinterpret_cast<const std::uint8_t*>(entry.value.data()), entry.value.size()});
    }
    config_.service = service_.data();
    config_.method = method_.data();
    config_.metadata_count = static_cast<std::uint32_t>(metadata_.size());
    config_.metadata = metadata_.empty() ? nullptr : metadata_.data();
    config_.initial_message = initial_message_.empty()
                                  ? nullptr
                                  : reinterpret_cast<const std::uint8_t*>(initial_message_.data());
  }

  [[nodiscard]] Result<void> start() noexcept {
    auto open_operation = runtime_->reserve_operation();
    if (!open_operation) {
      return open_operation.error();
    }
    auto close_operation = runtime_->reserve_operation();
    if (!close_operation) {
      runtime_->reject_operation(open_operation.value());
      return close_operation.error();
    }
    const std::uint64_t open_id = open_operation.value();
    const std::uint64_t close_id = close_operation.value();
    trevrpc_rpc_call_v1 call{};
    trevrpc_rpc_stream_v1 stream{};
    const int error = trevrpc_rpc_call_open_v1(runtime_->native_handle(), endpoint_, &config_,
                                               open_id, &call, &stream);
    if (error != 0) {
      runtime_->reject_operation(open_id);
      if (null_handle(call)) {
        runtime_->reject_operation(close_id);
        const auto now = std::chrono::steady_clock::now();
        if (error == -EALREADY && now < retry_deadline_) {
          auto self = shared_from_this();
          auto scheduled =
              runtime_->schedule_at(std::min(now + std::chrono::milliseconds(1), retry_deadline_),
                                    [self = std::move(self)]() mutable {
                                      auto started = self->start();
                                      if (!started) {
                                        self->complete(Result<RpcEvent>(started.error()));
                                      }
                                    });
          return scheduled;
        }
      }
      owner_->adopt_open_handles(call, stream, null_handle(call) ? 0 : close_id, false);
      auto cleanup = owner_->close_result();
      if (!cleanup) {
        RpcClientStream::schedule_cleanup(std::move(owner_));
      }
      return Error::runtime(error);
    }

    owner_->adopt_open_handles(call, stream, close_id, false);
    auto registered = runtime_->register_stream(stream);
    if (!registered) {
      runtime_->reject_operation(open_id);
      auto cleanup = owner_->close_result();
      if (!cleanup) {
        RpcClientStream::schedule_cleanup(std::move(owner_));
      }
      return registered.error();
    }
    owner_->adopt_open_handles(call, stream, close_id, true);

    auto self = shared_from_this();
    auto subscribed = runtime_->subscribe_operation(
        open_id, [self = std::move(self)](Result<RpcEvent> result) mutable {
          self->complete(std::move(result));
        });
    if (!subscribed) {
      runtime_->reject_operation(open_id);
      auto cleanup = owner_->close_result();
      if (!cleanup) {
        RpcClientStream::schedule_cleanup(std::move(owner_));
      }
      return subscribed.error();
    }
    return {};
  }

private:
  void complete(Result<RpcEvent> result) noexcept {
    Result<std::shared_ptr<RpcClientStream>> completion =
        Error::runtime(-EIO, "RPC call failed to become ready");
    if (!result) {
      completion = result.error();
    } else if (result.value().kind != TREVRPC_RPC_EVENT_CALL_READY || result.value().status != 0) {
      completion = Error::runtime(result.value().status == 0 ? -EIO : result.value().status,
                                  "RPC call failed to become ready");
    } else {
      completion = std::move(owner_);
    }
    if (!completion && owner_) {
      auto cleanup = owner_->close_result();
      if (!cleanup) {
        RpcClientStream::schedule_cleanup(std::move(owner_));
      }
    }
    try {
      callback_(std::move(completion));
    } catch (...) {
      (void)std::current_exception();
    }
  }

  std::shared_ptr<RpcEventRuntime> runtime_;
  std::shared_ptr<RpcClientStream> owner_;
  trevrpc_rpc_endpoint_v1 endpoint_{};
  trevrpc_rpc_call_config_v1 config_{};
  Metadata metadata_owner_;
  std::vector<trevrpc_rpc_metadata_entry_v1> metadata_;
  std::string service_;
  std::string method_;
  std::vector<std::byte> initial_message_;
  RpcClientStream::OpenCallback callback_;
  const std::chrono::steady_clock::time_point retry_deadline_ =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
};

} // namespace

Result<std::shared_ptr<RpcClientStream>>
RpcClientStream::open(const std::shared_ptr<ChannelCore>& channel, std::string_view service,
                      std::string_view method, std::uint32_t kind,
                      std::span<const std::byte> initial_message, const CallOptions& options) {
  if (!channel) {
    return Error::runtime(-EINVAL, "RPC channel must not be null");
  }
  if (!fits_u32(service.size()) || service.empty() || !fits_u32(method.size()) || method.empty() ||
      options.timeout.count() < 0 || options.response_idle_timeout.count() < 0 ||
      (initial_message.data() == nullptr && !initial_message.empty())) {
    return Error::runtime(-EINVAL, "RPC call configuration is out of range");
  }
  auto generation = channel->acquire_generation();
  if (!generation) {
    return generation.error();
  }
  auto runtime = generation.value().runtime();
  if (!runtime) {
    return Error::runtime(-ESHUTDOWN, "RPC channel generation is unavailable");
  }

  std::vector<trevrpc_rpc_metadata_entry_v1> metadata;
  try {
    metadata.reserve(options.metadata.entries().size());
    for (const auto& entry : options.metadata.entries()) {
      if (!fits_u32(entry.key.size())) {
        return Error::runtime(-EOVERFLOW, "RPC metadata key is too long");
      }
      metadata.push_back(trevrpc_rpc_metadata_entry_v1{
          entry.key.data(), static_cast<std::uint32_t>(entry.key.size()), 0,
          reinterpret_cast<const std::uint8_t*>(entry.value.data()), entry.value.size()});
    }
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to construct RPC metadata");
  }

  std::optional<RpcCancellation::AttachmentLease> cancellation;
  trevrpc_rpc_cancellation_v1 cancellation_handle{};
  if (options.cancellation != nullptr) {
    const auto* source = options.cancellation->detail_state();
    if (source == nullptr) {
      return Error::runtime(-EINVAL, "RPC cancellation source is empty");
    }
    auto attached = source->attach(runtime);
    if (!attached) {
      return attached.error();
    }
    cancellation.emplace(std::move(attached).value());
    cancellation_handle = cancellation->handle();
  }

  trevrpc_rpc_call_config_v1 config{};
  int error = trevrpc_rpc_call_config_v1_init(&config, sizeof(config));
  if (error != 0) {
    return Error::runtime(error);
  }
  config.kind = kind;
  config.service = service.data();
  config.service_len = static_cast<std::uint32_t>(service.size());
  config.method = method.data();
  config.method_len = static_cast<std::uint32_t>(method.size());
  config.metadata = metadata.empty() ? nullptr : metadata.data();
  config.metadata_count = static_cast<std::uint32_t>(metadata.size());
  config.cancellation = cancellation_handle;
  config.initial_message = initial_message.empty()
                               ? nullptr
                               : reinterpret_cast<const std::uint8_t*>(initial_message.data());
  config.initial_message_len = initial_message.size();
  if (options.timeout.count() != 0) {
    config.timeout_nanos = static_cast<std::uint64_t>(options.timeout.count());
  }
  config.max_response_body_size =
      options.max_response_body_size != 0
          ? options.max_response_body_size
          : static_cast<std::int64_t>(TREVRPC_RPC_DEFAULT_MAX_MESSAGE_SIZE);
  config.max_response_messages =
      options.max_response_messages != 0 ? options.max_response_messages : 4096;
  config.max_response_stream_body_size = options.max_response_stream_body_size != 0
                                             ? options.max_response_stream_body_size
                                             : static_cast<std::int64_t>(16ull * 1024ull * 1024ull);
  if (options.response_idle_timeout.count() != 0) {
    config.response_idle_timeout_nanos =
        static_cast<std::uint64_t>(options.response_idle_timeout.count());
  }

  const auto endpoint = generation.value().endpoint();
  std::shared_ptr<RpcClientStream> owner;
  try {
    if (cleanup_owner_allocation_failure().exchange(false, std::memory_order_acq_rel)) {
      throw std::bad_alloc();
    }
    owner = std::make_shared<RpcClientStream>(runtime, std::move(generation).value(),
                                              trevrpc_rpc_call_v1{}, trevrpc_rpc_stream_v1{}, 0,
                                              std::move(cancellation), kind, false);
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to allocate RPC cleanup owner");
  }
  std::uint64_t open_operation_id = 0;
  std::uint64_t close_operation_id = 0;
  trevrpc_rpc_call_v1 call{};
  trevrpc_rpc_stream_v1 stream{};
  auto close_with_owner = [&](bool stream_registered) {
    if (!stream_registered) {
      runtime->reject_operation(open_operation_id);
    }
    auto close_operation_for_owner = close_operation_id;
    if (null_handle(call)) {
      runtime->reject_operation(close_operation_for_owner);
      close_operation_for_owner = 0;
    }
    owner->adopt_open_handles(call, stream, close_operation_for_owner, stream_registered);
    auto cleanup = owner->close_result();
    if (!cleanup) {
      RpcClientStream::schedule_cleanup(std::move(owner));
    } else {
      owner.reset();
    }
  };

  const auto retry_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
  for (;;) {
    auto open_operation = runtime->reserve_operation();
    if (!open_operation) {
      return open_operation.error();
    }
    auto close_operation = runtime->reserve_operation();
    if (!close_operation) {
      runtime->reject_operation(open_operation.value());
      return close_operation.error();
    }
    open_operation_id = open_operation.value();
    close_operation_id = close_operation.value();
    call = {};
    stream = {};
    error = trevrpc_rpc_call_open_v1(runtime->native_handle(), endpoint, &config, open_operation_id,
                                     &call, &stream);
    if (error == 0) {
      owner->adopt_open_handles(call, stream, close_operation_id, false);
      break;
    }
    if (!null_handle(call) || !null_handle(stream)) {
      close_with_owner(false);
      return Error::runtime(error);
    }
    runtime->reject_operation(open_operation_id);
    runtime->reject_operation(close_operation_id);
    if (error != -EALREADY || std::chrono::steady_clock::now() >= retry_deadline) {
      return Error::runtime(error);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  auto registered = runtime->register_stream(stream);
  if (!registered) {
    close_with_owner(false);
    return registered.error();
  }
  owner->adopt_open_handles(call, stream, close_operation_id, true);
  auto ready = runtime->wait_operation(open_operation_id);
  if (!ready) {
    if (ready.error().code() == -EDEADLK) {
      if (!owner->defer_operation(open_operation_id)) {
        runtime->reject_operation(open_operation_id);
      }
    }
    close_with_owner(true);
    return ready.error();
  }
  if (ready.value().kind != TREVRPC_RPC_EVENT_CALL_READY || ready.value().status != 0) {
    const int ready_error = ready.value().status == 0 ? -EIO : ready.value().status;
    close_with_owner(true);
    return Error::runtime(ready_error, "RPC call failed to become ready");
  }

  return owner;
}

Result<void> RpcClientStream::open_async(const std::shared_ptr<ChannelCore>& channel,
                                         std::string service, std::string method,
                                         std::uint32_t kind, std::vector<std::byte> initial_message,
                                         CallOptions options, OpenCallback callback) {
  if (!channel || !callback || !fits_u32(service.size()) || service.empty() ||
      !fits_u32(method.size()) || method.empty() || options.timeout.count() < 0 ||
      options.response_idle_timeout.count() < 0) {
    return Error::runtime(-EINVAL, "RPC call configuration is out of range");
  }
  try {
    auto generation = channel->acquire_generation();
    if (!generation) {
      return generation.error();
    }
    auto runtime = generation.value().runtime();
    if (!runtime) {
      return Error::runtime(-ESHUTDOWN, "RPC channel generation is unavailable");
    }

    for (const auto& entry : options.metadata.entries()) {
      if (!fits_u32(entry.key.size())) {
        return Error::runtime(-EOVERFLOW, "RPC metadata key is too long");
      }
    }

    std::optional<RpcCancellation::AttachmentLease> cancellation;
    trevrpc_rpc_cancellation_v1 cancellation_handle{};
    if (options.cancellation != nullptr) {
      const auto* source = options.cancellation->detail_state();
      if (source == nullptr) {
        return Error::runtime(-EINVAL, "RPC cancellation source is empty");
      }
      auto attached = source->attach(runtime);
      if (!attached) {
        return attached.error();
      }
      cancellation.emplace(std::move(attached).value());
      cancellation_handle = cancellation->handle();
    }

    trevrpc_rpc_call_config_v1 config{};
    int error = trevrpc_rpc_call_config_v1_init(&config, sizeof(config));
    if (error != 0) {
      return Error::runtime(error);
    }
    config.kind = kind;
    config.service_len = static_cast<std::uint32_t>(service.size());
    config.method_len = static_cast<std::uint32_t>(method.size());
    config.metadata_count = 0;
    config.cancellation = cancellation_handle;
    config.initial_message_len = initial_message.size();
    if (options.timeout.count() != 0) {
      config.timeout_nanos = static_cast<std::uint64_t>(options.timeout.count());
    }
    config.max_response_body_size =
        options.max_response_body_size != 0
            ? options.max_response_body_size
            : static_cast<std::int64_t>(TREVRPC_RPC_DEFAULT_MAX_MESSAGE_SIZE);
    config.max_response_messages =
        options.max_response_messages != 0 ? options.max_response_messages : 4096;
    config.max_response_stream_body_size =
        options.max_response_stream_body_size != 0
            ? options.max_response_stream_body_size
            : static_cast<std::int64_t>(16ull * 1024ull * 1024ull);
    if (options.response_idle_timeout.count() != 0) {
      config.response_idle_timeout_nanos =
          static_cast<std::uint64_t>(options.response_idle_timeout.count());
    }

    const auto endpoint = generation.value().endpoint();
    auto owner = std::make_shared<RpcClientStream>(runtime, std::move(generation).value(),
                                                   trevrpc_rpc_call_v1{}, trevrpc_rpc_stream_v1{},
                                                   0, std::move(cancellation), kind, false);
    auto state = std::make_shared<AsyncClientOpen>(
        runtime, std::move(owner), endpoint, config, std::move(options.metadata),
        std::move(service), std::move(method), std::move(initial_message), std::move(callback));
    return state->start();
  } catch (const std::length_error&) {
    return Error::runtime(-EOVERFLOW, "RPC call configuration exceeds local allocation limits");
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to start asynchronous RPC call");
  }
}

RpcClientStream::~RpcClientStream() { close(); }

Result<void> RpcClientStream::close_result() noexcept {
  try {
    return close_impl();
  } catch (...) {
    schedule_deferred_cleanup();
    return Error::runtime(-ENOMEM);
  }
}

void RpcClientStream::adopt_open_handles(trevrpc_rpc_call_v1 call, trevrpc_rpc_stream_v1 stream,
                                         std::uint64_t close_operation,
                                         bool stream_registered) noexcept {
  std::lock_guard lock(mutex_);
  call_ = call;
  stream_ = stream;
  close_operation_ = close_operation;
  close_operation_consumed_ = close_operation == 0;
  stream_registered_ = stream_registered;
  stream_unregistering_ = false;
}

bool RpcClientStream::claim_cleanup_schedule() noexcept {
  std::lock_guard lock(mutex_);
  if (cleanup_scheduled_ || cleanup_abandoned_ || closed_) {
    return false;
  }
  cleanup_scheduled_ = true;
  return true;
}

void RpcClientStream::schedule_cleanup(std::shared_ptr<RpcClientStream> stream) noexcept {
  auto owner = std::move(stream);
  if (!owner || !owner->claim_cleanup_schedule()) {
    return;
  }
  std::shared_ptr<RpcEventRuntime> runtime;
  {
    std::lock_guard lock(owner->mutex_);
    runtime = owner->runtime_;
  }
  if (runtime) {
    runtime->schedule_cleanup(owner);
  } else {
    owner->interrupt_cleanup();
    owner->abandon_cleanup();
    abandon_cleanup_owner(owner);
  }
}

Result<void> RpcClientStream::cleanup_step() {
  std::shared_ptr<RpcEventRuntime> runtime;
  trevrpc_rpc_call_v1 call{};
  trevrpc_rpc_stream_v1 stream{};
  std::uint64_t close_operation = 0;
  bool submit_close = false;
  bool stream_registered = true;
  const auto owner_thread = std::this_thread::get_id();

  auto finish = [this](int result) -> Result<void> {
    std::lock_guard lock(mutex_);
    cleanup_result_code_ = result;
    cleanup_in_progress_ = false;
    cleanup_owner_thread_ = {};
    lifecycle_condition_.notify_all();
    return result == 0 ? Result<void>{} : Result<void>{Error::runtime(result)};
  };

  {
    std::lock_guard lock(mutex_);
    if (cleanup_abandoned_ || closed_) {
      return {};
    }
    if (cleanup_in_progress_) {
      return cleanup_owner_thread_ == owner_thread ? Result<void>{}
                                                   : Result<void>{Error::runtime(-EINPROGRESS)};
    }
    if (send_active_ != 0 || receive_active_ != 0 || cancel_active_ != 0) {
      return Error::runtime(-EAGAIN);
    }
    cleanup_in_progress_ = true;
    cleanup_owner_thread_ = owner_thread;
    close_requested_ = true;
    runtime = runtime_;
    call = call_;
    stream = stream_;
    close_operation = close_operation_;
    stream_registered = stream_registered_;
    if (!close_submitted_ && !close_operation_consumed_ && !null_handle(call)) {
      close_submitted_ = true;
      submit_close = true;
    }
    if (null_handle(call)) {
      close_operation_consumed_ = true;
      call_closed_ = true;
      stream_closed_ = true;
    }
  }

  if (!runtime) {
    return finish(0);
  }
  if (null_handle(call) && null_handle(stream)) {
    std::optional<RpcCancellation::AttachmentLease> cancellation;
    {
      std::lock_guard lock(mutex_);
      closed_ = true;
      runtime_.reset();
      generation_ = {};
      cancellation = std::move(cancellation_);
    }
    cancellation.reset();
    return finish(0);
  }

  for (;;) {
    std::uint64_t operation = 0;
    {
      std::lock_guard lock(mutex_);
      if (deferred_operation_count_ != 0) {
        operation = deferred_operations_[0];
      }
    }
    if (operation == 0) {
      break;
    }
    auto completed = runtime->try_wait_operation(operation);
    if (!completed) {
      return finish(completed.error().code());
    }
    if (!completed.value().has_value()) {
      return finish(-EAGAIN);
    }
    note_event(completed.value().value());
    {
      std::lock_guard lock(mutex_);
      if (operation == close_operation_) {
        close_operation_consumed_ = true;
      }
      for (std::size_t index = 1; index < deferred_operation_count_; ++index) {
        deferred_operations_[index - 1] = deferred_operations_[index];
      }
      if (deferred_operation_count_ != 0) {
        --deferred_operation_count_;
        deferred_operations_[deferred_operation_count_] = 0;
      }
    }
  }

  if (submit_close) {
    if (close_operation == 0) {
      auto replacement = runtime->reserve_operation();
      if (!replacement) {
        {
          std::lock_guard lock(mutex_);
          close_submitted_ = false;
        }
        return finish(replacement.error().code());
      }
      close_operation = replacement.value();
      std::lock_guard lock(mutex_);
      close_operation_ = close_operation;
    }
    const int error = trevrpc_rpc_call_close(runtime->native_handle(), call, close_operation,
                                             TREVRPC_RPC_CLOSE_FLAG_ABORT, 0);
    if (error != 0 && error != -EALREADY) {
      runtime->reject_operation(close_operation);
      {
        std::lock_guard lock(mutex_);
        close_submitted_ = false;
        close_operation_ = 0;
        close_operation_consumed_ = false;
      }
      return finish(error);
    }
    if (error == -EALREADY) {
      runtime->reject_operation(close_operation);
      std::lock_guard lock(mutex_);
      close_operation_consumed_ = true;
      call_closed_ = true;
    }
  }

  bool close_consumed = false;
  {
    std::lock_guard lock(mutex_);
    close_consumed = close_operation_consumed_;
  }
  if (!close_consumed) {
    auto event = runtime->try_wait_operation(close_operation);
    if (!event) {
      return finish(event.error().code());
    }
    if (!event.value().has_value()) {
      return finish(-EAGAIN);
    }
    note_event(event.value().value());
    if (event.value()->kind != TREVRPC_RPC_EVENT_CALL_CLOSED || event.value()->status != 0) {
      return finish(event.value()->status != 0 ? event.value()->status : -EIO);
    }
    std::lock_guard lock(mutex_);
    close_operation_consumed_ = true;
  }

  if (stream_registered) {
    bool terminal = false;
    {
      std::lock_guard lock(mutex_);
      terminal = stream_closed_ && call_closed_;
    }
    if (!terminal) {
      auto event = runtime->try_wait_stream(stream);
      if (!event) {
        const int error = event.error().code();
        if (!runtime_terminal_cleanup_error(error)) {
          return finish(error);
        }
        std::lock_guard lock(mutex_);
        stream_closed_ = true;
        call_closed_ = true;
      } else if (!event.value().has_value()) {
        return finish(-EAGAIN);
      } else {
        note_event(event.value().value());
      }
      bool terminal = false;
      {
        std::lock_guard lock(mutex_);
        terminal = stream_closed_ && call_closed_;
      }
      if (!terminal) {
        return finish(-EAGAIN);
      }
    }
    bool claim_unregister = false;
    {
      std::lock_guard lock(mutex_);
      if (!stream_unregistered_ && !stream_unregistering_ && !null_handle(stream)) {
        stream_unregistering_ = true;
        claim_unregister = true;
      }
    }
    if (claim_unregister) {
      runtime->unregister_stream(stream);
      std::lock_guard lock(mutex_);
      stream_unregistering_ = false;
      stream_unregistered_ = true;
    }
  } else {
    bool settled = false;
    {
      std::lock_guard lock(mutex_);
      settled = stream_unregistered_;
    }
    if (!settled && !null_handle(stream)) {
      auto result = runtime->try_settle_unregistered_stream(stream);
      if (!result) {
        const int error = result.error().code();
        if (!runtime_terminal_cleanup_error(error)) {
          return finish(error);
        }
        settled = true;
      } else if (!result.value()) {
        return finish(-EAGAIN);
      } else {
        settled = true;
      }
      if (settled) {
        std::lock_guard lock(mutex_);
        stream_unregistered_ = true;
        stream_closed_ = true;
      }
    }
  }

  int stream_error = 0;
  if (null_handle(stream)) {
    std::lock_guard lock(mutex_);
    stream_released_ = true;
  }
  if (!null_handle(stream)) {
    stream_error = trevrpc_rpc_stream_release(runtime->native_handle(), stream);
    if (stream_error == 0 || stream_error == -ESTALE) {
      std::lock_guard lock(mutex_);
      stream_released_ = true;
      stream_ = {};
    }
  }
  int call_error = 0;
  if (!null_handle(call)) {
    call_error = trevrpc_rpc_call_release(runtime->native_handle(), call);
    if (call_error == 0 || call_error == -ESTALE) {
      std::lock_guard lock(mutex_);
      call_released_ = true;
      call_ = {};
    }
  }
  if (stream_error != 0 && stream_error != -ESTALE) {
    return finish(stream_error);
  }
  if (call_error != 0 && call_error != -ESTALE) {
    return finish(call_error);
  }

  std::optional<RpcCancellation::AttachmentLease> cancellation;
  bool releases_complete = false;
  {
    std::lock_guard lock(mutex_);
    releases_complete = stream_released_ && (null_handle(call) || call_released_);
    if (releases_complete) {
      closed_ = true;
    }
    if (releases_complete) {
      runtime_.reset();
      generation_ = {};
      cancellation = std::move(cancellation_);
    }
  }
  if (!releases_complete) {
    return finish(-EBUSY);
  }
  cancellation.reset();
  return finish(0);
}

void RpcClientStream::interrupt_cleanup() noexcept {
  std::shared_ptr<RpcEventRuntime> runtime;
  {
    std::lock_guard lock(mutex_);
    if (cleanup_abandoned_) {
      return;
    }
    close_requested_ = true;
    runtime = runtime_;
  }
  if (runtime) {
    runtime->request_abandon();
  }
}

void RpcClientStream::abandon_cleanup() noexcept {
  RpcCancellation::AttachmentLease cancellation;
  {
    std::lock_guard lock(mutex_);
    cleanup_abandoned_ = true;
    close_requested_ = true;
    runtime_.reset();
    generation_ = {};
    if (cancellation_.has_value()) {
      cancellation = std::move(cancellation_).value();
      cancellation_.reset();
    }
    lifecycle_condition_.notify_all();
  }
}

RpcClientStream::ActivityGuard::~ActivityGuard() { release(); }

void RpcClientStream::ActivityGuard::release() noexcept {
  if (!active_) {
    return;
  }
  active_ = false;
  owner_->end_activity(activity_);
}

void RpcClientStream::test_fail_next_receive_allocation() noexcept {
  receive_allocation_failure().store(true, std::memory_order_release);
}

void RpcClientStream::test_fail_next_cleanup_owner_allocation() noexcept {
  cleanup_owner_allocation_failure().store(true, std::memory_order_release);
}

void RpcClientStream::test_fail_next_cleanup_task_allocation() noexcept {
  cleanup_task_allocation_failure().store(true, std::memory_order_release);
}

void RpcClientStream::test_fail_next_cleanup_requeue_allocation() noexcept {
  cleanup_requeue_allocation_failure().store(true, std::memory_order_release);
}

bool RpcClientStream::test_consume_cleanup_task_allocation_failure() noexcept {
  return cleanup_task_allocation_failure().exchange(false, std::memory_order_acq_rel);
}

bool RpcClientStream::test_consume_cleanup_requeue_allocation_failure() noexcept {
  return cleanup_requeue_allocation_failure().exchange(false, std::memory_order_acq_rel);
}

void RpcClientStream::test_shutdown_cleanup_service() noexcept {}

Result<RpcClientStream::NativeSnapshot> RpcClientStream::begin_activity(Activity activity) {
  std::lock_guard lock(mutex_);
  if (!runtime_ || closed_ || close_requested_) {
    return Error::runtime(-ESHUTDOWN, "RPC call is closed");
  }
  switch (activity) {
  case Activity::Send:
    ++send_active_;
    break;
  case Activity::Receive:
    ++receive_active_;
    break;
  case Activity::Cancel:
    ++cancel_active_;
    break;
  }
  return NativeSnapshot{runtime_, call_, stream_, close_operation_};
}

void RpcClientStream::end_activity(Activity activity) noexcept {
  std::lock_guard lock(mutex_);
  auto* count = [&]() -> std::size_t* {
    switch (activity) {
    case Activity::Send:
      return &send_active_;
    case Activity::Receive:
      return &receive_active_;
    case Activity::Cancel:
      return &cancel_active_;
    }
    return nullptr;
  }();
  if (count == nullptr || *count == 0) {
    std::terminate();
  }
  --*count;
  lifecycle_condition_.notify_all();
}

Result<std::uint64_t>
RpcClientStream::reserve_operation(const std::shared_ptr<RpcEventRuntime>& runtime) {
  if (!runtime) {
    return Error::runtime(-ESHUTDOWN, "RPC call is closed");
  }
  return runtime->reserve_operation();
}

void RpcClientStream::note_event(const RpcEvent& event) noexcept {
  std::lock_guard lock(mutex_);
  if (event.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED) {
    stream_closed_ = true;
  } else if (event.kind == TREVRPC_RPC_EVENT_CALL_CLOSED) {
    call_closed_ = true;
  } else if (event.kind == TREVRPC_RPC_EVENT_CALL_FAILED) {
    call_failed_ = true;
  }
}

Result<void> RpcClientStream::wait_operation(const std::shared_ptr<RpcEventRuntime>& runtime,
                                             std::uint64_t operation, std::uint32_t expected_kind,
                                             const char* description) {
  auto result = runtime->wait_operation(operation);
  if (!result) {
    if (result.error().code() == -EDEADLK) {
      if (defer_operation(operation)) {
        schedule_deferred_cleanup();
      } else {
        runtime->reject_operation(operation);
      }
    }
    return result.error();
  }
  note_event(result.value());
  bool is_close_operation = false;
  {
    std::lock_guard lock(mutex_);
    is_close_operation = operation == close_operation_;
    if (is_close_operation) {
      close_operation_consumed_ = true;
    }
  }
  if (result.value().kind != expected_kind) {
    return Error::runtime(-EIO, description);
  }
  if (result.value().status != 0) {
    return Error::runtime(result.value().status);
  }
  return {};
}

bool RpcClientStream::defer_operation(std::uint64_t operation) noexcept {
  if (operation == 0) {
    return false;
  }
  std::lock_guard lock(mutex_);
  if (cleanup_abandoned_ || closed_) {
    return false;
  }
  for (std::size_t index = 0; index < deferred_operation_count_; ++index) {
    if (deferred_operations_[index] == operation) {
      return true;
    }
  }
  if (deferred_operation_count_ == deferred_operations_.size()) {
    return false;
  }
  deferred_operations_[deferred_operation_count_++] = operation;
  close_requested_ = true;
  lifecycle_condition_.notify_all();
  return true;
}

void RpcClientStream::schedule_deferred_cleanup() noexcept {
  std::shared_ptr<RpcClientStream> owner;
  try {
    owner = shared_from_this();
  } catch (...) {
    std::shared_ptr<RpcEventRuntime> runtime;
    std::array<std::uint64_t, 4> operations{};
    std::size_t count = 0;
    {
      std::lock_guard lock(mutex_);
      runtime = runtime_;
      count = deferred_operation_count_;
      for (std::size_t index = 0; index < count; ++index) {
        operations[index] = deferred_operations_[index];
      }
      deferred_operation_count_ = 0;
    }
    if (runtime) {
      for (std::size_t index = 0; index < count; ++index) {
        runtime->reject_operation(operations[index]);
      }
      runtime->request_abandon();
    }
    abandon_cleanup();
    return;
  }
  schedule_cleanup(std::move(owner));
}

Result<void> RpcClientStream::send(std::span<const std::byte> body) {
  std::lock_guard send_lock(send_mutex_);
  if (body.data() == nullptr && !body.empty()) {
    return Error::runtime(-EINVAL, "RPC message body is invalid");
  }
  auto native = begin_activity(Activity::Send);
  if (!native) {
    return native.error();
  }
  auto finish_activity = [this] { end_activity(Activity::Send); };
  const auto& snapshot = native.value();
  bool send_closed = false;
  bool status_seen = false;
  {
    std::lock_guard lock(mutex_);
    send_closed = send_finished_;
    status_seen = status_seen_;
  }
  if (send_closed || status_seen) {
    finish_activity();
    return Error::runtime(-EPIPE, send_closed ? "RPC stream send side is closed"
                                              : "RPC stream already received terminal status");
  }
  auto operation = reserve_operation(snapshot.runtime);
  if (!operation) {
    finish_activity();
    return operation.error();
  }
  const int error = trevrpc_rpc_stream_send_copy_v1(
      snapshot.runtime->native_handle(), snapshot.stream, operation.value(),
      reinterpret_cast<const std::uint8_t*>(body.data()), body.size(), TREVRPC_RPC_SEND_FLAG_NONE);
  if (error != 0) {
    snapshot.runtime->reject_operation(operation.value());
    finish_activity();
    return Error::runtime(error);
  }
  auto result = wait_operation(snapshot.runtime, operation.value(), TREVRPC_RPC_EVENT_SEND_COMPLETE,
                               "RPC stream send produced an unexpected completion");
  finish_activity();
  return result;
}

Result<void> RpcClientStream::finish_send() {
  std::lock_guard send_lock(send_mutex_);
  {
    std::lock_guard lock(mutex_);
    if (closed_) {
      return Error::runtime(-EINVAL, "RPC stream is closed");
    }
    if (send_finished_) {
      return {};
    }
    if (status_seen_) {
      send_finished_ = true;
      return Error::runtime(-ECANCELED, "RPC stream received terminal status");
    }
  }
  auto native = begin_activity(Activity::Send);
  if (!native) {
    return native.error();
  }
  auto finish_activity = [this] { end_activity(Activity::Send); };
  const auto& snapshot = native.value();
  auto operation = reserve_operation(snapshot.runtime);
  if (!operation) {
    finish_activity();
    return operation.error();
  }
  const int error = trevrpc_rpc_stream_finish_send(snapshot.runtime->native_handle(),
                                                   snapshot.stream, operation.value());
  if (error != 0) {
    snapshot.runtime->reject_operation(operation.value());
    finish_activity();
    return Error::runtime(error);
  }
  auto result = wait_operation(snapshot.runtime, operation.value(), TREVRPC_RPC_EVENT_SEND_FINISHED,
                               "RPC stream finish produced an unexpected completion");
  if (result) {
    std::lock_guard lock(mutex_);
    send_finished_ = true;
  }
  finish_activity();
  return result;
}

Result<void> RpcClientStream::start_send(std::span<const std::byte> body,
                                         CompletionCallback callback) noexcept {
  if (!callback || (body.data() == nullptr && !body.empty())) {
    return Error::runtime(-EINVAL, "RPC async send is invalid");
  }
  try {
    auto native = begin_activity(Activity::Send);
    if (!native) {
      return native.error();
    }
    const auto& snapshot = native.value();
    bool send_closed = false;
    {
      std::lock_guard lock(mutex_);
      send_closed = send_finished_ || status_seen_;
    }
    if (send_closed) {
      end_activity(Activity::Send);
      return Error::runtime(-EPIPE, "RPC stream send side is closed");
    }
    auto operation = reserve_operation(snapshot.runtime);
    if (!operation) {
      end_activity(Activity::Send);
      return operation.error();
    }
    const int error = trevrpc_rpc_stream_send_copy_v1(
        snapshot.runtime->native_handle(), snapshot.stream, operation.value(),
        reinterpret_cast<const std::uint8_t*>(body.data()), body.size(),
        TREVRPC_RPC_SEND_FLAG_NONE);
    if (error != 0) {
      snapshot.runtime->reject_operation(operation.value());
      end_activity(Activity::Send);
      return Error::runtime(error);
    }
    auto self = shared_from_this();
    auto subscribed = snapshot.runtime->subscribe_operation(
        operation.value(),
        [self = std::move(self), callback = std::move(callback)](Result<RpcEvent> event) mutable {
          Result<void> result;
          if (!event) {
            result = event.error();
          } else {
            self->note_event(event.value());
            if (event.value().kind != TREVRPC_RPC_EVENT_SEND_COMPLETE) {
              result = Error::runtime(-EIO, "RPC stream send produced an unexpected completion");
            } else if (event.value().status != 0) {
              result = Error::runtime(event.value().status);
            }
          }
          self->end_activity(Activity::Send);
          callback(std::move(result));
        });
    if (!subscribed) {
      snapshot.runtime->reject_operation(operation.value());
      end_activity(Activity::Send);
      return subscribed.error();
    }
    return {};
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to start RPC async send");
  }
}

Result<void> RpcClientStream::start_finish_send(CompletionCallback callback) noexcept {
  if (!callback) {
    return Error::runtime(-EINVAL, "RPC async finish callback must not be empty");
  }
  try {
    bool already_finished = false;
    bool terminal_seen = false;
    {
      std::lock_guard lock(mutex_);
      if (closed_) {
        return Error::runtime(-EINVAL, "RPC stream is closed");
      }
      already_finished = send_finished_;
      terminal_seen = status_seen_;
      if (terminal_seen) {
        send_finished_ = true;
      }
    }
    if (already_finished) {
      callback(Result<void>{});
      return {};
    }
    if (terminal_seen) {
      return Error::runtime(-ECANCELED, "RPC stream received terminal status");
    }
    auto native = begin_activity(Activity::Send);
    if (!native) {
      return native.error();
    }
    const auto& snapshot = native.value();
    auto operation = reserve_operation(snapshot.runtime);
    if (!operation) {
      end_activity(Activity::Send);
      return operation.error();
    }
    const int error = trevrpc_rpc_stream_finish_send(snapshot.runtime->native_handle(),
                                                     snapshot.stream, operation.value());
    if (error != 0) {
      snapshot.runtime->reject_operation(operation.value());
      end_activity(Activity::Send);
      return Error::runtime(error);
    }
    auto self = shared_from_this();
    auto subscribed = snapshot.runtime->subscribe_operation(
        operation.value(),
        [self = std::move(self), callback = std::move(callback)](Result<RpcEvent> event) mutable {
          Result<void> result;
          if (!event) {
            result = event.error();
          } else {
            self->note_event(event.value());
            if (event.value().kind != TREVRPC_RPC_EVENT_SEND_FINISHED) {
              result = Error::runtime(-EIO, "RPC stream finish produced an unexpected completion");
            } else if (event.value().status != 0) {
              result = Error::runtime(event.value().status);
            } else {
              std::lock_guard lock(self->mutex_);
              self->send_finished_ = true;
            }
          }
          self->end_activity(Activity::Send);
          callback(std::move(result));
        });
    if (!subscribed) {
      snapshot.runtime->reject_operation(operation.value());
      end_activity(Activity::Send);
      return subscribed.error();
    }
    return {};
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to start RPC async finish");
  }
}

Result<RpcEvent> RpcClientStream::wait_stream_event(const std::shared_ptr<RpcEventRuntime>& runtime,
                                                    trevrpc_rpc_stream_v1 stream) {
  auto event = runtime->wait_stream(stream);
  if (event) {
    note_event(event.value());
  }
  return event;
}

void RpcClientStream::defer_receive_terminal(const RpcEvent& event) noexcept {
  note_event(event);
  std::lock_guard lock(mutex_);
  if (!deferred_receive_terminal_.has_value()) {
    deferred_receive_terminal_ = event;
  }
}

std::optional<Result<StreamFrame>> RpcClientStream::finish_deferred_receive_terminal() {
  std::lock_guard lock(mutex_);
  if (!deferred_receive_terminal_.has_value()) {
    return std::nullopt;
  }

  const RpcEvent event = std::move(deferred_receive_terminal_).value();
  deferred_receive_terminal_.reset();
  receive_finished_ = true;
  if (event.kind == TREVRPC_RPC_EVENT_CALL_FAILED) {
    return Error::runtime(event.status == 0 ? -EIO : event.status,
                          "RPC call failed while receiving");
  }
  if (event.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED) {
    return Error::protobuf("response stream closed before clean receive FIN");
  }
  if (event.kind != TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN) {
    return Error::runtime(-EIO, "RPC stream produced an unexpected terminal event");
  }

  receive_fin_seen_ = true;
  if ((event.flags & TREVRPC_RPC_EVENT_FLAG_CLEAN_FIN) == 0 || event.status != 0) {
    return Error::protobuf("response stream ended without clean receive FIN");
  }
  if (kind_ == TREVRPC_RPC_KIND_UNARY &&
      (!terminal_status_.has_value() || (terminal_status_->is_ok() && !unary_message_seen_))) {
    return Error::protobuf("unary RPC ended before response message");
  }
  if (kind_ != TREVRPC_RPC_KIND_UNARY && (!status_seen_ || !terminal_status_.has_value())) {
    return Error::protobuf("response stream ended before terminal status");
  }

  StreamFrame terminal;
  terminal.terminal = true;
  terminal.message = kind_ == TREVRPC_RPC_KIND_UNARY && unary_message_seen_;
  terminal.status = std::move(terminal_status_).value();
  terminal.body = std::move(unary_body_);
  return terminal;
}

Result<StreamFrame> RpcClientStream::decode_receive(trevrpc_rpc_receive* raw_receive) {
  if (raw_receive == nullptr) {
    return Error::runtime(-EIO, "RPC runtime returned no receive");
  }
  ReceiveGuard receive_guard(raw_receive);
  try {
    if (receive_allocation_failure().exchange(false, std::memory_order_acq_rel)) {
      throw std::bad_alloc();
    }
    trevrpc_rpc_receive_info_v1 info{};
    int error = trevrpc_rpc_receive_info_v1_init(&info, sizeof(info));
    if (error == 0) {
      error = trevrpc_rpc_receive_get_info_v1(raw_receive, &info);
    }
    if (error != 0) {
      return Error::runtime(error);
    }
    if (info.metadata_count != 0 && info.metadata == nullptr) {
      return Error::runtime(-EINVAL, "runtime returned invalid RPC metadata");
    }
    if (info.message_len != 0 && info.message == nullptr) {
      return Error::runtime(-EINVAL, "runtime returned invalid RPC status message");
    }
    const auto max_body_size = std::vector<std::byte>{}.max_size();
    if (info.data_len > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max()) ||
        info.data_len > max_body_size) {
      return Error::runtime(-EOVERFLOW, "RPC response body exceeds local allocation limits");
    }
    auto metadata = copy_rpc_metadata(info);
    if (!metadata) {
      return metadata.error();
    }
    if (info.kind == TREVRPC_RPC_RECEIVE_MESSAGE ||
        info.kind == TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE) {
      std::lock_guard lock(mutex_);
      if (kind_ == TREVRPC_RPC_KIND_UNARY) {
        if (unary_message_seen_) {
          return Error::protobuf("unary RPC returned more than one response message");
        }
        unary_message_seen_ = true;
        status_seen_ = true;
        terminal_status_.emplace(status_from_info(info, std::move(metadata).value()));
        unary_body_.resize(static_cast<std::size_t>(info.data_len));
        if (info.data_len != 0) {
          if (info.data == nullptr) {
            return Error::runtime(-EINVAL, "runtime returned an invalid RPC response body");
          }
          std::memcpy(unary_body_.data(), info.data, static_cast<std::size_t>(info.data_len));
        }
        return StreamFrame{};
      }
      if (status_seen_) {
        return Error::protobuf("response stream contained trailing data after terminal status");
      }
      StreamFrame frame;
      frame.message = true;
      frame.body.resize(static_cast<std::size_t>(info.data_len));
      if (info.data_len != 0) {
        if (info.data == nullptr) {
          return Error::runtime(-EINVAL, "runtime returned an invalid RPC response body");
        }
        std::memcpy(frame.body.data(), info.data, static_cast<std::size_t>(info.data_len));
      }
      return frame;
    }
    if (info.kind == TREVRPC_RPC_RECEIVE_STATUS) {
      std::lock_guard lock(mutex_);
      if (status_seen_) {
        return Error::protobuf(kind_ == TREVRPC_RPC_KIND_UNARY
                                   ? "unary RPC returned duplicate terminal status"
                                   : "response stream contained duplicate terminal status");
      }
      status_seen_ = true;
      terminal_status_.emplace(status_from_info(info, std::move(metadata).value()));
      return StreamFrame{};
    }
    return Error::protobuf("response stream contained an unknown receive kind");
  } catch (const std::length_error&) {
    return Error::runtime(-EOVERFLOW, "RPC receive result exceeds local allocation limits");
  } catch (const std::bad_alloc&) {
    return Error::runtime(-ENOMEM, "failed to allocate RPC receive result");
  } catch (const std::exception&) {
    return Error::runtime(-ENOMEM, "failed to decode RPC receive result");
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to decode RPC receive result");
  }
}

Result<StreamFrame> RpcClientStream::receive() {
  {
    std::lock_guard lock(mutex_);
    if (closed_) {
      return Error::runtime(-EINVAL, "RPC stream is closed");
    }
    if (receive_finished_) {
      return Error::runtime(-EPIPE, "RPC stream receive side is closed");
    }
  }
  auto native = begin_activity(Activity::Receive);
  if (!native) {
    return native.error();
  }
  ActivityGuard receive_activity(this, Activity::Receive);
  const auto& snapshot = native.value();
  for (;;) {
    trevrpc_rpc_receive* raw_receive = nullptr;
    const int error = trevrpc_rpc_stream_receive(snapshot.runtime->native_handle(), snapshot.stream,
                                                 &raw_receive);
    if (error == 0) {
      auto frame = decode_receive(raw_receive);
      if (!frame) {
        receive_activity.release();
        return frame.error();
      }
      if (frame.value().message) {
        receive_activity.release();
        return frame;
      }
      continue;
    }
    if (error != -EAGAIN) {
      receive_activity.release();
      return Error::runtime(error);
    }
    auto terminal = finish_deferred_receive_terminal();
    if (terminal.has_value()) {
      receive_activity.release();
      if (!terminal.value()) {
        return terminal->error();
      }
      auto cleanup = close_impl();
      if (!cleanup) {
        // The terminal frame is already the authoritative RPC result. Cleanup may
        // still be waiting for the peer's close event, so retry it independently
        // instead of turning a successful response into a timing-dependent error.
        schedule_deferred_cleanup();
      }
      return std::move(terminal).value().value();
    }
    auto event = wait_stream_event(snapshot.runtime, snapshot.stream);
    if (!event) {
      receive_activity.release();
      return event.error();
    }
    if (event.value().kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN ||
        event.value().kind == TREVRPC_RPC_EVENT_CALL_FAILED ||
        event.value().kind == TREVRPC_RPC_EVENT_STREAM_CLOSED) {
      defer_receive_terminal(event.value());
    }
  }
}

Result<std::optional<StreamFrame>> RpcClientStream::try_receive() {
  {
    std::lock_guard lock(mutex_);
    if (closed_) {
      return Error::runtime(-EINVAL, "RPC stream is closed");
    }
    if (receive_finished_) {
      return Error::runtime(-EPIPE, "RPC stream receive side is closed");
    }
  }
  auto native = begin_activity(Activity::Receive);
  if (!native) {
    return native.error();
  }
  ActivityGuard receive_activity(this, Activity::Receive);
  const auto& snapshot = native.value();
  for (;;) {
    trevrpc_rpc_receive* raw_receive = nullptr;
    const int error = trevrpc_rpc_stream_receive(snapshot.runtime->native_handle(), snapshot.stream,
                                                 &raw_receive);
    if (error == 0) {
      auto frame = decode_receive(raw_receive);
      if (!frame) {
        receive_activity.release();
        return frame.error();
      }
      if (frame.value().message) {
        receive_activity.release();
        return std::optional<StreamFrame>(std::move(frame).value());
      }
      continue;
    }
    if (error != -EAGAIN) {
      receive_activity.release();
      return Error::runtime(error);
    }
    auto terminal = finish_deferred_receive_terminal();
    if (terminal.has_value()) {
      receive_activity.release();
      if (!terminal.value()) {
        return terminal->error();
      }
      return std::optional<StreamFrame>(std::move(terminal).value().value());
    }
    auto event = snapshot.runtime->try_wait_stream(snapshot.stream);
    if (!event) {
      receive_activity.release();
      return event.error();
    }
    if (!event.value().has_value()) {
      receive_activity.release();
      return std::optional<StreamFrame>{};
    }
    const RpcEvent& received = event.value().value();
    if (received.kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN ||
        received.kind == TREVRPC_RPC_EVENT_CALL_FAILED ||
        received.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED) {
      defer_receive_terminal(received);
    } else {
      note_event(received);
    }
  }
}

Result<void> RpcClientStream::start_receive(ReceiveCallback callback) noexcept {
  if (!callback) {
    return Error::runtime(-EINVAL, "RPC async receive callback must not be empty");
  }
  try {
    {
      std::lock_guard lock(mutex_);
      if (closed_) {
        return Error::runtime(-EINVAL, "RPC stream is closed");
      }
      if (receive_finished_) {
        return Error::runtime(-EPIPE, "RPC stream receive side is closed");
      }
    }
    auto native = begin_activity(Activity::Receive);
    if (!native) {
      return native.error();
    }
    receive_async_step(std::move(native).value(), std::move(callback));
    return {};
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to start RPC async receive");
  }
}

Result<void> RpcClientStream::schedule_at(std::chrono::steady_clock::time_point deadline,
                                          std::function<void()> callback) noexcept {
  std::shared_ptr<RpcEventRuntime> runtime;
  {
    std::lock_guard lock(mutex_);
    runtime = runtime_;
  }
  if (!runtime) {
    return Error::runtime(-ESHUTDOWN, "RPC stream is closed");
  }
  try {
    return runtime->schedule_at(deadline, std::move(callback));
  } catch (...) {
    return Error::runtime(-ENOMEM, "failed to schedule RPC stream timer");
  }
}

void RpcClientStream::finish_async_receive(const ReceiveCallback& callback,
                                           Result<StreamFrame> result) noexcept {
  end_activity(Activity::Receive);
  try {
    callback(std::move(result));
  } catch (...) {
    (void)std::current_exception();
  }
}

bool RpcClientStream::handle_async_receive_event(ReceiveCallback&,
                                                 const RpcEvent& received) noexcept {
  if (received.kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN ||
      received.kind == TREVRPC_RPC_EVENT_CALL_FAILED ||
      received.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED) {
    defer_receive_terminal(received);
  } else {
    note_event(received);
  }
  return false;
}

void RpcClientStream::receive_async_step(NativeSnapshot native, ReceiveCallback callback) noexcept {
  for (;;) {
    trevrpc_rpc_receive* raw_receive = nullptr;
    const int error =
        trevrpc_rpc_stream_receive(native.runtime->native_handle(), native.stream, &raw_receive);
    if (error == 0) {
      auto frame = decode_receive(raw_receive);
      if (!frame) {
        finish_async_receive(callback, frame.error());
        return;
      }
      if (frame.value().message) {
        finish_async_receive(callback, std::move(frame));
        return;
      }
      continue;
    }
    if (error != -EAGAIN) {
      finish_async_receive(callback, Error::runtime(error));
      return;
    }

    auto terminal = finish_deferred_receive_terminal();
    if (terminal.has_value()) {
      finish_async_receive(callback, std::move(terminal).value());
      return;
    }

    auto event = native.runtime->try_wait_stream(native.stream);
    if (!event) {
      finish_async_receive(callback, event.error());
      return;
    }
    if (!event.value()) {
      try {
        auto self = shared_from_this();
        auto subscribed = native.runtime->subscribe_stream(
            native.stream,
            [self = std::move(self), native, callback](Result<RpcEvent> event) mutable {
              if (!event) {
                self->finish_async_receive(callback, event.error());
                return;
              }
              if (!self->handle_async_receive_event(callback, event.value())) {
                self->receive_async_step(std::move(native), std::move(callback));
              }
            });
        if (!subscribed) {
          finish_async_receive(callback, subscribed.error());
        }
      } catch (...) {
        finish_async_receive(callback,
                             Error::runtime(-ENOMEM, "failed to subscribe RPC async receive"));
      }
      return;
    }

    if (handle_async_receive_event(callback, event.value().value())) {
      return;
    }
  }
}

void RpcClientStream::cancel() noexcept {
  auto native = begin_activity(Activity::Cancel);
  if (!native) {
    return;
  }
  const auto& snapshot = native.value();
  auto operation = reserve_operation(snapshot.runtime);
  if (!operation) {
    end_activity(Activity::Cancel);
    return;
  }
  const int error = trevrpc_rpc_call_cancel(snapshot.runtime->native_handle(), snapshot.call,
                                            operation.value(), 0);
  if (error != 0) {
    snapshot.runtime->reject_operation(operation.value());
    end_activity(Activity::Cancel);
    return;
  }
  std::shared_ptr<RpcClientStream> self;
  try {
    self = shared_from_this();
  } catch (...) {
    snapshot.runtime->reject_operation(operation.value());
    end_activity(Activity::Cancel);
    return;
  }
  auto subscribed = snapshot.runtime->subscribe_operation(
      operation.value(), [self = std::move(self)](Result<RpcEvent> event) {
        if (event) {
          self->note_event(event.value());
        }
        self->end_activity(Activity::Cancel);
      });
  if (!subscribed) {
    snapshot.runtime->reject_operation(operation.value());
    end_activity(Activity::Cancel);
  }
}

Result<void> RpcClientStream::close_impl() {
  std::shared_ptr<RpcEventRuntime> runtime;
  trevrpc_rpc_call_v1 call{};
  trevrpc_rpc_stream_v1 stream{};
  std::uint64_t close_operation = 0;
  bool submit_close = false;
  bool defer_close = false;
  const auto cleanup_thread = std::this_thread::get_id();
  {
    std::unique_lock lock(mutex_);
    if (cleanup_abandoned_ || closed_) {
      return {};
    }
    if (cleanup_in_progress_) {
      const bool blocking_forbidden = runtime_ != nullptr && runtime_->blocking_wait_forbidden();
      if (cleanup_owner_thread_ == cleanup_thread || blocking_forbidden) {
        return {};
      }
      lifecycle_condition_.wait(lock, [this] { return !cleanup_in_progress_; });
      return cleanup_result_code_ == 0 ? Result<void>{}
                                       : Result<void>{Error::runtime(cleanup_result_code_)};
    }
    runtime = runtime_;
    if (runtime && runtime->blocking_wait_forbidden()) {
      close_requested_ = true;
      defer_close = true;
    }
    if (defer_close) {
      lock.unlock();
      schedule_deferred_cleanup();
      return Error::runtime(-EDEADLK);
    }
    cleanup_in_progress_ = true;
    cleanup_owner_thread_ = cleanup_thread;
    cleanup_result_code_ = 0;
    close_requested_ = true;
    runtime = runtime_;
    call = call_;
    stream = stream_;
    close_operation = close_operation_;
    if (!close_submitted_ && !close_operation_consumed_ && !null_handle(call)) {
      close_submitted_ = true;
      submit_close = true;
    }
  }

  auto finish = [this](int result) -> Result<void> {
    {
      std::lock_guard lock(mutex_);
      cleanup_result_code_ = result;
      cleanup_in_progress_ = false;
      cleanup_owner_thread_ = {};
      lifecycle_condition_.notify_all();
    }
    return result == 0 ? Result<void>{} : Result<void>{Error::runtime(result)};
  };

  if (!runtime) {
    std::optional<RpcCancellation::AttachmentLease> cancellation;
    {
      std::lock_guard lock(mutex_);
      closed_ = true;
      generation_ = {};
      cancellation = std::move(cancellation_);
      cleanup_result_code_ = 0;
      cleanup_in_progress_ = false;
      cleanup_owner_thread_ = {};
      lifecycle_condition_.notify_all();
    }
    cancellation.reset();
    return {};
  }

  if (null_handle(call) && null_handle(stream)) {
    std::optional<RpcCancellation::AttachmentLease> cancellation;
    {
      std::lock_guard lock(mutex_);
      closed_ = true;
      runtime_.reset();
      generation_ = {};
      cancellation = std::move(cancellation_);
      cleanup_result_code_ = 0;
      cleanup_in_progress_ = false;
      cleanup_owner_thread_ = {};
      lifecycle_condition_.notify_all();
    }
    cancellation.reset();
    return {};
  }

  if (null_handle(call)) {
    std::lock_guard lock(mutex_);
    close_operation_consumed_ = true;
    stream_unregistered_ = true;
    stream_closed_ = true;
    call_closed_ = true;
  }

  int error = 0;
  if (submit_close) {
    if (close_operation == 0) {
      auto replacement = reserve_operation(runtime);
      if (!replacement) {
        {
          std::lock_guard lock(mutex_);
          close_submitted_ = false;
        }
        return finish(replacement.error().code());
      }
      close_operation = replacement.value();
      std::lock_guard lock(mutex_);
      close_operation_ = close_operation;
    }
    constexpr unsigned int max_attempts = 8;
    for (unsigned int attempt = 0; attempt != max_attempts; ++attempt) {
      error = trevrpc_rpc_call_close(runtime->native_handle(), call, close_operation,
                                     TREVRPC_RPC_CLOSE_FLAG_ABORT, 0);
      if (error == 0) {
        break;
      }

      runtime->reject_operation(close_operation);
      {
        std::lock_guard lock(mutex_);
        close_operation_ = 0;
        close_operation_consumed_ = false;
      }
      if (error == -EALREADY) {
        std::lock_guard lock(mutex_);
        close_operation_consumed_ = true;
        error = 0;
        break;
      }

      const bool retryable = error == -EAGAIN || error == -EBUSY || error == -EINPROGRESS;
      if (!retryable || attempt + 1 == max_attempts) {
        {
          std::lock_guard lock(mutex_);
          close_submitted_ = false;
        }
        return finish(error);
      }

      auto replacement = reserve_operation(runtime);
      if (!replacement) {
        {
          std::lock_guard lock(mutex_);
          close_submitted_ = false;
        }
        return finish(error);
      }
      close_operation = replacement.value();
      {
        std::lock_guard lock(mutex_);
        close_operation_ = close_operation;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (error != 0) {
      {
        std::lock_guard lock(mutex_);
        close_submitted_ = false;
      }
      return finish(error);
    }
  }

  {
    std::unique_lock lock(mutex_);
    lifecycle_condition_.wait(
        lock, [this] { return send_active_ == 0 && receive_active_ == 0 && cancel_active_ == 0; });
  }

  bool close_operation_consumed = false;
  {
    std::lock_guard lock(mutex_);
    close_operation_consumed = close_operation_consumed_;
  }
  if (!close_operation_consumed) {
    auto completed = wait_operation(runtime, close_operation, TREVRPC_RPC_EVENT_CALL_CLOSED,
                                    "RPC call close produced an unexpected completion");
    if (!completed) {
      const int close_wait_error = completed.error().code();
      if (!runtime_terminal_cleanup_error(close_wait_error)) {
        return finish(close_wait_error);
      }
      std::lock_guard lock(mutex_);
      close_operation_consumed_ = true;
      call_closed_ = true;
      stream_closed_ = true;
    }
  }

  bool stream_registered = true;
  {
    std::lock_guard lock(mutex_);
    stream_registered = stream_registered_;
  }
  if (!stream_registered) {
    bool already_settled = false;
    {
      std::lock_guard lock(mutex_);
      already_settled = stream_unregistered_;
    }
    if (!already_settled && !null_handle(stream)) {
      auto settled = runtime->settle_unregistered_stream(stream);
      if (!settled && !runtime_terminal_cleanup_error(settled.error().code())) {
        return finish(settled.error().code());
      }
      std::lock_guard lock(mutex_);
      stream_unregistered_ = true;
      stream_closed_ = true;
    }
  } else {
    for (;;) {
      bool done = false;
      {
        std::lock_guard lock(mutex_);
        done = stream_closed_ && call_closed_;
      }
      if (done) {
        break;
      }
      auto terminal = wait_stream_event(runtime, stream);
      if (!terminal) {
        const int terminal_error = terminal.error().code();
        if (!runtime_terminal_cleanup_error(terminal_error)) {
          return finish(terminal_error);
        }
        std::lock_guard lock(mutex_);
        stream_closed_ = true;
        call_closed_ = true;
        break;
      }
    }

    bool claim_unregister = false;
    {
      std::lock_guard lock(mutex_);
      if (!stream_unregistered_ && !stream_unregistering_ && !null_handle(stream)) {
        stream_unregistering_ = true;
        claim_unregister = true;
      }
    }
    if (claim_unregister) {
      runtime->unregister_stream(stream);
      std::lock_guard lock(mutex_);
      stream_unregistering_ = false;
      stream_unregistered_ = true;
      lifecycle_condition_.notify_all();
    }
  }

  int stream_error = 0;
  if (!stream_released_ && !null_handle(stream)) {
    stream_error = trevrpc_rpc_stream_release(runtime->native_handle(), stream);
    if (stream_error == 0 || stream_error == -ESTALE) {
      std::lock_guard lock(mutex_);
      stream_released_ = true;
      stream_ = {};
    }
  }

  int call_error = 0;
  if (!call_released_ && !null_handle(call)) {
    call_error = trevrpc_rpc_call_release(runtime->native_handle(), call);
    if (call_error == 0 || call_error == -ESTALE) {
      std::lock_guard lock(mutex_);
      call_released_ = true;
      call_ = {};
    }
  }

  if (stream_error != 0 && stream_error != -ESTALE) {
    return finish(stream_error);
  }
  if (call_error != 0 && call_error != -ESTALE) {
    return finish(call_error);
  }

  {
    std::lock_guard lock(mutex_);
    if (!stream_released_ || !call_released_) {
      // A native release that did not succeed leaves its handle owned for a later retry.
      cleanup_result_code_ = -EBUSY;
      cleanup_in_progress_ = false;
      cleanup_owner_thread_ = {};
      lifecycle_condition_.notify_all();
      return Error::runtime(-EBUSY);
    }
  }

  std::optional<RpcCancellation::AttachmentLease> cancellation;
  {
    std::lock_guard lock(mutex_);
    closed_ = true;
    runtime_.reset();
    generation_ = {};
    cancellation = std::move(cancellation_);
    cleanup_result_code_ = 0;
    cleanup_in_progress_ = false;
    cleanup_owner_thread_ = {};
    lifecycle_condition_.notify_all();
  }
  cancellation.reset();
  return {};
}

void RpcClientStream::close() noexcept {
  const auto result = close_result();
  if (!result) {
    schedule_deferred_cleanup();
  }
}

} // namespace trevrpc::detail
