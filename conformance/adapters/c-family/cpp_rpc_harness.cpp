#include "cpp_rpc_harness.hpp"

#include "detail/channel_core.hpp"
#include "detail/rpc_client.hpp"
#include "detail/rpc_event_runtime.hpp"
#include "cpp_rpc_harness_support.h"
extern "C" {
#include "trevrpc_rpc_internal.h"
}

#include <cerrno>
#include <condition_variable>
#include <future>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

namespace trevrpc::detail {

struct RpcClientStreamTestPeer {
  [[nodiscard]] static ClientStream wrap(const std::shared_ptr<RpcClientStream>& stream) {
    return ClientStream(stream);
  }

  [[nodiscard]] static bool
  terminal_status_seen(const std::shared_ptr<RpcClientStream>& stream) noexcept {
    if (!stream) {
      return false;
    }
    std::lock_guard lock(stream->mutex_);
    return stream->status_seen_;
  }

  [[nodiscard]] static trevrpc_wire_diagnostic_reason
  receive_diagnostic(const std::shared_ptr<RpcClientStream>& stream) noexcept {
    if (!stream) {
      return TREVRPC_WIRE_DIAGNOSTIC_NONE;
    }
    std::shared_ptr<RpcEventRuntime> runtime;
    trevrpc_rpc_stream_v1 native_stream{};
    {
      std::lock_guard lock(stream->mutex_);
      runtime = stream->runtime_;
      native_stream = stream->stream_;
    }
    if (!runtime) {
      return TREVRPC_WIRE_DIAGNOSTIC_NONE;
    }
    auto native_runtime = runtime->native_handle();
    if (!native_runtime) {
      return TREVRPC_WIRE_DIAGNOSTIC_NONE;
    }
    return trevrpc_rpc_stream_last_receive_diagnostic(native_runtime, native_stream);
  }
};

} // namespace trevrpc::detail

namespace cf {
namespace {

using trevrpc::detail::ChannelCore;
using trevrpc::detail::ChannelCoreConfig;
using trevrpc::detail::RpcClientStream;
using trevrpc::detail::RpcClientStreamTestPeer;
using trevrpc::detail::RpcEventRuntime;

struct EndpointStart final {
  std::mutex mutex;
  std::condition_variable condition;
  bool completed = false;
  int result = -EINPROGRESS;
};

[[nodiscard]] trevrpc::Error runtime_error(int error, std::string_view message) {
  return trevrpc::Error::runtime(error, std::string(message));
}

void release_unadopted_runtime(trevrpc_rpc_runtime* runtime) noexcept {
  if (runtime == nullptr) {
    return;
  }
  (void)trevrpc_rpc_runtime_close(runtime, 1);
  (void)trevrpc_rpc_runtime_drain(runtime);
  (void)trevrpc_rpc_runtime_release(runtime);
}

} // namespace

struct RpcStreamHarness::Impl final {
  trevrpc_cpp_rpc_fake_fixture* fake = nullptr;
  std::shared_ptr<RpcEventRuntime> runtime;
  std::shared_ptr<ChannelCore> channel;
  std::shared_ptr<RpcClientStream> stream;
  std::size_t final_close_count = 0;
  bool shut_down = false;
};

RpcStreamHarness::RpcStreamHarness() : impl_(std::make_unique<Impl>()) {}

RpcStreamHarness::~RpcStreamHarness() { shutdown(); }

trevrpc::Result<trevrpc::detail::ClientStream> RpcStreamHarness::start(std::uint32_t kind) {
  if (!impl_ || impl_->fake != nullptr || impl_->runtime || impl_->channel || impl_->stream ||
      impl_->shut_down) {
    return runtime_error(-EALREADY, "RPC stream harness is already started");
  }

  trevrpc_rpc_runtime_config_v1 runtime_config{};
  int error = trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config));
  if (error != 0) {
    return runtime_error(error, "failed to initialize RPC runtime config");
  }
  runtime_config.event_capacity = 128;
  runtime_config.endpoint_capacity = 4;
  runtime_config.call_capacity = 4;
  runtime_config.stream_capacity = 4;

  trevrpc_rpc_runtime* native_runtime = nullptr;
  error = trevrpc_cpp_rpc_fake_fixture_create(&impl_->fake, &runtime_config, &native_runtime);
  if (error != 0) {
    return runtime_error(error, "failed to create fake RPC runtime");
  }

  auto adopted = RpcEventRuntime::adopt(native_runtime, runtime_config);
  if (!adopted) {
    release_unadopted_runtime(native_runtime);
    impl_->fake = nullptr;
    return adopted.error();
  }
  impl_->runtime = std::move(adopted).value();

  auto endpoint_start = std::make_shared<EndpointStart>();
  ChannelCoreConfig channel_config;
  channel_config.host = "conformance.invalid";
  channel_config.reconnect_initial_delay = std::chrono::milliseconds(0);
  channel_config.reconnect_max_delay = std::chrono::milliseconds(0);
  auto created = ChannelCore::create(
      impl_->runtime, std::move(channel_config),
      [fake = impl_->fake, endpoint_start](trevrpc_rpc_runtime* runtime, std::uint64_t operation_id,
                                           trevrpc_rpc_endpoint_v1* endpoint) {
        const int start_error = trevrpc_cpp_rpc_fake_start_endpoint(
            fake, runtime, TREVRPC_RPC_ENDPOINT_CLIENT, operation_id, endpoint);
        {
          std::lock_guard lock(endpoint_start->mutex);
          endpoint_start->result = start_error;
          endpoint_start->completed = true;
        }
        endpoint_start->condition.notify_all();
        return start_error;
      });
  if (!created) {
    shutdown();
    return created.error();
  }
  impl_->channel = std::move(created).value();

  {
    std::unique_lock lock(endpoint_start->mutex);
    endpoint_start->condition.wait(lock, [&] { return endpoint_start->completed; });
    if (endpoint_start->result != 0) {
      const int start_error = endpoint_start->result;
      lock.unlock();
      shutdown();
      return runtime_error(start_error, "failed to start fake RPC endpoint");
    }
  }

  error = trevrpc_cpp_rpc_fake_push_connection_ready(impl_->fake);
  if (error != 0) {
    shutdown();
    return runtime_error(error, "failed to make fake RPC endpoint ready");
  }
  auto ready = impl_->channel->wait_ready(std::chrono::seconds(1));
  if (!ready) {
    shutdown();
    return ready.error();
  }

  auto opening = std::async(std::launch::async, [channel = impl_->channel, kind] {
    return RpcClientStream::open(channel, "conformance.State", "State", kind, {},
                                 trevrpc::CallOptions{});
  });
  trevrpc_cpp_rpc_fake_wait_stream_open_calls(impl_->fake, 1);
  error = trevrpc_cpp_rpc_fake_push_stream_ready(impl_->fake);
  if (error == 0) {
    trevrpc_cpp_rpc_fake_wait_stream_send_calls(impl_->fake, 1);
    error = trevrpc_cpp_rpc_fake_push_last_send_complete(impl_->fake);
  }
  if (error != 0) {
    (void)trevrpc_cpp_rpc_fake_push_connection_closed(impl_->fake, error);
  }
  auto opened = opening.get();
  if (error != 0) {
    shutdown();
    return runtime_error(error, "failed to ready fake RPC stream");
  }
  if (!opened) {
    shutdown();
    return opened.error();
  }
  impl_->stream = std::move(opened).value();
  return RpcClientStreamTestPeer::wrap(impl_->stream);
}

int RpcStreamHarness::push_frame(std::span<const std::uint8_t> frame) {
  if (!impl_ || impl_->fake == nullptr || !impl_->stream || impl_->shut_down) {
    return -ESHUTDOWN;
  }
  int error = trevrpc_cpp_rpc_fake_push_receive(impl_->fake, frame.data(), frame.size());
  return error == 0 ? trevrpc_cpp_rpc_fake_push_stream_readable(impl_->fake) : error;
}

int RpcStreamHarness::finish_response() {
  if (!impl_ || impl_->fake == nullptr || !impl_->stream || impl_->shut_down) {
    return -ESHUTDOWN;
  }
  return trevrpc_cpp_rpc_fake_push_stream_receive_fin(impl_->fake);
}

int RpcStreamHarness::fail_receive(int error) {
  if (!impl_ || impl_->fake == nullptr || !impl_->stream || impl_->shut_down || error == 0) {
    return -EINVAL;
  }
  cf_rpc_fake_fail_receive(impl_->fake, error);
  return trevrpc_cpp_rpc_fake_push_stream_readable(impl_->fake);
}

bool RpcStreamHarness::wait_terminal_status_seen(std::chrono::milliseconds timeout) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!terminal_status_seen()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return true;
}

bool RpcStreamHarness::terminal_status_seen() const noexcept {
  return impl_ && RpcClientStreamTestPeer::terminal_status_seen(impl_->stream);
}

trevrpc_wire_diagnostic_reason RpcStreamHarness::receive_diagnostic() const noexcept {
  return impl_ ? RpcClientStreamTestPeer::receive_diagnostic(impl_->stream)
               : TREVRPC_WIRE_DIAGNOSTIC_NONE;
}

std::size_t RpcStreamHarness::close_count() const noexcept {
  if (!impl_) {
    return 0;
  }
  return impl_->fake == nullptr ? impl_->final_close_count : cf_rpc_fake_close_count(impl_->fake);
}

void RpcStreamHarness::shutdown() noexcept {
  if (!impl_ || impl_->shut_down) {
    return;
  }
  if (impl_->stream) {
    impl_->stream->close();
  }
  if (impl_->fake != nullptr) {
    impl_->final_close_count = cf_rpc_fake_close_count(impl_->fake);
  }
  impl_->stream.reset();
  if (impl_->channel) {
    (void)impl_->channel->close();
    impl_->channel.reset();
  }
  if (impl_->runtime) {
    (void)impl_->runtime->shutdown();
    impl_->runtime.reset();
  }
  impl_->fake = nullptr;
  impl_->shut_down = true;
}

} // namespace cf
