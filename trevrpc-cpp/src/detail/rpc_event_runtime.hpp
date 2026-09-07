#pragma once

#include <trevrpc/trevrpc.hpp>

#include <trevrpc_rpc.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace trevrpc::detail {

class ChannelCore;
class RpcSyncClient;
class RpcClientStream;

class RpcCleanupWork {
public:
  virtual ~RpcCleanupWork() = default;
  [[nodiscard]] virtual Result<void> cleanup_step() = 0;
  virtual void interrupt_cleanup() noexcept = 0;
  virtual void abandon_cleanup() noexcept = 0;
};

struct RpcIncomingCall {
  trevrpc_rpc_call_v1 call{};
  trevrpc_rpc_stream_v1 stream{};
  std::uint32_t rpc_kind = 0;
  std::string service;
  std::string method;
  std::vector<std::byte> initial_message;
  Metadata metadata;
  trevrpc_rpc_call_context_info_v1 context{};
  int preparation_status = 0;
};

struct RpcEvent {
  std::uint32_t kind = 0;
  std::uint32_t flags = 0;
  int status = 0;
  std::uint32_t subject_kind = 0;
  std::uint64_t sequence = 0;
  trevrpc_rpc_endpoint_v1 endpoint{};
  trevrpc_rpc_call_v1 call{};
  trevrpc_rpc_stream_v1 stream{};
  trevrpc_rpc_cancellation_v1 cancellation{};
  std::uint64_t operation_id = 0;
  std::uint32_t rpc_status = TREVRPC_RPC_STATUS_UNKNOWN;
  std::uint32_t rpc_kind = 0;
  std::string service;
  std::string method;
  std::uint64_t application_error_code = 0;
  std::uint64_t provider_error_code = 0;
};

class RpcEventRuntime final {
  struct SharedState;

public:
  class NativeRuntimeLease final {
  public:
    NativeRuntimeLease() noexcept = default;
    ~NativeRuntimeLease();
    NativeRuntimeLease(const NativeRuntimeLease&) = delete;
    NativeRuntimeLease& operator=(const NativeRuntimeLease&) = delete;
    NativeRuntimeLease(NativeRuntimeLease&& other) noexcept;
    NativeRuntimeLease& operator=(NativeRuntimeLease&& other) noexcept;

    [[nodiscard]] operator trevrpc_rpc_runtime*() const noexcept { return runtime_; }

  private:
    friend class RpcEventRuntime;

    NativeRuntimeLease(std::shared_ptr<SharedState> state, trevrpc_rpc_runtime* runtime) noexcept
        : state_(std::move(state)), runtime_(runtime) {}

    void reset() noexcept;

    std::shared_ptr<SharedState> state_;
    trevrpc_rpc_runtime* runtime_ = nullptr;
  };

  using EventCallback = std::function<void(Result<RpcEvent>)>;
  using IncomingCallback = std::function<void(Result<RpcIncomingCall>)>;

  // Ownership of runtime transfers only when this returns successfully.
  [[nodiscard]] static Result<std::shared_ptr<RpcEventRuntime>>
  adopt(trevrpc_rpc_runtime* runtime, const trevrpc_rpc_runtime_config_v1& config);

  ~RpcEventRuntime();
  RpcEventRuntime(const RpcEventRuntime&) = delete;
  RpcEventRuntime& operator=(const RpcEventRuntime&) = delete;

  [[nodiscard]] NativeRuntimeLease native_handle() const noexcept;
  [[nodiscard]] bool stopped() const noexcept;
  [[nodiscard]] bool callback_active() const noexcept;
  [[nodiscard]] bool blocking_wait_forbidden() const noexcept;
  [[nodiscard]] Result<void> schedule_at(std::chrono::steady_clock::time_point deadline,
                                         std::function<void()> callback);
  void schedule_cleanup(const std::shared_ptr<RpcCleanupWork>& work) noexcept;
  void drain_cleanup() noexcept;
  [[nodiscard]] Result<std::uint64_t> reserve_operation();
  void reject_operation(std::uint64_t operation_id) noexcept;
  [[nodiscard]] Result<RpcEvent> wait_operation(std::uint64_t operation_id);
  [[nodiscard]] Result<std::optional<RpcEvent>> try_wait_operation(std::uint64_t operation_id);
  [[nodiscard]] Result<void> subscribe_operation(std::uint64_t operation_id,
                                                 EventCallback callback);

  [[nodiscard]] Result<void> register_stream(trevrpc_rpc_stream_v1 stream);
  void unregister_stream(trevrpc_rpc_stream_v1 stream) noexcept;
  [[nodiscard]] Result<RpcEvent> wait_stream(trevrpc_rpc_stream_v1 stream);
  [[nodiscard]] Result<std::optional<RpcEvent>> try_wait_stream(trevrpc_rpc_stream_v1 stream);
  [[nodiscard]] Result<void> subscribe_stream(trevrpc_rpc_stream_v1 stream, EventCallback callback);

  [[nodiscard]] Result<void> register_endpoint(trevrpc_rpc_endpoint_v1 endpoint);
  void unregister_endpoint(trevrpc_rpc_endpoint_v1 endpoint) noexcept;
  [[nodiscard]] Result<RpcEvent> wait_endpoint(trevrpc_rpc_endpoint_v1 endpoint);
  [[nodiscard]] Result<void> subscribe_endpoint(trevrpc_rpc_endpoint_v1 endpoint,
                                                EventCallback callback);

  [[nodiscard]] Result<RpcIncomingCall> wait_incoming();
  void stop_incoming() noexcept;
  [[nodiscard]] Result<void> reject_incoming(const RpcIncomingCall& incoming);

  template <typename Callback>
    requires std::is_nothrow_invocable_r_v<void, Callback&, Result<RpcIncomingCall>>
  [[nodiscard]] Result<void> subscribe_incoming(Callback&& callback) {
    try {
      return subscribe_incoming_impl(IncomingCallback(std::forward<Callback>(callback)));
    } catch (...) {
      return Error::runtime(-ENOMEM, "failed to allocate RPC incoming callback");
    }
  }

  [[nodiscard]] Result<std::uint64_t> request_close();
  void request_abandon() noexcept;
  [[nodiscard]] Result<void> drain_and_release();
  [[nodiscard]] Result<void> shutdown();

private:
  friend class ChannelCore;
  friend class RpcEventRuntimeTestPeer;
  friend class RpcSyncClient;
  friend class RpcClientStream;

  [[nodiscard]] static Result<void> reserve_unadopted_cleanup();
  static void cancel_unadopted_cleanup() noexcept;
  static void reap_unadopted(trevrpc_rpc_runtime* runtime) noexcept;
  [[nodiscard]] Result<void> settle_unregistered_stream(trevrpc_rpc_stream_v1 stream);
  [[nodiscard]] Result<bool> try_settle_unregistered_stream(trevrpc_rpc_stream_v1 stream);
  [[nodiscard]] Result<void> settle_unregistered_endpoint(trevrpc_rpc_endpoint_v1 endpoint);

  explicit RpcEventRuntime(std::shared_ptr<SharedState> state) noexcept
      : state_(std::move(state)) {}

  [[nodiscard]] Result<void> subscribe_incoming_impl(IncomingCallback callback);

  void test_wait_for_operation_waiters(std::size_t count);
  void test_wait_for_stream_waiters(std::size_t count);
  void test_wait_for_pending_stream_events(trevrpc_rpc_stream_v1 stream, std::size_t count);
  void test_wait_for_endpoint_waiters(std::size_t count);
  void test_wait_for_shutdown_waiters(std::size_t count);
  [[nodiscard]] bool test_wait_for_close_submission_waiters(std::size_t count,
                                                            std::chrono::milliseconds timeout);
  void test_block_next_completed_strict_waiter();
  void test_wait_for_blocked_strict_waiter();
  void test_release_blocked_strict_waiter();
  void test_wait_for_stopped();
  void test_pause_finalization();
  void test_wait_for_finalization_pause();
  void test_release_finalization();
  void test_wait_for_incoming_backlog(std::size_t count);
  void test_wait_for_incoming_waiter();
  [[nodiscard]] int test_dispatch(RpcEvent event);
  void test_wait_for_rejected_streams(std::size_t count);
  void test_fail_driver(int error);
  void test_pause_driver();
  void test_resume_driver();
  void test_wait_for_terminal_settlement();
  void test_settle_driver_failure(int error);
  void test_fail_next_operation_reservation(int error);
  void test_fail_next_endpoint_registration(int error);
  void test_fail_next_stream_registration(int error);
  void test_fail_next_endpoint_registration_commit();
  void test_fail_next_control_write(int error);
  static void test_fail_next_adopt_start(int error) noexcept;
  static void test_wait_for_unadopted_cleanup() noexcept;
  [[nodiscard]] std::weak_ptr<void> test_state_observer() const noexcept;
  [[nodiscard]] std::thread::id test_driver_thread_id() const noexcept;

  std::shared_ptr<SharedState> state_;
};

} // namespace trevrpc::detail
