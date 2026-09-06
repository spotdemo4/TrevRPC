#pragma once

#include <trevrpc/trevrpc.hpp>

#include <trevrpc_rpc.h>

#include <array>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace trevrpc::detail {

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
public:
  [[nodiscard]] static Result<std::shared_ptr<RpcEventRuntime>> adopt(trevrpc_rpc_runtime* runtime);

  ~RpcEventRuntime();
  RpcEventRuntime(const RpcEventRuntime&) = delete;
  RpcEventRuntime& operator=(const RpcEventRuntime&) = delete;

  [[nodiscard]] trevrpc_rpc_runtime* native_handle() const noexcept { return runtime_; }
  [[nodiscard]] Result<std::uint64_t> reserve_operation();
  void reject_operation(std::uint64_t operation_id) noexcept;
  [[nodiscard]] Result<RpcEvent> wait_operation(std::uint64_t operation_id);

  [[nodiscard]] Result<void> register_stream(trevrpc_rpc_stream_v1 stream);
  void unregister_stream(trevrpc_rpc_stream_v1 stream) noexcept;
  [[nodiscard]] Result<RpcEvent> wait_stream(trevrpc_rpc_stream_v1 stream);
  [[nodiscard]] Result<void> register_endpoint(trevrpc_rpc_endpoint_v1 endpoint);
  void unregister_endpoint(trevrpc_rpc_endpoint_v1 endpoint) noexcept;
  [[nodiscard]] Result<RpcEvent> wait_endpoint(trevrpc_rpc_endpoint_v1 endpoint);
  [[nodiscard]] Result<RpcIncomingCall> wait_incoming();

  [[nodiscard]] Result<void> shutdown();

private:
  friend class RpcEventRuntimeTestPeer;

  struct HandleKey {
    std::uint64_t owner;
    std::uint32_t slot;
    std::uint32_t generation;

    [[nodiscard]] bool operator==(const HandleKey&) const noexcept = default;
  };

  struct HandleKeyHash {
    [[nodiscard]] std::size_t operator()(const HandleKey& key) const noexcept;
  };

  struct OperationSlot {
    bool ready = false;
    RpcEvent event;
  };

  struct StreamSlot {
    std::deque<RpcEvent> events;
    bool readable_pending = false;
  };

  using EndpointSlot = StreamSlot;

  explicit RpcEventRuntime(trevrpc_rpc_runtime* runtime) noexcept : runtime_(runtime) {}
  [[nodiscard]] Result<void> start();
  [[nodiscard]] Result<void> shutdown_without_driver();
  [[nodiscard]] Result<void> shutdown_impl();
  [[nodiscard]] Result<void> initialize_driver_wake();
  void request_driver_stop() noexcept;
  void close_driver_wake() noexcept;
  void driver_loop() noexcept;
  [[nodiscard]] int drain_events() noexcept;
  [[nodiscard]] int take_incoming(trevrpc_rpc_event* raw_event,
                                  const trevrpc_rpc_event_info_v1& info) noexcept;
  void dispatch(RpcEvent event) noexcept;
  void fail_driver(int error) noexcept;
  [[nodiscard]] bool should_stop() const noexcept;
  [[nodiscard]] static HandleKey key(trevrpc_rpc_stream_v1 stream) noexcept;
  [[nodiscard]] static HandleKey key(trevrpc_rpc_endpoint_v1 endpoint) noexcept;

  static constexpr std::size_t max_pending_stream_events = 64;
  static constexpr std::size_t max_pending_endpoint_events = 64;
  static constexpr std::size_t max_pending_incoming_calls = 64;

  trevrpc_rpc_runtime* runtime_ = nullptr;
  trevrpc_rpc_wake_source_v1 wake_{};
  std::thread driver_;
  int driver_wake_read_ = -1;
  int driver_wake_write_ = -1;

  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::unordered_map<std::uint64_t, OperationSlot> operations_;
  std::unordered_map<HandleKey, StreamSlot, HandleKeyHash> streams_;
  std::unordered_map<HandleKey, StreamSlot, HandleKeyHash> pending_streams_;
  std::unordered_map<HandleKey, EndpointSlot, HandleKeyHash> endpoints_;
  std::unordered_map<HandleKey, EndpointSlot, HandleKeyHash> pending_endpoints_;
  std::size_t pending_stream_event_count_ = 0;
  std::size_t pending_endpoint_event_count_ = 0;
  std::array<RpcIncomingCall, max_pending_incoming_calls> incoming_calls_{};
  std::size_t incoming_head_ = 0;
  std::size_t incoming_tail_ = 0;
  std::size_t incoming_count_ = 0;
  std::uint64_t next_operation_id_ = 1;
  std::uint64_t shutdown_operation_id_ = 0;
  int driver_error_ = 0;
  bool close_admitted_ = false;
  bool stopped_seen_ = false;
  bool released_ = false;
  bool driver_stop_requested_ = false;
  bool shutdown_in_progress_ = false;
  std::uint64_t shutdown_generation_ = 0;
  std::uint64_t shutdown_completed_generation_ = 0;
  std::size_t operation_waiters_ = 0;
  std::size_t stream_waiters_ = 0;
  std::size_t endpoint_waiters_ = 0;
  std::size_t shutdown_waiters_ = 0;
  int shutdown_status_ = 0;
};

} // namespace trevrpc::detail
