#pragma once

#include <trevrpc/trevrpc.hpp>

#include "channel_core.hpp"
#include "rpc_event_runtime.hpp"

#include <trevrpc_rpc.h>

#include <array>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string_view>
#include <thread>
#include <vector>

namespace trevrpc::detail {

class RpcClientStream final : public RpcCleanupWork,
                              public std::enable_shared_from_this<RpcClientStream> {
public:
  using OpenCallback =
      std::function<void(Result<std::shared_ptr<RpcClientStream>>) >;
  using CompletionCallback = std::function<void(Result<void>)>;
  using ReceiveCallback = std::function<void(Result<StreamFrame>)>;

  [[nodiscard]] static Result<std::shared_ptr<RpcClientStream>>
  open(const std::shared_ptr<ChannelCore>& channel, std::string_view service,
       std::string_view method, std::uint32_t kind,
       std::span<const std::byte> initial_message, const CallOptions& options);
  [[nodiscard]] static Result<void>
  open_async(const std::shared_ptr<ChannelCore>& channel, std::string service,
             std::string method, std::uint32_t kind,
             std::vector<std::byte> initial_message, CallOptions options, OpenCallback callback);

  ~RpcClientStream() override;
  RpcClientStream(const RpcClientStream&) = delete;
  RpcClientStream& operator=(const RpcClientStream&) = delete;

  [[nodiscard]] Result<void> send(std::span<const std::byte> body);
  [[nodiscard]] Result<void> finish_send();
  [[nodiscard]] Result<StreamFrame> receive();
  [[nodiscard]] Result<std::optional<StreamFrame>> try_receive();
  [[nodiscard]] Result<void> start_send(std::span<const std::byte> body,
                                        CompletionCallback callback) noexcept;
  [[nodiscard]] Result<void> start_finish_send(CompletionCallback callback) noexcept;
  [[nodiscard]] Result<void> start_receive(ReceiveCallback callback) noexcept;
  [[nodiscard]] Result<void> schedule_at(std::chrono::steady_clock::time_point deadline,
                                         std::function<void()> callback) noexcept;
  void cancel() noexcept;
  void close() noexcept;
  [[nodiscard]] Result<void> close_result() noexcept;
  static void schedule_cleanup(std::shared_ptr<RpcClientStream> stream) noexcept;

  void interrupt_cleanup() noexcept override;
  void abandon_cleanup() noexcept override;
  [[nodiscard]] Result<void> cleanup_step() override;

  void adopt_open_handles(trevrpc_rpc_call_v1 call, trevrpc_rpc_stream_v1 stream,
                          std::uint64_t close_operation, bool stream_registered) noexcept;

  RpcClientStream(std::shared_ptr<RpcEventRuntime> runtime, ChannelCore::GenerationLease generation,
                  trevrpc_rpc_call_v1 call, trevrpc_rpc_stream_v1 stream,
                  std::uint64_t close_operation,
                  std::optional<RpcCancellation::AttachmentLease> cancellation, std::uint32_t kind,
                  bool stream_registered = true) noexcept
      : runtime_(std::move(runtime)), generation_(std::move(generation)), call_(call),
        stream_(stream), close_operation_(close_operation), cancellation_(std::move(cancellation)),
        kind_(kind), stream_registered_(stream_registered),
        send_finished_(kind == TREVRPC_RPC_KIND_UNARY ||
                       kind == TREVRPC_RPC_KIND_SERVER_STREAMING) {}

private:
  friend class RpcEventRuntime;
  friend struct AsyncStreamAccess;
  friend struct RpcClientStreamTestPeer;

  enum class Activity : std::uint8_t { Send, Receive, Cancel };

  struct NativeSnapshot {
    std::shared_ptr<RpcEventRuntime> runtime;
    trevrpc_rpc_call_v1 call{};
    trevrpc_rpc_stream_v1 stream{};
    std::uint64_t close_operation = 0;
  };

  class ActivityGuard {
  public:
    ActivityGuard(RpcClientStream* owner, Activity activity) noexcept
        : owner_(owner), activity_(activity) {}
    ~ActivityGuard();
    ActivityGuard(const ActivityGuard&) = delete;
    ActivityGuard& operator=(const ActivityGuard&) = delete;
    void release() noexcept;

  private:
    RpcClientStream* owner_;
    Activity activity_;
    bool active_ = true;
  };

  [[nodiscard]] Result<NativeSnapshot> begin_activity(Activity activity);
  void end_activity(Activity activity) noexcept;
  [[nodiscard]] static Result<std::uint64_t>
  reserve_operation(const std::shared_ptr<RpcEventRuntime>& runtime);
  [[nodiscard]] Result<void> wait_operation(const std::shared_ptr<RpcEventRuntime>& runtime,
                                            std::uint64_t operation, std::uint32_t expected_kind,
                                            const char* description);
  [[nodiscard]] bool defer_operation(std::uint64_t operation) noexcept;
  void schedule_deferred_cleanup() noexcept;
  [[nodiscard]] Result<StreamFrame> decode_receive(trevrpc_rpc_receive* receive);
  [[nodiscard]] Result<RpcEvent> wait_stream_event(const std::shared_ptr<RpcEventRuntime>& runtime,
                                                   trevrpc_rpc_stream_v1 stream);
  void receive_async_step(NativeSnapshot native, ReceiveCallback callback) noexcept;
  [[nodiscard]] bool handle_async_receive_event(ReceiveCallback& callback,
                                                 const RpcEvent& event) noexcept;
  void defer_receive_terminal(const RpcEvent& event) noexcept;
  [[nodiscard]] std::optional<Result<StreamFrame>> finish_deferred_receive_terminal();
  void finish_async_receive(const ReceiveCallback& callback,
                            Result<StreamFrame> result) noexcept;
  void note_event(const RpcEvent& event) noexcept;
  static void test_fail_next_receive_allocation() noexcept;
  static void test_fail_next_cleanup_owner_allocation() noexcept;
  static void test_fail_next_cleanup_task_allocation() noexcept;
  static void test_fail_next_cleanup_requeue_allocation() noexcept;
  static bool test_consume_cleanup_task_allocation_failure() noexcept;
  static bool test_consume_cleanup_requeue_allocation_failure() noexcept;
  static void test_shutdown_cleanup_service() noexcept;
  [[nodiscard]] Result<void> close_impl();

  std::shared_ptr<RpcEventRuntime> runtime_;
  ChannelCore::GenerationLease generation_;
  trevrpc_rpc_call_v1 call_{};
  trevrpc_rpc_stream_v1 stream_{};
  std::uint64_t close_operation_ = 0;
  std::optional<RpcCancellation::AttachmentLease> cancellation_;
  std::uint32_t kind_ = TREVRPC_RPC_KIND_UNARY;
  bool stream_registered_ = true;

  mutable std::mutex mutex_;
  std::mutex send_mutex_;
  std::condition_variable lifecycle_condition_;
  bool send_finished_ = false;
  bool receive_finished_ = false;
  std::size_t send_active_ = 0;
  std::size_t receive_active_ = 0;
  std::size_t cancel_active_ = 0;
  bool status_seen_ = false;
  bool unary_message_seen_ = false;
  bool receive_fin_seen_ = false;
  bool close_requested_ = false;
  bool closed_ = false;
  bool close_submitted_ = false;
  bool close_operation_consumed_ = false;
  bool stream_unregistered_ = false;
  bool stream_unregistering_ = false;
  bool stream_released_ = false;
  bool call_released_ = false;
  bool cleanup_in_progress_ = false;
  std::thread::id cleanup_owner_thread_{};
  bool cleanup_abandoned_ = false;
  int cleanup_result_code_ = 0;
  bool stream_closed_ = false;
  bool call_closed_ = false;
  bool call_failed_ = false;
  std::optional<RpcEvent> deferred_receive_terminal_;
  std::array<std::uint64_t, 4> deferred_operations_{};
  std::size_t deferred_operation_count_ = 0;
  std::optional<Status> terminal_status_;
  std::vector<std::byte> unary_body_;
};

} // namespace trevrpc::detail
