#pragma once

#include <trevrpc/trevrpc.hpp>

#include "rpc_event_runtime.hpp"

#include <trevrpc_rpc.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace trevrpc::detail {

class ThreadCompletionToken;

enum class ChannelCorePhase {
  Connecting,
  Ready,
  Reconnecting,
  Closed,
};

enum class ChannelCoreEventKind {
  StateChanged,
  ConnectFailed,
  ConnectionShutdown,
};

struct ChannelCoreEvent {
  ChannelCoreEventKind kind = ChannelCoreEventKind::StateChanged;
  ChannelCorePhase phase = ChannelCorePhase::Connecting;
  std::uint64_t generation = 0;
  int error_code = 0;
};

class RpcCancellation final {
  struct SharedState;
  struct Attachment;

public:
  class AttachmentLease final {
  public:
    AttachmentLease() noexcept = default;
    ~AttachmentLease() = default;
    AttachmentLease(const AttachmentLease&) = delete;
    AttachmentLease& operator=(const AttachmentLease&) = delete;
    AttachmentLease(AttachmentLease&&) noexcept = default;
    AttachmentLease& operator=(AttachmentLease&&) noexcept = default;

    [[nodiscard]] trevrpc_rpc_cancellation_v1 handle() const noexcept;

  private:
    friend class RpcCancellation;
    explicit AttachmentLease(std::shared_ptr<Attachment> attachment) noexcept
        : attachment_(std::move(attachment)) {}

    std::shared_ptr<Attachment> attachment_;
  };

  class Subscription final {
  public:
    Subscription() noexcept = default;
    ~Subscription();
    Subscription(const Subscription&) = delete;
    Subscription& operator=(const Subscription&) = delete;
    Subscription(Subscription&& other) noexcept;
    Subscription& operator=(Subscription&& other) noexcept;

  private:
    friend class RpcCancellation;
    Subscription(std::shared_ptr<SharedState> state, std::uint64_t id) noexcept
        : state_(std::move(state)), id_(id) {}

    void reset() noexcept;

    std::shared_ptr<SharedState> state_;
    std::uint64_t id_ = 0;
  };

  RpcCancellation();
  ~RpcCancellation() = default;
  RpcCancellation(const RpcCancellation&) noexcept = default;
  RpcCancellation& operator=(const RpcCancellation&) noexcept = default;
  RpcCancellation(RpcCancellation&&) noexcept = default;
  RpcCancellation& operator=(RpcCancellation&&) noexcept = default;

  void cancel() noexcept;
  [[nodiscard]] bool cancelled() const noexcept;
  [[nodiscard]] Result<AttachmentLease>
  attach(const std::shared_ptr<RpcEventRuntime>& runtime) const;
  [[nodiscard]] Result<Subscription> subscribe(const std::function<void()>& callback) const;

private:
  std::shared_ptr<SharedState> state_;
};

struct ChannelCoreConfig {
  std::string host;
  std::uint16_t port = 0;
  std::string cert_file;
  std::string key_file;
  std::string ca_cert_file;
  bool skip_certificate_validation = false;
  std::chrono::milliseconds max_idle_timeout{0};
  std::chrono::milliseconds keep_alive{0};
  std::uint16_t peer_bidi_stream_count = 0;
  std::size_t max_pending_send_bytes = 0;
  std::size_t max_pending_send_count = 0;
  std::size_t max_frame_size = 0;
  std::uint32_t stream_recv_window = 0;
  std::uint32_t conn_flow_control_window = 0;

  std::chrono::milliseconds reconnect_initial_delay{100};
  std::chrono::milliseconds reconnect_max_delay{30'000};
  double reconnect_multiplier = 2.0;

  std::size_t lifecycle_capacity = 64;
  std::function<void(const ChannelCoreEvent&)> lifecycle_observer;
  std::function<void(std::exception_ptr)> callback_exception_sink;
};

class ChannelCore final {
  struct SharedState;
  struct Generation;

public:
  using EndpointStarter =
      std::function<int(trevrpc_rpc_runtime*, std::uint64_t, trevrpc_rpc_endpoint_v1*)>;
  using Backoff = std::function<std::chrono::milliseconds(std::size_t)>;

  class GenerationLease final {
  public:
    GenerationLease() noexcept = default;
    ~GenerationLease();
    GenerationLease(const GenerationLease&) = delete;
    GenerationLease& operator=(const GenerationLease&) = delete;
    GenerationLease(GenerationLease&& other) noexcept;
    GenerationLease& operator=(GenerationLease&& other) noexcept;

    [[nodiscard]] std::uint64_t generation() const noexcept;
    [[nodiscard]] trevrpc_rpc_endpoint_v1 endpoint() const noexcept;
    [[nodiscard]] std::shared_ptr<RpcEventRuntime> runtime() const noexcept;

  private:
    friend class ChannelCore;
    explicit GenerationLease(std::shared_ptr<Generation> generation) noexcept;
    void reset() noexcept;

    std::shared_ptr<Generation> generation_;
  };

  [[nodiscard]] static Result<std::shared_ptr<ChannelCore>>
  create(std::shared_ptr<RpcEventRuntime> runtime, ChannelCoreConfig config,
         EndpointStarter endpoint_starter, Backoff backoff = {});
  [[nodiscard]] static Result<std::shared_ptr<ChannelCore>> connect(ChannelCoreConfig config);

  ~ChannelCore();
  ChannelCore(const ChannelCore&) = delete;
  ChannelCore& operator=(const ChannelCore&) = delete;

  [[nodiscard]] ChannelCorePhase phase() const noexcept;
  [[nodiscard]] std::uint64_t generation() const noexcept;
  [[nodiscard]] Result<std::uint64_t>
  wait_ready(std::chrono::nanoseconds timeout = std::chrono::nanoseconds{0},
             const RpcCancellation* cancellation = nullptr) const;
  [[nodiscard]] Result<GenerationLease> acquire_generation();
  [[nodiscard]] Result<void> request_close() noexcept;
  [[nodiscard]] Result<void> close();

private:
  friend class ChannelCoreTestPeer;

  explicit ChannelCore(std::shared_ptr<SharedState> state) noexcept : state_(std::move(state)) {}

  [[nodiscard]] static Result<std::shared_ptr<RpcEventRuntime>>
  adopt_created_runtime(trevrpc_rpc_runtime* runtime, const trevrpc_rpc_runtime_config_v1& config);
  static void test_fail_next_start(int error) noexcept;
  void test_wait_for_attempts(std::size_t count);
  void test_wait_for_retired(std::size_t count);
  void test_wait_for_lifecycle_idle();
  void test_enqueue_lifecycle(ChannelCoreEvent event);
  [[nodiscard]] std::size_t test_live_generations() const noexcept;

  std::thread worker_;
  std::shared_ptr<ThreadCompletionToken> worker_completion_;
  std::shared_ptr<SharedState> state_;
};

} // namespace trevrpc::detail
