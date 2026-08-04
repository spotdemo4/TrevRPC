#pragma once

#include <trevrpc/trevrpc.hpp>

#include <exception>
#include <memory>
#include <string>
#include <string_view>

namespace trevrpc {

struct AuthorizationRequest {
  std::string service;
  std::string method;
  Metadata metadata;
  std::size_t body_size = 0;
  std::uint32_t kind = 0;
};

struct RpcStartedEvent {
  std::string service;
  std::string method;
  std::size_t request_body_size = 0;
};

struct RpcFinishedEvent {
  std::string service;
  std::string method;
  std::size_t request_body_size = 0;
  std::size_t response_body_size = 0;
  StatusCode status = StatusCode::Unknown;
  std::chrono::nanoseconds elapsed{0};
};

struct TransportEvent {
  std::uint32_t kind = 0;
  std::uint32_t transport = 0;
  int error_code = 0;
  std::string message;
};

struct LogEvent {
  std::uint32_t level = 0;
  std::string event;
  std::string message;
  std::string service;
  std::string method;
  int error_code = 0;
};

struct WebTransportAdmissionRequest {
  std::string path;
  std::string authority;
  std::string origin;
  bool secure = false;
};

struct Http3AdmissionRequest {
  std::string path;
  std::string authority;
  bool secure = false;
};

struct ChannelLifecycleEvent {
  std::uint32_t kind = 0;
  std::uint32_t state = 0;
  std::uint64_t generation = 0;
  int error_code = 0;
};

class CallbackExceptionSink {
public:
  virtual ~CallbackExceptionSink() = default;
  virtual void callback_exception(std::string_view callback, std::exception_ptr exception) = 0;
};

class Authorizer {
public:
  virtual ~Authorizer() = default;
  [[nodiscard]] virtual Status authorize(const CallContext& context,
                                         const AuthorizationRequest& request) = 0;
};

class MetricsObserver {
public:
  virtual ~MetricsObserver() = default;
  virtual void rpc_started(const RpcStartedEvent& event) = 0;
  virtual void rpc_finished(const RpcFinishedEvent& event) = 0;
};

class Logger {
public:
  virtual ~Logger() = default;
  virtual void log(const LogEvent& event) = 0;
};

class TransportObserver {
public:
  virtual ~TransportObserver() = default;
  virtual void transport_event(const TransportEvent& event) = 0;
};

class WebTransportAdmission {
public:
  virtual ~WebTransportAdmission() = default;
  [[nodiscard]] virtual bool admit(const WebTransportAdmissionRequest& request) = 0;
};

class Http3Admission {
public:
  virtual ~Http3Admission() = default;
  [[nodiscard]] virtual bool admit(const Http3AdmissionRequest& request) = 0;
};

class ChannelLifecycleObserver {
public:
  virtual ~ChannelLifecycleObserver() = default;
  virtual void channel_event(const ChannelLifecycleEvent& event) = 0;
};

} // namespace trevrpc
