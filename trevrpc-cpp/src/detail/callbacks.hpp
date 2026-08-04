#pragma once

#include <trevrpc/callbacks.hpp>

#include <memory>

namespace trevrpc::detail {

struct CallbackExceptionState {
  std::shared_ptr<CallbackExceptionSink> sink;
};

struct AuthorizerCallbackState : CallbackExceptionState {
  std::shared_ptr<Authorizer> callback;
};

struct MetricsCallbackState : CallbackExceptionState {
  std::shared_ptr<MetricsObserver> callback;
  trevrpc_metrics native{};
};

struct LoggerCallbackState : CallbackExceptionState {
  std::shared_ptr<Logger> callback;
  trevrpc_logger native{};
};

struct TransportCallbackState : CallbackExceptionState {
  std::shared_ptr<TransportObserver> callback;
  trevrpc_transport_observer native{};
};

struct WebTransportAdmissionState : CallbackExceptionState {
  std::shared_ptr<WebTransportAdmission> callback;
};

struct Http3AdmissionState : CallbackExceptionState {
  std::shared_ptr<Http3Admission> callback;
};

struct ChannelLifecycleCallbackState : CallbackExceptionState {
  std::shared_ptr<ChannelLifecycleObserver> callback;
};

[[nodiscard]] std::shared_ptr<AuthorizerCallbackState>
make_authorizer_state(std::shared_ptr<Authorizer> callback,
                      std::shared_ptr<CallbackExceptionSink> sink);
[[nodiscard]] std::shared_ptr<MetricsCallbackState>
make_metrics_state(std::shared_ptr<MetricsObserver> callback,
                   std::shared_ptr<CallbackExceptionSink> sink);
[[nodiscard]] std::shared_ptr<LoggerCallbackState>
make_logger_state(std::shared_ptr<Logger> callback, std::shared_ptr<CallbackExceptionSink> sink);
[[nodiscard]] std::shared_ptr<TransportCallbackState>
make_transport_state(std::shared_ptr<TransportObserver> callback,
                     std::shared_ptr<CallbackExceptionSink> sink);
[[nodiscard]] std::shared_ptr<WebTransportAdmissionState>
make_webtransport_admission_state(std::shared_ptr<WebTransportAdmission> callback,
                                  std::shared_ptr<CallbackExceptionSink> sink);
[[nodiscard]] std::shared_ptr<Http3AdmissionState>
make_http3_admission_state(std::shared_ptr<Http3Admission> callback,
                           std::shared_ptr<CallbackExceptionSink> sink);
[[nodiscard]] std::shared_ptr<ChannelLifecycleCallbackState>
make_channel_lifecycle_state(std::shared_ptr<ChannelLifecycleObserver> callback,
                             std::shared_ptr<CallbackExceptionSink> sink);

int authorizer_trampoline(void* user_data, const trevrpc_call_context* context,
                          const trevrpc_request* request, trevrpc_status* status) noexcept;
int webtransport_admission_trampoline(
    void* user_data, const trevrpc_webtransport_admission_request* request) noexcept;
int http3_admission_trampoline(void* user_data,
                               const trevrpc_http3_admission_request* request) noexcept;
void channel_lifecycle_trampoline(void* user_data, const trevrpc_channel_event* event) noexcept;

} // namespace trevrpc::detail
