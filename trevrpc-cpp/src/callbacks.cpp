#include "detail/callbacks.hpp"

#include "detail/lifecycle.hpp"

#include <cerrno>
#include <string>
#include <utility>

namespace trevrpc::detail {
namespace {

[[nodiscard]] std::string copy_string(const char* data, std::size_t size) {
  return data == nullptr ? std::string{} : std::string(data, size);
}

[[nodiscard]] Metadata copy_metadata(const trevrpc_metadata& metadata) {
  Metadata result;
  for (std::size_t index = 0; index < metadata.entries_len; ++index) {
    const auto& entry = metadata.entries[index];
    const auto* data = reinterpret_cast<const std::byte*>(entry.value);
    result.set(copy_string(entry.key, entry.key_len), std::span(data, entry.value_len));
  }
  return result;
}

void report_exception(const CallbackExceptionState& state, std::string_view callback,
                      std::exception_ptr&& exception) noexcept {
  if (!state.sink) {
    return;
  }
  try {
    state.sink->callback_exception(callback, std::move(exception));
  } catch (...) {
    (void)std::current_exception();
  }
}

template <typename State> [[nodiscard]] State* checked_state(void* user_data) noexcept {
  return static_cast<State*>(user_data);
}

void rpc_started_trampoline(void* user_data, const trevrpc_rpc_started_event* event) noexcept {
  auto* state = checked_state<MetricsCallbackState>(user_data);
  if (state == nullptr || !state->callback || event == nullptr) {
    return;
  }
  ServerCallbackContextGuard callback_context;
  try {
    state->callback->rpc_started(RpcStartedEvent{copy_string(event->service, event->service_len),
                                                 copy_string(event->method, event->method_len),
                                                 event->request_body_len});
  } catch (...) {
    report_exception(*state, "metrics.rpc_started", std::current_exception());
  }
}

void rpc_finished_trampoline(void* user_data, const trevrpc_rpc_finished_event* event) noexcept {
  auto* state = checked_state<MetricsCallbackState>(user_data);
  if (state == nullptr || !state->callback || event == nullptr) {
    return;
  }
  ServerCallbackContextGuard callback_context;
  try {
    state->callback->rpc_finished(
        RpcFinishedEvent{copy_string(event->service, event->service_len),
                         copy_string(event->method, event->method_len), event->request_body_len,
                         event->response_body_len,
                         static_cast<StatusCode>(trevrpc_status_code_from_uint32(event->status)),
                         std::chrono::nanoseconds(event->elapsed_nanos)});
  } catch (...) {
    report_exception(*state, "metrics.rpc_finished", std::current_exception());
  }
}

void logger_trampoline(void* user_data, const trevrpc_log_event* event) noexcept {
  auto* state = checked_state<LoggerCallbackState>(user_data);
  if (state == nullptr || !state->callback || event == nullptr) {
    return;
  }
  ServerCallbackContextGuard callback_context;
  try {
    state->callback->log(LogEvent{event->level, copy_string(event->event, event->event_len),
                                  copy_string(event->message, event->message_len),
                                  copy_string(event->service, event->service_len),
                                  copy_string(event->method, event->method_len),
                                  event->error_code});
  } catch (...) {
    report_exception(*state, "logger", std::current_exception());
  }
}

void transport_trampoline(void* user_data, const trevrpc_transport_event* event) noexcept {
  auto* state = checked_state<TransportCallbackState>(user_data);
  if (state == nullptr || !state->callback || event == nullptr) {
    return;
  }
  ServerCallbackContextGuard callback_context;
  try {
    state->callback->transport_event(
        TransportEvent{event->kind, event->transport, event->error_code,
                       copy_string(event->message, event->message_len)});
  } catch (...) {
    report_exception(*state, "transport_observer", std::current_exception());
  }
}

} // namespace

struct CallbackAccess {
  [[nodiscard]] static CallContext make_context(const trevrpc_call_context* context,
                                                const trevrpc_request* request) {
    return CallContext(context, request);
  }
};

std::shared_ptr<AuthorizerCallbackState>
make_authorizer_state(std::shared_ptr<Authorizer> callback,
                      std::shared_ptr<CallbackExceptionSink> sink) {
  auto state = std::make_shared<AuthorizerCallbackState>();
  state->callback = std::move(callback);
  state->sink = std::move(sink);
  return state;
}

std::shared_ptr<MetricsCallbackState>
make_metrics_state(std::shared_ptr<MetricsObserver> callback,
                   std::shared_ptr<CallbackExceptionSink> sink) {
  auto state = std::make_shared<MetricsCallbackState>();
  state->callback = std::move(callback);
  state->sink = std::move(sink);
  state->native = {rpc_started_trampoline, rpc_finished_trampoline, state.get()};
  return state;
}

std::shared_ptr<LoggerCallbackState>
make_logger_state(std::shared_ptr<Logger> callback, std::shared_ptr<CallbackExceptionSink> sink) {
  auto state = std::make_shared<LoggerCallbackState>();
  state->callback = std::move(callback);
  state->sink = std::move(sink);
  state->native = {logger_trampoline, state.get()};
  return state;
}

std::shared_ptr<TransportCallbackState>
make_transport_state(std::shared_ptr<TransportObserver> callback,
                     std::shared_ptr<CallbackExceptionSink> sink) {
  auto state = std::make_shared<TransportCallbackState>();
  state->callback = std::move(callback);
  state->sink = std::move(sink);
  state->native = {transport_trampoline, state.get()};
  return state;
}

std::shared_ptr<WebTransportAdmissionState>
make_webtransport_admission_state(std::shared_ptr<WebTransportAdmission> callback,
                                  std::shared_ptr<CallbackExceptionSink> sink) {
  auto state = std::make_shared<WebTransportAdmissionState>();
  state->callback = std::move(callback);
  state->sink = std::move(sink);
  return state;
}

std::shared_ptr<Http3AdmissionState>
make_http3_admission_state(std::shared_ptr<Http3Admission> callback,
                           std::shared_ptr<CallbackExceptionSink> sink) {
  auto state = std::make_shared<Http3AdmissionState>();
  state->callback = std::move(callback);
  state->sink = std::move(sink);
  return state;
}

std::shared_ptr<ChannelLifecycleCallbackState>
make_channel_lifecycle_state(std::shared_ptr<ChannelLifecycleObserver> callback,
                             std::shared_ptr<CallbackExceptionSink> sink) {
  auto state = std::make_shared<ChannelLifecycleCallbackState>();
  state->callback = std::move(callback);
  state->sink = std::move(sink);
  return state;
}

int authorizer_trampoline(void* user_data, const trevrpc_call_context* context,
                          const trevrpc_request* request, trevrpc_status* status) noexcept {
  auto* state = checked_state<AuthorizerCallbackState>(user_data);
  if (state == nullptr || !state->callback || request == nullptr || status == nullptr) {
    return -EINVAL;
  }
  ServerCallbackContextGuard callback_context;
  try {
    AuthorizationRequest owned{copy_string(request->service, request->service_len),
                               copy_string(request->method, request->method_len),
                               copy_metadata(request->metadata), request->body_len, request->kind};
    const Status result =
        state->callback->authorize(CallbackAccess::make_context(context, request), owned);
    thread_local std::string message;
    message = result.message();
    *status = trevrpc_status_new(static_cast<std::uint32_t>(result.code()), message.data(),
                                 message.size());
    return 0;
  } catch (...) {
    report_exception(*state, "authorizer", std::current_exception());
    constexpr std::string_view message = "authorizer callback threw";
    *status = trevrpc_status_internal(message.data(), message.size());
    return 0;
  }
}

int webtransport_admission_trampoline(
    void* user_data, const trevrpc_webtransport_admission_request* request) noexcept {
  auto* state = checked_state<WebTransportAdmissionState>(user_data);
  if (state == nullptr || !state->callback || request == nullptr) {
    return -1;
  }
  ServerCallbackContextGuard callback_context;
  try {
    return state->callback->admit(WebTransportAdmissionRequest{
               copy_string(request->path, request->path_len),
               copy_string(request->authority, request->authority_len),
               copy_string(request->origin, request->origin_len), request->secure != 0})
               ? 0
               : -1;
  } catch (...) {
    report_exception(*state, "webtransport_admission", std::current_exception());
    return -1;
  }
}

int http3_admission_trampoline(void* user_data,
                               const trevrpc_http3_admission_request* request) noexcept {
  auto* state = checked_state<Http3AdmissionState>(user_data);
  if (state == nullptr || !state->callback || request == nullptr) {
    return -1;
  }
  ServerCallbackContextGuard callback_context;
  try {
    return state->callback->admit(Http3AdmissionRequest{
               copy_string(request->path, request->path_len),
               copy_string(request->authority, request->authority_len), request->secure != 0})
               ? 0
               : -1;
  } catch (...) {
    report_exception(*state, "http3_admission", std::current_exception());
    return -1;
  }
}

void channel_lifecycle_trampoline(void* user_data, const trevrpc_channel_event* event) noexcept {
  auto* state = checked_state<ChannelLifecycleCallbackState>(user_data);
  if (state == nullptr || !state->callback || event == nullptr) {
    return;
  }
  ChannelCallbackContextGuard guard;
  try {
    state->callback->channel_event(
        ChannelLifecycleEvent{event->kind, event->state, event->generation, event->error_code});
  } catch (...) {
    report_exception(*state, "channel_lifecycle", std::current_exception());
  }
}

} // namespace trevrpc::detail
