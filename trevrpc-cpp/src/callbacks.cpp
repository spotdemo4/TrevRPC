#include "detail/callbacks.hpp"

#include <utility>

namespace trevrpc::detail {

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
  return state;
}

std::shared_ptr<LoggerCallbackState>
make_logger_state(std::shared_ptr<Logger> callback, std::shared_ptr<CallbackExceptionSink> sink) {
  auto state = std::make_shared<LoggerCallbackState>();
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

} // namespace trevrpc::detail
