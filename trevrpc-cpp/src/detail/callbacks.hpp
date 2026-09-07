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
};

struct LoggerCallbackState : CallbackExceptionState {
  std::shared_ptr<Logger> callback;
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
[[nodiscard]] std::shared_ptr<ChannelLifecycleCallbackState>
make_channel_lifecycle_state(std::shared_ptr<ChannelLifecycleObserver> callback,
                             std::shared_ptr<CallbackExceptionSink> sink);

} // namespace trevrpc::detail
