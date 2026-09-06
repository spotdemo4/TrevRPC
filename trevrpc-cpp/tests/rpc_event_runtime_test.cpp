#include <trevrpc_rpc.h>
#include <trevrpc_rpc_msquic.h>

#include "detail/rpc_event_runtime.hpp"

#include <cassert>
#include <cerrno>
#include <memory>

int main() {
  trevrpc_rpc_runtime_config_v1 runtime_config{};
  trevrpc_rpc_msquic_config_v1 provider_config{};
  assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
  assert(trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);

  trevrpc_rpc_runtime* raw_runtime = nullptr;
  assert(trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &raw_runtime) == 0);
  auto adopted = trevrpc::detail::RpcEventRuntime::adopt(raw_runtime);
  assert(adopted);
  std::shared_ptr<trevrpc::detail::RpcEventRuntime> runtime = std::move(adopted).value();

  auto first = runtime->reserve_operation();
  auto second = runtime->reserve_operation();
  assert(first);
  assert(second);
  assert(first.value() != 0);
  assert(second.value() != 0);
  assert(first.value() != second.value());

  runtime->reject_operation(first.value());
  auto operation_result = runtime->wait_operation(first.value());
  assert(!operation_result);
  assert(operation_result.error().code() == -EINVAL);
  runtime->reject_operation(second.value());

  assert(runtime->shutdown());

  auto after_shutdown = runtime->reserve_operation();
  assert(!after_shutdown);
  assert(after_shutdown.error().code() == -ESHUTDOWN);
  return 0;
}
