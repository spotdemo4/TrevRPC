#ifndef TREVRPC_CONFORMANCE_CPP_RPC_HARNESS_SUPPORT_H
#define TREVRPC_CONFORMANCE_CPP_RPC_HARNESS_SUPPORT_H

#include "rpc_event_runtime_fake_fixture.h"

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

void cf_rpc_fake_fail_receive(trevrpc_cpp_rpc_fake_fixture* fixture, int error);
size_t cf_rpc_fake_close_count(trevrpc_cpp_rpc_fake_fixture* fixture);

#ifdef __cplusplus
}
#endif

#endif
