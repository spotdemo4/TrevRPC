#include "cpp_rpc_harness_support.h"

#include "rpc_fake_transport_support.h"

#include <stdatomic.h>

void cf_rpc_fake_fail_receive(trevrpc_cpp_rpc_fake_fixture* fixture, int error) {
    fake_transport* fake = (fake_transport*)fixture;
    atomic_store_explicit(&fake->stream_receive_result, error, memory_order_release);
}

size_t cf_rpc_fake_close_count(trevrpc_cpp_rpc_fake_fixture* fixture) {
    return fake_close_count((fake_transport*)fixture);
}
