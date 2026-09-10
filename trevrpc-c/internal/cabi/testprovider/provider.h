#ifndef TREVRPC_GO_TEST_PROVIDER_H
#define TREVRPC_GO_TEST_PROVIDER_H

#include "trevrpc_engine_provider.h"
#include "trevrpc_transport_provider.h"

int trevrpc_go_test_engine_provider_create(const trevrpc_engine_provider_host_v1* host,
    uint64_t owner_cookie,
    const trevrpc_engine_provider_ops_v1** out_ops,
    void** out_context);
void trevrpc_go_test_engine_provider_dispose(void* context);
uint64_t trevrpc_go_test_engine_destroy_count(void);

int trevrpc_go_test_transport_provider_create(const trevrpc_transport_provider_ops_v1** out_ops, void** out_context);
void trevrpc_go_test_transport_provider_dispose(void* context);
void trevrpc_go_test_transport_ops_set_size(const trevrpc_transport_provider_ops_v1* operations, uint32_t size);
void trevrpc_go_test_transport_ops_set_version(const trevrpc_transport_provider_ops_v1* operations, uint32_t version);
void trevrpc_go_test_transport_ops_set_reserved(const trevrpc_transport_provider_ops_v1* operations, uint64_t value);
uint64_t trevrpc_go_test_transport_destroy_count(void);
uint64_t trevrpc_go_test_transport_destroy_before_drain_count(void);

#endif
