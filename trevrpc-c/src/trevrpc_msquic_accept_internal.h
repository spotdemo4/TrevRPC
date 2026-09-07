#ifndef TREVRPC_MSQUIC_ACCEPT_INTERNAL_H
#define TREVRPC_MSQUIC_ACCEPT_INTERNAL_H

#include "trevrpc_msquic_types_internal.h"

#include <errno.h>
#include <stdbool.h>

typedef void (*trevrpc_msquic_endpoint_lease_release)(void* lease);

/*
 * Private bridge state passed from the MsQuic listener to a canonical native
 * Engine/Transport adapter. Ownership remains with the accepting provider
 * until one of the bridge operations claims or wraps the connection.
 */
typedef struct trevrpc_msquic_accepted_connection {
    trevrpc_msquic_listener* listener;
    void* connection;
    void* registration;
    void* configuration;
    const uint8_t* negotiated_alpn;
    size_t negotiated_alpn_len;
    void* endpoint_lease;
    trevrpc_msquic_endpoint_lease_release endpoint_lease_release;
    bool claimed;
} trevrpc_msquic_accepted_connection;

typedef enum trevrpc_msquic_accept_disposition {
    TREV_MSQUIC_ACCEPT_FALLTHROUGH = 0,
    TREV_MSQUIC_ACCEPT_ADOPTED = 1,
    TREV_MSQUIC_ACCEPT_REJECTED = 2,
} trevrpc_msquic_accept_disposition;

typedef trevrpc_msquic_accept_disposition (*trevrpc_msquic_accept_dispatch)(
    void* context, trevrpc_msquic_accepted_connection* accepted);
typedef void (*trevrpc_msquic_context_destroy)(void* context);

static inline int trevrpc_msquic_accepted_connection_get_native(trevrpc_msquic_accepted_connection* accepted,
    void** out_connection,
    void** out_registration,
    void** out_configuration) {
    if (accepted == NULL || out_connection == NULL || out_registration == NULL || out_configuration == NULL ||
        accepted->claimed)
        return EINVAL;
    *out_connection = accepted->connection;
    *out_registration = accepted->registration;
    *out_configuration = accepted->configuration;
    return 0;
}

static inline int trevrpc_msquic_accepted_connection_take_endpoint_lease(trevrpc_msquic_accepted_connection* accepted,
    void** out_lease,
    trevrpc_msquic_endpoint_lease_release* out_release) {
    if (accepted == NULL || out_lease == NULL || out_release == NULL || accepted->claimed ||
        accepted->endpoint_lease == NULL || accepted->endpoint_lease_release == NULL)
        return EINVAL;
    *out_lease = accepted->endpoint_lease;
    *out_release = accepted->endpoint_lease_release;
    accepted->endpoint_lease = NULL;
    accepted->endpoint_lease_release = NULL;
    return 0;
}

static inline int trevrpc_msquic_accepted_connection_get_alpn(
    const trevrpc_msquic_accepted_connection* accepted, const uint8_t** out_alpn, size_t* out_alpn_len) {
    if (accepted == NULL || out_alpn == NULL || out_alpn_len == NULL)
        return EINVAL;
    *out_alpn = accepted->negotiated_alpn;
    *out_alpn_len = accepted->negotiated_alpn_len;
    return 0;
}

static inline int trevrpc_msquic_accepted_connection_claim_raw(trevrpc_msquic_accepted_connection* accepted) {
    if (accepted == NULL || accepted->claimed)
        return EINVAL;
    accepted->claimed = true;
    return 0;
}

int trevrpc_msquic_accepted_connection_wrap(
    trevrpc_msquic_accepted_connection* accepted, trevrpc_msquic_conn** out_connection);

#endif
