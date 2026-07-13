#ifndef TREVRPC_BINDING_H
#define TREVRPC_BINDING_H

#include "trevrpc_raw.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*trevrpc_raw_client_shutdown_callback)(void* user_data, int error_code);

/*
 * Connects a raw client and reports transport shutdown through callback. The
 * callback may run on a transport thread and may run before this call returns.
 */
int trevrpc_raw_client_connect_cancellable_with_shutdown_callback(const char* host,
    uint16_t port,
    const trevrpc_config* config,
    trevrpc_cancellation* cancellation,
    trevrpc_raw_client_shutdown_callback callback,
    void* user_data,
    trevrpc_raw_client** client);

/*
 * Disables the callback and waits for an active callback to return. This must
 * not be called from the callback itself.
 */
void trevrpc_raw_client_clear_shutdown_callback(trevrpc_raw_client* client);

#ifdef __cplusplus
}
#endif

#endif
