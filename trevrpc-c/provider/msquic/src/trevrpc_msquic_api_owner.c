#define QUIC_API_ENABLE_VERSIONED_FEATURES 1

#include "trevrpc_msquic_api_owner.h"

#include <errno.h> // NOLINT(misc-include-cleaner)
#include <pthread.h>
#include <stddef.h>

static pthread_mutex_t trevrpc_msquic_api_owner_mutex = PTHREAD_MUTEX_INITIALIZER;
static const QUIC_API_TABLE* trevrpc_msquic_api_owner_table;
static size_t trevrpc_msquic_api_owner_leases;

int trevrpc_msquic_api_owner_acquire(const QUIC_API_TABLE** out_api) {
    if (out_api == NULL) {
        return -EINVAL;
    }

    pthread_mutex_lock(&trevrpc_msquic_api_owner_mutex);
    if (trevrpc_msquic_api_owner_table == NULL) {
        QUIC_STATUS status = MsQuicOpen2(&trevrpc_msquic_api_owner_table);
        if (QUIC_FAILED(status)) {
            pthread_mutex_unlock(&trevrpc_msquic_api_owner_mutex);
            return -EIO;
        }
    }
    if (trevrpc_msquic_api_owner_leases == SIZE_MAX) {
        pthread_mutex_unlock(&trevrpc_msquic_api_owner_mutex);
        return -EOVERFLOW;
    }
    ++trevrpc_msquic_api_owner_leases;
    *out_api = trevrpc_msquic_api_owner_table;
    pthread_mutex_unlock(&trevrpc_msquic_api_owner_mutex);
    return 0;
}

void trevrpc_msquic_api_owner_release(const QUIC_API_TABLE* api) {
    pthread_mutex_lock(&trevrpc_msquic_api_owner_mutex);
    if (api == trevrpc_msquic_api_owner_table && trevrpc_msquic_api_owner_leases != 0) {
        --trevrpc_msquic_api_owner_leases;
#if !defined(TREVRPC_SANITIZER_BUILD)
        if (trevrpc_msquic_api_owner_leases == 0) {
            const QUIC_API_TABLE* table = trevrpc_msquic_api_owner_table;
            trevrpc_msquic_api_owner_table = NULL;
            MsQuicClose(table);
        }
#endif
    }
    pthread_mutex_unlock(&trevrpc_msquic_api_owner_mutex);
}
