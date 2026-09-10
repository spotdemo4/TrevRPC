#ifndef TREVRPC_MSQUIC_API_OWNER_H
#define TREVRPC_MSQUIC_API_OWNER_H

#if __has_include(<msquic.h>)
#include <msquic.h>
#else
#include <inc/msquic.h>
#endif

int trevrpc_msquic_api_owner_acquire(const QUIC_API_TABLE** out_api);
void trevrpc_msquic_api_owner_release(const QUIC_API_TABLE* api);

#endif
