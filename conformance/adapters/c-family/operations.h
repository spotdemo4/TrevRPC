#ifndef TREVRPC_CONFORMANCE_C_FAMILY_OPERATIONS_H
#define TREVRPC_CONFORMANCE_C_FAMILY_OPERATIONS_H

#include "peer.h"

int cf_dispatch_operation(const cf_command *command,
                          cf_state_dispatch_fn state_dispatch, cf_json *payload,
                          cf_error *error);

#endif
