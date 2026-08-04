#include <trevrpc.h>

#include <stddef.h>

int main(void) {
    trevrpc_call_options_v1 options;
    if (TREVRPC_C_ABI_VERSION != 6u || trevrpc_c_abi_version() != 6u ||
        trevrpc_call_options_v1_init(&options, sizeof(options)) != 0 ||
        options.request_body_lifetime != TREVRPC_REQUEST_BODY_COPY) {
        return 1;
    }
    return 0;
}
