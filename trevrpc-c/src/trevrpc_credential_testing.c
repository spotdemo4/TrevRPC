#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
#define _DARWIN_C_SOURCE
#endif
#define _POSIX_C_SOURCE 200809L

#include "trevrpc_credential_internal.h"
#include "trevrpc_credential_testing_internal.h"

static trevrpc_credential_test_hooks credential_test_hooks;

void trevrpc_credential_testing_set_hooks(const trevrpc_credential_test_hooks* hooks) {
    if (hooks == NULL) {
        credential_test_hooks = (trevrpc_credential_test_hooks){0};
        return;
    }
    credential_test_hooks = *hooks;
}

int trevrpc_credential_testing_should_fail_cleanup(void) {
    return credential_test_hooks.fail_cleanup != NULL && credential_test_hooks.fail_cleanup() != 0;
}

void trevrpc_credential_testing_before_cleanup(trevrpc_credential_files* files) {
    if (credential_test_hooks.before_cleanup != NULL)
        credential_test_hooks.before_cleanup(files);
}
