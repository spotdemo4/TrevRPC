#ifndef TREVRPC_CREDENTIAL_TESTING_INTERNAL_H
#define TREVRPC_CREDENTIAL_TESTING_INTERNAL_H

struct trevrpc_credential_files;

typedef struct trevrpc_credential_test_hooks {
    int (*fail_cleanup)(void);
    void (*before_cleanup)(struct trevrpc_credential_files* files);
} trevrpc_credential_test_hooks;

void trevrpc_credential_testing_set_hooks(const trevrpc_credential_test_hooks* hooks);

#endif
