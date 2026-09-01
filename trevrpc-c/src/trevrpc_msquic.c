/*
 * ABI 6 is implemented by the single callback-native implementation in
 * trevrpc_msquic_core.c.  This translation unit deliberately contains only an
 * archive anchor so libtrevrpc_msquic.a remains an independently versioned
 * public compatibility archive while all transport state and callbacks occur
 * exactly once in libtrevrpc_msquic_core.a.
 */
void trevrpc_msquic_abi6_shim_archive_anchor(void) {
}
