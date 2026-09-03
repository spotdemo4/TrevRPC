#include "trevrpc_msquic.h"

int main(void) {
    return trevrpc_msquic_error(0) != NULL ? 0 : 1;
}
