#include "peer.h"

int main(int argc, char **argv) {
  return cf_peer_main("c", argc, argv, cf_c_state_dispatch);
}
