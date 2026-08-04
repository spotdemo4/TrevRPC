#include "peer.h"

extern "C" int cf_cpp_state_dispatch(const cf_command *command,
                                     cf_json *payload, cf_error *error);

int main(int argc, char **argv) {
  return cf_peer_main("cpp", argc, argv, cf_cpp_state_dispatch);
}
