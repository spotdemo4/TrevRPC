#include <trevrpc_webtransport.h>

#include <charconv>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string_view>

int main(int argc, char** argv) {
  if (argc != 4) {
    std::cerr << "usage: webtransport-probe HOST PORT ORIGIN\n";
    return 2;
  }

  std::uint32_t port = 0;
  const std::string_view port_text(argv[2]);
  const auto [end, error] =
      std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
  if (error != std::errc{} || end != port_text.data() + port_text.size() || port == 0 ||
      port > std::numeric_limits<std::uint16_t>::max()) {
    std::cerr << "invalid WebTransport probe port\n";
    return 2;
  }

  trevrpc_wt_config config{};
  config.host = argv[1];
  config.port = static_cast<std::uint16_t>(port);
  config.path = "/trevrpc";
  config.origin = argv[3];
  config.skip_certificate_validation = 1;
  config.max_streams_per_session = 1;

  trevrpc_wt_session* session = nullptr;
  const int result = trevrpc_wt_dial(&config, &session);
  if (result != 0) {
    std::cerr << "WebTransport handshake failed: " << trevrpc_wt_error(result) << " (" << result
              << ")\n";
    return 1;
  }
  trevrpc_wt_session_shutdown(session);
  trevrpc_wt_session_close(session);
  return 0;
}
