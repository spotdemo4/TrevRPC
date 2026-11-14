{
  lib,
  stdenv,
  callPackage,
  trevrpc-erlang-interop,
  trevrpc-go-interop,
  trevrpc-js,
  trevrpc-rust,
}:
callPackage ./runner.nix {
  name = "trevrpc-interop";
  script = "interop/run.py";
  # trevrpc-interop-browser drives the browsers.
  runtimeInputs = [ trevrpc-js ];
  # Every server and native client, and the browsers playwright-driver ships, which is only on
  # Linux. Safari needs its certificate trusted through sudo, so it runs only when asked for.
  defaults = {
    server = [
      "go=${lib.getExe' trevrpc-go-interop "trevrpc-interop-go"}"
      "rust=${lib.getExe' trevrpc-rust "trevrpc-interop-rust"}"
      "erlang=${lib.getExe trevrpc-erlang-interop}"
    ];
    client = [
      "go=${lib.getExe' trevrpc-go-interop "trevrpc-interop-go"}"
      "rust=${lib.getExe' trevrpc-rust "trevrpc-interop-rust"}"
      "erlang=${lib.getExe trevrpc-erlang-interop}"
    ];
    # The Erlang runtime does not implement every transport and scenario yet; its cells skip the rest.
    may-skip = [ "erlang" ];
    browser = lib.optionals stdenv.hostPlatform.isLinux [
      "chromium"
      "firefox"
    ];
  };
}
