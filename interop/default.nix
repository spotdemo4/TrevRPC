{
  lib,
  stdenv,
  callPackage,
  trevrpc-go,
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
      "go=${lib.getExe' trevrpc-go "trevrpc-interop-go"}"
      "rust=${lib.getExe' trevrpc-rust "trevrpc-interop-rust"}"
    ];
    client = [
      "go=${lib.getExe' trevrpc-go "trevrpc-interop-go"}"
      "rust=${lib.getExe' trevrpc-rust "trevrpc-interop-rust"}"
    ];
    browser = lib.optionals stdenv.hostPlatform.isLinux [
      "chromium"
      "firefox"
    ];
  };
}
