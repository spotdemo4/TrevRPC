{
  lib,
  stdenv,
  callPackage,
  iproute2,
  util-linux,
  zstd,
  trevrpc-bench-baseline,
  trevrpc-go,
  trevrpc-js,
  trevrpc-rust,
}:
callPackage ../interop/runner.nix {
  name = "trevrpc-bench";
  script = "bench/run.py";
  # zstd trains the dictionary of the dictionary compression modes. --netem runs the benchmark
  # in network namespaces, which only Linux has: unshare creates them and ip and tc shape their
  # loopback link.
  # trevrpc-interop-browser drives the browsers' lanes.
  runtimeInputs = [
    trevrpc-js
    zstd
  ]
  ++ lib.optionals stdenv.hostPlatform.isLinux [
    iproute2
    util-linux
  ];
  # Every server and client, plus the grpc-go and connect-go baselines, and on Linux Chromium,
  # from the browsers playwright-driver ships.
  defaults = {
    server = [
      "go=${lib.getExe' trevrpc-go "trevrpc-interop-go"}"
      "rust=${lib.getExe' trevrpc-rust "trevrpc-interop-rust"}"
    ];
    client = [
      "go=${lib.getExe' trevrpc-go "trevrpc-interop-go"}"
      "rust=${lib.getExe' trevrpc-rust "trevrpc-interop-rust"}"
    ];
    baseline = lib.getExe trevrpc-bench-baseline;
    browser = lib.optionals stdenv.hostPlatform.isLinux [ "chromium" ];
  };
}
