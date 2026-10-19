{
  lib,
  stdenv,
  callPackage,
  iproute2,
  util-linux,
  trevrpc-bench-baseline,
  trevrpc-go,
  trevrpc-rust,
}:
callPackage ../interop/runner.nix {
  name = "trevrpc-bench";
  script = "bench/run.py";
  # --netem runs the benchmark in network namespaces, which only Linux has: unshare creates them
  # and ip and tc shape their loopback link.
  runtimeInputs = lib.optionals stdenv.hostPlatform.isLinux [
    iproute2
    util-linux
  ];
  # Every server and client, plus the grpc-go and connect-go baselines.
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
  };
}
