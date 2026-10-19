{
  lib,
  callPackage,
  trevrpc-bench-baseline,
  trevrpc-go,
  trevrpc-rust,
}:
callPackage ../interop/runner.nix {
  name = "trevrpc-bench";
  script = "bench/run.py";
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
