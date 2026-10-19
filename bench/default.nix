{
  lib,
  callPackage,
  trevrpc-bench-grpc,
  trevrpc-go,
  trevrpc-rust,
}:
callPackage ../interop/runner.nix {
  name = "trevrpc-bench";
  script = "bench/run.py";
  # Every server and client, plus the grpc-go baseline.
  defaults = {
    server = [
      "go=${lib.getExe' trevrpc-go "trevrpc-interop-go"}"
      "rust=${lib.getExe' trevrpc-rust "trevrpc-interop-rust"}"
    ];
    client = [
      "go=${lib.getExe' trevrpc-go "trevrpc-interop-go"}"
      "rust=${lib.getExe' trevrpc-rust "trevrpc-interop-rust"}"
    ];
    grpc = lib.getExe trevrpc-bench-grpc;
  };
}
