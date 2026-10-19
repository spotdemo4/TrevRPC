{
  benchstat,
  callPackage,
  cargo,
  go,
  rustc,
}:
callPackage ../interop/runner.nix {
  name = "trevrpc-bench-micro";
  script = "bench/micro.py";
  runtimeInputs = [
    go
    cargo
    rustc
    benchstat
  ];
}
