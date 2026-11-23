{
  benchstat,
  callPackage,
  cargo,
  go-latest,
  protobuf,
  rustc,
  stdenv,
}:
callPackage ../interop/runner.nix {
  name = "trevrpc-bench-micro";
  script = "bench/micro.py";
  runtimeInputs = [
    go-latest
    cargo
    rustc
    benchstat
    # The build script of prost-protovalidate-types, which the interop executable needs, runs
    # protoc.
    protobuf
    # cgo, for Go's zstd binding, and the C code that zstd-safe and ring build.
    stdenv.cc
  ];
}
