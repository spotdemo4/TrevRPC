{
  lib,
  callPackage,
  protobuf,
  interopProto,
}:
callPackage ../../trevrpc-go/module.nix { } {
  pname = "trevrpc-bench-baseline";
  version = "0.0.0";

  directory = ./.;
  # The module itself plus the shared interop proto its drift test regenerates
  # the Echo bindings from.
  fileset = lib.fileset.unions [
    interopProto
    ./.
  ];

  subPackages = [ "." ];

  # The echopb drift test regenerates code from interop.proto.
  nativeCheckInputs = [ protobuf ];
  vet32Bit = false;
  usage = "server|bench";

  meta = {
    mainProgram = "trevrpc-bench-baseline";
    description = "grpc-go and connect-go baselines for TrevRPC's load benchmarks";
  };
}
