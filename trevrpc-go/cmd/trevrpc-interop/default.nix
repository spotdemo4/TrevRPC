{
  lib,
  callPackage,
  protobuf,
  zstdGolden,
  interopProto,
  trevrpcProto,
}:
callPackage ../../module.nix { } {
  pname = "trevrpc-go-interop";
  version = "1.11.1";

  directory = ./.;
  # The module plus the TrevRPC and protovalidate modules its go.mod replaces
  # with ../../ and ../../protovalidatetrevrpc, and the shared files its tests
  # read: the compression golden vectors, and the interop and TrevRPC option
  # protos the generated bindings must match.
  fileset = lib.fileset.unions [
    zstdGolden
    interopProto
    trevrpcProto
    (lib.fileset.difference ../../. ../../oteltrevrpc)
  ];

  # The bindings test regenerates code from the interop and option protos.
  nativeCheckInputs = [ protobuf ];
  usage = "server|client";

  meta = {
    # The name of the one binary.
    mainProgram = "trevrpc-interop-go";
    description = "Go interop and load benchmark executable for TrevRPC";
  };
}
