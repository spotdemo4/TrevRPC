{
  callPackage,
}:
callPackage ../module.nix { } {
  pname = "trevrpc-go-protovalidate";
  version = "1.11.1";

  directory = ./.;
  # The module itself plus the TrevRPC module its go.mod replaces with ../.
  fileset = ../.;

  meta.description = "buf.validate request validation for TrevRPC Go servers";
}
