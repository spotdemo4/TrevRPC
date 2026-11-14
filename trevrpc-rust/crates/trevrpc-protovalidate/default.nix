{
  callPackage,
  licenseFile,
}:
callPackage ../crate-source.nix { inherit licenseFile; } {
  pname = "trevrpc-rust-protovalidate";
  version = "1.11.1";
  directory = ./.;
  description = "buf.validate request validation for TrevRPC servers";
}
