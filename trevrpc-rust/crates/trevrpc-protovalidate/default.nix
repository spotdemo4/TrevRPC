{
  callPackage,
  licenseFile,
}:
callPackage ../crate-source.nix { inherit licenseFile; } {
  pname = "trevrpc-rust-protovalidate";
  version = "1.12.0";
  directory = ./.;
  description = "buf.validate request validation for TrevRPC servers";
}
