{
  callPackage,
  licenseFile,
}:
callPackage ../crate-source.nix { inherit licenseFile; } {
  pname = "trevrpc-rust-otel";
  version = "1.11.1";
  directory = ./.;
  description = "OpenTelemetry metrics and tracing for TrevRPC servers and clients";
}
