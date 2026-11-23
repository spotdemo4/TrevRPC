{
  callPackage,
  licenseFile,
}:
callPackage ../crate-source.nix { inherit licenseFile; } {
  pname = "trevrpc-rust-otel";
  version = "1.13.0";
  directory = ./.;
  description = "OpenTelemetry metrics and tracing for TrevRPC servers and clients";
}
