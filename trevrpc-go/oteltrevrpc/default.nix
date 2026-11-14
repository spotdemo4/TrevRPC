{
  callPackage,
}:
callPackage ../module.nix { } {
  pname = "trevrpc-go-otel";
  version = "1.11.1";

  directory = ./.;
  # The module itself plus the TrevRPC module its go.mod replaces with ../.
  fileset = ../.;

  meta.description = "OpenTelemetry metrics and tracing for TrevRPC Go servers and clients";
}
