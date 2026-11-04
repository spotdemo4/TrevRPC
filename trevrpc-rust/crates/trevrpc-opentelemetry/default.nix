{
  lib,
  stdenvNoCC,
  licenseFile,
}:
# The crate's source, which the rust release job publishes to crates.io after trevrpc. The
# trevrpc-rust package builds, tests and lints the crate with the rest of the workspace.
stdenvNoCC.mkDerivation (final: {
  pname = "trevrpc-rust-otel";
  version = "1.5.0";

  # The workspace, whose manifest and lockfile the crate builds with, and the license it packages.
  src = lib.fileset.toSource {
    root = ../../..;
    fileset = lib.fileset.unions [
      licenseFile
      ../..
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-rust/crates/trevrpc-opentelemetry";

  dontConfigure = true;
  dontBuild = true;
  installPhase = ''
    runHook preInstall
    mkdir -p "$out"
    runHook postInstall
  '';

  meta = {
    description = "OpenTelemetry metrics and tracing for TrevRPC servers and clients";
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
    homepage = "https://trev.zip/llc/TrevRPC";
    sourceProvenance = [ lib.sourceTypes.fromSource ];
    maintainers = [ lib.maintainers.spotdemo4 ];
  };
})
