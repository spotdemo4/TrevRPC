{
  lib,
  mkGoModule,
}:
mkGoModule (final: {
  pname = "trevrpc-go-otel";
  version = "1.5.0";

  # The module itself plus the TrevRPC module its go.mod replaces with ../.
  src = lib.fileset.toSource {
    root = ../../.;
    fileset = lib.fileset.difference ../. (
      lib.fileset.unions [
        ../default.nix
        ../gomod2nix.toml
        ./default.nix
        ./gomod2nix.toml
      ]
    );
  };
  sourceRoot = "${final.src.name}/trevrpc-go/oteltrevrpc";
  modules = ./gomod2nix.toml;

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    export HOME=$(mktemp -d)
    go test ./...
    # int is 32 bits wide on 386 and arm. A cgo build would need a 32-bit C
    # toolchain, and PIE needs cgo there. GOOS is pinned because the host's
    # may have no 32-bit port (darwin/386 does not exist).
    CGO_ENABLED=0 GOOS=linux GOARCH=386 go vet -buildmode=default ./...
    runHook postCheck
  '';

  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck
    test "$(go list -m)" = 'trev.zip/llc/trevrpc/trevrpc-go/oteltrevrpc'
    runHook postInstallCheck
  '';

  meta = {
    description = "OpenTelemetry metrics and tracing for TrevRPC Go servers and clients";
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
    homepage = "https://trev.zip/llc/TrevRPC";
    sourceProvenance = [ lib.sourceTypes.fromSource ];
    maintainers = [ lib.maintainers.spotdemo4 ];
  };
})
