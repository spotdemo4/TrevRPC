{
  lib,
  mkGoModule,
  protobuf,
  zstdGolden,
  interopProto,
  trevrpcProto,
}:
mkGoModule (final: {
  pname = "trevrpc-go-interop";
  version = "1.11.1";

  # The module plus the TrevRPC and protovalidate modules its go.mod replaces
  # with ../../ and ../../protovalidatetrevrpc, and the shared files its tests
  # read: the compression golden vectors, and the interop and TrevRPC option
  # protos the generated bindings must match.
  src = lib.fileset.toSource {
    root = ../../../.;
    fileset = lib.fileset.unions [
      zstdGolden
      interopProto
      trevrpcProto
      (lib.fileset.difference ../../. (
        lib.fileset.unions [
          ../../default.nix
          ../../gomod2nix.toml
          ../../oteltrevrpc
          ../../protovalidatetrevrpc/default.nix
          ../../protovalidatetrevrpc/gomod2nix.toml
          ./default.nix
          ./gomod2nix.toml
        ]
      ))
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-go/cmd/trevrpc-interop";
  modules = ./gomod2nix.toml;
  # The one binary is named after meta.mainProgram, trevrpc-interop-go.

  doCheck = true;
  # The bindings test regenerates code from the interop and option protos.
  nativeCheckInputs = [ protobuf ];
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
    test "$(go list -m)" = 'trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-interop'
    test -x "$out/bin/trevrpc-interop-go"
    if "$out/bin/trevrpc-interop-go" 2> usage.out; then
      echo "trevrpc-interop-go without a subcommand must fail" >&2
      exit 1
    fi
    grep -q 'server|client' usage.out
    runHook postInstallCheck
  '';

  meta = {
    mainProgram = "trevrpc-interop-go";
    description = "Go interop and load benchmark executable for TrevRPC";
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
    homepage = "https://trev.zip/llc/TrevRPC";
    sourceProvenance = [ lib.sourceTypes.fromSource ];
    maintainers = [ lib.maintainers.spotdemo4 ];
  };
})
