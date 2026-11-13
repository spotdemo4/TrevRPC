{
  lib,
  mkGoModule,
  protobuf,
  pebble,
  wireGolden,
  qmuxGolden,
  zstdGolden,
  trevrpcProto,
}:
mkGoModule (final: {
  pname = "trevrpc-go";
  version = "1.11.1";

  # The module itself plus the shared files its tests read: the wire, QMux and
  # compression golden vectors, and the TrevRPC option protos the generated
  # bindings must match.
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      wireGolden
      qmuxGolden
      zstdGolden
      trevrpcProto
      (lib.fileset.difference ./. (
        lib.fileset.unions [
          ./default.nix
          ./gomod2nix.toml
          # separate modules, with their own packages
          ./oteltrevrpc
          ./protovalidatetrevrpc
          ./cmd/trevrpc-interop
        ]
      ))
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-go";
  modules = ./gomod2nix.toml;

  subPackages = [ "cmd/protoc-gen-trevrpc-go" ];

  doCheck = true;
  # The bindings tests regenerate code from the option protos, and the ACME
  # test gets a certificate from Pebble.
  nativeCheckInputs = [
    protobuf
    pebble
  ];
  checkPhase = ''
    runHook preCheck
    export HOME=$(mktemp -d)
    # The default build compresses with libzstd through cgo; trevrpc_purego
    # tests the pure-Go codec that builds without cgo use.
    test "$(go env CGO_ENABLED)" = 1
    go test ./...
    # The tag only changes internal/zstd, so only the packages whose tests
    # reach it run again; the RPC tests that compress are in the main package.
    zstd=$(go list ./internal/zstd)
    purego=$(go list -test -f '{{.Dir}} {{join .Deps " "}} ' ./... \
      | grep -F " $zstd " | cut -d' ' -f1 | sort -u)
    grep -qx "$PWD" <<< "$purego"
    go test -tags trevrpc_purego $purego
    # int is 32 bits wide on 386 and arm. A cgo build would need a 32-bit C
    # toolchain, and PIE needs cgo there. GOOS is pinned because the host's
    # may have no 32-bit port (darwin/386 does not exist).
    CGO_ENABLED=0 GOOS=linux GOARCH=386 go vet -buildmode=default ./...
    # One iteration each, so the benchmarks keep compiling and running.
    go test -run '^$' -bench . -benchtime 1x ./...
    runHook postCheck
  '';

  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck
    test "$(go list -m)" = 'trev.zip/llc/trevrpc/trevrpc-go'
    test -x "$out/bin/protoc-gen-trevrpc-go"
    runHook postInstallCheck
  '';

  meta = {
    mainProgram = "protoc-gen-trevrpc-go";
    description = "Go runtime and code generator for TrevRPC";
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
    homepage = "https://trev.zip/llc/TrevRPC";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases";
    changelog = "https://trev.zip/llc/TrevRPC/releases/tag/trevrpc-go/v${final.version}";
    sourceProvenance = [ lib.sourceTypes.fromSource ];
    maintainers = [ lib.maintainers.spotdemo4 ];
  };
})
