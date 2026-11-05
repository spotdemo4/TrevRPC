{
  lib,
  mkGoModule,
  protobuf,
  wireGolden,
  qmuxGolden,
  zstdGolden,
  interopProto,
  trevrpcProto,
}:
mkGoModule (final: {
  pname = "trevrpc-go";
  version = "1.5.0";

  # The module itself plus the shared files its tests read: the wire, QMux and
  # compression golden vectors, and the interop and TrevRPC option protos the
  # generated bindings must match.
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      wireGolden
      qmuxGolden
      zstdGolden
      interopProto
      trevrpcProto
      (lib.fileset.difference ./. (
        lib.fileset.unions [
          ./default.nix
          ./gomod2nix.toml
          # a separate module, with its own package
          ./oteltrevrpc
        ]
      ))
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-go";
  modules = ./gomod2nix.toml;

  subPackages = [
    "cmd/protoc-gen-trevrpc-go"
    "cmd/trevrpc-interop"
  ];

  postInstall = ''
    mv "$out/bin/trevrpc-interop" "$out/bin/trevrpc-interop-go"
  '';

  doCheck = true;
  # The bindings tests regenerate code from the interop and option protos.
  nativeCheckInputs = [ protobuf ];
  checkPhase = ''
    runHook preCheck
    export HOME=$(mktemp -d)
    # The default build compresses with libzstd through cgo; trevrpc_purego
    # tests the pure-Go codec that builds without cgo use.
    test "$(go env CGO_ENABLED)" = 1
    go test ./...
    go test -tags trevrpc_purego ./...
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
    test -x "$out/bin/trevrpc-interop-go"
    if "$out/bin/trevrpc-interop-go" 2> usage.out; then
      echo "trevrpc-interop-go without a subcommand must fail" >&2
      exit 1
    fi
    grep -q 'server|client' usage.out
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
