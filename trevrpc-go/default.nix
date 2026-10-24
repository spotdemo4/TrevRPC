{
  lib,
  stdenv,
  mkGoModule,
  protobuf,
  wireGolden,
  qmuxGolden,
  zstdGolden,
  interopProto,
  trevrpcProto,
}:
let
  canExecute = stdenv.buildPlatform.canExecute stdenv.hostPlatform;
in
mkGoModule (final: {
  pname = "trevrpc-go";
  version = "1.1.0";

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
      (lib.fileset.difference ./. ./default.nix)
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-go";
  vendorHash = "sha256-6M0gbE9HsHcXBx75SisSoFM9P3mlU62VFJS7i/nn3Hs=";

  subPackages = [
    "cmd/protoc-gen-trevrpc-go"
    "cmd/trevrpc-interop"
  ];

  postInstall = ''
    mv "$out/bin/trevrpc-interop" "$out/bin/trevrpc-interop-go"
  '';

  doCheck = canExecute;
  # The bindings tests regenerate code from the interop and option protos.
  nativeCheckInputs = [ protobuf ];
  checkPhase = ''
    runHook preCheck
    export HOME=$(mktemp -d)
    # Compression calls libzstd through cgo; without it its tests would skip.
    test "$(go env CGO_ENABLED)" = 1
    go test ./...
    # One iteration each, so the benchmarks keep compiling and running.
    go test -run '^$' -bench . -benchtime 1x ./...
    runHook postCheck
  '';

  doInstallCheck = canExecute;
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
    changelog = "https://trev.zip/llc/TrevRPC/releases";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/trevrpc-go/v${final.version}";
  };
})
