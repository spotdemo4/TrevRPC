{
  lib,
  stdenv,
  buildGoModule,
  go-tools,
  gotools,
  protobuf,
  wireGolden,
  qmuxGolden,
  interopProto,
}:
let
  canExecute = stdenv.buildPlatform.canExecute stdenv.hostPlatform;
in
buildGoModule (final: {
  pname = "trevrpc-go";
  version = "1.0.0";

  # The module itself plus the shared files its tests read: the wire and QMux
  # golden vectors and the interop proto the generated interop bindings must match.
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      wireGolden
      qmuxGolden
      interopProto
      (lib.fileset.difference ./. ./default.nix)
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-go";
  vendorHash = "sha256-w2GucM+UjhnMnOA1iDUOyfPBKbK4b/W8D/BlfamNro4=";
  proxyVendor = true;

  subPackages = [
    "cmd/protoc-gen-trevrpc-go"
    "cmd/trevrpc-interop"
  ];

  postInstall = ''
    mv "$out/bin/trevrpc-interop" "$out/bin/trevrpc-interop-go"
  '';

  doCheck = canExecute;
  nativeCheckInputs = [
    go-tools
    gotools
    # The interop bindings test regenerates code from interop.proto.
    protobuf
  ];
  checkPhase = ''
    runHook preCheck
    export HOME=$(mktemp -d)
    go test ./...
    go vet ./...
    staticcheck ./...
    modernize ./...
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
