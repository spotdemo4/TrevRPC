{
  stdenv,
  lib,
  buildGoModule,
  go-tools,
  gotools,
  gnugrep,
  benchProto,
  wireGolden,
}:
let
  goSource = lib.fileset.difference ./. (
    lib.fileset.unions [
      ./default.nix
      ./go.work
      ./go.work.sum
      ./quic-go
    ]
  );
  neutralModuleSource = lib.fileset.difference ../trevrpc-c (
    lib.fileset.unions [
      ../trevrpc-c/default.nix
      ../trevrpc-c/provider/msquic
      ../trevrpc-c/tests
    ]
  );
  providerBaseSource = lib.fileset.difference ../trevrpc-c/provider/msquic ../trevrpc-c/provider/msquic/lib;
  providerArchive =
    if stdenv.hostPlatform.system == "x86_64-linux" then
      ../trevrpc-c/provider/msquic/lib/linux_amd64/libmsquic.a
    else if stdenv.hostPlatform.system == "aarch64-linux" then
      ../trevrpc-c/provider/msquic/lib/linux_arm64/libmsquic.a
    else
      null;
  providerModuleSource = lib.fileset.unions (
    [ providerBaseSource ] ++ lib.optional (providerArchive != null) providerArchive
  );
  canExecute = stdenv.buildPlatform.canExecute stdenv.hostPlatform;
in
buildGoModule (final: {
  pname = "trevrpc-go";
  version = "0.2.2";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      benchProto
      wireGolden
      goSource
      neutralModuleSource
      providerModuleSource
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-go";
  vendorHash = "sha256-qCcEkaeYDS9jsGnL2fyIvFmd5OpWetEZOr8ofMqSaLI=";
  proxyVendor = true;

  postPatch = ''
    go mod edit \
      -replace=trev.zip/llc/trevrpc/trevrpc-c@v0.3.0=../trevrpc-c
    go mod edit \
      -replace=trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2@v2.6.0-trevrpc.1=../trevrpc-c/provider/msquic
  '';

  subPackages = [
    "cmd/protoc-gen-trevrpc-go"
    "cmd/trevrpc-bench-peer"
    "cmd/trevrpc-conformance-go"
  ];

  postInstall = ''
    mv "$out/bin/trevrpc-bench-peer" "$out/bin/trevrpc-bench-peer-go"
  '';

  doCheck = canExecute;
  nativeCheckInputs = [
    go-tools
    gotools
  ];
  checkPhase = ''
    runHook preCheck
    export HOME=$(mktemp -d)
    export GOWORK=off
    go test ./...
    go vet ./...
    staticcheck ./...
    modernize ./...
    runHook postCheck
  '';

  doInstallCheck = canExecute;
  nativeInstallCheckInputs = [ gnugrep ];
  installCheckPhase = ''
    runHook preInstallCheck
    export GOWORK=off
    test "$(go list -m)" = 'trev.zip/llc/trevrpc/trevrpc-go'
    go list -m \
      trev.zip/llc/trevrpc/trevrpc-c \
      trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2 > modules.out
    grep -Eq '^trev\.zip/llc/trevrpc/trevrpc-c v0\.3\.0 => \.\./trevrpc-c$' modules.out
    grep -Eq '^trev\.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2 v2\.6\.0-trevrpc\.1 => \.\./trevrpc-c/provider/msquic$' modules.out
    go list -deps ./... > packages.out
    ! grep -Eq '^github.com/(quic-go|spotdemo4)/' packages.out
    ! grep -Eq 'github.com/(quic-go/(quic-go|webtransport-go|qpack)|spotdemo4/webtransport-go)|trev\.zip/llc/trevrpc/trevrpc-go/quic-go' go.mod go.sum
    ! grep -q 'google.golang.org/grpc' go.mod
    test -x "$out/bin/trevrpc-bench-peer-go"
    test -x "$out/bin/trevrpc-conformance-go"
    printf 'STOP\n' | "$out/bin/trevrpc-conformance-go" --protocol 1 > peer.out
    grep -q '"event":"ready"' peer.out
    grep -q '"peer":"go"' peer.out
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
