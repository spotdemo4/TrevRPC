{
  stdenv,
  lib,
  buildGoModule,
  go-tools,
  gotools,
}:
let
  quicGoSource = lib.fileset.difference ./. ./default.nix;
  baseGoSource = lib.fileset.difference ../. (
    lib.fileset.unions [
      ../default.nix
      ../go.work
      ../go.work.sum
      ./.
    ]
  );
  neutralModuleSource = lib.fileset.difference ../../trevrpc-c (
    lib.fileset.unions [
      ../../trevrpc-c/default.nix
      ../../trevrpc-c/provider/msquic
      ../../trevrpc-c/tests
    ]
  );
  providerBaseSource = lib.fileset.difference ../../trevrpc-c/provider/msquic ../../trevrpc-c/provider/msquic/lib;
  providerArchive =
    if stdenv.hostPlatform.system == "x86_64-linux" then
      ../../trevrpc-c/provider/msquic/lib/linux_amd64/libmsquic.a
    else if stdenv.hostPlatform.system == "aarch64-linux" then
      ../../trevrpc-c/provider/msquic/lib/linux_arm64/libmsquic.a
    else
      null;
  providerModuleSource = lib.fileset.unions (
    [ providerBaseSource ] ++ lib.optional (providerArchive != null) providerArchive
  );
  canExecute = stdenv.buildPlatform.canExecute stdenv.hostPlatform;
in
buildGoModule (final: {
  pname = "trevrpc-go-quic-go";
  version = "0.1.0";

  src = lib.fileset.toSource {
    root = ../../.;
    fileset = lib.fileset.unions [
      baseGoSource
      quicGoSource
      neutralModuleSource
      providerModuleSource
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-go/quic-go";
  vendorHash = "sha256-WtDgl+RLsXC0QAK4F5MKK59T7E4D8JOJDqU21dDPOIA=";
  proxyVendor = true;

  postPatch = ''
    go mod edit -replace=trev.zip/llc/trevrpc/trevrpc-go=..
    go mod edit -replace=trev.zip/llc/trevrpc/trevrpc-c@v0.3.0=../../trevrpc-c
    go mod edit \
      -replace=trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2@v2.6.0-trevrpc.1=../../trevrpc-c/provider/msquic
  '';
  modBuildPhase = ''
    runHook preBuild
    export GOWORK=off
    go mod tidy
    mkdir -p "$GOPATH/pkg/mod/cache/download"
    go mod download
    runHook postBuild
  '';

  buildPhase = ''
    runHook preBuild
    export GOWORK=off
    go mod tidy
    go test -run '^$' ./...
    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall
    mkdir -p "$out"
    runHook postInstall
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
    CGO_ENABLED=0 go test ./...
    go vet ./...
    staticcheck ./...
    modernize ./...
    runHook postCheck
  '';

  doInstallCheck = canExecute;
  installCheckPhase = ''
    runHook preInstallCheck
    export GOWORK=off
    test "$(go list -m)" = 'trev.zip/llc/trevrpc/trevrpc-go/quic-go'
    go list -m \
      github.com/quic-go/quic-go \
      github.com/quic-go/webtransport-go \
      trev.zip/llc/trevrpc/trevrpc-go > modules.out
    grep -q '^github.com/quic-go/quic-go ' modules.out
    grep -q '^github.com/quic-go/webtransport-go ' modules.out
    ! grep -q 'github.com/spotdemo4/webtransport-go' modules.out
    grep -q '^trev.zip/llc/trevrpc/trevrpc-go .* => \.\.$' modules.out
    go list -deps ./... > packages.out
    grep -Eq '^github.com/quic-go/quic-go($|/)' packages.out
    grep -Eq '^github.com/quic-go/webtransport-go($|/)' packages.out
    runHook postInstallCheck
  '';

  meta = {
    description = "Explicit QUIC-go and WebTransport backend for TrevRPC Go";
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
    homepage = "https://trev.zip/llc/TrevRPC";
    changelog = "https://trev.zip/llc/TrevRPC/releases";
    # This derivation's version is a package snapshot, not the independently
    # released nested module's semantic version.
    downloadPage = "https://trev.zip/llc/TrevRPC/releases";
  };
})
