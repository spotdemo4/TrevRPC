{
  lib,
  buildGoModule,
  go-tools,
  gotools,
  gnugrep,
  repoRoot,
}:
let
  goSource = lib.fileset.difference ./. ./default.nix;
in
buildGoModule (final: {
  pname = "trevrpc-go";
  version = "0.1.0";

  src = lib.fileset.toSource {
    root = repoRoot;
    fileset = lib.fileset.unions [
      (repoRoot + "/bench/proto")
      (repoRoot + "/testdata/wire-golden-vectors.txt")
      goSource
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-go";
  env.GOWORK = "off";
  vendorHash = "sha256-oScgto4J7jT17Wq3tTrAOSW8hw8C9WGIMEMgTjHzTr0=";
  subPackages = [
    "cmd/protoc-gen-trevrpc-go"
    "cmd/trevrpc-conformance-go"
  ];

  postInstall = ''
    mkdir -p "$out/bin"
    mv cmd/trevrpc-bench-peer/go.mod cmd/trevrpc-bench-peer/go.mod.peer
    mv cmd/trevrpc-bench-peer/go.sum cmd/trevrpc-bench-peer/go.sum.peer
    (
      GOWORK=off go build -mod=vendor -o "$out/bin/trevrpc-bench-peer-go" \
        ./cmd/trevrpc-bench-peer
    )
    mv cmd/trevrpc-bench-peer/go.mod.peer cmd/trevrpc-bench-peer/go.mod
    mv cmd/trevrpc-bench-peer/go.sum.peer cmd/trevrpc-bench-peer/go.sum
  '';

  doCheck = true;
  nativeCheckInputs = [
    go-tools
    gotools
  ];
  checkPhase = ''
    runHook preCheck
    export HOME=$(mktemp -d)
    go test ./...
    go vet ./...
    staticcheck ./...
    modernize ./...
    mv cmd/trevrpc-bench-peer/go.mod cmd/trevrpc-bench-peer/go.mod.peer
    mv cmd/trevrpc-bench-peer/go.sum cmd/trevrpc-bench-peer/go.sum.peer
    (
      GOWORK=off go test -mod=vendor ./cmd/trevrpc-bench-peer/...
      GOWORK=off go vet -mod=vendor ./cmd/trevrpc-bench-peer/...
      GOWORK=off GOFLAGS=-mod=vendor staticcheck ./cmd/trevrpc-bench-peer/...
      GOWORK=off GOFLAGS=-mod=vendor modernize ./cmd/trevrpc-bench-peer/...
    )
    mv cmd/trevrpc-bench-peer/go.mod.peer cmd/trevrpc-bench-peer/go.mod
    mv cmd/trevrpc-bench-peer/go.sum.peer cmd/trevrpc-bench-peer/go.sum
    runHook postCheck
  '';

  doInstallCheck = true;
  nativeInstallCheckInputs = [ gnugrep ];
  installCheckPhase = ''
    runHook preInstallCheck
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
    downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/v${final.version}";
  };
})
