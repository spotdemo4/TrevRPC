{
  lib,
  buildGoModule,
  go-tools,
  gotools,
  gnugrep,
  benchProto,
  wireGolden,
}:
let
  goSource = lib.fileset.difference ./. ./default.nix;
in
buildGoModule (final: {
  pname = "trevrpc-go";
  version = "0.2.0";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      benchProto
      wireGolden
      goSource
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-go";
  vendorHash = "sha256-QpalUqPLCVi8BEOpyL4PIZ+kVjGoGeYQDogYFQCQZjM=";
  subPackages = [
    "cmd/protoc-gen-trevrpc-go"
    "cmd/trevrpc-bench-peer"
    "cmd/trevrpc-conformance-go"
  ];

  postInstall = ''
    mv "$out/bin/trevrpc-bench-peer" "$out/bin/trevrpc-bench-peer-go"
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
