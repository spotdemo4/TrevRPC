{
  stdenv,
  lib,
  buildGoModule,
  go-tools,
  gotools,
  gnugrep,
  pkg-config,
  trevrpcC,
  benchProto,
  wireGolden,
}:
let
  goSource = lib.fileset.difference ./. ./default.nix;
  nativeSupported =
    (stdenv.hostPlatform.isLinux || stdenv.hostPlatform.isDarwin)
    && (stdenv.hostPlatform.isx86_64 || stdenv.hostPlatform.isAarch64);
  canExecute = stdenv.buildPlatform.canExecute stdenv.hostPlatform;
  nativeTagArgs = lib.optionalString nativeSupported "-tags=trevrpc_native";
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
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-go";
  vendorHash = "sha256-yOGuL+KfNDMO/gkzVQUa0MgaNh88Taw9hLhHtZnjazo=";
  tags = lib.optionals nativeSupported [ "trevrpc_native" ];
  nativeBuildInputs = lib.optionals nativeSupported [ pkg-config ];
  buildInputs = lib.optionals nativeSupported [ trevrpcC ];
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
    go test ${nativeTagArgs} ./...
    go vet ${nativeTagArgs} ./...
    staticcheck ${nativeTagArgs} ./...
    modernize ${nativeTagArgs} ./...
    runHook postCheck
  '';

  doInstallCheck = canExecute;
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
    downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/trevrpc-go/v${final.version}";
  };
})
