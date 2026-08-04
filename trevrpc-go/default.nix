{
  lib,
  buildGoModule,
  go-tools,
  gotools,
  gnugrep,
  repoRoot,
  benchPeer ? false,
  conformancePeer ? false,
}:
assert lib.assertMsg (
  !(benchPeer && conformancePeer)
) "trevrpc-go benchPeer and conformancePeer are mutually exclusive";
let
  goSource = lib.fileset.difference ./. ./default.nix;

  normal = buildGoModule (final: {
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
    subPackages = [ "cmd/protoc-gen-trevrpc-go" ];

    doCheck = true;
    nativeCheckInputs = [
      go-tools
      gotools
    ];
    checkPhase = ''
      export HOME=$(mktemp -d)
      go test ./...
      go vet ./...
      staticcheck ./...
      modernize ./...
    '';

    doInstallCheck = true;
    nativeInstallCheckInputs = [ gnugrep ];
    installCheckPhase = ''
      runHook preInstallCheck
      ! grep -q 'google.golang.org/grpc' go.mod
      test ! -e "$out/bin/trevrpc-bench-peer-go"
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
  });

  peer = buildGoModule (final: {
    pname = "trevrpc-go-bench-peer";
    version = "0.1.0";

    src = lib.fileset.toSource {
      root = repoRoot;
      fileset = lib.fileset.unions [
        (repoRoot + "/bench/proto")
        goSource
      ];
    };
    sourceRoot = "${final.src.name}/trevrpc-go/cmd/trevrpc-bench-peer";
    env.GOWORK = "off";
    vendorHash = "sha256-l3f5gyohzNr8bqYMBw7h0TJK0p7ae+QCw4OK6j43bn0=";
    subPackages = [ "." ];
    postInstall = ''
      mv $out/bin/trevrpc-bench-peer $out/bin/trevrpc-bench-peer-go
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

    meta = {
      mainProgram = "trevrpc-bench-peer-go";
      description = "Go TrevRPC and gRPC benchmark peer";
      license = lib.licenses.mit;
      platforms = lib.platforms.linux;
    };
  });

  conformance = buildGoModule (final: {
    pname = "trevrpc-go-conformance-peer";
    version = "0.1.0";

    src = lib.fileset.toSource {
      root = repoRoot;
      fileset = lib.fileset.unions [
        (repoRoot + "/testdata/wire-golden-vectors.txt")
        goSource
      ];
    };
    sourceRoot = "${final.src.name}/trevrpc-go";
    env.GOWORK = "off";
    vendorHash = "sha256-oScgto4J7jT17Wq3tTrAOSW8hw8C9WGIMEMgTjHzTr0=";
    subPackages = [ "cmd/trevrpc-conformance-go" ];

    doCheck = true;
    checkPhase = ''
      runHook preCheck
      go test ./cmd/trevrpc-conformance-go
      runHook postCheck
    '';

    doInstallCheck = true;
    nativeInstallCheckInputs = [ gnugrep ];
    installCheckPhase = ''
      runHook preInstallCheck
      test "$(find $out/bin -maxdepth 1 -type f | wc -l)" -eq 1
      printf 'STOP\n' | $out/bin/trevrpc-conformance-go --protocol 1 > peer.out
      grep -q '"event":"ready"' peer.out
      grep -q '"peer":"go"' peer.out
      runHook postInstallCheck
    '';

    meta = {
      mainProgram = "trevrpc-conformance-go";
      description = "Go TrevRPC conformance process peer";
      license = lib.licenses.mit;
      platforms = lib.platforms.linux;
    };
  });
in
if conformancePeer then
  conformance
else if benchPeer then
  peer
else
  normal
