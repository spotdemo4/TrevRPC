{
  lib,
  rustPlatform,
  rustfmt,
  clippy,
  gnugrep,
  repoRoot,
  benchPeer ? false,
  conformancePeer ? false,
}:
rustPlatform.buildRustPackage (final: {
  pname =
    if benchPeer then
      "trevrpc-rust-bench-peer"
    else if conformancePeer then
      "trevrpc-rust-conformance-peer"
    else
      "trevrpc-rust";
  version = "0.1.0";

  src = lib.fileset.toSource {
    root = repoRoot;
    fileset = lib.fileset.unions [
      (repoRoot + "/bench/proto")
      (repoRoot + "/testdata/wire-golden-vectors.txt")
      ./.
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-rust";
  cargoLock.lockFile = ./Cargo.lock;
  cargoBuildFlags = [
    "--package"
    (
      if benchPeer then
        "trevrpc-bench-peer"
      else if conformancePeer then
        "trevrpc-conformance-rust"
      else
        "protoc-gen-trevrpc-rust"
    )
  ];

  doCheck = true;
  nativeCheckInputs = [
    rustfmt
    clippy
  ];
  checkPhase =
    if benchPeer then
      ''
        runHook preCheck
        cargo test --package trevrpc-bench-peer --offline
        runHook postCheck
      ''
    else if conformancePeer then
      ''
        runHook preCheck
        cargo test --package trevrpc-conformance-rust --offline
        runHook postCheck
      ''
    else
      ''
        cargo fmt --check
        cargo test --workspace --offline
        cargo clippy --workspace --all-targets --offline -- -D warnings
      '';

  installPhase =
    if benchPeer then
      ''
        runHook preInstall
        peer=$(find target -path '*/release/trevrpc-bench-peer-rust' -type f -perm -0100 | head -n1)
        install -Dm755 "$peer" $out/bin/trevrpc-bench-peer-rust
        runHook postInstall
      ''
    else if conformancePeer then
      ''
        runHook preInstall
        peer=$(find target -path '*/release/trevrpc-conformance-rust' -type f -perm -0100 | head -n1)
        install -Dm755 "$peer" $out/bin/trevrpc-conformance-rust
        runHook postInstall
      ''
    else
      ''
        runHook preInstall
        generator=$(find target -path '*/release/protoc-gen-trevrpc-rust' -type f -perm -0100 | head -n1)
        install -Dm755 "$generator" $out/bin/protoc-gen-trevrpc-rust
        runHook postInstall
      '';

  doInstallCheck = !benchPeer && !conformancePeer;
  nativeInstallCheckInputs = [ gnugrep ];
  installCheckPhase = ''
    runHook preInstallCheck
    ! grep -Eq '^tonic(-prost)?[[:space:]]*=' Cargo.toml
    test ! -e "$out/bin/trevrpc-bench-peer-rust"
    test ! -e "$out/bin/trevrpc-conformance-rust"
    runHook postInstallCheck
  '';

  meta =
    if benchPeer then
      {
        mainProgram = "trevrpc-bench-peer-rust";
        description = "Rust TrevRPC and gRPC benchmark peer";
        license = lib.licenses.mit;
        platforms = lib.platforms.linux;
      }
    else if conformancePeer then
      {
        mainProgram = "trevrpc-conformance-rust";
        description = "Rust TrevRPC process-protocol conformance peer";
        license = lib.licenses.mit;
        platforms = lib.platforms.linux;
      }
    else
      {
        mainProgram = "protoc-gen-trevrpc-rust";
        description = "Protobuf over QUIC, HTTP/3 & WebTransport";
        license = lib.licenses.mit;
        platforms = lib.platforms.all;
        homepage = "https://trev.zip/llc/TrevRPC";
        changelog = "https://trev.zip/llc/TrevRPC/releases";
        downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/v${final.version}";
      };
})
