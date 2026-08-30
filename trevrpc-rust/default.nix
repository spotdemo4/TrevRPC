{
  lib,
  rustPlatform,
  rustfmt,
  clippy,
  gnugrep,
  benchProto,
  wireGolden,
}:
rustPlatform.buildRustPackage (final: {
  pname = "trevrpc-rust";
  version = "0.1.11";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      benchProto
      wireGolden
      ./.
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-rust";
  cargoLock.lockFile = ./Cargo.lock;
  cargoBuildFlags = [ "--workspace" ];

  doCheck = true;
  nativeCheckInputs = [
    rustfmt
    clippy
  ];
  checkPhase = ''
    cargo fmt --check
    cargo check --lib --no-default-features --features http3 --offline
    cargo check --lib --no-default-features --features webtransport-client --offline
    cargo check --lib --no-default-features --features webtransport-server --offline
    cargo check --lib --no-default-features --features webtransport --offline
    cargo test --workspace --offline
    cargo clippy --workspace --all-targets --offline -- -D warnings
  '';

  installPhase = ''
    runHook preInstall
    generator=$(find target -path '*/release/protoc-gen-trevrpc-rust' -type f -perm -0100 | head -n1)
    bench_peer=$(find target -path '*/release/trevrpc-bench-peer-rust' -type f -perm -0100 | head -n1)
    conformance_peer=$(find target -path '*/release/trevrpc-conformance-rust' -type f -perm -0100 | head -n1)
    install -Dm755 "$generator" $out/bin/protoc-gen-trevrpc-rust
    install -Dm755 "$bench_peer" $out/bin/trevrpc-bench-peer-rust
    install -Dm755 "$conformance_peer" $out/bin/trevrpc-conformance-rust
    runHook postInstall
  '';

  doInstallCheck = true;
  nativeInstallCheckInputs = [ gnugrep ];
  installCheckPhase = ''
    runHook preInstallCheck
    ! grep -Eq '^tonic(-prost)?[[:space:]]*=' Cargo.toml
    test -x "$out/bin/trevrpc-bench-peer-rust"
    test -x "$out/bin/trevrpc-conformance-rust"
    printf 'STOP\n' | "$out/bin/trevrpc-conformance-rust" --protocol 1 > peer.out
    grep -q '"event":"ready"' peer.out
    grep -q '"peer":"rust"' peer.out
    runHook postInstallCheck
  '';

  meta = {
    mainProgram = "protoc-gen-trevrpc-rust";
    description = "Protobuf over QUIC, HTTP/3 & WebTransport";
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
    homepage = "https://trev.zip/llc/TrevRPC";
    changelog = "https://trev.zip/llc/TrevRPC/releases";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/trevrpc-rust/v${final.version}";
  };
})
