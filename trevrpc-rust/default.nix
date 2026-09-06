{
  lib,
  stdenv,
  rustPlatform,
  rustfmt,
  clippy,
  gnugrep,
  pkg-config,
  trevrpcC,
  trevrpcCTransportTesting,
  nativeFfiSanitizers ? false,
  benchProto,
  wireGolden,
}:
let
  canExecute = stdenv.buildPlatform.canExecute stdenv.hostPlatform;
  nativeTransportSupported =
    canExecute
    && builtins.elem stdenv.hostPlatform.system [
      "x86_64-linux"
      "aarch64-linux"
      "x86_64-darwin"
      "aarch64-darwin"
    ];
in
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

  doCheck = canExecute;
  nativeBuildInputs = [ pkg-config ];
  buildInputs = [ trevrpcC ];

  nativeCheckInputs = [
    rustfmt
    clippy
  ];
  checkInputs = lib.optionals nativeTransportSupported [
    trevrpcCTransportTesting
  ];
  checkPhase = ''
    ${lib.optionalString nativeFfiSanitizers ''
      export TREVRPC_NATIVE_FFI_SANITIZERS=1
      unset LD_PRELOAD
      # Nix may preload a build-sandbox helper ahead of the test binary's
      # first DT_NEEDED entry; libasan itself is linked before libc below.
      export ASAN_OPTIONS='verify_asan_link_order=0:detect_leaks=1:halt_on_error=1'
      export UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1'
    ''}
    cargo fmt --check
    cargo check --lib --no-default-features --features http3 --offline
    cargo check --lib --no-default-features --features webtransport-client --offline
    cargo check --lib --no-default-features --features webtransport-server --offline
    cargo check --lib --no-default-features --features webtransport --offline
    cargo check -p trevrpc-c-sys --no-default-features --offline
    cargo test -p trevrpc-c-sys --no-default-features --test abi --offline
    cargo check -p trevrpc-native --no-default-features --offline
    cargo check -p trevrpc-native --no-default-features --features native-c --offline
    cargo check --lib --no-default-features --features native-c --offline
    ${lib.optionalString nativeTransportSupported ''
      cargo test -p trevrpc-c-sys --features system-native --test abi --offline
      testing_pkg_config=${trevrpcCTransportTesting}/share/trevrpc-transport-testing/pkgconfig
      PKG_CONFIG_PATH="$testing_pkg_config:''${PKG_CONFIG_PATH:-}" \
        cargo test -p trevrpc-c-sys --features testing --test abi --offline
      cargo test -p trevrpc-native --features system-native --offline
      PKG_CONFIG_PATH="$testing_pkg_config:''${PKG_CONFIG_PATH:-}" \
        cargo test -p trevrpc-native --features testing --offline
      PKG_CONFIG_PATH="$testing_pkg_config:''${PKG_CONFIG_PATH:-}" \
        cargo test -p trevrpc-c-sys --all-features --test abi --offline
      PKG_CONFIG_PATH="$testing_pkg_config:''${PKG_CONFIG_PATH:-}" \
        cargo test -p trevrpc-native --all-features --offline
      ${lib.optionalString nativeFfiSanitizers ''
        PKG_CONFIG_PATH="$testing_pkg_config:''${PKG_CONFIG_PATH:-}" \
          cargo test -p trevrpc-native --no-default-features --features testing --offline \
            testing::tests::ffi_lifecycle_stress_is_leak_free -- \
            --ignored --exact --test-threads=1
        PKG_CONFIG_PATH="$testing_pkg_config:''${PKG_CONFIG_PATH:-}" \
          cargo test -p trevrpc-native --no-default-features --features testing --offline \
            testing::tests::emergency_cleanup_churn_has_bounded_threads -- \
            --ignored --exact --test-threads=1
      ''}
      cargo test --lib --no-default-features --features system-native --offline
      cargo test --no-default-features --features system-native --test native_server --offline
      cargo clippy -p trevrpc-c-sys --all-targets --features system-native --offline -- -D warnings
      PKG_CONFIG_PATH="$testing_pkg_config:''${PKG_CONFIG_PATH:-}" \
        cargo clippy -p trevrpc-c-sys --all-targets --features testing --offline -- -D warnings
      cargo clippy -p trevrpc-native --all-targets --features system-native --offline -- -D warnings
      PKG_CONFIG_PATH="$testing_pkg_config:''${PKG_CONFIG_PATH:-}" \
        cargo clippy -p trevrpc-native --all-targets --features testing --offline -- -D warnings
      cargo clippy --lib --no-default-features --features system-native --offline -- -D warnings
    ''}
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

  doInstallCheck = canExecute;
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
