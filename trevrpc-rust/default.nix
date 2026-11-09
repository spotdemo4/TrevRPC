{
  lib,
  mkRustPackage,
  protobuf,
  rustfmt,
  clippy,
  gnugrep,
  pebble,
  licenseFile,
  wireGolden,
  zstdGolden,
  interopProto,
  trevrpcProto,
}:
mkRustPackage (final: {
  pname = "trevrpc-rust";
  version = "1.7.0";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      licenseFile
      interopProto
      trevrpcProto
      wireGolden
      zstdGolden
      ./.
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-rust";
  cargoLock.lockFile = ./Cargo.lock;
  cargoBuildFlags = [ "--workspace" ];
  # The build script of prost-protovalidate-types, which trevrpc-protovalidate needs, runs protoc.
  nativeBuildInputs = [ protobuf ];

  doCheck = true;
  # The tests and benchmarks bind loopback UDP sockets and run the interop executable against itself.
  __darwinAllowLocalNetworking = true;
  # The generator tests format its output to compare against the checked-in bindings, and the ACME
  # test gets a certificate from Pebble.
  nativeCheckInputs = [
    rustfmt
    pebble
  ];
  checkPhase = ''
    runHook preCheck
    cargo test --workspace --all-features --offline
    # Run every benchmark once so they keep compiling and working; the numbers are not checked.
    cargo bench --offline --bench frames --bench loopback -- --test
    runHook postCheck
  '';

  # The dependencies are also checked for Clippy, whose lints run in the trevrpc-rust-lint check.
  cargoArtifactsArgs = {
    nativeCheckInputs = [
      rustfmt
      clippy
      pebble
    ];
    checkPhase = ''
      runHook preCheck
      cargo test --workspace --all-features --offline
      cargo bench --offline --bench frames --bench loopback -- --test
      cargo clippy --workspace --all-targets --offline
      cargo clippy --workspace --all-targets --all-features --offline
      runHook postCheck
    '';
  };

  installPhase = ''
    runHook preInstall
    for binary in protoc-gen-trevrpc-rust trevrpc-interop-rust; do
      path=$(find target -path "*/release/$binary" -type f -perm -0100 | head -n1)
      install -Dm755 "$path" "$out/bin/$binary"
    done
    runHook postInstall
  '';

  doInstallCheck = true;
  nativeInstallCheckInputs = [ gnugrep ];
  installCheckPhase = ''
    runHook preInstallCheck
    ! grep -Eq '^tonic(-prost)?[[:space:]]*=' Cargo.toml
    test -x "$out/bin/protoc-gen-trevrpc-rust"
    test -x "$out/bin/trevrpc-interop-rust"
    runHook postInstallCheck
  '';

  meta = {
    mainProgram = "protoc-gen-trevrpc-rust";
    description = "Protobuf over QUIC, HTTP/3 & WebTransport";
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
    homepage = "https://trev.zip/llc/TrevRPC";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases";
    changelog = "https://trev.zip/llc/TrevRPC/releases/tag/trevrpc-rust/v${final.version}";
    sourceProvenance = [ lib.sourceTypes.fromSource ];
    maintainers = [ lib.maintainers.spotdemo4 ];
  };
})
