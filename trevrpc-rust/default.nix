{
  lib,
  stdenv,
  mkRustPackage,
  rustfmt,
  clippy,
  gnugrep,
  licenseFile,
  wireGolden,
  interopProto,
  trevrpcProto,
}:
let
  canExecute = stdenv.buildPlatform.canExecute stdenv.hostPlatform;
in
mkRustPackage (final: {
  pname = "trevrpc-rust";
  version = "1.0.0";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      licenseFile
      interopProto
      trevrpcProto
      wireGolden
      ./.
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-rust";
  cargoLock.lockFile = ./Cargo.lock;
  cargoBuildFlags = [ "--workspace" ];

  doCheck = canExecute;
  # The tests and benchmarks bind loopback UDP sockets and run the interop executable against itself.
  __darwinAllowLocalNetworking = true;
  nativeCheckInputs = [
    # The generator tests format its output to compare against the checked-in bindings.
    rustfmt
    clippy
  ];
  checkPhase = ''
    runHook preCheck
    cargo test --workspace --offline
    # Run every benchmark once so they keep compiling and working; the numbers are not checked.
    cargo bench --offline --bench frames --bench loopback -- --test
    cargo clippy --workspace --all-targets --offline -- -D warnings
    runHook postCheck
  '';

  installPhase = ''
    runHook preInstall
    for binary in protoc-gen-trevrpc-rust trevrpc-interop-rust; do
      path=$(find target -path "*/release/$binary" -type f -perm -0100 | head -n1)
      install -Dm755 "$path" "$out/bin/$binary"
    done
    runHook postInstall
  '';

  doInstallCheck = canExecute;
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
    changelog = "https://trev.zip/llc/TrevRPC/releases";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/trevrpc-rust/v${final.version}";
  };
})
