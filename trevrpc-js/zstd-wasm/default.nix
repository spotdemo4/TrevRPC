{
  lib,
  stdenv,
  rustPlatform,
  cargo,
  rustc,
  lld,
  llvmPackages,
  binaryen,
}:
let
  cargoToml = lib.importTOML ./Cargo.toml;
in
# The libzstd WebAssembly module behind @trevrpc/trevrpc-js/zstd: $out/libzstd.wasm.
stdenv.mkDerivation {
  pname = cargoToml.package.name;
  inherit (cargoToml.package) version;

  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [
      ./.cargo
      ./Cargo.toml
      ./Cargo.lock
      ./src
    ];
  };

  cargoDeps = rustPlatform.importCargoLock { lockFile = ./Cargo.lock; };

  nativeBuildInputs = [
    rustPlatform.cargoSetupHook
    cargo
    rustc
    lld
    binaryen
  ];

  # zstd-sys compiles libzstd with cc; the wrapped compilers target the build machine, so the C
  # side uses the bare LLVM tools.
  env = {
    CC_wasm32_unknown_unknown = "${llvmPackages.clang-unwrapped}/bin/clang";
    AR_wasm32_unknown_unknown = "${llvmPackages.llvm}/bin/llvm-ar";
    CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_LINKER = "${lld}/bin/wasm-ld";
  };

  buildPhase = ''
    runHook preBuild
    cargo build --release --offline
    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall
    mkdir -p "$out"
    wasm-opt -O3 --enable-bulk-memory --enable-nontrapping-float-to-int --enable-sign-ext \
      --enable-mutable-globals --strip-debug --strip-producers \
      target/wasm32-unknown-unknown/release/trevrpc_zstd_wasm.wasm -o "$out/libzstd.wasm"
    runHook postInstall
  '';

  meta = {
    description = cargoToml.package.description;
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
  };
}
