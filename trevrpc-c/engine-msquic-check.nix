{
  stdenv,
  lib,
  cmake,
  ninja,
  openssl,
  pkg-config,
  libmsquic,
}:
stdenv.mkDerivation (final: {
  pname = "trevrpc-c-engine-msquic-check";
  version = "0.2.2";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = ./.;
  };
  sourceRoot = "${final.src.name}/trevrpc-c";

  nativeBuildInputs = [
    cmake
    ninja
    openssl
    pkg-config
  ];
  buildInputs = [ libmsquic ];

  configurePhase = ''
    runHook preConfigure
    cmake -S . -B build -G Ninja \
      -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_INSTALL_PREFIX="$out" \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DTREVRPC_BUILD_TESTS=ON \
      -DTREVRPC_BUILD_ENGINE=ON \
      -DTREVRPC_BUILD_MSQUIC=OFF \
      -DTREVRPC_BUILD_ENGINE_MSQUIC=ON \
      -DTREVRPC_BUILD_TRANSPORT=OFF \
      -DTREVRPC_BUILD_TRANSPORT_MSQUIC=OFF \
      -DTREVRPC_BUILD_RPC=OFF \
      -DTREVRPC_BUILD_RPC_MSQUIC=OFF \
      -DTREVRPC_BUILD_WEBTRANSPORT=OFF \
      -DTREVRPC_BUILD_RUNTIME=OFF \
      -DTREVRPC_BUILD_CODEGEN=OFF \
      -DTREVRPC_BUILD_BENCHMARKS=OFF \
      -DTREVRPC_BUILD_EXAMPLES=OFF
    runHook postConfigure
  '';

  buildPhase = ''
    runHook preBuild
    cmake --build build --parallel $NIX_BUILD_CORES --target \
      trevrpc_engine \
      trevrpc_msquic_api_owner \
      trevrpc_engine_msquic_support \
      trevrpc_engine_msquic \
      trevrpc_engine_msquic_api_test \
      trevrpc_engine_msquic_api_cpp_test \
      trevrpc_engine_msquic_smoke_test
    runHook postBuild
  '';

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    ctest --test-dir build --output-on-failure \
      -R '^trevrpc_engine_msquic_(api|api_cpp|abi1_symbols|smoke)$'
    runHook postCheck
  '';

  installPhase = ''
    runHook preInstall
    cmake --install build
    runHook postInstall
  '';

  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck
    test -f "$out/include/trevrpc_engine.h"
    test -f "$out/include/trevrpc_engine_msquic.h"
    test ! -e "$out/include/trevrpc_rpc.h"
    test ! -e "$out/include/trevrpc_rpc_msquic.h"
    test -f "$out/lib/libtrevrpc_engine.a"
    test -f "$out/lib/libtrevrpc_engine_msquic.a"
    test -f "$out/lib/libtrevrpc_msquic_api_owner.a"
    test -f "$out/lib/libtrevrpc_engine_msquic_support.a"
    test ! -e "$out/include/trevrpc.h"
    test ! -e "$out/include/trevrpc_binding.h"
    test ! -e "$out/include/trevrpc_msquic.h"
    test ! -e "$out/include/trevrpc_raw.h"
    test ! -e "$out/include/trevrpc_webtransport.h"
    test ! -e "$out/lib/libtrevrpc_core.a"
    test ! -e "$out/lib/libtrevrpc_rpc.a"
    test ! -e "$out/lib/libtrevrpc_rpc_msquic.a"
    test ! -e "$out/lib/libtrevrpc_msquic_native_core.a"
    test ! -e "$out/lib/libtrevrpc_msquic.a"
    test ! -e "$out/lib/pkgconfig/trevrpc_core.pc"
    test ! -e "$out/lib/pkgconfig/trevrpc_rpc.pc"
    test ! -e "$out/lib/pkgconfig/trevrpc_rpc_msquic.pc"
    test ! -e "$out/lib/cmake/trevrpc"
    test ! -e "$out/lib/cmake/trevrpc_rpc"
    test ! -e "$out/lib/cmake/trevrpc_rpc_msquic"
    test -f "$out/lib/pkgconfig/trevrpc_engine.pc"
    test -f "$out/lib/pkgconfig/trevrpc_engine_msquic.pc"
    test -f "$out/lib/cmake/trevrpc_engine/trevrpc_engineConfig.cmake"
    test -f "$out/lib/cmake/trevrpc_engine/trevrpcEngineTargets.cmake"
    test -f "$out/lib/cmake/trevrpc_engine_msquic/trevrpc_engine_msquicConfig.cmake"
    test -f "$out/lib/cmake/trevrpc_engine_msquic/trevrpcEngineMsquicTargets.cmake"

    cmake -S tests/install/cmake -B "$TMPDIR/engine-msquic-cmake-consumer" -G Ninja \
      -DCMAKE_PREFIX_PATH="$out" \
      -DTREVRPC_INSTALL_TEST_ENGINE_MSQUIC_ONLY=ON
    cmake --build "$TMPDIR/engine-msquic-cmake-consumer" --parallel $NIX_BUILD_CORES
    "$TMPDIR/engine-msquic-cmake-consumer/trevrpc-installed-engine-cmake-consumer"
    "$TMPDIR/engine-msquic-cmake-consumer/trevrpc-installed-engine-msquic-cmake-consumer"
    "$TMPDIR/engine-msquic-cmake-consumer/trevrpc-installed-engine-msquic-cmake-cpp-consumer"

    export PKG_CONFIG_PATH="$out/lib/pkgconfig"
    test "$(pkg-config --variable=trevrpc_engine_msquic_abi_version trevrpc_engine_msquic)" = "1"
    cc tests/install/pkg-config/engine_msquic_main.c \
      -o "$TMPDIR/engine-msquic-pkg-config-consumer" \
      $(pkg-config --static --cflags --libs trevrpc_engine_msquic)
    "$TMPDIR/engine-msquic-pkg-config-consumer"
    runHook postInstallCheck
  '';

  passthru.msquicProvider = libmsquic;
})
