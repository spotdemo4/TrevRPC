{
  stdenv,
  lib,
  cmake,
  ninja,
  pkg-config,
}:
stdenv.mkDerivation (final: {
  pname = "trevrpc-c-transport-check";
  version = "0.2.2";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = ./.;
  };
  sourceRoot = "${final.src.name}/trevrpc-c";

  nativeBuildInputs = [
    cmake
    ninja
    pkg-config
  ];

  configurePhase = ''
    runHook preConfigure
    cmake -S . -B build -G Ninja \
      -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_INSTALL_PREFIX="$out" \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DTREVRPC_BUILD_TESTS=ON \
      -DTREVRPC_BUILD_ENGINE=OFF \
      -DTREVRPC_BUILD_MSQUIC=OFF \
      -DTREVRPC_BUILD_ENGINE_MSQUIC=OFF \
      -DTREVRPC_BUILD_TRANSPORT=ON \
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
      trevrpc_transport \
      trevrpc_transport_api_test \
      trevrpc_transport_api_cpp_test
    runHook postBuild
  '';

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    ctest --test-dir build --output-on-failure \
      -R '^trevrpc_transport_(api|api_cpp|abi1_symbols)$'
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
    test -f "$out/include/trevrpc_transport.h"
    test -f "$out/lib/libtrevrpc_transport.a"
    test -f "$out/lib/pkgconfig/trevrpc_transport.pc"
    test -f "$out/lib/cmake/trevrpc_transport/trevrpc_transportConfig.cmake"
    test -f "$out/lib/cmake/trevrpc_transport/trevrpcTransportTargets.cmake"
    test ! -e "$out/include/trevrpc_engine.h"
    test ! -e "$out/include/trevrpc_transport_msquic.h"
    test ! -e "$out/include/trevrpc_rpc.h"
    test ! -e "$out/lib/libtrevrpc_engine.a"
    test ! -e "$out/lib/libtrevrpc_transport_msquic.a"
    test ! -e "$out/lib/libtrevrpc_rpc.a"

    cmake -S tests/install/cmake -B "$TMPDIR/transport-cmake-consumer" -G Ninja \
      -DCMAKE_PREFIX_PATH="$out" \
      -DTREVRPC_INSTALL_TEST_TRANSPORT_ONLY=ON
    cmake --build "$TMPDIR/transport-cmake-consumer" --parallel $NIX_BUILD_CORES
    "$TMPDIR/transport-cmake-consumer/trevrpc-installed-transport-cmake-consumer"

    export PKG_CONFIG_PATH="$out/lib/pkgconfig"
    test "$(pkg-config --variable=trevrpc_transport_abi_version trevrpc_transport)" = "1"
    cc tests/install/pkg-config/transport_main.c \
      -o "$TMPDIR/transport-pkg-config-consumer" \
      $(pkg-config --static --cflags --libs trevrpc_transport)
    "$TMPDIR/transport-pkg-config-consumer"
    runHook postInstallCheck
  '';
})
