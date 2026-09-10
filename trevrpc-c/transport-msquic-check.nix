{
  stdenv,
  lib,
  cmake,
  ninja,
  openssl,
  pkg-config,
  libmsquic,
}:
let
  cSources = import ./source.nix { inherit lib; };
in
stdenv.mkDerivation (final: {
  pname = "trevrpc-c-transport-msquic-check";
  version = "0.2.2";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = cSources.msquic;
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
      -DTREVRPC_TRANSPORT_PRIVATE_INSTALL_LIBDIR="$out/lib/trevrpc/transport-private-check" \
      -DTREVRPC_BUILD_TESTS=ON \
      -DTREVRPC_BUILD_ENGINE=ON \
      -DTREVRPC_BUILD_ENGINE_MSQUIC=ON \
      -DTREVRPC_BUILD_TRANSPORT=ON \
      -DTREVRPC_BUILD_TRANSPORT_MSQUIC=ON \
      -DTREVRPC_BUILD_RPC=OFF \
      -DTREVRPC_BUILD_RPC_MSQUIC=OFF \
      -DTREVRPC_BUILD_CODEGEN=OFF \
      -DTREVRPC_BUILD_BENCHMARKS=OFF \
      -DTREVRPC_BUILD_EXAMPLES=OFF
    runHook postConfigure
  '';

  buildPhase = ''
    runHook preBuild
    cmake --build build --parallel $NIX_BUILD_CORES --target \
      trevrpc_engine \
      trevrpc_engine_msquic \
      trevrpc_transport \
      trevrpc_transport_msquic \
      trevrpc_transport_api_test \
      trevrpc_transport_api_cpp_test \
      trevrpc_transport_msquic_smoke_test \
      trevrpc_transport_native_roundtrip_test
    runHook postBuild
  '';

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    ctest --test-dir build --output-on-failure \
      -R '^(trevrpc_transport_(api|api_cpp|abi1_symbols|msquic_abi1_symbols|msquic_smoke|native_roundtrip)|trevrpc_removed_abi6_surface)$'
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
    test -f "$out/include/trevrpc_transport.h"
    test -f "$out/include/trevrpc_transport_msquic.h"
    test -f "$out/lib/libtrevrpc_engine.a"
    test -f "$out/lib/libtrevrpc_engine_msquic.a"
    test -f "$out/lib/libtrevrpc_transport.a"
    test -f "$out/lib/libtrevrpc_transport_msquic.a"
    test -f "$out/lib/trevrpc/transport-private-check/libtrevrpc_transport_msquic_private.a"
    test -f "$out/lib/trevrpc/transport-private-check/libtrevrpc_transport_private_core.a"
    test -f "$out/lib/trevrpc/transport-private-check/libtrevrpc_transport_private_msquic_native.a"
    test -f "$out/lib/trevrpc/transport-private-check/libtrevrpc_transport_private_protocol.a"
    test ! -e "$out/include/trevrpc_rpc.h"
    test ! -e "$out/include/trevrpc_rpc_msquic.h"
    test ! -e "$out/lib/libtrevrpc_rpc.a"
    test ! -e "$out/lib/libtrevrpc_rpc_msquic.a"
    cmake \
      -DTREVRPC_NM="$(command -v nm)" \
      -DTREVRPC_AR="$(command -v ar)" \
      -DTREVRPC_INSTALL_PREFIXES="$out" \
      -DTREVRPC_SOURCE_DIR="$PWD" \
      -P tests/abi/check_removed_abi6.cmake

    cmake -S tests/install/cmake -B "$TMPDIR/transport-msquic-cmake-consumer" -G Ninja \
      -DCMAKE_PREFIX_PATH="$out" \
      -DTREVRPC_INSTALL_TEST_TRANSPORT_MSQUIC_ONLY=ON
    cmake --build "$TMPDIR/transport-msquic-cmake-consumer" --parallel $NIX_BUILD_CORES
    "$TMPDIR/transport-msquic-cmake-consumer/trevrpc-installed-transport-cmake-consumer"
    "$TMPDIR/transport-msquic-cmake-consumer/trevrpc-installed-transport-msquic-cmake-consumer"

    export PKG_CONFIG_PATH="$out/lib/pkgconfig"
    test "$(pkg-config --variable=trevrpc_transport_msquic_abi_version trevrpc_transport_msquic)" = "1"
    cc tests/install/pkg-config/transport_msquic_main.c \
      -o "$TMPDIR/transport-msquic-pkg-config-consumer" \
      $(pkg-config --static --cflags --libs trevrpc_transport_msquic)
    "$TMPDIR/transport-msquic-pkg-config-consumer"
    runHook postInstallCheck
  '';

  passthru.msquicProvider = libmsquic;
})
