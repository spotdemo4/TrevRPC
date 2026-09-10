{
  stdenv,
  lib,
  cmake,
  ninja,
  pkg-config,
  sanitizers ? false,
}:
let
  cSources = import ./source.nix { inherit lib; };
in
stdenv.mkDerivation (final: {
  pname = "trevrpc-c-transport-check";
  outputs = [
    "out"
    "testing"
  ];
  version = "0.2.2";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = cSources.neutral;
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
      -DTREVRPC_ENABLE_SANITIZERS=${if sanitizers then "ON" else "OFF"} \
      -DTREVRPC_BUILD_ENGINE=OFF \
      -DTREVRPC_BUILD_ENGINE_MSQUIC=OFF \
      -DTREVRPC_BUILD_TRANSPORT=ON \
      -DTREVRPC_BUILD_TRANSPORT_MSQUIC=OFF \
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
      trevrpc_transport \
      trevrpc_transport_provider_abi_test \
      trevrpc_transport_api_test \
      trevrpc_transport_api_cpp_test \
      trevrpc_transport_testing_test
    runHook postBuild
  '';

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    ctest --test-dir build --output-on-failure \
      -R '^trevrpc_transport_(api|api_cpp|provider_abi|abi1_symbols|testing)$'
    runHook postCheck
  '';

  installPhase = ''
    runHook preInstall
    cmake --install build

    testing_root="$testing/share/trevrpc-transport-testing"
    testing_lib="$testing/lib/trevrpc-transport-testing"
    mkdir -p "$testing_root/include" "$testing_root/pkgconfig" "$testing_lib"
    cp build/libtrevrpc_transport_testing.a "$testing_lib/"
    cp build/libtrevrpc_transport.a "$testing_lib/"
    cp include/trevrpc_transport.h "$testing_root/include/"
    cp tests/trevrpc_transport_testing.h "$testing_root/include/"
    substitute build/trevrpc_transport_testing.pc \
      "$testing_root/pkgconfig/trevrpc_transport_testing.pc" \
      --replace-fail "prefix=$PWD/build" "prefix=$testing_root" \
      --replace-fail 'libdir=''${prefix}' "libdir=$testing_lib" \
      --replace-fail "includedir=$PWD/include" 'includedir=''${prefix}/include' \
      --replace-fail "testincludedir=$PWD/tests" 'testincludedir=''${prefix}/include'
    runHook postInstall
  '';

  doInstallCheck = !sanitizers;
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
    testing_root="$testing/share/trevrpc-transport-testing"
    testing_lib="$testing/lib/trevrpc-transport-testing"
    test -f "$testing_root/include/trevrpc_transport.h"
    test -f "$testing_root/include/trevrpc_transport_testing.h"
    test -f "$testing_lib/libtrevrpc_transport.a"
    test -f "$testing_lib/libtrevrpc_transport_testing.a"
    test -f "$testing_root/pkgconfig/trevrpc_transport_testing.pc"
    PKG_CONFIG_PATH="$testing_root/pkgconfig" \
      pkg-config --static --exists trevrpc_transport_testing

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
