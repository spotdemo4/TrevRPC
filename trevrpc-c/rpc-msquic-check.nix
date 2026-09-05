{
  stdenv,
  lib,
  cmake,
  ninja,
  openssl,
  pkg-config,
  protobuf,
  protobufc,
  libmsquic,
}:
stdenv.mkDerivation (final: {
  pname = "trevrpc-c-rpc-msquic-check";
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
    protobuf
    protobufc
  ];
  buildInputs = [
    libmsquic
    protobufc
  ];

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
      -DTREVRPC_BUILD_RPC=ON \
      -DTREVRPC_BUILD_RPC_MSQUIC=ON \
      -DTREVRPC_BUILD_WEBTRANSPORT=OFF \
      -DTREVRPC_BUILD_RUNTIME=OFF \
      -DTREVRPC_BUILD_CODEGEN=ON \
      -DTREVRPC_BUILD_BENCHMARKS=ON \
      -DTREVRPC_BUILD_EXAMPLES=ON
    runHook postConfigure
  '';

  buildPhase = ''
    runHook preBuild
    build_targets="protoc-gen-trevrpc-c trevrpc_engine trevrpc_engine_msquic_support trevrpc_engine_msquic trevrpc_msquic_api_owner trevrpc_rpc trevrpc_rpc_msquic trevrpc_rpc_api_test trevrpc_rpc_api_cpp_test trevrpc_rpc_fake_transport_test trevrpc_rpc_msquic_smoke_test trevrpc_rpc_transport_h3_test trevrpc_rpc_transport_msquic_lifecycle_test trevrpc_rpc_native_roundtrip_test trevrpc_rpc_native_shapes_test trevrpc_rpc_h3_shapes_test trevrpc_rpc_wt_shapes_test trevrpc_generated_service_test trevrpc_greeter_server trevrpc_greeter_client trevrpc_bench_peer_c"
    cmake --build build --parallel $NIX_BUILD_CORES --target $build_targets
    runHook postBuild
  '';

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    ctest --test-dir build --output-on-failure \
      -R '^(trevrpc_rpc_(api|api_cpp|fake_transport|abi1_symbols|msquic_abi1_symbols|msquic_smoke|native_roundtrip|native_shapes|h3_shapes|wt_shapes.*|transport_h3.*|transport_msquic_lifecycle)|trevrpc_generated_service_.*|trevrpc_greeter_(native|http3|webtransport)_smoke|trevrpc_bench_peer_(capabilities|native_smoke|webtransport_config|webtransport_smoke))$'
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
    test -f "$out/include/trevrpc_rpc.h"
    test -f "$out/include/trevrpc_rpc_msquic.h"
    test -f "$out/lib/libtrevrpc_engine.a"
    test -f "$out/lib/libtrevrpc_engine_msquic.a"
    test -f "$out/lib/libtrevrpc_engine_msquic_support.a"
    test -f "$out/lib/libtrevrpc_msquic_api_owner.a"
    test -f "$out/lib/libtrevrpc_rpc.a"
    test -f "$out/lib/libtrevrpc_rpc_msquic.a"
    test -f "$out/lib/trevrpc/rpc-abi1/libtrevrpc_rpc_private_core.a"
    test -f "$out/lib/trevrpc/rpc-abi1/libtrevrpc_rpc_private_msquic_native.a"
    test -f "$out/lib/trevrpc/rpc-abi1/libtrevrpc_rpc_private_protocol.a"
    test ! -e "$out/lib/libtrevrpc_core.a"
    test ! -e "$out/lib/libtrevrpc_msquic_native_core.a"
    test ! -e "$out/lib/libtrevrpc_protocol_core.a"
    test ! -e "$out/lib/pkgconfig/trevrpc_core.pc"
    test -f "$out/lib/pkgconfig/trevrpc_engine.pc"
    test -f "$out/lib/pkgconfig/trevrpc_engine_msquic.pc"
    test -f "$out/lib/pkgconfig/trevrpc_rpc.pc"
    test -f "$out/lib/pkgconfig/trevrpc_rpc_msquic.pc"
    test -f "$out/lib/cmake/trevrpc_engine/trevrpc_engineConfig.cmake"
    test -f "$out/lib/cmake/trevrpc_engine/trevrpcEngineTargets.cmake"
    test -f "$out/lib/cmake/trevrpc_engine_msquic/trevrpc_engine_msquicConfig.cmake"
    test -f "$out/lib/cmake/trevrpc_engine_msquic/trevrpcEngineMsquicTargets.cmake"
    test -f "$out/lib/cmake/trevrpc_rpc/trevrpc_rpcConfig.cmake"
    test -f "$out/lib/cmake/trevrpc_rpc/trevrpcRpcTargets.cmake"
    test -f "$out/lib/cmake/trevrpc_rpc_msquic/trevrpc_rpc_msquicConfig.cmake"
    test -f "$out/lib/cmake/trevrpc_rpc_msquic/trevrpcRpcMsquicTargets.cmake"

    # The aggregate ABI-6 facade and its legacy transport artifacts are not
    # part of the independently versioned Engine/RPC package set.
    test ! -e "$out/include/trevrpc.h"
    test ! -e "$out/include/trevrpc_binding.h"
    test ! -e "$out/include/trevrpc_msquic.h"
    test ! -e "$out/include/trevrpc_raw.h"
    test ! -e "$out/include/trevrpc_webtransport.h"
    test -x "$out/bin/protoc-gen-trevrpc-c"
    test ! -e "$out/lib/libtrevrpc.a"
    test ! -e "$out/lib/libtrevrpc_msquic.a"
    test ! -e "$out/lib/libtrevrpc_webtransport.a"
    test ! -e "$out/lib/pkgconfig/trevrpc.pc"
    test ! -e "$out/lib/pkgconfig/trevrpc_msquic.pc"
    test ! -e "$out/lib/pkgconfig/trevrpc_webtransport.pc"
    test ! -e "$out/lib/cmake/trevrpc"

    cmake -S tests/install/cmake -B "$TMPDIR/rpc-msquic-cmake-consumer" -G Ninja \
      -DCMAKE_PREFIX_PATH="$out" \
      -DTREVRPC_INSTALL_TEST_RPC_MSQUIC_ONLY=ON
    cmake --build "$TMPDIR/rpc-msquic-cmake-consumer" --parallel $NIX_BUILD_CORES
    "$TMPDIR/rpc-msquic-cmake-consumer/trevrpc-installed-engine-cmake-consumer"
    "$TMPDIR/rpc-msquic-cmake-consumer/trevrpc-installed-engine-msquic-cmake-consumer"
    "$TMPDIR/rpc-msquic-cmake-consumer/trevrpc-installed-engine-msquic-cmake-cpp-consumer"
    "$TMPDIR/rpc-msquic-cmake-consumer/trevrpc-installed-rpc-cmake-consumer"
    "$TMPDIR/rpc-msquic-cmake-consumer/trevrpc-installed-rpc-msquic-cmake-consumer"

    export PKG_CONFIG_PATH="$out/lib/pkgconfig"
    test "$(pkg-config --variable=trevrpc_rpc_abi_version trevrpc_rpc)" = "1"
    test "$(pkg-config --variable=trevrpc_rpc_msquic_abi_version trevrpc_rpc_msquic)" = "1"
    cc tests/install/pkg-config/rpc_main.c \
      -o "$TMPDIR/rpc-pkg-config-consumer" \
      $(pkg-config --static --cflags --libs trevrpc_rpc)
    "$TMPDIR/rpc-pkg-config-consumer"
    cc tests/install/pkg-config/rpc_msquic_main.c \
      -o "$TMPDIR/rpc-msquic-pkg-config-consumer" \
      $(pkg-config --static --cflags --libs trevrpc_rpc_msquic)
    "$TMPDIR/rpc-msquic-pkg-config-consumer"
    sh tests/rpc-abi1/check_installed_closure.sh "$out" rpc-msquic "$PWD"
    runHook postInstallCheck
  '';

  passthru.msquicProvider = libmsquic;
})
