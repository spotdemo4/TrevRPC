{
  stdenv,
  lib,
  cmake,
  ninja,
  pkg-config,
  protobuf,
  protobufc,
}:
stdenv.mkDerivation (final: {
  pname = "trevrpc-c-rpc-check";
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
    protobuf
    protobufc
  ];
  buildInputs = [ protobufc ];

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
      -DTREVRPC_BUILD_TRANSPORT=OFF \
      -DTREVRPC_BUILD_TRANSPORT_MSQUIC=OFF \
      -DTREVRPC_BUILD_RPC=ON \
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
    cmake --build build --parallel $NIX_BUILD_CORES
    runHook postBuild
  '';

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    ctest --test-dir build --output-on-failure -R '^trevrpc_rpc_(api|api_cpp|fake_transport|abi1_symbols)$'
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
    test -f "$out/include/trevrpc_rpc.h"
    test -f "$out/lib/libtrevrpc_rpc.a"
    test -f "$out/lib/trevrpc/rpc-abi1/libtrevrpc_rpc_private_core.a"
    test ! -e "$out/lib/libtrevrpc_core.a"
    test ! -e "$out/lib/pkgconfig/trevrpc_core.pc"
    test -f "$out/lib/pkgconfig/trevrpc_rpc.pc"
    test -f "$out/lib/cmake/trevrpc_rpc/trevrpc_rpcConfig.cmake"
    test -f "$out/lib/cmake/trevrpc_rpc/trevrpcRpcTargets.cmake"

    test ! -e "$out/include/trevrpc.h"
    test ! -e "$out/include/trevrpc_binding.h"
    test ! -e "$out/include/trevrpc_engine.h"
    test ! -e "$out/include/trevrpc_engine_msquic.h"
    test ! -e "$out/include/trevrpc_msquic.h"
    test ! -e "$out/include/trevrpc_raw.h"
    test ! -e "$out/include/trevrpc_webtransport.h"
    test ! -e "$out/bin/protoc-gen-trevrpc-c"
    test ! -e "$out/lib/libtrevrpc.a"
    test ! -e "$out/lib/libtrevrpc_msquic.a"
    test ! -e "$out/lib/libtrevrpc_msquic_api_owner.a"
    test ! -e "$out/lib/libtrevrpc_engine_msquic_support.a"
    test ! -e "$out/lib/libtrevrpc_msquic_native_core.a"
    test ! -e "$out/lib/libtrevrpc_protocol_core.a"
    test ! -e "$out/lib/libtrevrpc_webtransport.a"
    test ! -e "$out/lib/pkgconfig/trevrpc.pc"
    test ! -e "$out/lib/pkgconfig/trevrpc_msquic.pc"
    test ! -e "$out/lib/pkgconfig/trevrpc_webtransport.pc"
    test ! -e "$out/lib/pkgconfig/trevrpc_engine.pc"
    test ! -e "$out/lib/pkgconfig/trevrpc_engine_msquic.pc"
    test ! -e "$out/lib/pkgconfig/trevrpc_rpc_msquic.pc"
    test ! -e "$out/lib/cmake/trevrpc"
    test ! -e "$out/lib/cmake/trevrpc_engine"
    test ! -e "$out/lib/cmake/trevrpc_engine_msquic"
    test ! -e "$out/lib/cmake/trevrpc_rpc_msquic"

    mkdir -p "$TMPDIR/rpc-cmake-consumer"
    printf '%s\n' \
      'cmake_minimum_required(VERSION 3.20)' \
      'project(trevrpc-installed-rpc-cmake-consumer LANGUAGES C)' \
      'find_package(trevrpc_rpc CONFIG REQUIRED)' \
      'if(NOT TREVRPC_RPC_ABI_VERSION EQUAL 1)' \
      '  message(FATAL_ERROR "installed TrevRPC RPC package does not expose RPC ABI 1")' \
      'endif()' \
      'add_executable(trevrpc-installed-rpc-cmake-consumer rpc_main.c)' \
      'target_link_libraries(trevrpc-installed-rpc-cmake-consumer PRIVATE trevrpc::trevrpc_rpc)' \
      > "$TMPDIR/rpc-cmake-consumer/CMakeLists.txt"
    cp tests/install/cmake/rpc_main.c "$TMPDIR/rpc-cmake-consumer/rpc_main.c"
    cmake -S "$TMPDIR/rpc-cmake-consumer" -B "$TMPDIR/rpc-cmake-consumer-build" -G Ninja \
      -DCMAKE_PREFIX_PATH="$out"
    cmake --build "$TMPDIR/rpc-cmake-consumer-build" --parallel $NIX_BUILD_CORES
    "$TMPDIR/rpc-cmake-consumer-build/trevrpc-installed-rpc-cmake-consumer"

    export PKG_CONFIG_PATH="$out/lib/pkgconfig"
    test "$(pkg-config --variable=trevrpc_rpc_abi_version trevrpc_rpc)" = "1"
    cc tests/install/pkg-config/rpc_main.c \
      -o "$TMPDIR/rpc-pkg-config-consumer" \
      $(pkg-config --static --cflags --libs trevrpc_rpc)
    "$TMPDIR/rpc-pkg-config-consumer"

    if nm -u -P "$out/lib/libtrevrpc_rpc.a" | \
      grep ' U' | \
      grep -E -i '(msquic|webtransport|protobuf|trevrpc_(msquic|webtransport))'; then
      echo "RPC archive has a forbidden transport dependency" >&2
      exit 1
    fi
    sh tests/rpc-abi1/check_installed_closure.sh "$out" rpc "$PWD"
    runHook postInstallCheck
  '';
})
