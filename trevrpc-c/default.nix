{
  stdenv,
  lib,
  clang-tools,
  cmake,
  ninja,
  libmsquic,
  openssl,
  pkg-config,
  protobuf,
  protobufc,
  benchProto,
  wireGolden,
  peerBinaries ? [ ],
  sanitizers ? false,
  threadSanitizer ? false,
  legacyCompatibility ? false,
}:
assert !(sanitizers && threadSanitizer);
stdenv.mkDerivation (
  final: with lib; {
    pname = "trevrpc-c";
    version = "0.2.2";
    outputs = [
      "out"
      "dev"
      "lib"
    ];

    src = fileset.toSource {
      root = ../.;
      fileset = fileset.unions [
        benchProto
        wireGolden
        ../bench/http3-lifecycle-test.sh
        ./.
      ];
    };
    sourceRoot = "${final.src.name}/trevrpc-c";

    configurePhase = ''
      runHook preConfigure
      cmake -S . -B build -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_BINDIR="$out/bin" \
        -DCMAKE_INSTALL_INCLUDEDIR="$dev/include" \
        -DCMAKE_INSTALL_LIBDIR="$lib/lib" \
        -DCMAKE_INSTALL_LIBEXECDIR="$lib/libexec" \
        -DTREVRPC_INSTALL_CMAKEDIR="$dev/lib/cmake/trevrpc" \
        -DTREVRPC_ENGINE_INSTALL_CMAKEDIR="$dev/lib/cmake/trevrpc_engine" \
        -DTREVRPC_ENGINE_MSQUIC_INSTALL_CMAKEDIR="$dev/lib/cmake/trevrpc_engine_msquic" \
        -DTREVRPC_TRANSPORT_INSTALL_CMAKEDIR="$dev/lib/cmake/trevrpc_transport" \
        -DTREVRPC_TRANSPORT_MSQUIC_INSTALL_CMAKEDIR="$dev/lib/cmake/trevrpc_transport_msquic" \
        -DTREVRPC_RPC_INSTALL_CMAKEDIR="$dev/lib/cmake/trevrpc_rpc" \
        -DTREVRPC_RPC_MSQUIC_INSTALL_CMAKEDIR="$dev/lib/cmake/trevrpc_rpc_msquic" \
        -DTREVRPC_INSTALL_PKGCONFIGDIR="$dev/lib/pkgconfig" \
        -DTREVRPC_BUILD_BENCHMARKS=${if legacyCompatibility then "OFF" else "ON"} \
        -DTREVRPC_BUILD_TESTS=${if legacyCompatibility then "OFF" else "ON"} \
        -DTREVRPC_BUILD_CODEGEN=${if legacyCompatibility then "OFF" else "ON"} \
        -DTREVRPC_BUILD_ENGINE=${if legacyCompatibility then "OFF" else "ON"} \
        -DTREVRPC_BUILD_MSQUIC=${if legacyCompatibility then "ON" else "OFF"} \
        -DTREVRPC_BUILD_WEBTRANSPORT=${if legacyCompatibility then "ON" else "OFF"} \
        -DTREVRPC_BUILD_RUNTIME=${if legacyCompatibility then "ON" else "OFF"} \
        -DTREVRPC_BUILD_ENGINE_MSQUIC=${if legacyCompatibility then "OFF" else "ON"} \
        -DTREVRPC_BUILD_TRANSPORT=${if legacyCompatibility then "OFF" else "ON"} \
        -DTREVRPC_BUILD_TRANSPORT_MSQUIC=${if legacyCompatibility then "OFF" else "ON"} \
        -DTREVRPC_BUILD_RPC=${if legacyCompatibility then "OFF" else "ON"} \
        -DTREVRPC_BUILD_RPC_MSQUIC=${if legacyCompatibility then "OFF" else "ON"} \
        -DTREVRPC_ENABLE_SANITIZERS=${if sanitizers then "ON" else "OFF"} \
        -DTREVRPC_ENABLE_THREAD_SANITIZER=${if threadSanitizer then "ON" else "OFF"}
      runHook postConfigure
    '';

    nativeBuildInputs = [
      clang-tools
      cmake
      ninja
      openssl
      pkg-config
      protobuf
      protobufc
    ];
    buildInputs = [ protobufc ];
    propagatedBuildInputs = [ libmsquic ];
    buildPhase = ''
      runHook preBuild
      cmake --build build --parallel $NIX_BUILD_CORES
      runHook postBuild
    '';

    # The legacy lane is an internal build input for C++/Node migration only.
    doCheck = !legacyCompatibility && stdenv.buildPlatform.canExecute stdenv.hostPlatform;
    checkPhase = ''
      runHook preCheck
      export HOME=$TMPDIR
      find examples src tests tools -name '*.c' \
        ! -path 'tests/abi5/*' \
        ! -path 'tests/golden/*' \
        ! -path 'tests/install/*' -print0 | \
        xargs -0 -P $NIX_BUILD_CORES -I{} clang-tidy --quiet {} -- \
        -x c \
        -std=c11 \
        -DQUIC_API_ENABLE_PREVIEW_FEATURES \
        -DTREVRPC_GENERATED_TESTING \
        -DTREVRPC_H3_INGRESS_TESTING \
        -DTREVRPC_RPC_TRANSPORT_H3_TESTING \
        -DTREVRPC_MSQUIC_PROVIDER_CAPABILITIES \
        '-DTREVRPC_MSQUIC_TEST_CERT="build/msquic-test-cert.pem"' \
        '-DTREVRPC_MSQUIC_TEST_KEY="build/msquic-test-key.pem"' \
        '-DTREVRPC_GENERATED_HEADER="build/generated-service-test/greeter.trevrpc.h"' \
        '-DTREVRPC_GENERATED_SOURCE="build/generated-service-test/greeter.trevrpc.c"' \
        -Iinclude \
        -Isrc \
        -Ibuild/protoc-gen-trevrpc-c-protos \
        -Ibuild/generated-service-test \
        -Ibuild/generated-greeter-example \
        -isystem ${libmsquic}/include
      ctest --test-dir build --output-on-failure -j $NIX_BUILD_CORES \
        ${optionalString threadSanitizer "-E '^trevrpc_bench_peer_(http3_lifecycle|webtransport_smoke)$'"}
      runHook postCheck
    '';

    installPhase = ''
      runHook preInstall
      cmake --install build
      runHook postInstall
    '';

    postInstall = ''
      mkdir -p "$out"
      ${optionalString stdenv.hostPlatform.isLinux (
        concatMapStringsSep "\n" (
          peer: "install -Dm755 ${peer.package}/bin/${peer.binary} $out/bin/${peer.binary}"
        ) peerBinaries
      )}
    '';

    doInstallCheck = !legacyCompatibility && stdenv.buildPlatform.canExecute stdenv.hostPlatform;
    installCheckPhase = ''
      runHook preInstallCheck
      test -x "$out/bin/protoc-gen-trevrpc-c"
      test -x "$out/bin/trevrpc-bench-peer-c"
      ${optionalString stdenv.hostPlatform.isLinux ''
        test -x "$out/bin/trevrpc-conformance-c"
      ''}
      test ! -e "$dev/include/trevrpc.h"
      test ! -e "$dev/include/trevrpc_binding.h"
      test -f "$dev/include/trevrpc_engine.h"
      test -f "$dev/include/trevrpc_engine_msquic.h"
      test -f "$dev/include/trevrpc_transport.h"
      test -f "$dev/include/trevrpc_transport_msquic.h"
      test -f "$dev/include/trevrpc_rpc.h"
      test -f "$dev/include/trevrpc_rpc_msquic.h"
      test ! -e "$dev/include/trevrpc_msquic.h"
      test ! -e "$dev/include/trevrpc_raw.h"
      test ! -e "$dev/include/trevrpc_webtransport.h"
      test ! -e "$dev/include/trevrpc_preview.h"
      test ! -e "$dev/lib/cmake/trevrpc"
      test -f "$dev/lib/cmake/trevrpc_engine/trevrpc_engineConfig.cmake"
      test -f "$dev/lib/cmake/trevrpc_engine/trevrpcEngineTargets.cmake"
      test -f "$dev/lib/cmake/trevrpc_engine_msquic/trevrpc_engine_msquicConfig.cmake"
      test -f "$dev/lib/cmake/trevrpc_engine_msquic/trevrpcEngineMsquicTargets.cmake"
      test -f "$dev/lib/cmake/trevrpc_transport/trevrpc_transportConfig.cmake"
      test -f "$dev/lib/cmake/trevrpc_transport/trevrpcTransportTargets.cmake"
      test -f "$dev/lib/cmake/trevrpc_transport_msquic/trevrpc_transport_msquicConfig.cmake"
      test -f "$dev/lib/cmake/trevrpc_transport_msquic/trevrpcTransportMsquicTargets.cmake"
      test -f "$dev/lib/cmake/trevrpc_rpc/trevrpc_rpcConfig.cmake"
      test -f "$dev/lib/cmake/trevrpc_rpc/trevrpcRpcTargets.cmake"
      test -f "$dev/lib/cmake/trevrpc_rpc_msquic/trevrpc_rpc_msquicConfig.cmake"
      test -f "$dev/lib/cmake/trevrpc_rpc_msquic/trevrpcRpcMsquicTargets.cmake"
      test ! -e "$dev/lib/pkgconfig/trevrpc.pc"
      test ! -e "$dev/lib/pkgconfig/trevrpc_core.pc"
      test ! -e "$dev/lib/pkgconfig/trevrpc_msquic.pc"
      test ! -e "$dev/lib/pkgconfig/trevrpc_webtransport.pc"
      test -f "$dev/lib/pkgconfig/trevrpc_engine.pc"
      test -f "$dev/lib/pkgconfig/trevrpc_engine_msquic.pc"
      test -f "$dev/lib/pkgconfig/trevrpc_transport.pc"
      test -f "$dev/lib/pkgconfig/trevrpc_transport_msquic.pc"
      test -f "$dev/lib/pkgconfig/trevrpc_rpc.pc"
      test -f "$dev/lib/pkgconfig/trevrpc_rpc_msquic.pc"
      test ! -e "$lib/lib/libtrevrpc.a"
      test -f "$lib/lib/libtrevrpc_engine.a"
      test -f "$lib/lib/libtrevrpc_engine_msquic.a"
      test -f "$lib/lib/libtrevrpc_transport.a"
      test -f "$lib/lib/libtrevrpc_transport_msquic.a"
      test -f "$lib/lib/libtrevrpc_rpc.a"
      test -f "$lib/lib/libtrevrpc_rpc_msquic.a"
      test -f "$lib/lib/libtrevrpc_msquic_api_owner.a"
      test -f "$lib/lib/trevrpc/transport-abi1/libtrevrpc_transport_msquic_private.a"
      test -f "$lib/lib/trevrpc/transport-abi1/libtrevrpc_transport_private_core.a"
      test -f "$lib/lib/trevrpc/transport-abi1/libtrevrpc_transport_private_msquic_native.a"
      test -f "$lib/lib/trevrpc/transport-abi1/libtrevrpc_transport_private_protocol.a"
      test -f "$lib/lib/trevrpc/rpc-abi1/libtrevrpc_rpc_private_core.a"
      test ! -e "$lib/lib/libtrevrpc_core.a"
      test ! -e "$lib/lib/libtrevrpc_msquic_native_core.a"
      test ! -e "$lib/lib/libtrevrpc_protocol_core.a"
      ! grep -R -E 'add_library\(trevrpc::(trevrpc_core|trevrpc_protocol_core|trevrpc_msquic_native_core|trevrpc_rpc_private_[A-Za-z0-9_]+)' "$dev/lib/cmake"
      test ! -e "$out/include"
      test ! -e "$out/lib"

      test "$(pkg-config \
        --define-prefix \
        --variable=trevrpc_engine_abi_version "$dev/lib/pkgconfig/trevrpc_engine.pc")" = "1"
      test "$(pkg-config \
        --define-prefix \
        --variable=trevrpc_engine_msquic_abi_version "$dev/lib/pkgconfig/trevrpc_engine_msquic.pc")" = "1"
      test "$(pkg-config \
        --define-prefix \
        --variable=trevrpc_transport_abi_version "$dev/lib/pkgconfig/trevrpc_transport.pc")" = "1"
      test "$(pkg-config \
        --define-prefix \
        --variable=trevrpc_transport_msquic_abi_version "$dev/lib/pkgconfig/trevrpc_transport_msquic.pc")" = "1"
      test "$(pkg-config \
        --define-prefix \
        --variable=trevrpc_rpc_abi_version "$dev/lib/pkgconfig/trevrpc_rpc.pc")" = "1"
      test "$(pkg-config \
        --define-prefix \
        --variable=trevrpc_rpc_msquic_abi_version "$dev/lib/pkgconfig/trevrpc_rpc_msquic.pc")" = "1"

      consumer_sanitizer_flags="${optionalString sanitizers "-fsanitize=address,undefined -fno-omit-frame-pointer"}${optionalString threadSanitizer "-fsanitize=thread -fno-omit-frame-pointer"}"
      export CFLAGS="$consumer_sanitizer_flags''${CFLAGS:+ $CFLAGS}"
      export LDFLAGS="$consumer_sanitizer_flags''${LDFLAGS:+ $LDFLAGS}"
      cmake -S tests/install/cmake -B "$TMPDIR/trevrpc-installed-cmake" -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_PREFIX_PATH="$dev;$lib" \
        -DTREVRPC_C_GENERATOR_EXECUTABLE="$out/bin/protoc-gen-trevrpc-c"
      cmake --build "$TMPDIR/trevrpc-installed-cmake" --parallel $NIX_BUILD_CORES
      "$TMPDIR/trevrpc-installed-cmake/trevrpc-installed-engine-cmake-consumer"
      "$TMPDIR/trevrpc-installed-cmake/trevrpc-installed-engine-msquic-cmake-consumer"
      "$TMPDIR/trevrpc-installed-cmake/trevrpc-installed-engine-msquic-cmake-cpp-consumer"
      "$TMPDIR/trevrpc-installed-cmake/trevrpc-installed-transport-cmake-consumer"
      "$TMPDIR/trevrpc-installed-cmake/trevrpc-installed-transport-msquic-cmake-consumer"
      "$TMPDIR/trevrpc-installed-cmake/trevrpc-installed-rpc-cmake-consumer"
      "$TMPDIR/trevrpc-installed-cmake/trevrpc-installed-rpc-msquic-cmake-consumer"
      "$TMPDIR/trevrpc-installed-cmake/trevrpc-installed-cmake-consumer"

      export PKG_CONFIG_PATH="$dev/lib/pkgconfig''${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
      cc $consumer_sanitizer_flags \
        tests/install/pkg-config/engine_main.c \
        -o "$TMPDIR/trevrpc-installed-engine-pkg-config-consumer" \
        $(pkg-config --static --cflags --libs trevrpc_engine)
      "$TMPDIR/trevrpc-installed-engine-pkg-config-consumer"
      cc $consumer_sanitizer_flags \
        tests/install/pkg-config/engine_msquic_main.c \
        -o "$TMPDIR/trevrpc-installed-engine-msquic-pkg-config-consumer" \
        $(pkg-config --static --cflags --libs trevrpc_engine_msquic)
      "$TMPDIR/trevrpc-installed-engine-msquic-pkg-config-consumer"
      cc $consumer_sanitizer_flags \
        tests/install/pkg-config/transport_main.c \
        -o "$TMPDIR/trevrpc-installed-transport-pkg-config-consumer" \
        $(pkg-config --static --cflags --libs trevrpc_transport)
      "$TMPDIR/trevrpc-installed-transport-pkg-config-consumer"
      cc $consumer_sanitizer_flags \
        tests/install/pkg-config/transport_msquic_main.c \
        -o "$TMPDIR/trevrpc-installed-transport-msquic-pkg-config-consumer" \
        $(pkg-config --static --cflags --libs trevrpc_transport_msquic)
      "$TMPDIR/trevrpc-installed-transport-msquic-pkg-config-consumer"
      cc $consumer_sanitizer_flags \
        tests/install/pkg-config/rpc_main.c \
        -o "$TMPDIR/trevrpc-installed-rpc-pkg-config-consumer" \
        $(pkg-config --static --cflags --libs trevrpc_rpc)
      "$TMPDIR/trevrpc-installed-rpc-pkg-config-consumer"
      cc $consumer_sanitizer_flags \
        tests/install/pkg-config/rpc_msquic_main.c \
        -o "$TMPDIR/trevrpc-installed-rpc-msquic-pkg-config-consumer" \
        $(pkg-config --static --cflags --libs trevrpc_rpc_msquic)
      "$TMPDIR/trevrpc-installed-rpc-msquic-pkg-config-consumer"

      pkg_codegen="$TMPDIR/trevrpc-installed-pkg-config-codegen"
      mkdir -p "$pkg_codegen"
      protoc-c -I tests/install/codegen --c_out="$pkg_codegen" tests/install/codegen/greeter.proto
      protoc -I tests/install/codegen \
        --plugin=protoc-gen-trevrpc-c="$out/bin/protoc-gen-trevrpc-c" \
        --trevrpc-c_out="$pkg_codegen" tests/install/codegen/greeter.proto
      cc $consumer_sanitizer_flags \
        -I"$pkg_codegen" \
        tests/install/codegen/main.c \
        "$pkg_codegen/greeter.pb-c.c" \
        "$pkg_codegen/greeter.trevrpc.c" \
        -o "$TMPDIR/trevrpc-installed-pkg-config-consumer" \
        $(pkg-config --static --cflags --libs trevrpc_rpc libprotobuf-c)
      "$TMPDIR/trevrpc-installed-pkg-config-consumer"

      syntax_codegen="$TMPDIR/trevrpc-installed-direct-codegen"
      mkdir -p "$syntax_codegen"
      protoc-c -I tests/install/codegen --c_out="$syntax_codegen" tests/install/codegen/greeter.proto
      protoc -I tests/install/codegen \
        --plugin=protoc-gen-trevrpc-c="$out/bin/protoc-gen-trevrpc-c" \
        --trevrpc-c_out="$syntax_codegen" tests/install/codegen/greeter.proto
      cc -std=c11 -Wall -Wextra -Werror \
        ${optionalString stdenv.cc.isClang "-Wno-unused-command-line-argument"} \
        -fsyntax-only \
        -I"$syntax_codegen" \
        $(pkg-config --cflags trevrpc_rpc libprotobuf-c) \
        tests/install/codegen/main.c \
        "$syntax_codegen/greeter.pb-c.c" \
        "$syntax_codegen/greeter.trevrpc.c"
      runHook postInstallCheck
    '';

    passthru.msquicProvider = libmsquic;

    meta = {
      mainProgram = "protoc-gen-trevrpc-c";
      description = "C runtime and code generator for TrevRPC";
      license = licenses.mit;
      platforms = platforms.all;
      homepage = "https://trev.zip/llc/TrevRPC";
      changelog = "https://trev.zip/llc/TrevRPC/releases";
      downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/v${final.version}";
    };
  }
)
