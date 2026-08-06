{
  stdenv,
  lib,
  clang-tools,
  cmake,
  ninja,
  openssl,
  protobuf,
  benchProto,
  trevrpcC,
  peerBinaries ? [ ],
  sanitizers ? false,
}:
stdenv.mkDerivation (
  final: with lib; {
    pname = "trevrpc-cpp";
    version = "0.1.1";
    outputs = [
      "out"
      "dev"
      "lib"
    ];

    src = fileset.toSource {
      root = ../.;
      fileset = fileset.unions [
        benchProto
        ./.
      ];
    };
    sourceRoot = "${final.src.name}/trevrpc-cpp";

    configurePhase = ''
      runHook preConfigure
      cmake -S . -B build -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        -DCMAKE_INSTALL_BINDIR="$out/bin" \
        -DCMAKE_INSTALL_INCLUDEDIR="$dev/include" \
        -DCMAKE_INSTALL_LIBDIR="$lib/lib" \
        -DCMAKE_INSTALL_LIBEXECDIR="$lib/libexec" \
        -DTREVRPC_CPP_INSTALL_CMAKEDIR="$dev/lib/cmake/trevrpc-cpp" \
        -DTREVRPC_CPP_TREVRPC_C_ROOT= \
        -DTREVRPC_CPP_BUILD_BENCHMARKS=ON \
        -DTREVRPC_CPP_BUILD_CODEGEN=ON \
        -DTREVRPC_CPP_BUILD_EXAMPLES=ON \
        -DTREVRPC_CPP_BUILD_TESTS=ON \
        -DTREVRPC_CPP_ENABLE_SANITIZERS=${if sanitizers then "ON" else "OFF"}
      runHook postConfigure
    '';

    nativeBuildInputs = [
      clang-tools
      cmake
      ninja
      openssl
      protobuf
    ];
    buildInputs = [
      trevrpcC
      protobuf
    ];
    propagatedBuildInputs = [
      trevrpcC
      protobuf
    ];
    buildPhase = ''
      runHook preBuild
      cmake --build build --parallel $NIX_BUILD_CORES
      runHook postBuild
    '';

    doCheck = true;
    checkPhase = ''
      runHook preCheck
      ${optionalString (!sanitizers) ''
        printf '%s\0' \
          src/trevrpc.cpp \
          src/async.cpp \
          src/callbacks.cpp \
          src/detail/abi6_bridge.cpp \
          src/detail/async_core.cpp \
          src/detail/lifecycle.cpp \
          tools/protoc-gen-trevrpc-cpp/generator.cpp \
          tools/protoc-gen-trevrpc-cpp/main.cpp \
          tests/codec_test.cpp \
          tests/generated_service_test.cpp \
          tests/generated_async_service_test.cpp \
          tests/generator_options_test.cpp \
          tests/async_core_test.cpp \
          tests/async_stream_test.cpp \
          tests/async_server_core_test.cpp \
          tests/callbacks_test.cpp \
          tests/server_lifecycle_test.cpp \
          examples/greeter/client.cpp \
          examples/greeter/server.cpp | xargs -0 -P $NIX_BUILD_CORES -n1 clang-tidy --quiet -p build
      ''}
      ctest --test-dir build --output-on-failure -j $NIX_BUILD_CORES
      ${optionalString (!sanitizers) ''
        cmake --install build
        cmake -S tests/consumer -B build/consumer -G Ninja \
          -DCMAKE_PREFIX_PATH="$dev"
        cmake --build build/consumer --parallel $NIX_BUILD_CORES
        ctest --test-dir build/consumer --output-on-failure -j $NIX_BUILD_CORES
      ''}
      runHook postCheck
    '';

    installPhase = ''
      runHook preInstall
      cmake --install build
      runHook postInstall
    '';

    postInstall = concatMapStringsSep "\n" (
      peer: "install -Dm755 ${peer.package}/bin/${peer.binary} $out/bin/${peer.binary}"
    ) peerBinaries;

    doInstallCheck = true;
    installCheckPhase = ''
      runHook preInstallCheck
      test -x "$out/bin/protoc-gen-trevrpc-cpp"
      test -x "$out/bin/trevrpc-bench-peer-cpp"
      test -x "$out/bin/trevrpc-conformance-cpp"
      test -f "$dev/include/trevrpc/trevrpc.hpp"
      test -f "$dev/include/trevrpc/async.hpp"
      test -f "$dev/include/trevrpc/callbacks.hpp"
      test -f "$dev/lib/cmake/trevrpc-cpp/trevrpc-cppConfig.cmake"
      test -f "$lib/lib/libtrevrpc_cpp.a"
      test ! -e "$out/include"
      test ! -e "$out/lib"
      runHook postInstallCheck
    '';

    meta = {
      mainProgram = "protoc-gen-trevrpc-cpp";
      description = "C++20 runtime and protobuf code generator for TrevRPC";
      license = licenses.mit;
      platforms = platforms.linux;
      homepage = "https://trev.zip/llc/TrevRPC";
      changelog = "https://trev.zip/llc/TrevRPC/releases";
      downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/v${final.version}";
    };
  }
)
