{
  stdenv,
  lib,
  clang-tools,
  cmake,
  grpc,
  openssl,
  protobuf,
  repoRoot,
  trevrpcC,
  benchPeer ? false,
}:
stdenv.mkDerivation (
  final: with lib; {
    pname = if benchPeer then "trevrpc-cpp-bench-peer" else "trevrpc-cpp";
    version = "0.1.0";
    outputs =
      if benchPeer then
        [ "out" ]
      else
        [
          "out"
          "dev"
          "lib"
        ];

    src = fileset.toSource {
      root = repoRoot;
      fileset = fileset.unions [
        (repoRoot + "/bench/proto")
        ./.
      ];
    };
    sourceRoot = "${final.src.name}/trevrpc-cpp";

    configurePhase =
      if benchPeer then
        ''
          runHook preConfigure
          cmake -S . -B build \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_BINDIR="$out/bin" \
            -DTREVRPC_CPP_TREVRPC_C_ROOT= \
            -DTREVRPC_CPP_BUILD_BENCHMARKS=ON \
            -DTREVRPC_CPP_BUILD_CODEGEN=ON \
            -DTREVRPC_CPP_BUILD_EXAMPLES=OFF \
            -DTREVRPC_CPP_BUILD_TESTS=ON
          runHook postConfigure
        ''
      else
        ''
          runHook preConfigure
          cmake -S . -B build \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
            -DCMAKE_INSTALL_BINDIR="$out/bin" \
            -DCMAKE_INSTALL_INCLUDEDIR="$dev/include" \
            -DCMAKE_INSTALL_LIBDIR="$lib/lib" \
            -DCMAKE_INSTALL_LIBEXECDIR="$lib/libexec" \
            -DTREVRPC_CPP_INSTALL_CMAKEDIR="$dev/lib/cmake/trevrpc-cpp" \
            -DTREVRPC_CPP_TREVRPC_C_ROOT= \
            -DTREVRPC_CPP_BUILD_BENCHMARKS=OFF \
            -DTREVRPC_CPP_BUILD_TESTS=ON \
            -DTREVRPC_CPP_BUILD_EXAMPLES=ON
          runHook postConfigure
        '';

    nativeBuildInputs = [
      clang-tools
      cmake
      openssl
      protobuf
    ]
    ++ optional benchPeer grpc;
    buildInputs = optionals benchPeer [
      trevrpcC
      protobuf
      grpc
    ];
    propagatedBuildInputs = optionals (!benchPeer) [
      trevrpcC
      protobuf
    ];
    buildPhase = ''
      runHook preBuild
      cmake --build build
      runHook postBuild
    '';

    doCheck = true;
    checkPhase =
      if benchPeer then
        ''
          runHook preCheck
          ctest --test-dir build --output-on-failure
          runHook postCheck
        ''
      else
        ''
          runHook preCheck
          clang-format --dry-run --Werror $(find . \
            -path './build' -prune -o \
            \( -name '*.cpp' -o -name '*.hpp' \) -print)
          clang-tidy --quiet -p build \
            src/trevrpc.cpp \
            tools/protoc-gen-trevrpc-cpp/generator.cpp \
            tools/protoc-gen-trevrpc-cpp/main.cpp \
            tests/codec_test.cpp \
             tests/generated_service_test.cpp \
             tests/generator_options_test.cpp \
             examples/greeter/client.cpp \
            examples/greeter/server.cpp
          ctest --test-dir build --output-on-failure
          cmake --install build
          cmake -S tests/consumer -B build/consumer \
            -DCMAKE_PREFIX_PATH="$dev"
          cmake --build build/consumer
          runHook postCheck
        '';

    installPhase = ''
      runHook preInstall
      cmake --install build${optionalString benchPeer " --component benchmark-peer"}
      runHook postInstall
    '';

    doInstallCheck = !benchPeer;
    installCheckPhase = ''
      runHook preInstallCheck
      test -x "$out/bin/protoc-gen-trevrpc-cpp"
      test ! -e "$out/bin/trevrpc-bench-peer-cpp"
      test -f "$dev/include/trevrpc/trevrpc.hpp"
      test -f "$dev/lib/cmake/trevrpc-cpp/trevrpc-cppConfig.cmake"
      test -f "$lib/lib/libtrevrpc_cpp.a"
      test ! -e "$out/include"
      test ! -e "$out/lib"
      runHook postInstallCheck
    '';

    meta =
      if benchPeer then
        {
          mainProgram = "trevrpc-bench-peer-cpp";
          description = "C++ TrevRPC and gRPC benchmark peer";
          license = licenses.mit;
          platforms = platforms.linux;
        }
      else
        {
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
