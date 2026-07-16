{
  pkgs,
  repoRoot,
  trevrpcC,
}:
let
  package = pkgs.stdenv.mkDerivation (
    final: with pkgs.lib; {
      pname = "trevrpc-cpp";
      version = "0.1.0";
      outputs = [
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

      configurePhase = ''
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

      nativeBuildInputs = with pkgs; [
        clang-tools
        cmake
        openssl
        protobuf
      ];
      propagatedBuildInputs = [
        trevrpcC
        pkgs.protobuf
      ];
      buildPhase = ''
        runHook preBuild
        cmake --build build
        runHook postBuild
      '';

      doCheck = true;
      checkPhase = ''
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
        cmake --install build
        runHook postInstall
      '';

      doInstallCheck = true;
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
  );

  benchPeer = package.overrideAttrs (
    old: with pkgs.lib; {
      pname = "trevrpc-cpp-bench-peer";
      outputs = [ "out" ];
      doInstallCheck = false;

      configurePhase = ''
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
      '';
      nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ pkgs.grpc ];
      buildInputs = (old.propagatedBuildInputs or [ ]) ++ [ pkgs.grpc ];
      propagatedBuildInputs = [ ];

      doCheck = true;
      checkPhase = ''
        runHook preCheck
        ctest --test-dir build --output-on-failure
        runHook postCheck
      '';
      installPhase = ''
        runHook preInstall
        cmake --install build --component benchmark-peer
        runHook postInstall
      '';

      meta = {
        mainProgram = "trevrpc-bench-peer-cpp";
        description = "C++ TrevRPC and gRPC benchmark peer";
        license = licenses.mit;
        platforms = platforms.linux;
      };
    }
  );
in
{
  inherit package benchPeer;
}
