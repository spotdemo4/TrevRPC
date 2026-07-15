{
  pkgs,
  repoRoot,
}:
let
  package = pkgs.stdenv.mkDerivation (
    final: with pkgs.lib; {
      pname = "trevrpc-c";
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
          (repoRoot + "/testdata/wire-golden-vectors.txt")
          ./.
        ];
      };
      sourceRoot = "${final.src.name}/trevrpc-c";

      configurePhase = ''
        runHook preConfigure
        cmake -S . -B build \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_INSTALL_BINDIR="$out/bin" \
          -DCMAKE_INSTALL_INCLUDEDIR="$dev/include" \
          -DCMAKE_INSTALL_LIBDIR="$lib/lib" \
          -DCMAKE_INSTALL_LIBEXECDIR="$lib/libexec" \
          -DTREVRPC_INSTALL_CMAKEDIR="$dev/lib/cmake/trevrpc" \
          -DTREVRPC_INSTALL_PKGCONFIGDIR="$dev/lib/pkgconfig" \
          -DTREVRPC_BUILD_BENCHMARKS=OFF \
          -DTREVRPC_BUILD_TESTS=ON
        runHook postConfigure
      '';

      nativeBuildInputs = with pkgs; [
        clang-tools
        cmake
        openssl
        pkg-config
        protobuf
        protobufc
      ];
      buildInputs = with pkgs; [
        protobufc
      ];
      propagatedBuildInputs = with pkgs; [
        libmsquic
      ];
      buildPhase = ''
        runHook preBuild
        cmake --build build
        runHook postBuild
      '';

      doCheck = false;
      checkPhase = ''
        runHook preCheck
        export HOME=$TMPDIR
        clang-format --dry-run --Werror $(find bench examples include src tests tools \( -name '*.c' -o -name '*.h' \))
        clang-tidy --quiet $(find examples src tests tools -name '*.c') -- \
          -x c \
          -std=c11 \
          -Iinclude \
          -Isrc \
          -Ibuild/protoc-gen-trevrpc-c-protos \
          -Ibuild/generated-service-test \
          -Ibuild/generated-greeter-example \
          -isystem ${pkgs.libmsquic}/include
        ctest --test-dir build --output-on-failure
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
        test -x "$out/bin/protoc-gen-trevrpc-c"
        test ! -e "$out/bin/trevrpc-bench-peer-c"
        test -f "$dev/include/trevrpc_binding.h"
        test -f "$dev/lib/cmake/trevrpc/trevrpcConfig.cmake"
        test -f "$dev/lib/pkgconfig/trevrpc.pc"
        test -f "$lib/lib/libtrevrpc.a"
        test ! -e "$out/include"
        test ! -e "$out/lib"

        test "$(pkg-config \
          --define-prefix \
          --variable=libdir "$dev/lib/pkgconfig/trevrpc.pc")" = "$lib/lib"
        test "$(pkg-config \
          --define-prefix \
          --variable=includedir "$dev/lib/pkgconfig/trevrpc.pc")" = "$dev/include"
        runHook postInstallCheck
      '';

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
  );

  benchPeer = package.overrideAttrs (
    old: with pkgs.lib; {
      pname = "trevrpc-c-bench-peer";
      outputs = [ "out" ];
      doInstallCheck = false;

      configurePhase = ''
        runHook preConfigure
        cmake -S . -B build \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_INSTALL_BINDIR="$out/bin" \
          -DTREVRPC_BUILD_BENCHMARKS=ON \
          -DTREVRPC_BUILD_TESTS=ON
        runHook postConfigure
      '';
      buildInputs = (old.buildInputs or [ ]) ++ [ pkgs.grpc ];

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
        mainProgram = "trevrpc-bench-peer-c";
        description = "C TrevRPC and gRPC benchmark peer";
        license = licenses.mit;
        platforms = platforms.linux;
      };
    }
  );
in
{
  inherit package benchPeer;
}
