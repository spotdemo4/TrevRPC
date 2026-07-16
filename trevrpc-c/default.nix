{
  stdenv,
  lib,
  clang-tools,
  cmake,
  grpc,
  libmsquic,
  openssl,
  pkg-config,
  protobuf,
  protobufc,
  repoRoot,
  benchPeer ? false,
  sanitizers ? false,
}:
stdenv.mkDerivation (
  final: with lib; {
    pname = if benchPeer then "trevrpc-c-bench-peer" else "trevrpc-c";
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
        (repoRoot + "/testdata/wire-golden-vectors.txt")
        ./.
      ];
    };
    sourceRoot = "${final.src.name}/trevrpc-c";

    configurePhase =
      if benchPeer then
        ''
          runHook preConfigure
          cmake -S . -B build \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_BINDIR="$out/bin" \
            -DTREVRPC_BUILD_BENCHMARKS=ON \
            -DTREVRPC_BUILD_TESTS=ON \
            -DTREVRPC_ENABLE_SANITIZERS=${if sanitizers then "ON" else "OFF"}
          runHook postConfigure
        ''
      else
        ''
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
            -DTREVRPC_BUILD_TESTS=ON \
            -DTREVRPC_ENABLE_SANITIZERS=${if sanitizers then "ON" else "OFF"}
          runHook postConfigure
        '';

    nativeBuildInputs = [
      clang-tools
      cmake
      openssl
      pkg-config
      protobuf
      protobufc
    ];
    buildInputs = [ protobufc ] ++ optional benchPeer grpc;
    propagatedBuildInputs = [ libmsquic ];
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
            -isystem ${libmsquic}/include
          ctest --test-dir build --output-on-failure
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

    meta =
      if benchPeer then
        {
          mainProgram = "trevrpc-bench-peer-c";
          description = "C TrevRPC and gRPC benchmark peer";
          license = licenses.mit;
          platforms = platforms.linux;
        }
      else
        {
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
