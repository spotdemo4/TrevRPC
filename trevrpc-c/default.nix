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
  threadSanitizer ? false,
}:
assert !(sanitizers && threadSanitizer);
stdenv.mkDerivation (
  final: with lib; {
    pname = if benchPeer then "trevrpc-c-bench-peer" else "trevrpc-c";
    version = "0.2.0";
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
            -DTREVRPC_ENABLE_SANITIZERS=${if sanitizers then "ON" else "OFF"} \
            -DTREVRPC_ENABLE_THREAD_SANITIZER=${if threadSanitizer then "ON" else "OFF"}
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
            -DTREVRPC_ENABLE_SANITIZERS=${if sanitizers then "ON" else "OFF"} \
            -DTREVRPC_ENABLE_THREAD_SANITIZER=${if threadSanitizer then "ON" else "OFF"}
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
          ctest --test-dir build --output-on-failure --no-tests=error -R '^trevrpc_bench_peer_'
          runHook postCheck
        ''
      else
        ''
          runHook preCheck
          export HOME=$TMPDIR
          clang-format --dry-run --Werror $(find bench examples include src tests tools \( -name '*.c' -o -name '*.h' -o -name '*.cpp' \))
          clang-tidy --quiet $(find examples src tests tools -name '*.c' \
            ! -path 'tests/abi5/*' \
            ! -path 'tests/golden/*' \
            ! -path 'tests/install/*') -- \
            -x c \
            -std=c11 \
            -DQUIC_API_ENABLE_PREVIEW_FEATURES \
            -DTREVRPC_GENERATED_TESTING \
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
      test ! -e "$dev/include/trevrpc_preview.h"
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
      test "$(pkg-config \
        --define-prefix \
        --variable=trevrpc_c_abi_version "$dev/lib/pkgconfig/trevrpc.pc")" = "6"

      consumer_sanitizer_flags="${optionalString sanitizers "-fsanitize=address,undefined -fno-omit-frame-pointer"}${optionalString threadSanitizer "-fsanitize=thread -fno-omit-frame-pointer"}"
      export CFLAGS="$consumer_sanitizer_flags''${CFLAGS:+ $CFLAGS}"
      export LDFLAGS="$consumer_sanitizer_flags''${LDFLAGS:+ $LDFLAGS}"
      cmake -S tests/install/cmake -B "$TMPDIR/trevrpc-installed-cmake" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_PREFIX_PATH="$dev;$lib" \
        -DTREVRPC_C_GENERATOR_EXECUTABLE="$out/bin/protoc-gen-trevrpc-c"
      cmake --build "$TMPDIR/trevrpc-installed-cmake"
      "$TMPDIR/trevrpc-installed-cmake/trevrpc-installed-cmake-consumer"

      export PKG_CONFIG_PATH="$dev/lib/pkgconfig''${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
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
        $(pkg-config --static --cflags --libs trevrpc libprotobuf-c)
      "$TMPDIR/trevrpc-installed-pkg-config-consumer"

      syntax_codegen="$TMPDIR/trevrpc-installed-direct-codegen"
      mkdir -p "$syntax_codegen"
      protoc-c -I tests/install/codegen --c_out="$syntax_codegen" tests/install/codegen/greeter.proto
      protoc -I tests/install/codegen \
        --plugin=protoc-gen-trevrpc-c="$out/bin/protoc-gen-trevrpc-c" \
        --trevrpc-c_out="$syntax_codegen" tests/install/codegen/greeter.proto
      cc -std=c11 -Wall -Wextra -Werror -fsyntax-only \
        -I"$syntax_codegen" \
        $(pkg-config --cflags trevrpc libprotobuf-c) \
        tests/install/codegen/main.c \
        "$syntax_codegen/greeter.pb-c.c" \
        "$syntax_codegen/greeter.trevrpc.c"
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
