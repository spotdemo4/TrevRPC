{
  stdenv,
  lib,
  clang-tools,
  cmake,
  libmsquic,
  protobuf,
  python3,
  repoRoot,
  sanitizers ? false,
}:
stdenv.mkDerivation (final: {
  pname = "trevrpc-c-family-conformance-peers";
  version = "0.1.0";

  src = lib.fileset.toSource {
    root = repoRoot;
    fileset = lib.fileset.unions [
      ./.
      (repoRoot + "/trevrpc-c")
      (repoRoot + "/trevrpc-cpp")
    ];
  };
  sourceRoot = "${final.src.name}/conformance/adapters/c-family";

  nativeBuildInputs = [
    clang-tools
    cmake
    protobuf
    python3
  ];
  buildInputs = [
    libmsquic
    protobuf
  ];

  configurePhase = ''
    runHook preConfigure
    cmake -S . -B build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX="$out" \
      -DCMAKE_INSTALL_BINDIR=bin \
      -DTREVRPC_C_FAMILY_ENABLE_SANITIZERS=${if sanitizers then "ON" else "OFF"}
    runHook postConfigure
  '';

  buildPhase = ''
    runHook preBuild
    cmake --build build
    runHook postBuild
  '';

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    clang-format --dry-run --Werror $(find . -path './build' -prune -o \
      \( -name '*.c' -o -name '*.h' -o -name '*.cpp' \) -print)
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
    test -x "$out/bin/trevrpc-conformance-c"
    test -x "$out/bin/trevrpc-conformance-cpp"
    runHook postInstallCheck
  '';

  meta = {
    description = "C and C++ TrevRPC conformance process peers";
    license = lib.licenses.mit;
    platforms = lib.platforms.linux;
  };
})
