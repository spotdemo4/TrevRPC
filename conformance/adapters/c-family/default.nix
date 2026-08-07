{
  stdenv,
  lib,
  cmake,
  ninja,
  libmsquic,
  protobuf,
  python3,
  trevrpcCSrc,
  trevrpcCppSrc,
  sanitizers ? false,
}:
stdenv.mkDerivation (final: {
  pname = "trevrpc-c-family-conformance-peers";
  version = "0.1.6";

  src = lib.fileset.toSource {
    root = ../../../.;
    fileset = lib.fileset.unions [
      ./.
      trevrpcCSrc
      trevrpcCppSrc
    ];
  };
  sourceRoot = "${final.src.name}/conformance/adapters/c-family";

  nativeBuildInputs = [
    cmake
    ninja
    protobuf
    python3
  ];
  buildInputs = [
    libmsquic
    protobuf
  ];

  configurePhase = ''
    runHook preConfigure
    cmake -S . -B build -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX="$out" \
      -DCMAKE_INSTALL_BINDIR=bin \
      -DTREVRPC_C_FAMILY_ENABLE_SANITIZERS=${if sanitizers then "ON" else "OFF"}
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
    ctest --test-dir build --output-on-failure -j $NIX_BUILD_CORES
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
