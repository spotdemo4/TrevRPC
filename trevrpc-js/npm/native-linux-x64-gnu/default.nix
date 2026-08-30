{
  binutils,
  cmake,
  lib,
  libmsquic,
  nodejs_24,
  openssl,
  packageManifestWriter,
  patchelf,
  perl,
  pkg-config,
  publicationVerifier,
  sourceTree,
  trevrpcCSrc,
  jsNativeSrc,
  jsNativePackageSrc,
  jsLicense,
  jsPackage,
  stdenv,
}:
stdenv.mkDerivation (final: {
  pname = "trevrpc-js-native-linux-x64-gnu";
  inherit (jsPackage) version;
  outputs = [
    "out"
    "npm"
  ];

  src = lib.fileset.toSource {
    root = sourceTree;
    fileset = lib.fileset.unions [
      trevrpcCSrc
      jsNativeSrc
      jsNativePackageSrc
      jsLicense
    ];
  };

  sourceRoot = ".";
  strictDeps = true;
  nativeBuildInputs = [
    cmake
    nodejs_24
    patchelf
    perl
    pkg-config
  ];
  buildInputs = [
    libmsquic
    openssl
  ];

  configurePhase = ''
    runHook preConfigure
    cmake \
      -S "$src/trevrpc-js/native" \
      -B build \
      -DCMAKE_BUILD_TYPE=Release \
      -DTREVRPC_NODE_TEST_HOOKS=OFF \
      -DTREVRPC_C_ROOT="$src/trevrpc-c" \
      -DNODE_INCLUDE_DIR="${nodejs_24}/include/node"
    runHook postConfigure
  '';

  buildPhase = ''
    runHook preBuild
    cmake --build build --parallel "$NIX_BUILD_CORES"
    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall
    export HOME="$TMPDIR/home"
    mkdir -p "$HOME" "$out/package" "$npm"
    cp build/trevrpc_native.node "$out/package/trevrpc_native.node"
    cp "${libmsquic}/lib/libmsquic.so.2" "$out/package/libmsquic.so.2"
    cp "$src/trevrpc-js/npm/native-linux-x64-gnu/package.template.json" \
      "$out/package/package.json"
    chmod u+w "$out/package/package.json"
    node ${packageManifestWriter} native "$out/package/package.json" "${final.version}"
    cp "$src/trevrpc-js/npm/native-linux-x64-gnu/README.md" "$out/package/README.md"
    cp "$src/trevrpc-js/LICENSE" "$out/package/LICENSE"
    cp "$src/trevrpc-js/npm/native-linux-x64-gnu/THIRD_PARTY_NOTICES.md" \
      "$out/package/THIRD_PARTY_NOTICES.md"
    chmod u+w "$out/package/trevrpc_native.node" "$out/package/libmsquic.so.2"

    patchelf --set-rpath '$ORIGIN' "$out/package/trevrpc_native.node"
    patchelf --remove-rpath "$out/package/libmsquic.so.2"
    strip --strip-unneeded "$out/package/trevrpc_native.node"
    strip --strip-unneeded "$out/package/libmsquic.so.2"
    # MsQuic keeps inactive runpath bytes and compile-time source paths in release
    # strings. Rewrite each fixed-width prefix so packed binaries disclose no host.
    perl -0pi -e \
      's{/nix/store}{/trev/path}g; s{/home/}{/trev/}g; s{/build/}{/trevr/}g' \
      "$out/package/trevrpc_native.node" "$out/package/libmsquic.so.2"

    native_name="$(npm pack "$out/package" --pack-destination "$npm")"
    test "$native_name" = "trevrpc-trevrpc-js-native-linux-x64-gnu-${final.version}.tgz"
    test -f "$npm/$native_name"
    runHook postInstall
  '';

  dontFixup = true;

  doInstallCheck = true;
  nativeInstallCheckInputs = [ binutils ];
  installCheckPhase = ''
    runHook preInstallCheck
    test "$(find "$npm" -maxdepth 1 -type f | wc -l)" -eq 1
    node ${publicationVerifier} native \
      "$npm/trevrpc-trevrpc-js-native-linux-x64-gnu-${final.version}.tgz" \
      "${final.version}"
    runHook postInstallCheck
  '';

  meta = {
    description = "Portable TrevRPC native Node addon for Linux x86-64 glibc";
    license = lib.licenses.mit;
    platforms = [ "x86_64-linux" ];
  };
})
