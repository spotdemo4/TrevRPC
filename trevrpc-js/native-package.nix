{
  cmake,
  lib,
  libmsquic,
  nodejs_24,
  openssl,
  patchelf,
  perl,
  pkg-config,
  trevrpcCSrc,
  jsNativeSrc,
  jsNativePackageSrc,
  jsLicense,
  stdenv,
}:
stdenv.mkDerivation {
  pname = "trevrpc-js-native-linux-x64-gnu";
  version = "0.1.2";

  src = lib.fileset.toSource {
    root = ../.;
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
    mkdir -p "$out/package"
    cp build/trevrpc_native.node "$out/package/trevrpc_native.node"
    cp "${libmsquic}/lib/libmsquic.so.2" "$out/package/libmsquic.so.2"
    cp "$src/trevrpc-js/npm/native-linux-x64-gnu/package.json" "$out/package/package.json"
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

    test "$(find "$out/package" -maxdepth 1 -name '*.node' -type f | wc -l)" -eq 1
    readelf -d "$out/package/trevrpc_native.node" | grep -E '(RPATH|RUNPATH).*\$ORIGIN'
    ! grep -a -E '/nix/store|/home/|/build/' \
      "$out/package/trevrpc_native.node" "$out/package/libmsquic.so.2"

    max_glibc="$({
      readelf --version-info "$out/package/trevrpc_native.node"
      readelf --version-info "$out/package/libmsquic.so.2"
    } | grep -o 'GLIBC_[0-9][0-9.]*' | sort -Vu | tail -1)"
    test "$(printf '%s\n' "$max_glibc" 'GLIBC_2.42' | sort -V | tail -1)" = 'GLIBC_2.42'
    runHook postInstall
  '';

  dontFixup = true;

  meta = {
    description = "Portable TrevRPC native Node addon for Linux x86-64 glibc";
    license = lib.licenses.mit;
    platforms = [ "x86_64-linux" ];
  };
}
