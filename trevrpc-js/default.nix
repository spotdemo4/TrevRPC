{
  lib,
  buildNpmPackage,
  importNpmLock,
  nodejs_24,
  cmake,
  clang-tools,
  openssl,
  oxfmt,
  oxlint,
  protobuf,
  gnugrep,
  makeWrapper,
  benchProto,
  wireGolden,
  trevrpcC,
  nativePackage ? null,
}:
buildNpmPackage (final: {
  pname = "trevrpc-js";
  version = "0.1.1";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      benchProto
      wireGolden
      ./.
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-js";
  nodejs = nodejs_24;

  npmConfigHook = importNpmLock.npmConfigHook;
  npmDeps = importNpmLock {
    npmRoot = ./.;
  };

  npmBuildScript = "build:native";
  dontUseCmakeConfigure = true;
  NODE_INCLUDE_DIR = "${nodejs_24}/include/node";
  PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = "1";

  nativeBuildInputs = [
    cmake
    makeWrapper
  ];
  buildInputs = [
    trevrpcC
  ];

  doCheck = true;
  nativeCheckInputs = [
    clang-tools
    openssl
    oxfmt
    oxlint
    protobuf
  ];
  checkPhase = ''
    runHook preCheck
    rm -rf build
    patchShebangs bin/protoc-gen-trevrpc-js.js
    oxfmt --check
    oxlint --deny-warnings
    npm run typecheck
    npm run build:native:test
    clang-format --dry-run --Werror native/trevrpc_node.c
    clang-tidy -p build/native native/trevrpc_node.c
    npm test
    rm -rf bench/node_modules
    mkdir -p bench/node_modules/@trevrpc
    ln -s "$PWD" bench/node_modules/@trevrpc/trevrpc-js
    npm --prefix bench test
    rm -rf bench/node_modules
    npm run build:native
    npm run verify:native:production
    runHook postCheck
  '';

  postInstall = ''
    native_dir="$out/lib/node_modules/@trevrpc/trevrpc-js/node_modules/@trevrpc/trevrpc-js-native-linux-x64-gnu"
    ${lib.optionalString (nativePackage != null) ''
      mkdir -p "$(dirname "$native_dir")"
      cp -R "${nativePackage}/package" "$native_dir"
    ''}

    mkdir -p "$out/lib/node_modules/trevrpc-bench-peer-js/node_modules/@trevrpc"
    cp bench/package.json bench/common.js bench/trevrpc-bench-peer.js \
      "$out/lib/node_modules/trevrpc-bench-peer-js/"
    ln -s "$out/lib/node_modules/@trevrpc/trevrpc-js" \
      "$out/lib/node_modules/trevrpc-bench-peer-js/node_modules/@trevrpc/trevrpc-js"
    makeWrapper ${nodejs_24}/bin/node "$out/bin/trevrpc-bench-peer-js" \
      --add-flags "$out/lib/node_modules/trevrpc-bench-peer-js/trevrpc-bench-peer.js"

    cp -R conformance "$out/lib/node_modules/@trevrpc/trevrpc-js/conformance"
    makeWrapper ${nodejs_24}/bin/node "$out/bin/trevrpc-conformance-js" \
      --add-flags "--no-addons" \
      --add-flags "$out/lib/node_modules/@trevrpc/trevrpc-js/conformance/trevrpc-conformance-js.js"
  '';

  doInstallCheck = true;
  nativeInstallCheckInputs = [ gnugrep ];
  installCheckPhase = ''
    runHook preInstallCheck
    ! grep -q '@grpc/' package.json
    ${nodejs_24}/bin/node --input-type=module -e \
      'import(process.argv[1]).then((module) => module.loadNativeAddon())' \
      "$out/lib/node_modules/@trevrpc/trevrpc-js/src/native-loader.js"
    test -x "$out/bin/trevrpc-bench-peer-js"
    test -x "$out/bin/trevrpc-conformance-js"
    printf 'STOP\n' | "$out/bin/trevrpc-conformance-js" --protocol 1 > peer.out
    grep -q '"event":"ready"' peer.out
    grep -q '"peer":"js"' peer.out
    runHook postInstallCheck
  '';

  meta = {
    mainProgram = "protoc-gen-trevrpc-js";
    description = "JavaScript WebTransport runtime and protobuf.js code generator for TrevRPC";
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
    badPlatforms = [ lib.systems.inspect.platformPatterns.isStatic ];
    homepage = "https://trev.zip/llc/TrevRPC";
    changelog = "https://trev.zip/llc/TrevRPC/releases";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/v${final.version}";
  };
})
