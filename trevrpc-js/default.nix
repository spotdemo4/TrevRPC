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
  repoRoot,
  trevrpcC,
  nativePackage ? null,
  benchPeer ? false,
  conformancePeer ? false,
}:
assert lib.assertMsg (
  !(benchPeer && conformancePeer)
) "trevrpc-js benchPeer and conformancePeer are mutually exclusive";
let
  normal = buildNpmPackage (final: {
    pname = "trevrpc-js";
    version = "0.2.0";

    src = lib.fileset.toSource {
      root = repoRoot;
      fileset = lib.fileset.unions [
        (repoRoot + "/testdata/wire-golden-vectors.txt")
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
      rm -rf build
      patchShebangs bin/protoc-gen-trevrpc-js.js
      oxfmt --check
      oxlint --deny-warnings
      npm run typecheck
      npm run build:native:test
      clang-format --dry-run --Werror native/trevrpc_node.c
      clang-tidy -p build/native native/trevrpc_node.c
      npm test
      npm run build:native
      npm run verify:native:production
    '';

    postInstall = lib.optionalString (nativePackage != null) ''
      native_dir="$out/lib/node_modules/trevrpc-js/node_modules/@trev/trevrpc-js-native-linux-x64-gnu"
      mkdir -p "$(dirname "$native_dir")"
      cp -R "${nativePackage}/package" "$native_dir"
    '';

    doInstallCheck = true;
    nativeInstallCheckInputs = [ gnugrep ];
    installCheckPhase = ''
      runHook preInstallCheck
      ! grep -q '@grpc/' package.json
      ${nodejs_24}/bin/node --input-type=module -e \
        'import(process.argv[1]).then((module) => module.loadNativeAddon())' \
        "$out/lib/node_modules/trevrpc-js/src/native-loader.js"
      test ! -e "$out/bin/trevrpc-bench-peer-js"
      test ! -e "$out/bin/trevrpc-conformance-js"
      runHook postInstallCheck
    '';

    meta = {
      mainProgram = "protoc-gen-trevrpc-js";
      description = "JavaScript WebTransport runtime and code generator for TrevRPC";
      license = lib.licenses.mit;
      platforms = lib.platforms.all;
      badPlatforms = [ lib.systems.inspect.platformPatterns.isStatic ];
      homepage = "https://trev.zip/llc/TrevRPC";
      changelog = "https://trev.zip/llc/TrevRPC/releases";
      downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/v${final.version}";
    };
  });

  peer = buildNpmPackage (final: {
    pname = "trevrpc-js-bench-peer";
    version = "0.1.0";

    src = lib.fileset.toSource {
      root = repoRoot;
      fileset = lib.fileset.unions [
        (repoRoot + "/bench/proto")
        ./.
      ];
    };
    sourceRoot = "${final.src.name}/trevrpc-js/bench";
    nodejs = nodejs_24;

    npmConfigHook = importNpmLock.npmConfigHook;
    npmDeps = importNpmLock {
      npmRoot = ./bench;
    };

    dontNpmBuild = true;
    preBuild = ''
      ln -s ${normal}/lib/node_modules/trevrpc-js \
        node_modules/trevrpc-js
    '';

    doCheck = true;
    nativeCheckInputs = [ openssl ];
    checkPhase = ''
      runHook preCheck
      npm test
      runHook postCheck
    '';
    postInstall = ''
      mkdir -p $out/lib/node_modules/trevrpc-bench-peer-js/node_modules
      ln -s ${normal}/lib/node_modules/trevrpc-js \
        $out/lib/node_modules/trevrpc-bench-peer-js/node_modules/trevrpc-js
    '';

    meta = {
      mainProgram = "trevrpc-bench-peer-js";
      description = "JavaScript TrevRPC and gRPC benchmark peer";
      license = lib.licenses.mit;
      platforms = lib.platforms.linux;
    };
  });

  conformance = buildNpmPackage (final: {
    pname = "trevrpc-js-conformance-peer";
    version = "0.1.0";

    src = lib.fileset.toSource {
      root = repoRoot;
      fileset = ./.;
    };
    sourceRoot = "${final.src.name}/trevrpc-js";
    nodejs = nodejs_24;

    npmConfigHook = importNpmLock.npmConfigHook;
    npmDeps = importNpmLock {
      npmRoot = ./.;
    };

    dontNpmBuild = true;

    doCheck = true;
    nativeCheckInputs = [
      oxfmt
      oxlint
    ];
    checkPhase = ''
      runHook preCheck
      oxfmt --check
      oxlint --deny-warnings
      npm run typecheck
      node --no-addons --test test/conformance.test.js
      node --no-addons --test test/conformance-process.test.js
      runHook postCheck
    '';

    nativeBuildInputs = [ makeWrapper ];
    postInstall = ''
      rm -rf $out/bin
      cp -r conformance $out/lib/node_modules/trevrpc-js/conformance
      mkdir -p $out/bin
      makeWrapper ${nodejs_24}/bin/node $out/bin/trevrpc-conformance-js \
        --add-flags "--no-addons" \
        --add-flags "$out/lib/node_modules/trevrpc-js/conformance/trevrpc-conformance-js.js"
    '';

    doInstallCheck = true;
    nativeInstallCheckInputs = [ gnugrep ];
    installCheckPhase = ''
      runHook preInstallCheck
      test "$(find $out/bin -maxdepth 1 -type f | wc -l)" -eq 1
      printf 'STOP\n' | $out/bin/trevrpc-conformance-js --protocol 1 > peer.out
      grep -q '"event":"ready"' peer.out
      grep -q '"peer":"js"' peer.out
      runHook postInstallCheck
    '';

    meta = {
      mainProgram = "trevrpc-conformance-js";
      description = "JavaScript TrevRPC conformance process peer";
      license = lib.licenses.mit;
      platforms = lib.platforms.linux;
    };
  });
in
if conformancePeer then
  conformance
else if benchPeer then
  peer
else
  normal
