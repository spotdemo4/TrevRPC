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
  gnugrep,
  repoRoot,
  trevrpcC,
  benchPeer ? false,
}:
let
  normal = buildNpmPackage (final: {
    pname = "trevrpc-js";
    version = "0.1.0";

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
    ];
    checkPhase = ''
      rm -rf build
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

    doInstallCheck = true;
    nativeInstallCheckInputs = [ gnugrep ];
    installCheckPhase = ''
      runHook preInstallCheck
      ! grep -q '@grpc/' package.json
      ${nodejs_24}/bin/node -e \
        'require(process.argv[1])' \
        "$out/lib/node_modules/trevrpc-js/build/native/trevrpc_native.node"
      test ! -e "$out/bin/trevrpc-bench-peer-js"
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
in
if benchPeer then peer else normal
