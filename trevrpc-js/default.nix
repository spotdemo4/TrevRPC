{
  lib,
  stdenv,
  callPackage,
  runCommand,
  buildNpmPackage,
  importNpmLock,
  nodejs_26,
  makeWrapper,
  protobuf,
  gnugrep,
  playwright-driver,
  wireGolden,
  qmuxGolden,
  zstdGolden,
  interopProto,
  trevrpcProto,
}:
let
  jsPackage = builtins.fromJSON (builtins.readFile ./package.json);
  packageLock = lib.importJSON ./package-lock.json;
  playwrightManifestVersion = jsPackage.devDependencies.playwright;
  playwrightVersion = packageLock.packages."node_modules/playwright".version;
  playwrightCoreVersion = packageLock.packages."node_modules/playwright-core".version;
  expectedPlaywrightVersion = playwright-driver.version;
  # The libzstd WebAssembly module that the zstd entry point embeds.
  zstdWasm = callPackage ./zstd-wasm { };
  # protoc-gen-trevrpc-js, which this package provides like the Go, Rust and Kotlin packages provide
  # their plugins. It is an npm package of its own, and flake-release publishes one package.json per
  # flake attribute, so the release publishes it from `.#trevrpc-js.protocGen`.
  protocGen = callPackage ./protoc-gen-trevrpc-js { inherit interopProto trevrpcProto; };

  # The interop descriptors are generated from the shared interop protos, which import the TrevRPC
  # schema options, and the wire, QMux and compression tests read the shared golden vectors; all
  # of them live outside this directory, as does the root oxlint config that ours extends. The wasm
  # module is built on its own; only the script that embeds it is needed here. The Effect adapter
  # in effect/ and the protovalidate adapter in protovalidate/ are packages of their own. The source
  # of protoc-gen-trevrpc-js/ is kept: the build runs it to generate the codecs it tests.
  sourceTree = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../.oxlintrc.json
      wireGolden
      qmuxGolden
      zstdGolden
      interopProto
      trevrpcProto
      (lib.fileset.difference ./. (
        lib.fileset.unions [
          ./zstd-wasm
          ./effect
          ./protovalidate
          ./protoc-gen-trevrpc-js/default.nix
          ./release-check.nix
        ]
      ))
      ./zstd-wasm/embed.js
    ];
  };
in
assert lib.assertMsg (playwrightManifestVersion == expectedPlaywrightVersion) ''
  trevrpc-js playwright manifest ${playwrightManifestVersion} must exactly match
  nixpkgs playwright-driver ${expectedPlaywrightVersion}
'';
assert lib.assertMsg (playwrightVersion == expectedPlaywrightVersion) ''
  trevrpc-js locked playwright ${playwrightVersion} must match
  nixpkgs playwright-driver ${expectedPlaywrightVersion}
'';
assert lib.assertMsg (playwrightCoreVersion == expectedPlaywrightVersion) ''
  trevrpc-js locked playwright-core ${playwrightCoreVersion} must match
  nixpkgs playwright-driver ${expectedPlaywrightVersion}
'';
buildNpmPackage (final: {
  pname = "trevrpc-js";
  inherit (jsPackage) version;

  # The source tree with src/zstd-wasm.js already embedded, which .gitignore keeps out of the
  # checkout. The release job publishes this source and not the build: flake-release evaluates
  # `.#trevrpc-js.src`, copies it and runs `npm publish` in its sourceRoot, where the .npmrc
  # (ignore-scripts) rules out generating the file with a `prepack` script. Without it the
  # published package cannot load its zstd entry point (#416). The trevrpc-js-release check packs
  # this source as the release does.
  src =
    runCommand "source"
      {
        nativeBuildInputs = [ nodejs_26 ];
        TREVRPC_ZSTD_WASM = "${zstdWasm}/libzstd.wasm";
      }
      ''
        cp -R ${sourceTree} "$out"
        chmod -R u+w "$out"
        node "$out/trevrpc-js/zstd-wasm/embed.js"
      '';
  sourceRoot = "${final.src.name}/trevrpc-js";
  nodejs = nodejs_26;

  npmConfigHook = importNpmLock.npmConfigHook;
  npmDeps = importNpmLock {
    npmRoot = ./.;
  };

  # Embeds the libzstd module into src/zstd-wasm.js, generates the protobuf-es descriptors of the
  # interop and test protos with protoc, and bundles the interop page into build/interop/.
  npmBuildScript = "build";
  PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = "1";
  TREVRPC_ZSTD_WASM = "${zstdWasm}/libzstd.wasm";

  nativeBuildInputs = [
    makeWrapper
    protobuf
  ];

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    npm test
    runHook postCheck
  '';

  # The pruned install drops devDependencies; the interop CLI needs playwright at runtime, and the
  # library its peer dependency @bufbuild/protobuf, which the installed package carries so that it
  # loads on its own, as the Effect adapter's build and the checks below load it.
  preInstall = ''
    mkdir -p "$TMPDIR/interop-node_modules" "$TMPDIR/peer-node_modules/@bufbuild"
    cp -R node_modules/playwright node_modules/playwright-core "$TMPDIR/interop-node_modules/"
    cp -R node_modules/@bufbuild/protobuf "$TMPDIR/peer-node_modules/@bufbuild/"
  '';

  postInstall = ''
    library="$out/lib/node_modules/@trevrpc/trevrpc-js"
    mkdir -p "$library/node_modules"
    cp -R "$TMPDIR/peer-node_modules/@bufbuild" "$library/node_modules/"

    interop="$out/lib/trevrpc-interop-browser"
    mkdir -p "$interop/interop-browser" "$interop/build/interop"
    cp interop-browser/cli.js interop-browser/arguments.js interop-browser/interop-cli.js \
      interop-browser/driver.js interop-browser/names.js interop-browser/safari.js \
      interop-browser/bench-cli.js interop-browser/bench-lanes.js \
      "$interop/interop-browser/"
    cp build/interop/page.js build/interop/bench.js "$interop/build/interop/"
    cp -R "$TMPDIR/interop-node_modules" "$interop/node_modules"
    chmod -R u+w "$interop"
    makeWrapper ${nodejs_26}/bin/node "$out/bin/trevrpc-interop-browser" \
      --add-flags "$interop/interop-browser/cli.js" \
      --set PLAYWRIGHT_BROWSERS_PATH "${playwright-driver.browsers}"

    ln -s ${protocGen}/bin/protoc-gen-trevrpc-js "$out/bin/protoc-gen-trevrpc-js"
  '';

  doInstallCheck = true;
  nativeInstallCheckInputs = [ gnugrep ];
  installCheckPhase = ''
    runHook preInstallCheck
    library="$out/lib/node_modules/@trevrpc/trevrpc-js"

    test -x "$out/bin/trevrpc-interop-browser"
    test -x "$out/bin/protoc-gen-trevrpc-js"
    mkdir -p "$TMPDIR/generated"
    protoc --plugin=protoc-gen-trevrpc-js="$out/bin/protoc-gen-trevrpc-js" \
      --trevrpc-js_out="$TMPDIR/generated" --proto_path=../interop/proto --proto_path=../proto \
      interop.proto
    test -s "$TMPDIR/generated/interop_trevrpc.js"

    # What the published tarball holds is checked in release-check.nix, against the tarball the
    # release packs from the source rather than one packed from this build.

    (
      cd "$out/lib"
      ${nodejs_26}/bin/node --input-type=module -e '
        import assert from "node:assert/strict";
        const runtime = await import("@trevrpc/trevrpc-js");
        assert.deepEqual(Object.keys(runtime).sort(), [
          "Channel", "Code", "TrevRpcError", "bearerMetadata", "codeName", "connect", "createChannel",
          "createClient", "fetchServerCertificateHashes",
        ]);
        const codec = await import("@trevrpc/trevrpc-js/codec");
        assert.deepEqual(Object.keys(codec).sort(), [
          "EmptyBytes", "Int64Zero", "Reader", "RecursionLimit", "Writer", "boolKey", "defineCodec",
          "depthError", "emptyMessage", "foreignError", "int64Key", "intKey", "readMessage",
          "setMapEntry", "uint64Key", "writeMessage",
        ]);
        const zstd = await import("@trevrpc/trevrpc-js/zstd");
        assert.deepEqual(Object.keys(zstd), ["createCompression"]);
        // Without dictionaries, a channel still queries compression without one (ID 0).
        assert.deepEqual((await zstd.createCompression()).query, [{}]);
      '
    )

    # The load benchmark's subcommand validates its arguments before launching a browser.
    status=0
    "$out/bin/trevrpc-interop-browser" bench 2> bench.out || status=$?
    test "$status" -eq 2
    grep -q 'usage: trevrpc-interop-browser bench' bench.out

    ${lib.optionalString stdenv.hostPlatform.isLinux ''
      # The CLI must launch each browser and report a clean failure when no server listens.
      export HOME="$TMPDIR/home"
      mkdir -p "$HOME"
      hash=0000000000000000000000000000000000000000000000000000000000000000
      for browser in chromium firefox; do
        status=0
        "$out/bin/trevrpc-interop-browser" --browser "$browser" \
          --url https://127.0.0.1:49999/trevrpc --cert-hash "$hash" > "$browser.out" || status=$?
        test "$status" -eq 1
        test "$(grep -c '^fail ' "$browser.out")" -eq 19
        ! grep -q '^ok ' "$browser.out"
      done
    ''}

    runHook postInstallCheck
  '';

  passthru = { inherit zstdWasm protocGen; };

  meta = {
    mainProgram = "protoc-gen-trevrpc-js";
    description = "Browser WebTransport client and code generator for TrevRPC, on protobuf-es";
    license = lib.licenses.mit;
    # The interop CLI needs the Playwright browsers from nixpkgs.
    platforms = [
      "x86_64-linux"
      "aarch64-linux"
      "aarch64-darwin"
    ];
    homepage = "https://trev.zip/llc/TrevRPC";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases";
    changelog = "https://trev.zip/llc/TrevRPC/releases/tag/trevrpc-js/v${final.version}";
    sourceProvenance = [ lib.sourceTypes.fromSource ];
    maintainers = [ lib.maintainers.spotdemo4 ];
  };
})
