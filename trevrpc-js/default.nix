{
  lib,
  stdenv,
  buildNpmPackage,
  importNpmLock,
  nodejs_26,
  makeWrapper,
  oxlint,
  protobuf,
  gnugrep,
  playwright-driver,
  wireGolden,
  qmuxGolden,
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

  # The interop clients are generated from the shared interop protos, which import the TrevRPC
  # schema options, and the wire and QMux tests read the shared golden vectors; all of them live
  # outside this directory.
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      wireGolden
      qmuxGolden
      interopProto
      trevrpcProto
      ./.
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-js";
  nodejs = nodejs_26;

  npmConfigHook = importNpmLock.npmConfigHook;
  npmDeps = importNpmLock {
    npmRoot = ./.;
  };

  # Generates the Echo and UnreliableEcho clients with protoc and bundles the interop page into
  # build/interop/.
  npmBuildScript = "build";
  PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = "1";

  nativeBuildInputs = [
    makeWrapper
    protobuf
  ];

  # protoc runs the plugin directly, so it needs a shebang that exists in the sandbox.
  postPatch = ''
    patchShebangs bin/protoc-gen-trevrpc-js.js
  '';

  doCheck = true;
  nativeCheckInputs = [
    oxlint
  ];
  checkPhase = ''
    runHook preCheck
    oxlint --deny-warnings
    npm run typecheck
    npm test
    runHook postCheck
  '';

  # The pruned install drops devDependencies; the interop CLI needs playwright at runtime.
  preInstall = ''
    mkdir -p "$TMPDIR/interop-node_modules"
    cp -R node_modules/playwright node_modules/playwright-core "$TMPDIR/interop-node_modules/"
  '';

  postInstall = ''
    interop="$out/lib/trevrpc-interop-browser"
    mkdir -p "$interop/interop-browser" "$interop/build/interop"
    cp interop-browser/cli.js interop-browser/names.js interop-browser/safari.js \
      "$interop/interop-browser/"
    cp build/interop/page.js "$interop/build/interop/"
    cp -R "$TMPDIR/interop-node_modules" "$interop/node_modules"
    chmod -R u+w "$interop"
    makeWrapper ${nodejs_26}/bin/node "$out/bin/trevrpc-interop-browser" \
      --add-flags "$interop/interop-browser/cli.js" \
      --set PLAYWRIGHT_BROWSERS_PATH "${playwright-driver.browsers}"
  '';

  doInstallCheck = true;
  nativeInstallCheckInputs = [ gnugrep ];
  installCheckPhase = ''
    runHook preInstallCheck
    library="$out/lib/node_modules/@trevrpc/trevrpc-js"

    IFS= read -r installed_shebang < "$library/bin/protoc-gen-trevrpc-js.js"
    test "$installed_shebang" = "#!${nodejs_26}/bin/node"
    test -x "$out/bin/protoc-gen-trevrpc-js"
    test -x "$out/bin/trevrpc-interop-browser"

    # The published package is the browser client and the generator, nothing else.
    export HOME="$TMPDIR/home"
    mkdir -p "$HOME"
    tarball="$(npm pack . --silent --pack-destination "$TMPDIR")"
    test "$tarball" = "trevrpc-trevrpc-js-${final.version}.tgz"
    tar -tzf "$TMPDIR/$tarball" | sort > tarball.list
    grep -qx 'package/src/index.js' tarball.list
    grep -qx 'package/src/index.d.ts' tarball.list
    grep -qx 'package/bin/protoc-gen-trevrpc-js.js' tarball.list
    ! grep -E 'interop|test/|build/|native|node_modules' tarball.list

    (
      cd "$out/lib"
      ${nodejs_26}/bin/node --input-type=module -e '
        import assert from "node:assert/strict";
        const runtime = await import("@trevrpc/trevrpc-js");
        assert.deepEqual(Object.keys(runtime).sort(), [
          "Channel", "Code", "TrevRpcError", "codeName", "connect", "createRoot",
          "createServiceClient", "protobuf",
        ]);
      '
    )

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
        test "$(grep -c '^fail ' "$browser.out")" -eq 12
        ! grep -q '^ok ' "$browser.out"
      done
    ''}

    runHook postInstallCheck
  '';

  meta = {
    mainProgram = "protoc-gen-trevrpc-js";
    description = "Browser WebTransport client and protobuf.js code generator for TrevRPC";
    license = lib.licenses.mit;
    # The interop CLI needs the Playwright browsers from nixpkgs.
    platforms = [
      "x86_64-linux"
      "aarch64-linux"
      "aarch64-darwin"
    ];
    homepage = "https://trev.zip/llc/TrevRPC";
    changelog = "https://trev.zip/llc/TrevRPC/releases";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/v${final.version}";
  };
})
