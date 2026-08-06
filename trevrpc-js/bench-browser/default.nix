{
  lib,
  buildNpmPackage,
  importNpmLock,
  nodejs_24,
  makeWrapper,
  playwright-driver,
  repoRoot,
  trevrpcJs,
}:
buildNpmPackage (final: {
  pname = "trevrpc-bench-peer-chromium";
  version = "0.1.0";

  src = lib.fileset.toSource {
    root = repoRoot;
    fileset = ./.;
  };
  sourceRoot = "${final.src.name}/trevrpc-js/bench-browser";
  nodejs = nodejs_24;

  npmConfigHook = importNpmLock.npmConfigHook;
  npmDeps = importNpmLock {
    npmRoot = ./.;
  };
  dontNpmBuild = true;
  PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = "1";

  nativeBuildInputs = [ makeWrapper ];
  preBuild = ''
    mkdir -p node_modules/@trevrpc
    ln -s ${trevrpcJs}/lib/node_modules/@trevrpc/trevrpc-js node_modules/@trevrpc/trevrpc-js
    ln -s ${trevrpcJs}/lib/node_modules/trevrpc-bench-peer-js \
      node_modules/trevrpc-bench-peer-js
  '';

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    npm test
    runHook postCheck
  '';

  postInstall = ''
    package=$out/lib/node_modules/trevrpc-bench-peer-chromium
    mkdir -p "$package/node_modules/@trevrpc"
    ln -s ${trevrpcJs}/lib/node_modules/@trevrpc/trevrpc-js \
      "$package/node_modules/@trevrpc/trevrpc-js"
    ln -s ${trevrpcJs}/lib/node_modules/trevrpc-bench-peer-js \
      "$package/node_modules/trevrpc-bench-peer-js"

    chromium=
    for candidate in ${playwright-driver.browsers}/chromium-*/chrome-linux*/chrome; do
      chromium=$candidate
      break
    done
    test -n "$chromium"
    wrapProgram $out/bin/trevrpc-bench-peer-chromium \
      --set TREVRPC_BROWSER_CHROMIUM "$chromium"
  '';

  meta = {
    mainProgram = "trevrpc-bench-peer-chromium";
    description = "Chromium WebTransport benchmark peer for TrevRPC";
    license = lib.licenses.mit;
    platforms = [ "x86_64-linux" ];
  };
})
