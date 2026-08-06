{
  lib,
  buildNpmPackage,
  importNpmLock,
  nodejs_24,
  makeWrapper,
  nssTools,
  playwright-driver,
  trevrpcJs,
}:
buildNpmPackage (final: {
  pname = "trevrpc-bench-peer-browser";
  version = "0.1.5";

  src = lib.fileset.toSource {
    root = ../../.;
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
    package=$out/lib/node_modules/trevrpc-bench-peer-browser
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

    firefox=
    for candidate in ${playwright-driver.browsers}/firefox-*/firefox/firefox; do
      firefox=$candidate
      break
    done
    test -n "$firefox"

    # WebKit/Safari use the browsers directory via PLAYWRIGHT_BROWSERS_PATH, but
    # also expose a dedicated env for explicit executable overrides.
    wrapProgram $out/bin/trevrpc-bench-peer-chromium \
      --set TREVRPC_BROWSER_CHROMIUM "$chromium" \
      --set PLAYWRIGHT_BROWSERS_PATH "${playwright-driver.browsers}" \
      --prefix PATH : "${lib.makeBinPath [ nssTools ]}"
    wrapProgram $out/bin/trevrpc-bench-peer-firefox \
      --set TREVRPC_BROWSER_FIREFOX "$firefox" \
      --set PLAYWRIGHT_BROWSERS_PATH "${playwright-driver.browsers}" \
      --prefix PATH : "${lib.makeBinPath [ nssTools ]}"
    wrapProgram $out/bin/trevrpc-bench-peer-webkit \
      --set PLAYWRIGHT_BROWSERS_PATH "${playwright-driver.browsers}" \
      --prefix PATH : "${lib.makeBinPath [ nssTools ]}"
    wrapProgram $out/bin/trevrpc-bench-peer-safari \
      --set PLAYWRIGHT_BROWSERS_PATH "${playwright-driver.browsers}" \
      --prefix PATH : "${lib.makeBinPath [ nssTools ]}"

    # Keep legacy package name symlink for backwards compatibility.
    if [ ! -e "$out/lib/node_modules/trevrpc-bench-peer-chromium" ]; then
      ln -s trevrpc-bench-peer-browser "$out/lib/node_modules/trevrpc-bench-peer-chromium"
    fi
  '';

  meta = {
    mainProgram = "trevrpc-bench-peer-chromium";
    description = "Browser WebTransport benchmark peers for TrevRPC (Chromium, Firefox, WebKit/Safari)";
    license = lib.licenses.mit;
    platforms = [ "x86_64-linux" ];
  };
})
