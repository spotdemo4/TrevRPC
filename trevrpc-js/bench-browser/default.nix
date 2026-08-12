{
  lib,
  buildNpmPackage,
  importNpmLock,
  nodejs_24,
  makeWrapper,
  nssTools,
  playwright-driver,
  stdenv,
  trevrpcJs,
}:
let
  packageLock = lib.importJSON ./package-lock.json;
  playwrightCoreVersion = packageLock.packages."node_modules/playwright-core".version;
  isX86Linux = stdenv.hostPlatform.system == "x86_64-linux";
  isDarwin = stdenv.hostPlatform.system == "aarch64-darwin";
  browsers =
    if isX86Linux then
      playwright-driver.selectBrowsers {
        withChromium = true;
        withChromiumHeadlessShell = false;
        withFirefox = true;
        withWebkit = false;
        withFfmpeg = false;
      }
    else if isDarwin then
      playwright-driver.selectBrowsers {
        withChromium = false;
        withChromiumHeadlessShell = false;
        withFirefox = false;
        withWebkit = true;
        withFfmpeg = false;
      }
    else
      null;
  inherit (playwright-driver.browsersJSON) chromium firefox webkit;
in
assert lib.assertMsg (playwrightCoreVersion == playwright-driver.version) ''
  trevrpc-bench-peer-browser playwright-core ${playwrightCoreVersion} must match
  nixpkgs playwright-driver ${playwright-driver.version}
'';
buildNpmPackage (final: {
  pname = "trevrpc-bench-peer-browser";
  version = "0.2.1";

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

    # Keep explicit executable paths paired with the matching Nix browser
    # bundle so Playwright never downloads browsers at runtime.
    ${lib.optionalString isX86Linux ''
      chromium=${browsers}/chromium-${chromium.revision}/chrome-linux64/chrome
      firefox=${browsers}/firefox-${firefox.revision}/firefox/firefox
      test -x "$chromium"
      test -x "$firefox"

      wrapProgram $out/bin/trevrpc-bench-peer-chromium \
        --set TREVRPC_BROWSER_CHROMIUM "$chromium" \
        --set PLAYWRIGHT_BROWSERS_PATH "${browsers}" \
        --prefix PATH : "${lib.makeBinPath [ nssTools ]}"
      wrapProgram $out/bin/trevrpc-bench-peer-firefox \
        --set TREVRPC_BROWSER_FIREFOX "$firefox" \
        --set PLAYWRIGHT_BROWSERS_PATH "${browsers}" \
        --prefix PATH : "${lib.makeBinPath [ nssTools ]}"
    ''}
    ${lib.optionalString isDarwin ''
      webkit=${browsers}/webkit-${webkit.revision}/pw_run.sh
      playwright=${browsers}/webkit-${webkit.revision}/Playwright.app/Contents/MacOS/Playwright
      test -x "$webkit"
      test -x "$playwright"

      wrapProgram $out/bin/trevrpc-bench-peer-webkit \
        --set TREVRPC_BROWSER_WEBKIT "$webkit" \
        --set PLAYWRIGHT_BROWSERS_PATH "${browsers}"
    ''}

    # Keep legacy package name symlink for backwards compatibility.
    if [ ! -e "$out/lib/node_modules/trevrpc-bench-peer-chromium" ]; then
      ln -s trevrpc-bench-peer-browser "$out/lib/node_modules/trevrpc-bench-peer-chromium"
    fi
  '';

  meta = {
    mainProgram = if isDarwin then "trevrpc-bench-peer-webkit" else "trevrpc-bench-peer-chromium";
    description = "Platform-native browser WebTransport benchmark peers for TrevRPC";
    license = lib.licenses.mit;
    platforms = [
      "x86_64-linux"
      "aarch64-darwin"
    ];
  };
})
