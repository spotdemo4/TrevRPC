{
  lib,
  stdenv,
  runCommand,
  trevrpc-go,
  trevrpc-interop,
  trevrpc-js,
  trevrpc-rust,
}:
let
  # playwright-driver only ships the browsers on Linux.
  browsers = lib.optionals stdenv.hostPlatform.isLinux [
    "chromium"
    "firefox"
  ];
  # run.sh parses `--option value`, not GNU `--option=value`.
  args =
    lib.cli.toCommandLineShell
      (name: {
        option = "--${name}";
        sep = null;
        explicitBool = false;
      })
      {
        server = [
          "go=${trevrpc-go}/bin/trevrpc-interop-go"
          "rust=${trevrpc-rust}/bin/trevrpc-interop-rust"
        ];
        client = [
          "go=${trevrpc-go}/bin/trevrpc-interop-go"
          "rust=${trevrpc-rust}/bin/trevrpc-interop-rust"
        ];
        browser = browsers;
      };
in
runCommand "trevrpc-interop-check"
  {
    nativeBuildInputs = [ trevrpc-interop ] ++ lib.optional (browsers != [ ]) trevrpc-js;
    __darwinAllowLocalNetworking = true;
  }
  ''
    export HOME=$TMPDIR
    trevrpc-interop --out "$out" ${args}
  ''
