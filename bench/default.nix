{
  lib,
  runCommand,
  trevrpc-bench,
  trevrpc-bench-grpc,
  trevrpc-go,
  trevrpc-rust,
  # Extra arguments for run.sh; the check passes a very short matrix.
  extraArgs ? [ ],
}:
let
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
        grpc = "${trevrpc-bench-grpc}/bin/trevrpc-bench-grpc";
      };
in
{
  inherit args;
  # Keeps the load tool working; the timings it records are not meaningful.
  check =
    runCommand "trevrpc-bench-check"
      {
        nativeBuildInputs = [ trevrpc-bench ];
        __darwinAllowLocalNetworking = true;
      }
      ''
        export HOME=$TMPDIR
        trevrpc-bench --out "$out" ${args} ${lib.escapeShellArgs extraArgs}
      '';
}
