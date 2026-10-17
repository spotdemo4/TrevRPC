{
  writeShellApplication,
  coreutils,
  gawk,
  jq,
  openssl,
}:
writeShellApplication {
  name = "trevrpc-bench";
  runtimeInputs = [
    coreutils
    gawk
    jq
    openssl
  ];
  # run.sh sources ../interop/lib.sh; the packaged script inlines it.
  text =
    builtins.replaceStrings
      [ ''source "$(dirname "''${BASH_SOURCE[0]}")/../interop/lib.sh"'' ]
      [ (builtins.readFile ../interop/lib.sh) ]
      (builtins.readFile ./run.sh);
}
