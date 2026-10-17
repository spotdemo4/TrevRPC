{
  writeShellApplication,
  coreutils,
  jq,
  openssl,
}:
writeShellApplication {
  name = "trevrpc-interop";
  runtimeInputs = [
    coreutils
    jq
    openssl
  ];
  # run.sh sources lib.sh next to itself; the packaged script inlines it.
  text =
    builtins.replaceStrings
      [ ''source "$(dirname "''${BASH_SOURCE[0]}")/lib.sh"'' ]
      [ (builtins.readFile ./lib.sh) ]
      (builtins.readFile ./run.sh);
}
