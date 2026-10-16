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
  text = builtins.readFile ./run.sh;
}
