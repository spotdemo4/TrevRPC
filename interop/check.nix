# The packaged interop matrix.
{
  runCommand,
  trevrpc-interop,
}:
runCommand "trevrpc-interop-check"
  {
    nativeBuildInputs = [ trevrpc-interop ];
    __darwinAllowLocalNetworking = true;
  }
  ''
    export HOME=$TMPDIR
    trevrpc-interop --out "$out"
  ''
