# Keeps the packaged load benchmark working with a very short matrix; the timings it records are
# not meaningful. Every row starts a client process, and for a browser lane a browser, so the
# matrix is what this check costs: it keeps every server, client, lane, compression mode and rule
# set, and leaves out the dimensions that only repeat them. What a scenario does at each
# concurrency and with each kind of call is tested where the benchmark clients are.
{
  runCommand,
  trevrpc-bench,
}:
runCommand "trevrpc-bench-check"
  {
    nativeBuildInputs = [ trevrpc-bench ];
    __darwinAllowLocalNetworking = true;
  }
  ''
    export HOME=$TMPDIR
    # Every scenario over every lane, with several workers.
    trevrpc-bench --out "$out/plain" --duration 0.2 --warmup 0 --concurrency 4 --messages 10
    # Every compression mode, on a unary and a streaming scenario.
    trevrpc-bench --out "$out/compression" --duration 0.2 --warmup 0 --concurrency 1 --messages 10 \
      --scenarios unary,bidi --payload records \
      --compressions none,gzip,zstd,dictionary,stream,stream-dictionary
    # The rule sets; the run above already has the validation scenarios without rules.
    trevrpc-bench --out "$out/validation" --duration 0.2 --warmup 0 --concurrency 1 --messages 10 \
      --scenarios validation-unary,validation-client-stream --validations standard,cel
  ''
