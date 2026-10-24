# Keeps the packaged load benchmark working with a very short matrix; the timings it records are
# not meaningful.
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
    trevrpc-bench --out "$out/plain" --duration 0.2 --warmup 0 --concurrency 1,4 --messages 10
    trevrpc-bench --out "$out/compression" --duration 0.2 --warmup 0 --concurrency 1 --messages 10 \
      --scenarios unary,client-stream,bidi --payload records \
      --compressions none,gzip,zstd,dictionary,stream,stream-dictionary
  ''
