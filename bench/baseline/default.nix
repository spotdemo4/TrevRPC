{
  lib,
  stdenv,
  mkGoModule,
  protobuf,
  interopProto,
}:
let
  canExecute = stdenv.buildPlatform.canExecute stdenv.hostPlatform;
in
mkGoModule (final: {
  pname = "trevrpc-bench-baseline";
  version = "0.0.0";

  # The module itself plus the shared interop proto its drift test regenerates
  # the Echo bindings from.
  src = lib.fileset.toSource {
    root = ../../.;
    fileset = lib.fileset.unions [
      interopProto
      (lib.fileset.difference ./. ./default.nix)
    ];
  };
  sourceRoot = "${final.src.name}/bench/baseline";
  vendorHash = "sha256-6pxN/+kofpVBWQSblToMw7AcLQdsAQrxF/DRdMe22No=";

  subPackages = [ "." ];

  doCheck = canExecute;
  # The echopb drift test regenerates code from interop.proto.
  nativeCheckInputs = [ protobuf ];
  checkPhase = ''
    runHook preCheck
    export HOME=$(mktemp -d)
    go test ./...
    runHook postCheck
  '';

  doInstallCheck = canExecute;
  installCheckPhase = ''
    runHook preInstallCheck
    test "$(go list -m)" = 'trev.zip/llc/trevrpc/bench/baseline'
    test -x "$out/bin/trevrpc-bench-baseline"
    if "$out/bin/trevrpc-bench-baseline" 2> usage.out; then
      echo "trevrpc-bench-baseline without a subcommand must fail" >&2
      exit 1
    fi
    grep -q 'server|bench' usage.out
    runHook postInstallCheck
  '';

  meta = {
    mainProgram = "trevrpc-bench-baseline";
    description = "grpc-go and connect-go baselines for TrevRPC's load benchmarks";
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
    homepage = "https://trev.zip/llc/TrevRPC";
  };
})
