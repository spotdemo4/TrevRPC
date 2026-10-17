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
  pname = "trevrpc-bench-grpc";
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
  sourceRoot = "${final.src.name}/bench/grpc-go";
  vendorHash = "sha256-qUL6EkK+9lPgMEbnHc//mtKws5028ZYSfEYZI411jGU=";

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
    test "$(go list -m)" = 'trev.zip/llc/trevrpc/bench/grpc-go'
    test -x "$out/bin/trevrpc-bench-grpc"
    if "$out/bin/trevrpc-bench-grpc" 2> usage.out; then
      echo "trevrpc-bench-grpc without a subcommand must fail" >&2
      exit 1
    fi
    grep -q 'server|bench' usage.out
    runHook postInstallCheck
  '';

  meta = {
    mainProgram = "trevrpc-bench-grpc";
    description = "grpc-go baseline for TrevRPC's load benchmarks";
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
    homepage = "https://trev.zip/llc/TrevRPC";
  };
})
