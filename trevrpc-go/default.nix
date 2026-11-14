{
  lib,
  callPackage,
  protobuf,
  pebble,
  wireGolden,
  qmuxGolden,
  zstdGolden,
  trevrpcProto,
}:
callPackage ./module.nix { } rec {
  pname = "trevrpc-go";
  version = "1.11.1";

  directory = ./.;
  # The module itself plus the shared files its tests read: the wire, QMux and
  # compression golden vectors, and the TrevRPC option protos the generated
  # bindings must match.
  fileset = lib.fileset.unions [
    wireGolden
    qmuxGolden
    zstdGolden
    trevrpcProto
    (lib.fileset.difference ./. (
      # separate modules, with their own packages
      lib.fileset.unions [
        ./oteltrevrpc
        ./protovalidatetrevrpc
        ./cmd/trevrpc-interop
      ]
    ))
  ];

  subPackages = [ "cmd/protoc-gen-trevrpc-go" ];

  # The bindings tests regenerate code from the option protos, and the ACME
  # test gets a certificate from Pebble.
  nativeCheckInputs = [
    protobuf
    pebble
  ];
  tests = ''
    # The default build compresses with libzstd through cgo; trevrpc_purego
    # tests the pure-Go codec that builds without cgo use.
    test "$(go env CGO_ENABLED)" = 1
    go test ./...
    # The tag only changes internal/zstd, so only the packages whose tests
    # reach it run again; the RPC tests that compress are in the main package.
    zstd=$(go list ./internal/zstd)
    purego=$(go list -test -f '{{.Dir}} {{join .Deps " "}} ' ./... \
      | grep -F " $zstd " | cut -d' ' -f1 | sort -u)
    grep -qx "$PWD" <<< "$purego"
    go test -tags trevrpc_purego $purego
  '';
  benchmarks = ''
    # One iteration each, so the benchmarks keep compiling and running.
    go test -run '^$' -bench . -benchtime 1x ./...
  '';

  meta = {
    mainProgram = "protoc-gen-trevrpc-go";
    description = "Go runtime and code generator for TrevRPC";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases";
    changelog = "https://trev.zip/llc/TrevRPC/releases/tag/trevrpc-go/v${version}";
  };
}
