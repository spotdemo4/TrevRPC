# The Python client, which pytest checks against the wire, QMux and zstd golden vectors and a test
# server over loopback QUIC and QMux over TLS.
{
  callPackage,
  lib,
  mkUvPackage,
  protobuf,
  python314,
  qmuxGolden,
  wireGolden,
  zstdGolden,
}:
let
  # protoc-gen-trevrpc-py, which this package provides like the other runtimes' packages provide
  # their plugins. It is a PyPI package of its own, and flake-release publishes one pyproject.toml
  # per flake attribute, so the release publishes it from `.#trevrpc-py.protocGen`.
  protocGen = callPackage ./protoc-gen-trevrpc-py { };
in
mkUvPackage (finalAttrs: {
  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [
      ./LICENSE
      ./README.md
      ./pyproject.toml
      ./uv.lock
      ./src
      ./tests
    ];
  };
  python = python314;
  groups = [ "dev" ];

  env = {
    TREVRPC_WIRE_GOLDEN = "${wireGolden}";
    TREVRPC_QMUX_GOLDEN = "${qmuxGolden}";
    TREVRPC_ZSTD_GOLDEN = "${zstdGolden}";
  };

  postInstall = ''
    ln -s ${protocGen}/bin/protoc-gen-trevrpc-py "$out/bin/protoc-gen-trevrpc-py"
  '';

  # The plugin runs from the package and generates a client.
  doInstallCheck = true;
  nativeInstallCheckInputs = [ protobuf ];
  installCheckPhase = ''
    runHook preInstallCheck
    mkdir -p "$TMPDIR/proto" "$TMPDIR/generated"
    cat > "$TMPDIR/proto/echo.proto" <<'EOF'
    syntax = "proto3";
    package echo;
    message Message { string text = 1; }
    service Echo { rpc Say(Message) returns (Message); }
    EOF
    protoc --plugin=protoc-gen-trevrpc-py="$out/bin/protoc-gen-trevrpc-py" \
      --trevrpc-py_out="$TMPDIR/generated" --proto_path="$TMPDIR/proto" echo.proto
    test -s "$TMPDIR/generated/echo_trevrpc.py"
    runHook postInstallCheck
  '';

  passthru = { inherit protocGen; };

  meta = {
    mainProgram = "protoc-gen-trevrpc-py";
    description = "A client and code generator for TrevRPC, a protobuf RPC framework over QUIC";
    homepage = "https://trev.zip/llc/TrevRPC";
    license = lib.licenses.mit;
  };
})
