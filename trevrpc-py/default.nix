# The Python client, which pytest checks against the wire golden vectors and a test server over
# loopback QUIC.
{
  lib,
  mkUvPackage,
  python314,
  wireGolden,
}:
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

  env.TREVRPC_WIRE_GOLDEN = "${wireGolden}";

  meta = {
    description = "A client for TrevRPC, a protobuf RPC framework over QUIC";
    homepage = "https://trev.zip/llc/TrevRPC";
    license = lib.licenses.mit;
  };
})
