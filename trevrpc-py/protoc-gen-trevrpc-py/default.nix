# protoc-gen-trevrpc-py, which generates TrevRPC clients for protobuf-py's messages.
{
  lib,
  mkUvPackage,
  python314,
}:
mkUvPackage {
  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [
      ./LICENSE
      ./README.md
      ./pyproject.toml
      ./uv.lock
      ./src
    ];
  };
  python = python314;

  meta = {
    description = "Generates TrevRPC clients for protobuf-py messages";
    homepage = "https://trev.zip/llc/TrevRPC";
    license = lib.licenses.mit;
    mainProgram = "protoc-gen-trevrpc-py";
  };
}
