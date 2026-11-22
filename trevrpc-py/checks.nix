# The Python client's checks, which the flake names trevrpc-py-<name>. The package runs the tests.
{
  mkUvEnv,
  protobuf,
  pyright,
  python314,
  self,
  stdenv,
}:
let
  packages = self.packages.${stdenv.hostPlatform.system};

  # The locked dependencies of the runtime and of the plugin, for pyright and the generators.
  runtimeEnv = mkUvEnv {
    src = ./.;
    python = python314;
    groups = [ "dev" ];
  };
  pluginEnv = mkUvEnv {
    src = ./protoc-gen-trevrpc-py;
    python = python314;
    groups = [ "dev" ];
  };
in
{
  # Strict pyright over the runtime, its tests, the interop client and the plugin, with the locked
  # dependencies. The interop bindings are checked as the generated code users import.
  typecheck = {
    root = ./.;
    files = [
      ./pyproject.toml
      ./src
      ./tests
      ./interop
      ./protoc-gen-trevrpc-py/src
    ];
    packages = [ pyright ];
    script = ''
      pyright --pythonpath ${runtimeEnv}/bin/python
    '';
  };

  # The interop bindings are what protoc-gen-py and protoc-gen-trevrpc-py generate from
  # interop/proto, and the plugin's _options_pb.py what protoc-gen-py generates from
  # trevrpc/options.proto.
  generated = {
    root = ../.;
    files = [
      ../interop/proto
      ../proto
      ./interop/generate.sh
      ./interop/gen
      ./protoc-gen-trevrpc-py/src/protoc_gen_trevrpc_py/_options_pb.py
    ];
    packages = [
      protobuf
      packages.trevrpc-py-protoc-gen
      pluginEnv
    ];
    script = ''
      bash trevrpc-py/interop/generate.sh "$TMPDIR/gen"
      diff -ru trevrpc-py/interop/gen "$TMPDIR/gen"

      mkdir "$TMPDIR/options"
      protoc -I proto --py_out="$TMPDIR/options" trevrpc/options.proto
      diff -u trevrpc-py/protoc-gen-trevrpc-py/src/protoc_gen_trevrpc_py/_options_pb.py \
        "$TMPDIR/options/trevrpc/options_pb.py"
    '';
  };
}
