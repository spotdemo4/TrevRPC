# The Python client's checks, which the flake names trevrpc-py-<name>. The package runs the tests.
{
  mkUvEnv,
  protobuf,
  pyright,
  python314,
  self,
  stdenv,
  uv,
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

  # The distributions the release publishes to PyPI. flake-release does not publish what Nix builds:
  # it builds each package's src, copies that tree and builds the sdist and wheel there with uv. This
  # builds the same trees the same way, offline with the uv_build backend nixpkgs provides, so a file
  # a package's fileset leaves out, such as py.typed, fails here instead of in a release. PyPI's
  # checks are twine's, and flake-release requires every artifact to carry the release's version.
  release = {
    root = ./.;
    files = [ ./pyproject.toml ];
    packages = [
      uv
      (python314.withPackages (ps: [
        ps.uv-build
        ps.twine
      ]))
    ];
    script = ''
      export HOME="$TMPDIR" UV_NO_CACHE=1 UV_OFFLINE=1 UV_PYTHON_DOWNLOADS=never
      dist="$TMPDIR/dist"
      for source in ${packages.trevrpc-py.src} ${packages.trevrpc-py.protocGen.src}; do
        staged=$(mktemp -d)
        cp -R "$source"/. "$staged"
        chmod -R u+w "$staged"
        uv build --quiet --no-build-isolation --python "$(command -v python3)" --out-dir "$dist" "$staged"
      done
      twine check --strict "$dist"/*.whl "$dist"/*.tar.gz

      # Both packages release at the runtimes' shared version.
      version=${packages.trevrpc-py.version}
      [ "${packages.trevrpc-py.protocGen.version}" = "$version" ]
      for name in trevrpc-$version-py3-none-any.whl trevrpc-$version.tar.gz \
        protoc_gen_trevrpc_py-$version-py3-none-any.whl protoc_gen_trevrpc_py-$version.tar.gz; do
        [ -f "$dist/$name" ] || { echo "missing $name" >&2; ls "$dist" >&2; exit 1; }
      done

      # The runtime is typed, and the distributions hold the packages, not the tests.
      python3 -c '
      import sys, zipfile, tarfile
      wheel = zipfile.ZipFile(sys.argv[1]).namelist()
      assert "trevrpc/py.typed" in wheel, "the trevrpc wheel has no py.typed"
      plugin = zipfile.ZipFile(sys.argv[2]).namelist()
      assert any(n.endswith("entry_points.txt") for n in plugin), "the plugin wheel has no entry point"
      for archive in sys.argv[3:]:
          names = tarfile.open(archive).getnames()
          assert not any("/tests/" in n for n in names), f"{archive} holds tests"
      ' "$dist/trevrpc-$version-py3-none-any.whl" \
        "$dist/protoc_gen_trevrpc_py-$version-py3-none-any.whl" "$dist"/*.tar.gz
    '';
  };

  # The interop bindings are what interop/generate.sh generates from interop/proto with
  # protoc-gen-py and protoc-gen-trevrpc-py, the plugin's _options_pb.py its trevrpc/options_pb.py,
  # and the protovalidate tests' bindings what it generates from tests/proto.
  generated = {
    root = ../.;
    files = [
      ../interop/proto
      ../proto
      ./interop/generate.sh
      ./interop/gen
      ./tests/proto
      ./tests/gen
      ./protoc-gen-trevrpc-py/src/protoc_gen_trevrpc_py/_options_pb.py
    ];
    packages = [
      protobuf
      packages.trevrpc-py
      pluginEnv
    ];
    script = ''
      bash trevrpc-py/interop/generate.sh "$TMPDIR/gen" "$TMPDIR/tests-gen"
      diff -ru trevrpc-py/interop/gen "$TMPDIR/gen"
      diff -ru trevrpc-py/tests/gen "$TMPDIR/tests-gen"
      diff -u trevrpc-py/protoc-gen-trevrpc-py/src/protoc_gen_trevrpc_py/_options_pb.py \
        "$TMPDIR/gen/trevrpc/options_pb.py"
    '';
  };
}
