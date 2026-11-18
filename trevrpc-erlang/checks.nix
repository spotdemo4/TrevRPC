# The Erlang runtime's and the Gleam binding's checks, which the flake names trevrpc-erlang-<name>.
# The packages run the tests.
{
  callPackage,
  erlfmt,
  pkgs,
  protobuf,
  python3,
  stdenv,
  self,
}:
let
  packages = self.packages.${stdenv.hostPlatform.system};
in
{
  lint = {
    src = packages.trevrpc-erlang;
    packages = [ erlfmt ];
    script = ''
      erlfmt --check rebar.config src/*.erl src/*.app.src test/*.erl
    '';
  };

  # The generated bindings in src/generated are left as generated.
  interop-lint = {
    root = ./interop;
    files = [
      ./interop/rebar.config
      ./interop/src
    ];
    packages = [ erlfmt ];
    script = ''
      erlfmt --check rebar.config src/*.erl src/*.app.src
    '';
  };

  # The interop bindings are what protoc-gen-trevrpc-erlang generates from interop/proto.
  interop-generated = {
    root = ../.;
    files = [
      ../interop/proto
      ../proto
      ./interop/generate.sh
      ./interop/src/generated
    ];
    packages = [
      protobuf
      packages.trevrpc-erlang
    ];
    script = ''
      PROTOC_GEN_TREVRPC_ERLANG=$(command -v protoc-gen-trevrpc-erlang) \
        bash trevrpc-erlang/interop/generate.sh "$TMPDIR/generated"
      diff -ru trevrpc-erlang/interop/src/generated "$TMPDIR/generated"
    '';
  };

  # trevrpc as Gleam builds and publishes it: the binding's tests, a Gleam server and client on
  # loopback.
  gleam = packages.trevrpc-erlang.gleam;

  # The Gleam code protoc-gen-trevrpc-gleam generates compiles, every value survives gpb, and
  # protoc reads the bytes.
  protoc-gen-gleam = callPackage ./protoc-gen-trevrpc-gleam/test/gleam {
    protocGen = packages.trevrpc-erlang.protocGenGleam;
  };

  gleam-lint = {
    root = ../.;
    files = [
      ./src
      ./test
      ./protoc-gen-trevrpc-gleam/test/gleam/test
    ];
    # pkgs.gleam, as gleam is the check above
    packages = [ pkgs.gleam ];
    script = ''
      gleam format --check trevrpc-erlang/src trevrpc-erlang/test \
        trevrpc-erlang/protoc-gen-trevrpc-gleam/test/gleam/test
    '';
  };

  protoc-gen-gleam-lint = {
    src = packages.trevrpc-erlang.protocGenGleam;
    packages = [ erlfmt ];
    script = ''
      erlfmt --check rebar.config src/*.erl src/*.app.src test/*.erl
    '';
  };

  # trevrpc's rebar3 files are what sync-erlang-deps makes of gleam.toml and manifest.toml.
  deps = {
    root = ../.;
    files = [
      ./erlang_deps.py
      ./gleam.toml
      ./manifest.toml
      ./rebar.config
      ./rebar.lock
      ./interop/rebar.lock
      ./protoc-gen-trevrpc-erlang/rebar.config
      ./protoc-gen-trevrpc-erlang/rebar.lock
      ./protoc-gen-trevrpc-gleam/rebar.config
      ./protoc-gen-trevrpc-gleam/rebar.lock
    ];
    packages = [ python3 ];
    script = ''
      python3 -I trevrpc-erlang/erlang_deps.py check
    '';
  };

  protoc-gen-lint = {
    src = packages.trevrpc-erlang.protocGen;
    packages = [ erlfmt ];
    script = ''
      erlfmt --check rebar.config src/*.erl src/*.app.src test/*.erl test/support/*.erl
    '';
  };
}
