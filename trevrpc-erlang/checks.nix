# The Erlang runtime's and the Gleam binding's checks, which the flake names trevrpc-erlang-<name>.
# The packages run the tests.
{
  beamMinimal29Packages,
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
  # trevrpc/options.proto, which the Elixir fixture's proto imports.
  trevrpcProto = ../proto;
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

  # The Elixir code protoc-gen-trevrpc-elixir generates compiles, and an Elixir server and client
  # call each other through it.
  protoc-gen-elixir = callPackage ./protoc-gen-trevrpc-elixir/test/elixir {
    inherit (packages) trevrpc-erlang;
    inherit trevrpcProto;
    protocGen = packages.trevrpc-erlang.protocGenElixir;
  };

  elixir-lint = {
    root = ./protoc-gen-trevrpc-elixir/test/elixir;
    files = [
      ./protoc-gen-trevrpc-elixir/test/elixir/mix.exs
      ./protoc-gen-trevrpc-elixir/test/elixir/test
    ];
    packages = [ beamMinimal29Packages.elixir ];
    script = ''
      HOME=$(mktemp -d) mix format --check-formatted mix.exs 'test/**/*.exs'
    '';
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
      ./protoc-gen-trevrpc-elixir/rebar.config
      ./protoc-gen-trevrpc-elixir/rebar.lock
    ];
    packages = [ python3 ];
    script = ''
      python3 -I trevrpc-erlang/erlang_deps.py check
    '';
  };

  protoc-gen-elixir-lint = {
    src = packages.trevrpc-erlang.protocGenElixir;
    packages = [ erlfmt ];
    script = ''
      erlfmt --check rebar.config src/*.erl src/*.app.src test/*.erl
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
