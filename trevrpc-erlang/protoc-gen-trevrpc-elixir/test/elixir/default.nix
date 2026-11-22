# The Elixir code protoc-gen-trevrpc-elixir generates from proto/: Mix compiles it with warnings
# as errors, and an Elixir server and client run every kind of call through it on loopback.
{
  lib,
  stdenv,
  beamMinimal29Packages,
  protobuf,
  trevrpcProto,
  trevrpc-erlang,
  protocGen,
}:
let
  inherit (beamMinimal29Packages) elixir;
  # trevrpc and every application it loads, which the fixture takes from ERL_LIBS.
  closure = deps: lib.unique (lib.concatMap (dep: [ dep ] ++ closure (dep.beamDeps or [ ])) deps);
  erlLibs = lib.concatMapStringsSep ":" (dep: "${dep}/lib/erlang/lib") (closure [ trevrpc-erlang ]);
in
stdenv.mkDerivation (final: {
  pname = "protoc-gen-trevrpc-elixir-fixture";
  inherit (protocGen) version;

  # The fixture, and trevrpc/options.proto, which its proto imports.
  src = lib.fileset.toSource {
    root = ../../../..;
    fileset = lib.fileset.unions [
      trevrpcProto
      ./mix.exs
      ./proto
      ./test
      ./generate.sh
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-erlang/protoc-gen-trevrpc-elixir/test/elixir";

  nativeBuildInputs = [
    elixir
    protobuf
    protocGen
  ];

  env = {
    ERL_LIBS = erlLibs;
    LANG = "C.UTF-8";
    MIX_ENV = "test";
  };

  dontConfigure = true;

  buildPhase = ''
    runHook preBuild
    export HOME=$(mktemp -d)
    PROTOC_GEN_TREVRPC_ELIXIR=${lib.getExe' protocGen "protoc-gen-trevrpc-elixir"} \
      TREVRPC_PROTO=$PWD/../../../../proto bash generate.sh
    mix compile
    runHook postBuild
  '';

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    mix test
    runHook postCheck
  '';

  installPhase = ''
    runHook preInstall
    mkdir -p $out
    cp -R gen $out/
    runHook postInstall
  '';
})
