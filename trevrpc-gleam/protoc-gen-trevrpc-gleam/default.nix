{
  lib,
  callPackage,
  stdenv,
  beamMinimal29Packages,
  protobuf,
  trevrpcProto,
}:
let
  inherit (beamMinimal29Packages) erlang rebar3;
  lock = callPackage ../../trevrpc-erlang/deps.nix { } ./rebar.lock;
  # protoc-gen-trevrpc-erlang, which this builds on: its modules, the test helpers and protos
  # the tests share, and its rebar.config, with which rebar3 compiles it.
  erlangPlugin = ../../trevrpc-erlang/protoc-gen-trevrpc-erlang;
in
stdenv.mkDerivation (final: {
  pname = "protoc-gen-trevrpc-gleam";
  version = "1.11.1";

  # The plugin, protoc-gen-trevrpc-erlang, and trevrpc/options.proto, which the tests' protos
  # import.
  src = lib.fileset.toSource {
    root = ../..;
    fileset = lib.fileset.unions [
      trevrpcProto
      (erlangPlugin + "/rebar.config")
      (erlangPlugin + "/src")
      (erlangPlugin + "/test/support")
      (erlangPlugin + "/test/proto")
      (lib.fileset.difference ./. (
        lib.fileset.unions [
          ./default.nix
          # The link to protoc-gen-trevrpc-erlang, which configurePhase copies instead.
          ./_checkouts
          # The Gleam fixture, which builds this.
          ./test/gleam/default.nix
          (lib.fileset.maybeMissing ./_build)
        ]
      ))
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-gleam/protoc-gen-trevrpc-gleam";

  nativeBuildInputs = [ rebar3 ];
  # The escript runs on this Erlang, which patchShebangs points it at.
  buildInputs = [ erlang ];
  nativeCheckInputs = [ protobuf ];
  nativeInstallCheckInputs = [ protobuf ];

  # rebar3 takes gpb and protoc-gen-trevrpc-erlang from _checkouts instead of fetching them, and
  # the escript carries them.
  configurePhase = ''
    runHook preConfigure
    export HOME=$(mktemp -d)
    ${lock.checkouts}
    cp -R ../../trevrpc-erlang/protoc-gen-trevrpc-erlang _checkouts/protoc_gen_trevrpc_erlang
    chmod -R u+w _checkouts
    runHook postConfigure
  '';

  buildPhase = ''
    runHook preBuild
    rebar3 escriptize
    runHook postBuild
  '';

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    rebar3 eunit
    rebar3 xref
    rebar3 dialyzer
    runHook postCheck
  '';

  installPhase = ''
    runHook preInstall
    install -Dm755 _build/default/bin/protoc-gen-trevrpc-gleam $out/bin/protoc-gen-trevrpc-gleam
    runHook postInstall
  '';

  # protoc runs the installed plugin.
  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck
    out_dir=$(mktemp -d)
    protoc --plugin=protoc-gen-trevrpc-gleam=$out/bin/protoc-gen-trevrpc-gleam \
      -I test/gleam/proto --trevrpc-gleam_out=$out_dir fixture/v1/shapes.proto
    test -f $out_dir/fixture/v1/shapes.gleam
    test -f $out_dir/fixture/v1/fixture@v1@shapes_shim.erl
    runHook postInstallCheck
  '';

  meta = {
    mainProgram = "protoc-gen-trevrpc-gleam";
    description = "protoc plugin generating Gleam code for TrevRPC services";
    # The escript carries gpb, which is LGPL-2.1-or-later.
    license = with lib.licenses; [
      mit
      lgpl21Plus
    ];
    homepage = "https://trev.zip/llc/TrevRPC";
    platforms = erlang.meta.platforms;
  };
})
