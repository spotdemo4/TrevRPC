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
  lock = callPackage ../deps.nix { } ./rebar.lock;
in
stdenv.mkDerivation (final: {
  pname = "protoc-gen-trevrpc-erlang";
  version = "1.11.1";

  # The plugin plus trevrpc/options.proto, which its tests' protos import.
  src = lib.fileset.toSource {
    root = ../..;
    fileset = lib.fileset.unions [
      trevrpcProto
      (lib.fileset.difference ./. (
        lib.fileset.unions [
          ./default.nix
          (lib.fileset.maybeMissing ./_build)
        ]
      ))
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-erlang/protoc-gen-trevrpc-erlang";

  nativeBuildInputs = [ rebar3 ];
  # The escript runs on this Erlang, which patchShebangs points it at.
  buildInputs = [ erlang ];
  nativeCheckInputs = [ protobuf ];
  nativeInstallCheckInputs = [ protobuf ];

  # rebar3 takes gpb from _checkouts instead of fetching it, and the escript carries it.
  configurePhase = ''
    runHook preConfigure
    export HOME=$(mktemp -d)
    ${lock.checkouts}
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
    install -Dm755 _build/default/bin/protoc-gen-trevrpc-erlang $out/bin/protoc-gen-trevrpc-erlang
    runHook postInstall
  '';

  # protoc runs the installed plugin, and refuses it an editions file, as the plugin does not
  # declare editions support.
  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck
    out_dir=$(mktemp -d)
    protoc --plugin=protoc-gen-trevrpc-erlang=$out/bin/protoc-gen-trevrpc-erlang \
      -I test/proto -I ../../proto --trevrpc-erlang_out=$out_dir methods.proto
    test -f $out_dir/methods_pb.erl
    test -f $out_dir/example_methods_v1_methods.erl
    if protoc --plugin=protoc-gen-trevrpc-erlang=$out/bin/protoc-gen-trevrpc-erlang \
      -I test/proto --trevrpc-erlang_out=$out_dir errors/editions.proto 2>$out_dir/editions.log; then
      echo "protoc accepted an editions file" >&2
      exit 1
    fi
    grep -q "edition" $out_dir/editions.log
    runHook postInstallCheck
  '';

  meta = {
    mainProgram = "protoc-gen-trevrpc-erlang";
    description = "protoc plugin generating Erlang code for TrevRPC services";
    # The escript carries gpb, which is LGPL-2.1-or-later.
    license = with lib.licenses; [
      mit
      lgpl21Plus
    ];
    homepage = "https://trev.zip/llc/TrevRPC";
    platforms = erlang.meta.platforms;
  };
})
