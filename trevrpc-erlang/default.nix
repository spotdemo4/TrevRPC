{
  lib,
  callPackage,
  beamMinimal29Packages,
  wireGolden,
  qmuxGolden,
  zstdGolden,
  trevrpcProto,
}:
let
  inherit (beamMinimal29Packages) buildRebar3;
  lock = callPackage ./deps.nix { } ./rebar.lock;
  # protoc-gen-trevrpc-erlang, which this package provides like the other runtimes' packages
  # provide their plugins. It is an escript of its own, built and tested in its own derivation.
  protocGen = callPackage ./protoc-gen-trevrpc-erlang { inherit trevrpcProto; };
in
(buildRebar3 (final: {
  name = "trevrpc";
  version = "1.11.1";

  # The application plus the golden vectors its tests read.
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      wireGolden
      qmuxGolden
      zstdGolden
      (lib.fileset.difference ./. (
        lib.fileset.unions [
          ./default.nix
          ./deps.nix
          (lib.fileset.maybeMissing ./_build)
          # the interop executable and the protoc plugin, packages of their own
          ./interop
          ./protoc-gen-trevrpc-erlang
        ]
      ))
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-erlang";

  # The direct dependencies; the rest come with them.
  beamDeps = lock.direct;

  doCheck = true;
  # rebar3 takes the dependencies from _checkouts instead of fetching them.
  checkPhase = ''
    runHook preCheck
    export HOME=$(mktemp -d)
    ${lock.checkouts}
    rebar3 eunit
    rebar3 xref
    rebar3 dialyzer
    runHook postCheck
  '';

  postInstall = ''
    mkdir -p $out/bin
    ln -s ${protocGen}/bin/protoc-gen-trevrpc-erlang $out/bin/protoc-gen-trevrpc-erlang
  '';

  meta = {
    mainProgram = "protoc-gen-trevrpc-erlang";
    description = "Erlang runtime for TrevRPC";
    license = lib.licenses.mit;
    homepage = "https://trev.zip/llc/TrevRPC";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases";
  };
})).overrideAttrs
  # buildRebar3 sets passthru itself. The interop executable's checks take the dependencies from
  # lock, as this package's do.
  (
    previous: {
      passthru = previous.passthru // {
        inherit protocGen lock;
      };
    }
  )
