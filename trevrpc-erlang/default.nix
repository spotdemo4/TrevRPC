{
  lib,
  beamMinimal28Packages,
  wireGolden,
  zstdGolden,
}:
beamMinimal28Packages.buildRebar3 (final: {
  name = "trevrpc";
  version = "1.11.1";

  # The application plus the golden vectors its tests read.
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      wireGolden
      zstdGolden
      (lib.fileset.difference ./. (
        lib.fileset.unions [
          ./default.nix
          (lib.fileset.maybeMissing ./_build)
        ]
      ))
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-erlang";

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    export HOME=$(mktemp -d)
    rebar3 eunit
    rebar3 xref
    rebar3 dialyzer
    runHook postCheck
  '';

  meta = {
    description = "Erlang runtime for TrevRPC";
    license = lib.licenses.mit;
    homepage = "https://trev.zip/llc/TrevRPC";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases";
  };
})
