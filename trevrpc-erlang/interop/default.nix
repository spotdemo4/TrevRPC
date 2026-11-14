{
  lib,
  beamMinimal29Packages,
  makeWrapper,
  stdenvNoCC,
  trevrpc-erlang,
}:
let
  inherit (beamMinimal29Packages) buildRebar3 erlang;

  app = buildRebar3 {
    name = "trevrpc_interop";
    version = "1.11.1";
    # Without _checkouts, which links the runtime for local builds.
    src = lib.fileset.toSource {
      root = ./.;
      fileset = lib.fileset.unions [
        ./rebar.config
        ./src
      ];
    };
    beamDeps = [ trevrpc-erlang ];

    # xref, and dialyzer, which checks the generated bindings' specs against the runtime's types.
    # rebar3 takes the runtime and its dependencies from _checkouts instead of fetching them.
    doCheck = true;
    checkPhase = ''
      runHook preCheck
      export HOME=$(mktemp -d)
      ${trevrpc-erlang.lock.checkouts}
      cp -R ${trevrpc-erlang.src}/trevrpc-erlang _checkouts/trevrpc
      chmod -R u+w _checkouts
      rebar3 xref
      rebar3 dialyzer
      runHook postCheck
    '';
  };

  # Every application the executable loads.
  closure = deps: lib.unique (lib.concatMap (dep: [ dep ] ++ closure (dep.beamDeps or [ ])) deps);
  libs = closure [ app ];
in
stdenvNoCC.mkDerivation {
  pname = "trevrpc-interop-erlang";
  inherit (app) version;

  dontUnpack = true;
  nativeBuildInputs = [ makeWrapper ];

  installPhase = ''
    runHook preInstall
    makeWrapper ${erlang}/bin/erl $out/bin/trevrpc-interop-erlang \
      --set ERL_LIBS ${lib.concatMapStringsSep ":" (dep: "${dep}/lib/erlang/lib") libs} \
      --add-flags "-noshell -s trevrpc_interop main -extra"
    runHook postInstall
  '';

  # The VM boots and loads the executable: a bad command line is a usage error.
  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck
    export HOME=$(mktemp -d)
    status=0
    $out/bin/trevrpc-interop-erlang nope 2>/dev/null || status=$?
    test "$status" = 2
    runHook postInstallCheck
  '';

  meta = {
    mainProgram = "trevrpc-interop-erlang";
    description = "TrevRPC interop executable for the Erlang runtime";
    license = lib.licenses.mit;
    platforms = erlang.meta.platforms;
  };
}
