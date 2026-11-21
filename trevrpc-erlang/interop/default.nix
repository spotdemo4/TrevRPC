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
        ./test
      ];
    };
    beamDeps = [ trevrpc-erlang ];

    # xref, dialyzer, which checks the generated bindings' specs against the runtime's types, and
    # the tests, which check the bench client's records payload against bench/payload.py.
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
      rebar3 eunit
      runHook postCheck
    '';
  };

  # Every application the executable loads.
  closure = deps: lib.unique (lib.concatMap (dep: [ dep ] ++ closure (dep.beamDeps or [ ])) deps);
  libs = closure [ app ];
  # The executable's ERL_LIBS, which the install check loads too.
  erlLibs = lib.concatMapStringsSep ":" (dep: "${dep}/lib/erlang/lib") libs;

  # erlang_quic's crypto NIF loads, and shares OTP's libcrypto: once crypto and the NIF are both
  # loaded, the VM maps one libcrypto. Platforms without /proc/self/maps check only the first.
  nifCheck = ''
    true = quic_crypto_nif:is_loaded(),
    _ = crypto:info_lib(),
    case file:read_file("/proc/self/maps") of
        {ok, Maps} ->
            [_] = lists:usort([
                lists:last(string:lexemes(Line, " "))
             || Line <- string:lexemes(binary_to_list(Maps), "\n"),
                string:find(Line, "libcrypto") =/= nomatch
            ]);
        {error, _} ->
            ok
    end,
    halt().
  '';
in
stdenvNoCC.mkDerivation {
  pname = "trevrpc-interop-erlang";
  inherit (app) version;

  dontUnpack = true;
  nativeBuildInputs = [ makeWrapper ];

  installPhase = ''
    runHook preInstall
    makeWrapper ${erlang}/bin/erl $out/bin/trevrpc-interop-erlang \
      --set ERL_LIBS ${erlLibs} \
      --add-flags "-noshell -s trevrpc_interop main -extra"
    runHook postInstall
  '';

  # The VM boots and loads the executable: a bad command line is a usage error. erlang_quic's crypto
  # NIF loads too (nifCheck); without it, erlang_quic falls back to OTP's crypto, which works but is
  # slower.
  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck
    export HOME=$(mktemp -d)
    status=0
    $out/bin/trevrpc-interop-erlang nope 2>/dev/null || status=$?
    test "$status" = 2
    ERL_LIBS=${erlLibs} ${erlang}/bin/erl -noshell -eval ${lib.escapeShellArg nifCheck}
    runHook postInstallCheck
  '';

  meta = {
    mainProgram = "trevrpc-interop-erlang";
    description = "TrevRPC interop executable for the Erlang runtime";
    license = lib.licenses.mit;
    platforms = erlang.meta.platforms;
  };
}
