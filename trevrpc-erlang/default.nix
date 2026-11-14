{
  lib,
  beamMinimal29Packages,
  wireGolden,
  qmuxGolden,
  zstdGolden,
}:
let
  inherit (beamMinimal29Packages) buildRebar3 fetchHex;

  # The Hex dependencies, read from rebar.lock so that it is the one place their versions and
  # hashes live. pkg_hash_ext is the sha256 of the package tarball, which fetchHex checks.
  lock = builtins.readFile ./rebar.lock;
  matches = regex: text: builtins.filter builtins.isList (builtins.split regex text);
  # {<<"name">>,{pkg,<<"package">>,<<"version">>},level}
  locked = map (match: {
    name = builtins.elemAt match 0;
    pkg = builtins.elemAt match 1;
    version = builtins.elemAt match 2;
    level = lib.toInt (builtins.elemAt match 3);
  }) (matches ''\{<<"([a-z0-9_]+)">>,\{pkg,<<"([a-z0-9_]+)">>,<<"([^"]+)">>},([0-9]+)}'' lock);
  hashes = lib.listToAttrs (
    map (match: lib.nameValuePair (builtins.elemAt match 0) (lib.toLower (builtins.elemAt match 1))) (
      matches ''\{<<"([a-z0-9_]+)">>, *<<"([0-9A-F]+)">>}'' (
        lib.last (lib.splitString "pkg_hash_ext" lock)
      )
    )
  );

  sources = lib.listToAttrs (
    map (
      dep:
      lib.nameValuePair dep.name (fetchHex {
        inherit (dep) pkg version;
        sha256 = hashes.${dep.name};
      })
    ) locked
  );

  # rebar.lock records how deep each dependency sits, not what depends on what, so each one gets
  # the deeper ones as its dependencies.
  deps = lib.listToAttrs (
    map (
      dep:
      lib.nameValuePair dep.name (buildRebar3 {
        inherit (dep) name version;
        src = sources.${dep.name};
        beamDeps = map (deeper: deps.${deeper.name}) (
          builtins.filter (other: other.level > dep.level) locked
        );
      })
    ) locked
  );
in
buildRebar3 (final: {
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
          (lib.fileset.maybeMissing ./_build)
          # the interop executable, a package of its own
          ./interop
        ]
      ))
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-erlang";

  # The direct dependencies; the rest come with them.
  beamDeps = map (dep: deps.${dep.name}) (builtins.filter (dep: dep.level == 0) locked);

  doCheck = true;
  # rebar3 takes the dependencies from _checkouts instead of fetching them.
  checkPhase = ''
    runHook preCheck
    export HOME=$(mktemp -d)
    mkdir -p _checkouts
    ${lib.concatMapStrings (name: "cp -R ${sources.${name}} _checkouts/${name}\n") (
      builtins.attrNames sources
    )}
    chmod -R u+w _checkouts
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
