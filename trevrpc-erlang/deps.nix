# The Hex dependencies of a rebar3 project, read from its rebar.lock so that it is the one place
# their versions and hashes live: the fetched `sources` by name, their builds as `deps`, and `direct`,
# the builds of the project's own dependencies (the rest come with them).
{
  lib,
  beamMinimal29Packages,
}:
lockFile:
let
  inherit (beamMinimal29Packages) buildRebar3 fetchHex;

  lock = builtins.readFile lockFile;
  matches = regex: text: builtins.filter builtins.isList (builtins.split regex text);
  # {<<"name">>,{pkg,<<"package">>,<<"version">>},level}
  locked = map (match: {
    name = builtins.elemAt match 0;
    pkg = builtins.elemAt match 1;
    version = builtins.elemAt match 2;
    level = lib.toInt (builtins.elemAt match 3);
  }) (matches ''\{<<"([a-z0-9_]+)">>,\{pkg,<<"([a-z0-9_]+)">>,<<"([^"]+)">>},([0-9]+)}'' lock);
  # pkg_hash_ext is the sha256 of the package tarball, which fetchHex checks.
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
{
  inherit sources deps;
  direct = map (dep: deps.${dep.name}) (builtins.filter (dep: dep.level == 0) locked);

  # Shell commands that put the sources in _checkouts, where rebar3 takes dependencies from
  # instead of fetching them.
  checkouts = ''
    mkdir -p _checkouts
    ${lib.concatMapStrings (name: "cp -R ${sources.${name}} _checkouts/${name}\n") (
      builtins.attrNames sources
    )}
    chmod -R u+w _checkouts
  '';
}
