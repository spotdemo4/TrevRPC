{
  lib,
  callPackage,
  beamMinimal29Packages,
  wireGolden,
  zstdGolden,
  trevrpcProto,
  # The qmux package next door (./qmux), a dependency rebar3 takes from _checkouts.
  qmux,
}:
let
  inherit (beamMinimal29Packages) buildRebar3;
  lock = callPackage ./deps.nix { } ./rebar.lock;
  # Shell commands that put every dependency in _checkouts: the locked Hex packages and qmux.
  checkouts = ''
    ${lock.checkouts}
    cp -R ${qmux.src}/trevrpc-erlang/qmux _checkouts/qmux
    chmod -R u+w _checkouts
  '';
  # protoc-gen-trevrpc-erlang, protoc-gen-trevrpc-gleam and protoc-gen-trevrpc-elixir, which this
  # package provides like the other runtimes' packages provide their plugins. Each is an escript of
  # its own, built and tested in its own derivation.
  protocGen = callPackage ./protoc-gen-trevrpc-erlang { inherit trevrpcProto; };
  protocGenGleam = callPackage ./protoc-gen-trevrpc-gleam { inherit trevrpcProto; };
  protocGenElixir = callPackage ./protoc-gen-trevrpc-elixir { inherit trevrpcProto; };
in
(buildRebar3 (final: {
  name = "trevrpc";
  version = "1.12.0";

  # The application plus the golden vectors its tests read. rebar3 leaves the Gleam binding's
  # sources alone; gleam.nix builds and tests them. qmux, Gleam's path dependency, stays for
  # gleam publish, which flake-release runs here.
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      wireGolden
      zstdGolden
      (lib.fileset.difference ./. (
        lib.fileset.unions [
          ./checks.nix
          ./default.nix
          ./deps.nix
          ./gleam.nix
          ./erlang_deps.py
          ./test_erlang_deps.py
          (lib.fileset.maybeMissing ./_build)
          (lib.fileset.maybeMissing ./build)
          (lib.fileset.maybeMissing ./test/greet)
          # links for local builds; the build copies what they link
          ./_checkouts
          ./qmux/default.nix
          (lib.fileset.maybeMissing ./qmux/_build)
          # the interop executable and the protoc plugins, packages of their own
          ./interop
          ./protoc-gen-trevrpc-erlang
          ./protoc-gen-trevrpc-gleam
          ./protoc-gen-trevrpc-elixir
        ]
      ))
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-erlang";

  # The direct dependencies; the rest come with them.
  beamDeps = lock.direct ++ [ qmux ];

  doCheck = true;
  # rebar3 takes the dependencies from _checkouts instead of fetching them.
  checkPhase = ''
    runHook preCheck
    export HOME=$(mktemp -d)
    ${checkouts}
    rebar3 eunit
    rebar3 xref
    rebar3 dialyzer
    runHook postCheck
  '';

  postInstall = ''
    mkdir -p $out/bin
    ln -s ${protocGen}/bin/protoc-gen-trevrpc-erlang $out/bin/protoc-gen-trevrpc-erlang
    ln -s ${protocGenGleam}/bin/protoc-gen-trevrpc-gleam $out/bin/protoc-gen-trevrpc-gleam
    ln -s ${protocGenElixir}/bin/protoc-gen-trevrpc-elixir $out/bin/protoc-gen-trevrpc-elixir
  '';

  meta = {
    mainProgram = "protoc-gen-trevrpc-erlang";
    description = "Erlang runtime and Gleam binding for TrevRPC, with Erlang, Gleam and Elixir codegen";
    license = lib.licenses.mit;
    homepage = "https://trev.zip/llc/TrevRPC";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases";
  };
})).overrideAttrs
  # buildRebar3 sets passthru itself. The interop executable's checks take the dependencies from
  # checkouts, as this package's do. gleam is the same package built and tested by Gleam, as gleam
  # publish builds it for Hex.
  (
    previous: {
      passthru = previous.passthru // {
        inherit
          protocGen
          protocGenGleam
          protocGenElixir
          checkouts
          ;
        gleam = callPackage ./gleam.nix {
          inherit (previous) version;
          inherit trevrpcProto;
          protocGen = protocGenGleam;
        };
      };
    }
  )
