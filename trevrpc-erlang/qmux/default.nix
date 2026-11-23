# The qmux Hex package: QMux for Erlang, which trevrpc depends on. It needs only OTP, so it has no
# lock; its tests read the shared QMux golden vectors.
{
  lib,
  beamMinimal29Packages,
  qmuxGolden,
}:
let
  inherit (beamMinimal29Packages) buildRebar3;
in
buildRebar3 (final: {
  name = "qmux";
  version = "1.13.0";

  src = lib.fileset.toSource {
    root = ../../.;
    fileset = lib.fileset.unions [
      qmuxGolden
      (lib.fileset.difference ./. (
        lib.fileset.unions [
          ./default.nix
          (lib.fileset.maybeMissing ./_build)
          (lib.fileset.maybeMissing ./build)
        ]
      ))
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-erlang/qmux";

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
    description = "QMux (draft-ietf-quic-qmux) for Erlang: QUIC streams and datagrams over TLS or WebSocket";
    license = lib.licenses.mit;
    homepage = "https://trev.zip/llc/TrevRPC/src/branch/main/trevrpc-erlang/qmux";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases";
  };
})
