{
  lib,
  callPackage,
  qmuxGolden,
}:
callPackage ../module.nix { } {
  pname = "trevrpc-go-qmux";
  version = "1.12.0";

  directory = ./.;
  # The module itself, the shared QMux golden vectors its tests read, and the
  # lint configuration of the Go modules.
  fileset = lib.fileset.unions [
    qmuxGolden
    ../.golangci.yaml
    ./.
  ];

  meta.description = "QMux (draft-ietf-quic-qmux) for Go: QUIC streams and datagrams over TLS or WebSocket";
}
