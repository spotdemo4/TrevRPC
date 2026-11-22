# @trevrpc/qmux: QMux over WebSocket for JavaScript, which trevrpc-js depends on. It has no
# dependencies, so it has no lock; its tests read the shared QMux golden vectors. The release
# publishes `.#trevrpc-js-qmux.src` from its sourceRoot, as it does the other npm packages.
{
  lib,
  stdenvNoCC,
  nodejs_26,
  qmuxGolden,
}:
let
  qmuxPackage = lib.importJSON ./package.json;
in
stdenvNoCC.mkDerivation (final: {
  pname = "trevrpc-js-qmux";
  inherit (qmuxPackage) version;

  # The package, the golden vectors its tests read, and the root oxlint config and the trevrpc-js
  # oxfmt config that the qmux-lint check uses.
  src = lib.fileset.toSource {
    root = ../../.;
    fileset = lib.fileset.unions [
      qmuxGolden
      ../../.oxlintrc.json
      ../.oxfmtrc.json
      (lib.fileset.difference ./. (
        lib.fileset.unions [
          ./default.nix
          (lib.fileset.maybeMissing ./node_modules)
        ]
      ))
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-js/qmux";

  nativeBuildInputs = [ nodejs_26 ];

  dontConfigure = true;
  dontBuild = true;

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    node --test --test-timeout=60000 test/*.test.js
    runHook postCheck
  '';

  # Installs what `npm pack` puts in the tarball, as an installed npm package would be laid out.
  installPhase = ''
    runHook preInstall
    export HOME="$TMPDIR/home"
    mkdir -p "$HOME" "$out/lib/node_modules/@trevrpc/qmux"
    tarball="$(npm pack . --silent --pack-destination "$TMPDIR")"
    tar -xzf "$TMPDIR/$tarball" -C "$out/lib/node_modules/@trevrpc/qmux" --strip-components=1
    runHook postInstall
  '';

  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck
    (
      cd "$out/lib"
      node --input-type=module -e '
        import assert from "node:assert/strict";
        const qmux = await import("@trevrpc/qmux");
        assert.deepEqual(Object.keys(qmux).sort(), [
          "DefaultMaxRecordSize", "DefaultParameters", "QMuxError", "QMuxProtocolError",
          "QMuxSession", "TransportError",
        ]);
      '
    )
    runHook postInstallCheck
  '';

  meta = {
    description = "QMux over WebSocket, shaped like WebTransport: QUIC streams, flow control and datagrams where UDP is blocked";
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
    homepage = "https://trev.zip/llc/TrevRPC";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases";
    changelog = "https://trev.zip/llc/TrevRPC/releases/tag/trevrpc-js/qmux/v${final.version}";
    sourceProvenance = [ lib.sourceTypes.fromSource ];
    maintainers = [ lib.maintainers.spotdemo4 ];
  };
})
