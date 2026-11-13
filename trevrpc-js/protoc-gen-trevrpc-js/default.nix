{
  lib,
  buildNpmPackage,
  importNpmLock,
  nodejs_26,
  protobuf,
  interopProto,
  trevrpcProto,
}:
let
  pluginPackage = lib.importJSON ./package.json;
in
buildNpmPackage (final: {
  pname = "trevrpc-js-protoc-gen";
  inherit (pluginPackage) version;

  # The plugin, the root oxlint config that ours extends, and the interop protos the checks
  # generate from. trevrpc-js runs this plugin from source for its own tests, which check the
  # generated codecs against protobuf-es.
  src = lib.fileset.toSource {
    root = ../../.;
    fileset = lib.fileset.unions [
      ../../.oxlintrc.json
      interopProto
      trevrpcProto
      (lib.fileset.difference ./. ./default.nix)
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-js/protoc-gen-trevrpc-js";
  nodejs = nodejs_26;

  npmConfigHook = importNpmLock.npmConfigHook;
  npmDeps = importNpmLock {
    npmRoot = ./.;
  };

  dontNpmBuild = true;
  nativeBuildInputs = [ protobuf ];
  # The checks run the plugin from the source, which protoc executes directly.
  postPatch = ''
    patchShebangs bin
  '';

  doCheck = true;
  checkPhase = ''
    runHook preCheck

    # The published package is the plugin, nothing else.
    export HOME="$TMPDIR/home"
    mkdir -p "$HOME"
    tarball="$(npm pack . --silent --pack-destination "$TMPDIR")"
    test "$tarball" = "trevrpc-protoc-gen-trevrpc-js-${final.version}.tgz"
    tar -tzf "$TMPDIR/$tarball" | sort > tarball.list
    printf '%s\n' package/LICENSE package/README.md package/bin/protoc-gen-trevrpc-js \
      package/package.json package/src/plugin.js | diff -u - tarball.list

    # It generates a codec file per proto file, in each target.
    for target in js+dts ts; do
      generated="$TMPDIR/generated-$target"
      mkdir -p "$generated"
      protoc --plugin=protoc-gen-trevrpc-js=bin/protoc-gen-trevrpc-js \
        --trevrpc-js_out="$generated" --trevrpc-js_opt=target=$target \
        --proto_path=../../interop/proto --proto_path=../../proto \
        interop.proto compressed.proto
      ls "$generated" | sort > generated.list
      if [ "$target" = ts ]; then
        printf '%s\n' compressed_trevrpc.ts interop_trevrpc.ts | diff -u - generated.list
      else
        printf '%s\n' compressed_trevrpc.d.ts compressed_trevrpc.js interop_trevrpc.d.ts \
          interop_trevrpc.js | diff -u - generated.list
        echo '{ "type": "module" }' > "$generated/package.json"
        node --check "$generated/interop_trevrpc.js"
        grep -q 'export const EchoCodecs = \[EchoRequestCodec, EchoResponseCodec\];' \
          "$generated/interop_trevrpc.js"
      fi
    done

    runHook postCheck
  '';

  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck
    test -x "$out/bin/protoc-gen-trevrpc-js"
    mkdir -p "$TMPDIR/installed"
    protoc --plugin=protoc-gen-trevrpc-js="$out/bin/protoc-gen-trevrpc-js" \
      --trevrpc-js_out="$TMPDIR/installed" --proto_path=../../interop/proto interop.proto
    test -s "$TMPDIR/installed/interop_trevrpc.js"
    runHook postInstallCheck
  '';

  meta = {
    mainProgram = "protoc-gen-trevrpc-js";
    description = "Generates message codecs for TrevRPC browser clients";
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
    homepage = "https://trev.zip/llc/TrevRPC";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases";
    changelog = "https://trev.zip/llc/TrevRPC/releases/tag/trevrpc-js/protoc-gen-trevrpc-js/v${final.version}";
    sourceProvenance = [ lib.sourceTypes.fromSource ];
    maintainers = [ lib.maintainers.spotdemo4 ];
  };
})
