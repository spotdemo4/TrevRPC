{
  lib,
  buildNpmPackage,
  importNpmLock,
  nodejs_26,
  protobuf,
  trevrpc-js,
}:
let
  effectPackage = lib.importJSON ./package.json;
  packageLock = lib.importJSON ./package-lock.json;
  # The adapter is developed and tested against the trevrpc-js in this checkout, which npm links as
  # `file:..`. Its lock records the link but none of trevrpc-js's own dependencies, so the link is
  # left out of the install and made by hand below, next to those dependencies.
  withoutTrevrpc = attrs: removeAttrs attrs [ "@trevrpc/trevrpc-js" ];
  installedPackage = effectPackage // {
    devDependencies = withoutTrevrpc effectPackage.devDependencies;
    peerDependencies = withoutTrevrpc effectPackage.peerDependencies;
  };
  installedLock = packageLock // {
    packages =
      removeAttrs packageLock.packages [
        ".."
        "node_modules/@trevrpc/trevrpc-js"
      ]
      // {
        "" = installedPackage;
      };
  };
  trevrpcModule = "${trevrpc-js}/lib/node_modules/@trevrpc/trevrpc-js";
in
buildNpmPackage (final: {
  pname = "trevrpc-js-effect";
  inherit (effectPackage) version;

  # The adapter and the trevrpc-js sources it links to, whose test server the tests reuse, plus the
  # root oxlint config that ours extends.
  src = lib.fileset.toSource {
    root = ../../.;
    fileset = lib.fileset.unions [
      ../../.oxlintrc.json
      (lib.fileset.difference ../. (
        lib.fileset.unions [
          ../zstd-wasm
          ../protovalidate
          ../default.nix
          ../release-check.nix
          ./default.nix
        ]
      ))
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-js/effect";
  nodejs = nodejs_26;

  npmConfigHook = importNpmLock.npmConfigHook;
  npmDeps = importNpmLock {
    npmRoot = ./.;
    package = installedPackage;
    packageLock = installedLock;
  };

  nativeBuildInputs = [ protobuf ];

  # Links trevrpc-js from this checkout, with the production dependencies of its built package; the
  # tests import it both through the link and, for its test server, by relative path. The versions
  # are compared here rather than in an assert, which would make the flake's package set refer to
  # itself.
  preBuild = ''
    ${lib.optionalString (effectPackage.version != trevrpc-js.version) (
      throw "@trevrpc/effect ${effectPackage.version} must have the version of trevrpc-js ${trevrpc-js.version}"
    )}
    chmod -R u+w ..
    ln -s ${trevrpcModule}/node_modules ../node_modules
    mkdir -p node_modules/@trevrpc
    ln -s ../../.. node_modules/@trevrpc/trevrpc-js
  '';

  # Generates the Echo service descriptor of the runtime and type tests with protoc-gen-es.
  npmBuildScript = "build";

  doCheck = true;
  checkPhase = ''
    runHook preCheck
    npm test

    # The packed adapter loads against the built trevrpc-js and effect, its peers; what the
    # tarball holds is checked in release-check.nix.
    export HOME="$TMPDIR/home"
    mkdir -p "$HOME"
    tarball="$(npm pack . --silent --pack-destination "$TMPDIR")"

    consumer="$TMPDIR/consumer/node_modules"
    mkdir -p "$consumer/@trevrpc/effect"
    tar -xzf "$TMPDIR/$tarball" -C "$consumer/@trevrpc/effect" --strip-components=1
    ln -s ${trevrpcModule} "$consumer/@trevrpc/trevrpc-js"
    ln -s "$PWD/node_modules/effect" "$consumer/effect"
    (
      cd "$TMPDIR/consumer"
      node --input-type=module -e '
        import assert from "node:assert/strict";
        const adapter = await import("@trevrpc/effect");
        assert.deepEqual(Object.keys(adapter).sort(), ["createEffectClient", "toTrevRpcError"]);
      '
    )
    runHook postCheck
  '';

  # The installed package is the adapter alone; the links to the checkout are not part of it.
  preInstall = ''
    rm node_modules/@trevrpc/trevrpc-js
  '';

  meta = {
    description = "Effect adapter for TrevRPC browser clients";
    license = lib.licenses.mit;
    # Those of trevrpc-js, which the tests run against; it cannot be read here, as that would make
    # the flake's package set refer to itself.
    platforms = [
      "x86_64-linux"
      "aarch64-linux"
      "aarch64-darwin"
    ];
    homepage = "https://trev.zip/llc/TrevRPC";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases";
    changelog = "https://trev.zip/llc/TrevRPC/releases/tag/trevrpc-js/effect/v${final.version}";
    sourceProvenance = [ lib.sourceTypes.fromSource ];
    maintainers = [ lib.maintainers.spotdemo4 ];
  };
})
