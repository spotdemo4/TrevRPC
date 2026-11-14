# Builds and checks one of the npm adapters next to trevrpc-js, like @trevrpc/effect: packages of
# their own that are developed and tested against the trevrpc-js in this checkout. What differs
# between the adapters is passed in.
{
  lib,
  buildNpmPackage,
  importNpmLock,
  nodejs_26,
  protobuf,
  trevrpc-js,
}:
{
  pname,
  # The adapter's directory, which holds its package.json and package-lock.json.
  directory,
  # Files outside trevrpc-js that its build or tests read.
  files ? [ ],
  # The names the adapter exports.
  exports,
  # JavaScript that checks the packed adapter further, with it imported as `adapter`.
  importCheck ? "",
  description,
}:
let
  adapterPackage = lib.importJSON (directory + "/package.json");
  packageLock = lib.importJSON (directory + "/package-lock.json");
  # The adapter's directory within the repository.
  path = lib.removePrefix "./" (lib.path.removePrefix ../. directory);
  # The adapter links the trevrpc-js in this checkout as `file:..`. Its lock records the link but
  # none of trevrpc-js's own dependencies, so the link is left out of the install and made by hand
  # below, next to those dependencies.
  withoutTrevrpc = attrs: removeAttrs attrs [ "@trevrpc/trevrpc-js" ];
  installedPackage = adapterPackage // {
    devDependencies = withoutTrevrpc adapterPackage.devDependencies;
    peerDependencies = withoutTrevrpc adapterPackage.peerDependencies;
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
  inherit pname;
  inherit (adapterPackage) version;

  # The adapter and the trevrpc-js sources it links to, whose test server the tests reuse, plus the
  # root oxlint config that ours extends. The other adapters, the wasm module and the Nix files are
  # left out.
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions (
      [
        ../.oxlintrc.json
        (lib.fileset.difference ./. (
          lib.fileset.unions [
            ./zstd-wasm
            ./effect
            ./protovalidate
            (lib.fileset.fileFilter (file: file.hasExt "nix") ./.)
          ]
        ))
        (lib.fileset.difference directory (lib.fileset.fileFilter (file: file.hasExt "nix") directory))
      ]
      ++ files
    );
  };
  sourceRoot = "${final.src.name}/${path}";
  nodejs = nodejs_26;

  npmConfigHook = importNpmLock.npmConfigHook;
  npmDeps = importNpmLock {
    npmRoot = directory;
    package = installedPackage;
    packageLock = installedLock;
  };

  nativeBuildInputs = [ protobuf ];

  # Links trevrpc-js from this checkout, with the production dependencies of its built package; the
  # tests import it both through the link and, for its test server, by relative path. The versions
  # are compared here rather than in an assert, which would make the flake's package set refer to
  # itself.
  preBuild = ''
    ${lib.optionalString (adapterPackage.version != trevrpc-js.version) (
      throw "${adapterPackage.name} ${adapterPackage.version} must have the version of trevrpc-js ${trevrpc-js.version}"
    )}
    chmod -R u+w ..
    ln -s ${trevrpcModule}/node_modules ../node_modules
    mkdir -p node_modules/@trevrpc
    ln -s ../../.. node_modules/@trevrpc/trevrpc-js
  '';

  # Generates what the runtime and type tests need, such as service descriptors, with
  # protoc-gen-es.
  npmBuildScript = "build";

  # The packed adapter loads against the built trevrpc-js and its other peers, which a consumer
  # installs next to it; what the tarball holds is checked in release-check.nix.
  doCheck = true;
  checkPhase = ''
    runHook preCheck
    npm test

    export HOME="$TMPDIR/home"
    mkdir -p "$HOME"
    tarball="$(npm pack . --silent --pack-destination "$TMPDIR")"

    consumer="$TMPDIR/consumer/node_modules"
    mkdir -p "$consumer/${adapterPackage.name}"
    tar -xzf "$TMPDIR/$tarball" -C "$consumer/${adapterPackage.name}" --strip-components=1
    ln -s ${trevrpcModule} "$consumer/@trevrpc/trevrpc-js"
    for peer in ${lib.escapeShellArgs (lib.attrNames installedPackage.peerDependencies)}; do
      mkdir -p "$(dirname "$consumer/$peer")"
      ln -s "$PWD/node_modules/$peer" "$consumer/$peer"
    done
    (
      cd "$TMPDIR/consumer"
      node --input-type=module -e '
        import assert from "node:assert/strict";
        const adapter = await import("${adapterPackage.name}");
        assert.deepEqual(Object.keys(adapter).sort(), ${builtins.toJSON (lib.sort lib.lessThan exports)});
        ${importCheck}
      '
    )
    runHook postCheck
  '';

  # The installed package is the adapter alone; the links to the checkout are not part of it.
  preInstall = ''
    rm node_modules/@trevrpc/trevrpc-js
  '';

  meta = {
    inherit description;
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
    changelog = "https://trev.zip/llc/TrevRPC/releases/tag/${path}/v${final.version}";
    sourceProvenance = [ lib.sourceTypes.fromSource ];
    maintainers = [ lib.maintainers.spotdemo4 ];
  };
})
