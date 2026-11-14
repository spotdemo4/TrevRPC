# The source of one of the workspace's crates, which the rust release job publishes to crates.io
# after trevrpc. The trevrpc-rust package builds, tests and lints the crate with the rest of the
# workspace.
{
  lib,
  stdenvNoCC,
  licenseFile,
}:
{
  pname,
  version,
  # The crate's directory.
  directory,
  description,
}:
stdenvNoCC.mkDerivation (final: {
  inherit pname version;

  # The workspace, whose manifest and lockfile the crate builds with, and the license it packages.
  # Like trevrpc-rust's, it leaves out the Nix files.
  src = lib.fileset.toSource {
    root = ../..;
    fileset = lib.fileset.unions [
      licenseFile
      (lib.fileset.difference ../. (lib.fileset.fileFilter (file: file.hasExt "nix") ../.))
    ];
  };
  sourceRoot = "${final.src.name}/${lib.removePrefix "./" (lib.path.removePrefix ../.. directory)}";

  dontConfigure = true;
  dontBuild = true;
  installPhase = ''
    runHook preInstall
    mkdir -p "$out"
    runHook postInstall
  '';

  meta = {
    inherit description;
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
    homepage = "https://trev.zip/llc/TrevRPC";
    sourceProvenance = [ lib.sourceTypes.fromSource ];
    maintainers = [ lib.maintainers.spotdemo4 ];
  };
})
