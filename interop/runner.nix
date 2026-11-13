# A Python runner from interop/ or bench/, packaged with the shared harness next to it. The
# runners' unit tests are not part of the package: the python-test check runs them.
{
  lib,
  makeWrapper,
  openssl,
  python3,
  stdenvNoCC,
  # The executable's name and the runner it starts, relative to the repository root.
  name,
  script,
  # Tools the runner calls besides openssl.
  runtimeInputs ? [ ],
  # The matrix the runner falls back to when none of its matrix options is given, as option names
  # mapped to a value or a list of values; see parse_with_defaults in harness.py.
  defaults ? { },
}:
let
  # Without the unit tests, so that a change to one does not rebuild the runners and rerun the
  # interop and bench checks that depend on them.
  runnerSource = file: file.hasExt "py" && !lib.hasPrefix "test_" file.name;
  defaultFlags = lib.cli.toCommandLineShell (name: {
    option = "--${name}";
    sep = null;
    explicitBool = false;
  }) defaults;
in
stdenvNoCC.mkDerivation {
  inherit name;

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      (lib.fileset.fileFilter runnerSource ./.)
      (lib.fileset.fileFilter runnerSource ../bench)
      # The interop runner takes its compression dictionary from these.
      ../testdata/zstd-golden-vectors.txt
    ];
  };

  nativeBuildInputs = [ makeWrapper ];

  # The runners find the harness at ../interop and the compression golden vectors at ../testdata
  # relative to themselves, as in the checkout.
  installPhase = ''
    runHook preInstall
    share=$out/share/${name}
    mkdir -p $out/bin "$share"
    cp --parents interop/*.py bench/*.py testdata/zstd-golden-vectors.txt "$share/"
    makeWrapper ${python3}/bin/python3 $out/bin/${name} \
      --add-flags "-I $share/${script}" \
      --prefix PATH : ${lib.makeBinPath ([ openssl ] ++ runtimeInputs)} \
      --set TREVRPC_RUNNER_DEFAULTS ${lib.escapeShellArg defaultFlags}
    runHook postInstall
  '';
}
