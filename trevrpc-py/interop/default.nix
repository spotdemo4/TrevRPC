# The Python interop client, trevrpc-interop-python: the scenarios in trevrpc_interop.py and the
# bindings in gen/, run on trevrpc's environment.
{
  lib,
  makeWrapper,
  stdenvNoCC,
  trevrpc-py,
}:
stdenvNoCC.mkDerivation {
  pname = "trevrpc-interop-python";
  inherit (trevrpc-py) version;

  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [
      ./trevrpc_interop.py
      ./gen
    ];
  };

  nativeBuildInputs = [ makeWrapper ];

  # Python puts the script's directory first on sys.path, which finds gen/. -E and -s keep the
  # environment and the user's site-packages out of it.
  installPhase = ''
    runHook preInstall
    mkdir -p $out/share/trevrpc-interop-python
    cp -R . $out/share/trevrpc-interop-python
    makeWrapper ${trevrpc-py}/bin/python $out/bin/trevrpc-interop-python \
      --add-flags "-E -s $out/share/trevrpc-interop-python/trevrpc_interop.py"
    runHook postInstall
  '';

  # The client imports the runtime and the generated bindings before it parses its arguments.
  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck
    $out/bin/trevrpc-interop-python client --help >/dev/null
    runHook postInstallCheck
  '';

  meta = {
    description = "The Python TrevRPC interop client";
    license = lib.licenses.mit;
    mainProgram = "trevrpc-interop-python";
  };
}
