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

  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck
    set +e
    $out/bin/trevrpc-interop-python client --addr 127.0.0.1:1 --ca /dev/null --transport tcp
    status=$?
    set -e
    if [ "$status" -ne 77 ]; then
      echo "expected exit 77 for an unsupported transport, got $status" >&2
      exit 1
    fi
    runHook postInstallCheck
  '';

  meta = {
    description = "The Python TrevRPC interop client";
    license = lib.licenses.mit;
    mainProgram = "trevrpc-interop-python";
  };
}
