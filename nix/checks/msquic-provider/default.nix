{
  stdenv,
  lib,
  libmsquic,
  expectedDescriptor,
  requestedMask ? null,
}:
assert expectedDescriptor -> builtins.elem requestedMask [ 1 2 3 ];
assert (!expectedDescriptor) -> requestedMask == null;
stdenv.mkDerivation {
  pname =
    if expectedDescriptor then
      "trevrpc-msquic-provider-mask-${toString requestedMask}-check"
    else
      "trevrpc-msquic-stock-provider-check";
  version = "2.6.0";

  dontUnpack = true;
  strictDeps = true;
  buildInputs = [ libmsquic ];

  buildPhase = ''
    runHook preBuild
    $CC -std=c11 -Wall -Wextra -Werror \
      -DEXPECT_DESCRIPTOR=${if expectedDescriptor then "1" else "0"} \
      -DREQUESTED_MASK=${if requestedMask == null then "0" else toString requestedMask}u \
      -DEXPECTED_PROVIDER_PATH='"${libmsquic}"' \
      -I${libmsquic}/include \
      ${./provider-check.c} \
      -L${libmsquic}/lib \
      -Wl,-rpath,${libmsquic}/lib \
      -lmsquic \
      -ldl \
      -o trevrpc-msquic-provider-check
    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall
    install -Dm755 trevrpc-msquic-provider-check \
      $out/bin/trevrpc-msquic-provider-check
    runHook postInstall
  '';

  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck
    $out/bin/trevrpc-msquic-provider-check
    runHook postInstallCheck
  '';

  passthru = {
    msquicProvider = libmsquic;
    inherit expectedDescriptor requestedMask;
  };

  meta = {
    description = "Runtime identity and RESET_STREAM_AT capability check for MsQuic";
    license = lib.licenses.mit;
    platforms = lib.platforms.linux;
  };
}
