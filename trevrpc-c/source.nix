{ lib }:
let
  cFiles = lib.fileset.fileFilter (
    file:
    !(
      builtins.elem file.name [
        "core.1066676"
        "go.mod"
        "go.sum"
      ]
      || file.hasExt "go"
    )
  ) ./.;
  source = lib.fileset.difference cFiles (
    lib.fileset.unions [
      ./provider/msquic/lib
      ./tests/go-provider-spike/provider/lib
    ]
  );
in
{
  # CMake's shared test targets reference protocol translation units even when
  # their MsQuic library targets are disabled. Both C-only closures therefore
  # retain provider source and headers, but never Go metadata or archives.
  neutral = source;
  msquic = source;

  # Go module checks keep only production module trees. The provider archive is
  # selected by the consumer's target-specific source closure.
  goNeutral = lib.fileset.difference ./. (
    lib.fileset.unions [
      ./provider/msquic
      ./tests
    ]
  );
  goProvider = lib.fileset.difference ./provider/msquic ./provider/msquic/lib;
}
