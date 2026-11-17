# trevrpc-gleam, as a Gleam library: its docs, and its tests, a Gleam server and
# client on loopback through the code protoc-gen-trevrpc-gleam generates. Gleam
# builds the runtime it depends on by path from its sources. It provides
# protoc-gen-trevrpc-gleam, as bin/ and passthru.protocGen.
{
  lib,
  callPackage,
  beamMinimal29Packages,
  mkGleamPackage,
  protobuf,
  trevrpcProto,
}:
let
  protocGen = callPackage ./protoc-gen-trevrpc-gleam { inherit trevrpcProto; };
in
mkGleamPackage (finalAttrs: {
  pname = "trevrpc-gleam";
  version = "1.11.1";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      trevrpcProto
      ../trevrpc-erlang/gleam.toml
      ../trevrpc-erlang/src
      (lib.fileset.difference ./. (
        lib.fileset.unions [
          ./default.nix
          # Built on its own, as protocGen.
          ./protoc-gen-trevrpc-gleam
          (lib.fileset.maybeMissing ./build)
          (lib.fileset.maybeMissing ./test/greet)
        ]
      ))
    ];
  };
  # sourceRoot, not gleamRoot, so that flake-release finds gleam.toml there; the rest of the
  # source, with the runtime's path dependency, is still unpacked beside it.
  sourceRoot = "${finalAttrs.src.name}/trevrpc-gleam";

  gleamLibrary = true;
  # The runtime needs Erlang/OTP 29.
  beamPackages = beamMinimal29Packages;
  nativeBuildInputs = [ protobuf ];

  # gleam build compiles the tests too, with the test service's code.
  preBuild = ''
    PROTOC_GEN_TREVRPC_GLEAM=${lib.getExe protocGen} bash generate.sh
  '';

  postInstall = ''
    mkdir -p $out/bin
    ln -s ${lib.getExe protocGen} $out/bin/protoc-gen-trevrpc-gleam
  '';

  passthru = { inherit protocGen; };

  meta = {
    mainProgram = "protoc-gen-trevrpc-gleam";
    description = "Gleam binding for the TrevRPC Erlang runtime";
    license = lib.licenses.mit;
    homepage = "https://trev.zip/llc/TrevRPC";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases";
  };
})
