# The Gleam code protoc-gen-trevrpc-gleam generates from proto/: it compiles,
# its tests round-trip every kind of field through gpb, and protoc decodes the
# bytes they encode to what expected/ says.
{
  lib,
  beamMinimal29Packages,
  mkGleamPackage,
  protobuf,
  protocGen,
}:

mkGleamPackage (finalAttrs: {
  pname = "protoc-gen-trevrpc-gleam-fixture";
  inherit (protocGen) version;

  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [
      ./gleam.toml
      ./manifest.toml
      ./proto
      ./test
      ./expected
      ./generate.sh
      ./compare.sh
    ];
  };

  gleamLibrary = true;
  # The runtime needs Erlang/OTP 29.
  beamPackages = beamMinimal29Packages;
  nativeBuildInputs = [
    protobuf
    protocGen
  ];

  # gleam build compiles the generated code.
  preBuild = ''
    PROTOC_GEN_TREVRPC_GLEAM=${lib.getExe' protocGen "protoc-gen-trevrpc-gleam"} bash generate.sh
  '';

  postCheck = ''
    bash compare.sh
  '';
})
