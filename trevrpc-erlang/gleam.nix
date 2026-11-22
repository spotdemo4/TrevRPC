# The trevrpc package as Gleam builds it, which is how gleam publish builds it for Hex: the
# runtime's Erlang modules and the Gleam binding, its docs, and the Gleam tests, a Gleam server and
# client on loopback through the code protoc-gen-trevrpc-gleam generates. default.nix builds and
# tests the runtime with rebar3; this is its passthru.gleam.
{
  lib,
  beamMinimal29Packages,
  mkGleamPackage,
  protobuf,
  trevrpcProto,
  # protoc-gen-trevrpc-gleam, which generates the tests' service.
  protocGen,
  version,
}:

mkGleamPackage (finalAttrs: {
  pname = "trevrpc-erlang-gleam";
  inherit version;

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      trevrpcProto
      (lib.fileset.difference ./. (
        lib.fileset.unions [
          ./checks.nix
          ./default.nix
          ./deps.nix
          ./gleam.nix
          ./erlang_deps.py
          ./test_erlang_deps.py
          (lib.fileset.maybeMissing ./_build)
          (lib.fileset.maybeMissing ./build)
          (lib.fileset.maybeMissing ./test/greet)
          # Packages of their own.
          ./interop
          ./protoc-gen-trevrpc-erlang
          ./protoc-gen-trevrpc-gleam
          ./protoc-gen-trevrpc-elixir
        ]
      ))
    ];
  };
  # sourceRoot, not gleamRoot, so that flake-release finds gleam.toml there; the rest of the
  # source, with trevrpc/options.proto, is still unpacked beside it.
  sourceRoot = "${finalAttrs.src.name}/trevrpc-erlang";

  gleamLibrary = true;
  # The runtime needs Erlang/OTP 29.
  beamPackages = beamMinimal29Packages;
  nativeBuildInputs = [ protobuf ];

  # gleam build compiles the tests too, with the test service's code.
  preBuild = ''
    PROTOC_GEN_TREVRPC_GLEAM=${lib.getExe protocGen} bash gleam-generate.sh
  '';

  meta = {
    description = "Erlang runtime and Gleam binding for TrevRPC, built by Gleam";
    license = lib.licenses.mit;
    homepage = "https://trev.zip/llc/TrevRPC";
  };
})
