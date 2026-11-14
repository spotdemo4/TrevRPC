{
  callPackage,
  trevrpc-js,
}:
callPackage ../adapter.nix { inherit trevrpc-js; } {
  pname = "trevrpc-js-effect";
  directory = ./.;
  exports = [
    "createEffectClient"
    "toTrevRpcError"
  ];
  description = "Effect adapter for TrevRPC browser clients";
}
