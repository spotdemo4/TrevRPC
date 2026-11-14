{
  callPackage,
  trevrpc-js,
  interopProto,
}:
callPackage ../adapter.nix { inherit trevrpc-js; } {
  pname = "trevrpc-js-protovalidate";
  directory = ./.;
  # The buf.validate schema that the test protos import.
  files = [ (interopProto + "/buf/validate/validate.proto") ];
  exports = [ "createRequestValidator" ];
  importCheck = ''
    assert.equal(typeof adapter.createRequestValidator().validateFunc, "function");
  '';
  description = "protovalidate request validation for TrevRPC browser clients";
}
