# The Go modules' checks, which the flake names trevrpc-go-<name>. The packages run the tests.
{
  golangci-lint,
  stdenv,
  self,
}:
let
  packages = self.packages.${stdenv.hostPlatform.system};
in
{
  lint = {
    src = packages.trevrpc-go;
    packages = [ golangci-lint ];
    script = ''
      golangci-lint run ./...
    '';
  };

  qmux-lint = {
    src = packages.trevrpc-go-qmux;
    packages = [ golangci-lint ];
    script = ''
      golangci-lint run ./...
    '';
  };

  otel-lint = {
    src = packages.trevrpc-go-otel;
    packages = [ golangci-lint ];
    script = ''
      golangci-lint run ./...
    '';
  };

  interop-lint = {
    src = packages.trevrpc-go-interop;
    packages = [ golangci-lint ];
    script = ''
      golangci-lint run ./...
    '';
  };

  protovalidate-lint = {
    src = packages.trevrpc-go-protovalidate;
    packages = [ golangci-lint ];
    script = ''
      golangci-lint run ./...
    '';
  };
}
