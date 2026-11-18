# The baselines' checks, which the flake names trevrpc-bench-baseline-<name>. The package runs the
# tests.
{
  golangci-lint,
  stdenv,
  self,
}:
{
  lint = {
    src = self.packages.${stdenv.hostPlatform.system}.trevrpc-bench-baseline;
    packages = [ golangci-lint ];
    script = ''
      golangci-lint run ./...
    '';
  };
}
