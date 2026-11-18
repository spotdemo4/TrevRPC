# The Rust workspace's checks, which the flake names trevrpc-rust-<name>. The package runs the tests.
{
  clippy,
  rustfmt,
  stdenv,
  self,
}:
{
  lint = {
    src = self.packages.${stdenv.hostPlatform.system}.trevrpc-rust;
    packages = [
      rustfmt
      clippy
    ];
    script = ''
      cargo fmt --all --check
      cargo clippy --workspace --all-targets --offline -- -D warnings
      cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
    '';
  };
}
