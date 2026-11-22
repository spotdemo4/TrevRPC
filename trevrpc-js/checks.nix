# The npm packages' checks, which the flake names trevrpc-js-<name>. The packages run the tests.
{
  callPackage,
  oxfmt,
  oxlint,
  stdenv,
  typescript,
  self,
}:
let
  packages = self.packages.${stdenv.hostPlatform.system};
in
{
  # oxlint's type-aware rules and tsc read the interop clients the build generates.
  lint = {
    src = packages.trevrpc-js;
    packages = [
      oxfmt
      oxlint
    ];
    script = ''
      oxfmt --check
      npm run build
      oxlint --deny-warnings
      npm exec -- tsc -p tsconfig.json
    '';
  };

  # oxlint's type-aware rules and tsc read the test client the build generates. `npm run lint` gives
  # oxlint the adapter's own directories, so it doesn't follow the link to trevrpc-js.
  effect-lint = {
    src = packages.trevrpc-js-effect;
    packages = [
      oxfmt
      oxlint
    ];
    script = ''
      oxfmt --check
      npm run build
      npm run lint
      npm exec -- tsc -p tsconfig.json
    '';
  };

  # As effect-lint.
  protovalidate-lint = {
    src = packages.trevrpc-js-protovalidate;
    packages = [
      oxfmt
      oxlint
    ];
    script = ''
      oxfmt --check
      npm run build
      npm run lint
      npm exec -- tsc -p tsconfig.json
    '';
  };

  # The package has no dependencies, so tsc comes from nixpkgs; its tsconfig maps @trevrpc/qmux to
  # its declarations for the type tests.
  qmux-lint = {
    src = packages.trevrpc-js-qmux;
    packages = [
      oxfmt
      oxlint
      typescript
    ];
    script = ''
      oxfmt --check
      oxlint --deny-warnings src test
      tsc -p tsconfig.json
    '';
  };

  protoc-gen-lint = {
    src = packages.trevrpc-js.protocGen;
    packages = [
      oxfmt
      oxlint
    ];
    script = ''
      oxfmt --check
      npm run lint
    '';
  };

  # The npm tarballs as the release job packs them, from the sources and not the builds.
  release = callPackage ./release-check.nix {
    inherit (packages)
      trevrpc-js
      trevrpc-js-effect
      trevrpc-js-protovalidate
      trevrpc-js-qmux
      ;
  };
}
