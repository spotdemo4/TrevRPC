# The Kotlin client's checks, which the flake names trevrpc-kotlin-<name>. The package runs the
# tests.
{
  ktlint,
  stdenv,
  self,
}:
{
  # ktlint needs none of the dependencies, so it reads the sources instead of the package.
  lint = {
    root = ./.;
    filter = file: file.hasExt "kt" || file.hasExt "kts";
    packages = [ ktlint ];
    script = ''
      ktlint --relative "**/src/**/*.kt" "**/*.gradle.kts"
    '';
  };

  # The Android interop harness, which the android workflow runs on an emulator.
  interop-android = self.packages.${stdenv.hostPlatform.system}.trevrpc-kotlin.interopAndroid;
}
