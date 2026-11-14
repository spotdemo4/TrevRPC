# Builds one of the repository's Go modules with mkGoModule and checks it: its tests, a vet for
# 32-bit targets and any benchmarks, then its module path and main program. What differs between
# the modules is passed in; other arguments go to mkGoModule as they are.
{
  lib,
  mkGoModule,
}:
let
  root = ../.;
in
{
  # The module's directory, which holds its gomod2nix.toml.
  directory,
  # The files of the source: the module, the modules its go.mod replaces and the shared files its
  # tests read. The Nix files and the gomod2nix.toml lock files are left out.
  fileset,
  # The tests, before the vet.
  tests ? ''
    go test ./...
  '',
  vet32Bit ? true,
  # Run after the vet.
  benchmarks ? "",
  # A pattern of the usage message that meta.mainProgram prints when it fails without a subcommand,
  # for executables that need one.
  usage ? null,
  meta,
  ...
}@args:
let
  # The module's directory within the repository, which its module path ends with.
  path = lib.removePrefix "./" (lib.path.removePrefix root directory);
  mainProgram = meta.mainProgram or null;
in
mkGoModule (
  final:
  removeAttrs args [
    "directory"
    "fileset"
    "tests"
    "vet32Bit"
    "benchmarks"
    "usage"
  ]
  // {
    src = lib.fileset.toSource {
      inherit root;
      fileset = lib.fileset.difference fileset (
        lib.fileset.fileFilter (file: file.hasExt "nix" || file.name == "gomod2nix.toml") root
      );
    };
    sourceRoot = "${final.src.name}/${path}";
    modules = directory + "/gomod2nix.toml";

    doCheck = true;
    checkPhase = ''
      runHook preCheck
      export HOME=$(mktemp -d)
    ''
    + tests
    + lib.optionalString vet32Bit ''
      # int is 32 bits wide on 386 and arm. A cgo build would need a 32-bit C
      # toolchain, and PIE needs cgo there. GOOS is pinned because the host's
      # may have no 32-bit port (darwin/386 does not exist).
      CGO_ENABLED=0 GOOS=linux GOARCH=386 go vet -buildmode=default ./...
    ''
    + benchmarks
    + ''
      runHook postCheck
    '';

    doInstallCheck = true;
    installCheckPhase = ''
      runHook preInstallCheck
      test "$(go list -m)" = 'trev.zip/llc/trevrpc/${path}'
    ''
    + lib.optionalString (mainProgram != null) ''
      test -x "$out/bin/${mainProgram}"
    ''
    + lib.optionalString (usage != null) ''
      if "$out/bin/${mainProgram}" 2> usage.out; then
        echo "${mainProgram} without a subcommand must fail" >&2
        exit 1
      fi
      grep -q '${usage}' usage.out
    ''
    + ''
      runHook postInstallCheck
    '';

    meta = {
      license = lib.licenses.mit;
      platforms = lib.platforms.all;
      homepage = "https://trev.zip/llc/TrevRPC";
      sourceProvenance = [ lib.sourceTypes.fromSource ];
      maintainers = [ lib.maintainers.spotdemo4 ];
    }
    // meta;
  }
)
