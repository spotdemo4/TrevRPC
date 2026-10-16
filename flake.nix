{
  description = "Protobuf over QUIC, HTTP/3 & WebTransport";

  nixConfig = {
    extra-substituters = [
      "https://nix.trev.zip"
    ];
    extra-trusted-public-keys = [
      "trev:I39N/EsnHkvfmsbx8RUW+ia5dOzojTQNCTzKYij1chU="
    ];
  };

  inputs = {
    systems.url = "github:spotdemo4/systems";
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    trevpkgs = {
      url = "github:spotdemo4/trevpkgs";
      inputs.systems.follows = "systems";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      trevpkgs,
      ...
    }:
    trevpkgs.libs.mkFlake (
      system: pkgs:
      let
        wireGolden = ./testdata/wire-golden-vectors.txt;
        interopProto = ./interop/proto;
        packageSet = {
          trevrpc-go = pkgs.callPackage ./trevrpc-go { inherit wireGolden interopProto; };
          trevrpc-interop = pkgs.callPackage ./interop/runner.nix { };
          trevrpc-js = pkgs.callPackage ./trevrpc-js { inherit wireGolden interopProto; };
          trevrpc-kotlin = pkgs.callPackage ./trevrpc-kotlin {
            inherit wireGolden interopProto;
            licenseFile = ./LICENSE;
          };
          trevrpc-rust = pkgs.callPackage ./trevrpc-rust { inherit wireGolden interopProto; };
        };
      in
      {

        # nix develop [#...]
        devShells = {
          default = pkgs.mkShell {
            RUST_SRC_PATH = pkgs.rustPlatform.rustLibSrc;
            PLAYWRIGHT_BROWSERS_PATH = "${pkgs.playwright-driver.browsers}";
            shellHook = ''
              ${pkgs.shellhook.ref}
              for chromium in "$PLAYWRIGHT_BROWSERS_PATH"/chromium-*/chrome-linux*/chrome; do
                export TREVRPC_BROWSER_CHROMIUM="$chromium"
                break
              done
              for firefox in "$PLAYWRIGHT_BROWSERS_PATH"/firefox-*/firefox/firefox; do
                export TREVRPC_BROWSER_FIREFOX="$firefox"
                break
              done
              for webkit in "$PLAYWRIGHT_BROWSERS_PATH"/webkit-*/pw_run.sh; do
                export TREVRPC_BROWSER_WEBKIT="$webkit"
                break
              done
            '';
            packages = with pkgs; [
              # rust
              rustc
              cargo
              clippy
              cargo-audit
              rustfmt

              # go
              go
              gopls
              gotools
              go-tools
              protobuf

              # javascript
              nodejs_24
              playwright-driver.browsers
              oxlint
              oxfmt

              # kotlin / android
              jdk25
              gradle_9
              androidenv.androidPkgs.androidsdk
              kotlin-lsp
              ktlint

              # python
              python3
              ruff
              basedpyright

              # nix
              nixd
              nixfmt

              # util
              treefmt
              bumper
              fix-hash
              jq
              openssl
            ];
          };

          bump = pkgs.mkShell {
            packages = with pkgs; [
              bumper
            ];
          };

          release = pkgs.mkShell {
            packages = with pkgs; [
              flake-release
              # rust
              rustc
              cargo
              # go
              go
              # javascript
              nodejs_24
              # kotlin
              jdk25
              gradle_9
              python3
            ];
          };

          update = pkgs.mkShell {
            packages = with pkgs; [
              renovate
              # rust
              rustc
              cargo
              # go
              go
              fix-hash
              # javascript
              nodejs_24
              # kotlin
              jdk25
              gradle_9
            ];
          };

          vulnerable = pkgs.mkShell {
            packages = with pkgs; [
              # rust
              cargo-audit
              # go
              go
              govulncheck
              # javascript
              nodejs_24
              # nix
              flake-checker
              # actions
              zizmor
            ];
          };
        };

        # nix run [#...]
        apps = pkgs.mkApps {
          update-go-deps = {
            packages = with pkgs; [
              go
              fix-hash
            ];
            script = ''
              go -C trevrpc-go mod tidy
              fix-hash .#trevrpc-go
            '';
          };

          update-kotlin-deps = {
            packages = with pkgs; [
              androidenv.androidPkgs.androidsdk
              jdk25
              oxfmt
            ];
            script = ''
              export ANDROID_HOME=$(dirname "$(dirname "$(command -v sdkmanager)")")/libexec/android-sdk
              export ANDROID_SDK_ROOT=$ANDROID_HOME
              trevrpc-kotlin/gradlew \
                --project-dir trevrpc-kotlin \
                --no-configuration-cache \
                --refresh-dependencies \
                --write-locks \
                --write-verification-metadata sha256 \
                -PtrevrpcAndroidInterop=true \
                updateGradleDependencyState

              update_script=$(nix build .#trevrpc-kotlin.interopAndroid.mitmCache.updateScript --no-link --print-out-paths)
              USE_BWRAP=0 "$update_script"

              oxfmt --write trevrpc-kotlin/gradle/deps.json
            '';
          };

          sync-playwright = {
            packages = [ pkgs.nodejs_24 ];
            script = ''
              export PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
              npm install \
                --prefix trevrpc-js \
                --package-lock-only \
                --ignore-scripts \
                --no-audit \
                --no-fund \
                --save-dev \
                --save-exact \
                "playwright@${pkgs.playwright-driver.version}"
            '';
          };

        };

        # nix build [#...]
        packages = packageSet;

        # nix fmt
        formatter = pkgs.treefmt.withConfig {
          configFile = ./treefmt.toml;
          runtimeInputs = with pkgs; [
            rustfmt
            go
            nixfmt
            oxfmt
            ktlint
            ruff
          ];
        };

        # nix flake check
        checks = pkgs.mkChecks {
          go = packageSet.trevrpc-go;
          js = packageSet.trevrpc-js;
          kotlin = packageSet.trevrpc-kotlin;
          rust = packageSet.trevrpc-rust;
          interop = pkgs.callPackage ./interop {
            inherit (packageSet)
              trevrpc-go
              trevrpc-interop
              trevrpc-js
              trevrpc-rust
              ;
          };
          ${if packageSet.trevrpc-kotlin.interopAndroid != null then "kotlin-interop-android" else null} =
            packageSet.trevrpc-kotlin.interopAndroid;

          release-contract =
            pkgs.runCommand "trevrpc-release-contract"
              {
                nativeBuildInputs = with pkgs; [
                  bash
                  coreutils
                  git
                  gnugrep
                ];
              }
              ''
                set -euo pipefail
                export HOME="$TMPDIR/home"
                mkdir -p "$HOME"
                git config --global user.email release-contract@example.invalid
                git config --global user.name "Release Contract"

                helper=${./.forgejo/scripts/release-nix-package.sh}
                fake_bin="$TMPDIR/fake-bin"
                mkdir -p "$fake_bin"

                cat > "$fake_bin/nix" <<'SH'
                #!${pkgs.runtimeShell}
                set -euo pipefail
                if [ "$#" -ne 3 ] || [ "$1" != "eval" ] || [ "$2" != "--raw" ]; then
                  echo "unexpected nix invocation: $*" >&2
                  exit 1
                fi
                case "$3" in
                  .#packages.x86_64-linux.trevrpc-go.x86_64-unknown-linux-musl.version)
                    printf '%s' "$FAKE_GO_VERSION"
                    ;;
                  .#packages.x86_64-linux.trevrpc-rust.x86_64-unknown-linux-musl.version)
                    printf '%s' "$FAKE_RUST_VERSION"
                    ;;
                  *)
                    echo "unexpected Nix output: $3" >&2
                    exit 1
                    ;;
                esac
                SH
                chmod +x "$fake_bin/nix"

                cat > "$fake_bin/flake-release" <<'SH'
                #!${pkgs.runtimeShell}
                set -euo pipefail
                test -n "$RELEASE_LOG"
                printf '%s|%s\n' "$TAG" "$PACKAGES" >> "$RELEASE_LOG"
                SH
                chmod +x "$fake_bin/flake-release"

                export PATH="$fake_bin:$PATH"
                export FAKE_GO_VERSION=0.2.1
                export FAKE_RUST_VERSION=0.1.10
                export RELEASE_LOG="$TMPDIR/release.log"

                new_fixture() {
                  fixture=$(mktemp -d "$TMPDIR/fixture.XXXXXX")
                  git init --bare --quiet "$fixture/origin.git"
                  git init --quiet "$fixture/work"
                  printf 'initial\n' > "$fixture/work/state"
                  git -C "$fixture/work" add state
                  git -C "$fixture/work" commit --quiet -m initial
                  git -C "$fixture/work" remote add origin "$fixture/origin.git"
                  printf '%s\n' "$fixture/work"
                }

                add_commit() {
                  fixture=$1
                  message=$2
                  printf '%s\n' "$message" >> "$fixture/state"
                  git -C "$fixture" add state
                  git -C "$fixture" commit --quiet -m "$message"
                }

                push_fixture() {
                  git -C "$1" push --quiet --force --tags origin HEAD:refs/heads/main
                }

                set_event() {
                  export GITHUB_REF_NAME=$1
                  export GITHUB_REF="refs/tags/$1"
                }

                run_release() {
                  fixture=$1
                  package=$2
                  (cd "$fixture" && bash "$helper" "$package")
                }

                expect_failure() {
                  if "$@"; then
                    echo "command unexpectedly succeeded: $*" >&2
                    exit 1
                  fi
                }

                fixture=$(new_fixture)
                git -C "$fixture" tag trevrpc-go/v0.2.1
                git -C "$fixture" tag trevrpc-go/tools/v9.9.9
                push_fixture "$fixture"
                set_event trevrpc-go/v0.2.1
                : > "$RELEASE_LOG"
                TAG=untrusted PACKAGES=untrusted run_release "$fixture" trevrpc-go
                test "$(wc -l < "$RELEASE_LOG")" -eq 1
                grep -Fx 'trevrpc-go/v0.2.1|packages.x86_64-linux.trevrpc-go.x86_64-unknown-linux-musl' "$RELEASE_LOG"

                fixture=$(new_fixture)
                git -C "$fixture" tag trevrpc-rust/v0.1.10
                git -C "$fixture" tag trevrpc-go/v9.9.9
                push_fixture "$fixture"
                set_event trevrpc-rust/v0.1.10
                : > "$RELEASE_LOG"
                run_release "$fixture" trevrpc-rust
                test "$(wc -l < "$RELEASE_LOG")" -eq 1
                grep -Fx 'trevrpc-rust/v0.1.10|packages.x86_64-linux.trevrpc-rust.x86_64-unknown-linux-musl' "$RELEASE_LOG"

                fixture=$(new_fixture)
                git -C "$fixture" tag trevrpc-go/v9.9.9
                push_fixture "$fixture"
                set_event trevrpc-go/v9.9.9
                : > "$RELEASE_LOG"
                expect_failure run_release "$fixture" trevrpc-go
                test ! -s "$RELEASE_LOG"

                fixture=$(new_fixture)
                git -C "$fixture" tag trevrpc-go/v0.2.1
                add_commit "$fixture" newer-head
                push_fixture "$fixture"
                set_event trevrpc-go/v0.2.1
                : > "$RELEASE_LOG"
                expect_failure run_release "$fixture" trevrpc-go
                test ! -s "$RELEASE_LOG"

                fixture=$(new_fixture)
                git -C "$fixture" tag trevrpc-go/v0.2.1
                git -C "$fixture" tag trevrpc-rust/v0.1.10
                push_fixture "$fixture"
                set_event trevrpc-rust/v0.1.10
                : > "$RELEASE_LOG"
                expect_failure run_release "$fixture" trevrpc-go
                test ! -s "$RELEASE_LOG"

                fixture=$(new_fixture)
                git -C "$fixture" tag trevrpc-go/tools/v0.2.1
                push_fixture "$fixture"
                set_event trevrpc-go/tools/v0.2.1
                : > "$RELEASE_LOG"
                expect_failure run_release "$fixture" trevrpc-go
                test ! -s "$RELEASE_LOG"

                fixture=$(new_fixture)
                git -C "$fixture" tag v1.0.0
                push_fixture "$fixture"
                set_event v1.0.0
                : > "$RELEASE_LOG"
                expect_failure run_release "$fixture" trevrpc-go
                test ! -s "$RELEASE_LOG"

                fixture=$(new_fixture)
                git -C "$fixture" tag trevrpc-go/v0.2.1
                push_fixture "$fixture"
                export GITHUB_REF_NAME=trevrpc-go/v0.2.1
                export GITHUB_REF=refs/heads/main
                : > "$RELEASE_LOG"
                expect_failure run_release "$fixture" trevrpc-go
                test ! -s "$RELEASE_LOG"

                fixture=$(new_fixture)
                push_fixture "$fixture"
                set_event trevrpc-go/v0.2.1
                : > "$RELEASE_LOG"
                expect_failure run_release "$fixture" trevrpc-go
                expect_failure run_release "$fixture" trevrpc-python
                test ! -s "$RELEASE_LOG"

                touch $out
              '';

          nix = {
            root = ./.;
            filter = file: file.hasExt "nix";
            packages = with pkgs; [
              nixfmt
            ];
            script = ''
              nixfmt --check "$file"
            '';
          };

          shell = {
            root = ./.;
            filter = file: file.hasExt "sh";
            packages = with pkgs; [
              shellcheck
            ];
            script = ''
              shellcheck "$file"
            '';
          };

          python =
            let
              # basedpyright's Nixpkgs build-time test closure pulls in ffmpeg-headless,
              # whose FATE tests fail on aarch64-darwin; retain Ruff there.
              enableBasedpyright = !pkgs.stdenv.hostPlatform.isDarwin;
            in
            {
              root = ./.;
              filter = file: file.hasExt "py" || file.name == "pyrightconfig.json";
              packages = [ pkgs.ruff ] ++ pkgs.lib.optional enableBasedpyright pkgs.basedpyright;
              script = ''
                ruff check
                ${pkgs.lib.optionalString enableBasedpyright "basedpyright"}
              '';
            };

          actions-gh = {
            root = ./.github/workflows;
            filter = file: file.hasExt "yaml";
            packages = with pkgs; [
              action-validator
              actionlint
              zizmor
            ];
            script = ''
              action-validator "$file"
              actionlint "$file"
              zizmor --offline "$file"
            '';
          };

          actions-fj = {
            root = ./.forgejo/workflows;
            filter = file: file.hasExt "yaml";
            packages = with pkgs; [
              forgejo-runner
              zizmor
            ];
            script = ''
              forgejo-runner validate --workflow --path "$file"
              zizmor --offline "$file"
            '';
          };

          # The config is platform-independent, and renovate is often a slow uncached build on Darwin.
          ${if pkgs.stdenv.hostPlatform.isLinux then "renovate" else null} = {
            root = ./.forgejo;
            files = ./.forgejo/renovate.json;
            packages = with pkgs; [
              renovate
            ];
            script = ''
              renovate-config-validator renovate.json
            '';
          };

          config = {
            root = ./.;
            filter = file: file.hasExt "json" || file.hasExt "yaml" || file.hasExt "toml" || file.hasExt "md";
            packages = with pkgs; [
              oxfmt
            ];
            script = ''
              oxfmt --check
            '';
          };

        };
      }
    );
}
