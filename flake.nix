{
  description = "Protobuf over QUIC & WebTransport";

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
      self,
      trevpkgs,
      ...
    }:
    trevpkgs.libs.mkFlake (
      system: pkgs: {

        # nix develop [#...]
        devShells = {
          default = pkgs.mkShell {
            RUST_SRC_PATH = pkgs.rustPlatform.rustLibSrc;
            shellHook = pkgs.shellhook.ref;
            packages = with pkgs; [
              # rust
              rustc
              cargo

              # go
              go
              gopls
              gotools
              protobuf

              # c
              cmake
              gcc
              clang-tools
              openssl
              pkg-config
              protobufc
              libmsquic

              # javascript
              nodejs_24

              # lint
              clippy
              cargo-audit
              go-tools
              oxlint
              nixd
              nil

              # format
              rustfmt
              nixfmt
              oxfmt
              treefmt

              # util
              bumper
              fix-hash
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
          dev = "cargo run --manifest-path trevrpc-rust/Cargo.toml";
          test = "nix flake check";
        };

        # nix build [#...]
        packages = {
          trevrpc-c = pkgs.stdenv.mkDerivation {
            pname = "trevrpc-c";
            version = "0.1.0";
            src = ./trevrpc-c;

            configurePhase = ''
              runHook preConfigure
              export PATH=${self.packages.${system}.trevrpc-go}/bin:$PATH
              mkdir -p ../testdata
              cp ${./testdata/wire-golden-vectors.txt} ../testdata/wire-golden-vectors.txt
              cmake -S . -B build -DTREVRPC_BUILD_TESTS=ON
              runHook postConfigure
            '';

            nativeBuildInputs = with pkgs; [
              clang-tools
              cmake
              openssl
              protobuf
              protobufc
            ];
            buildInputs = with pkgs; [
              libmsquic
              protobufc
            ];
            buildPhase = ''
              runHook preBuild
              export GOCACHE=$TMPDIR/go-cache
              export GOMODCACHE=$TMPDIR/go-mod-cache
              cmake --build build
              runHook postBuild
            '';

            doCheck = true;
            checkPhase = ''
              runHook preCheck
              export HOME=$TMPDIR
              clang-format --dry-run --Werror $(find bench examples include src tests \( -name '*.c' -o -name '*.h' \))
              clang-tidy --quiet $(find bench examples src tests -name '*.c') -- \
                -x c \
                -std=c11 \
                -Iinclude \
                -Isrc \
                -Ibuild/generated-service-test \
                -Ibuild/generated-greeter-example \
                -isystem ${pkgs.libmsquic}/include
              ctest --test-dir build --output-on-failure
              runHook postCheck
            '';

            installPhase = ''
              runHook preInstall
              cmake --install build --prefix $out
              runHook postInstall
            '';
          };

          trevrpc-rust = pkgs.rustPlatform.buildRustPackage (
            final: with pkgs.lib; {
              pname = "trevrpc-rust";
              version = "0.1.0";

              src = ./trevrpc-rust;
              cargoLock.lockFile = ./trevrpc-rust/Cargo.lock;
              cargoBuildFlags = [ "--workspace" ];

              nativeCheckInputs = with pkgs; [
                rustfmt
                clippy
              ];
              checkPhase = ''
                mkdir -p ../testdata
                cp ${./testdata/wire-golden-vectors.txt} ../testdata/wire-golden-vectors.txt
                cargo fmt --check
                cargo test --workspace --offline
                cargo clippy --workspace --all-targets --offline -- -D warnings
              '';

              meta = {
                mainProgram = "protoc-gen-trevrpc-rust";
                description = "Protobuf over QUIC & WebTransport";
                license = licenses.mit;
                platforms = platforms.all;
                homepage = "https://trev.zip/llc/TrevRPC";
                changelog = "https://trev.zip/llc/TrevRPC/releases";
                downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/v${final.version}";
              };
            }
          );

          trevrpc-go = pkgs.buildGoModule (
            final: with pkgs.lib; {
              pname = "trevrpc-go";
              version = "0.1.0";

              src = ./trevrpc-go;
              vendorHash = "sha256-iE1MhGE0XVsNR1bh5ETnKNEdiQafriz1eI9gAZN5uHk=";
              subPackages = [
                "cmd/protoc-gen-trevrpc-c"
                "cmd/protoc-gen-trevrpc-go"
              ];

              nativeCheckInputs = with pkgs; [
                go-tools
                gotools
              ];
              checkPhase = ''
                export HOME=$(mktemp -d)
                mkdir -p ../testdata
                cp ${./testdata/wire-golden-vectors.txt} ../testdata/wire-golden-vectors.txt
                go test ./...
                go vet ./...
                staticcheck ./...
                modernize ./...
              '';

              meta = {
                mainProgram = "protoc-gen-trevrpc-go";
                description = "Go runtime and code generator for TrevRPC";
                license = licenses.mit;
                platforms = platforms.all;
                homepage = "https://trev.zip/llc/TrevRPC";
                changelog = "https://trev.zip/llc/TrevRPC/releases";
                downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/v${final.version}";
              };
            }
          );

          trevrpc-js = pkgs.buildNpmPackage (
            final: with pkgs.lib; {
              pname = "trevrpc-js";
              version = "0.1.0";

              src = ./trevrpc-js;
              nodejs = pkgs.nodejs_24;

              npmConfigHook = pkgs.importNpmLock.npmConfigHook;
              npmDeps = pkgs.importNpmLock {
                npmRoot = final.src;
              };

              npmBuildScript = "build:native";
              dontUseCmakeConfigure = true;
              TREVRPC_C_ROOT = ./trevrpc-c;
              NODE_INCLUDE_DIR = "${pkgs.nodejs_24}/include/node";
              PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = "1";

              nativeBuildInputs = with pkgs; [
                cmake
              ];
              nativeCheckInputs = with pkgs; [
                oxfmt
                oxlint
              ];
              buildInputs = with pkgs; [
                libmsquic
              ];
              checkPhase = ''
                mkdir -p ../testdata
                cp ${./testdata/wire-golden-vectors.txt} ../testdata/wire-golden-vectors.txt
                rm -rf build
                oxfmt --check
                oxlint --deny-warnings
                npm run typecheck
                npm run build:native
                npm test
              '';

              meta = {
                mainProgram = "protoc-gen-trevrpc-js";
                description = "JavaScript WebTransport runtime and code generator for TrevRPC";
                license = licenses.mit;
                platforms = platforms.all;
                badPlatforms = [ systems.inspect.platformPatterns.isStatic ];
                homepage = "https://trev.zip/llc/TrevRPC";
                changelog = "https://trev.zip/llc/TrevRPC/releases";
                downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/v${final.version}";
              };
            }
          );
        };

        # nix fmt
        formatter = pkgs.treefmt.withConfig {
          configFile = ./treefmt.toml;
          runtimeInputs = with pkgs; [
            rustfmt
            go
            clang-tools
            nixfmt
            oxfmt
          ];
        };

        # nix flake check
        checks = pkgs.mkChecks {
          c = self.packages.${system}.trevrpc-c.overrideAttrs {
            installPhase = ''
              touch $out
            '';
          };

          c-sanitizers = self.packages.${system}.trevrpc-c.overrideAttrs {
            configurePhase = ''
              runHook preConfigure
              export PATH=${self.packages.${system}.trevrpc-go}/bin:$PATH
              mkdir -p ../testdata
              cp ${./testdata/wire-golden-vectors.txt} ../testdata/wire-golden-vectors.txt
              cmake -S . -B build -DTREVRPC_BUILD_TESTS=ON -DTREVRPC_ENABLE_SANITIZERS=ON
              runHook postConfigure
            '';
            installPhase = ''
              touch $out
            '';
          };

          rust = self.packages.${system}.trevrpc-rust.overrideAttrs {
            dontBuild = true;
            installPhase = ''
              touch $out
            '';
          };

          go = self.packages.${system}.trevrpc-go.overrideAttrs {
            dontBuild = true;
            installPhase = ''
              touch $out
            '';
          };

          js = self.packages.${system}.trevrpc-js.overrideAttrs {
            dontBuild = true;
            installPhase = ''
              touch $out
            '';
          };

          cross-runtime =
            let
              crossRuntimeGo = pkgs.buildGoModule (final: {
                pname = "trevrpc-cross-runtime-go";
                version = "0.1.0";

                src = ./trevrpc-go;
                vendorHash = "sha256-iE1MhGE0XVsNR1bh5ETnKNEdiQafriz1eI9gAZN5uHk=";
                subPackages = [ "cmd/trevrpc-xruntime-go" ];

                meta.mainProgram = "trevrpc-xruntime-go";
              });
            in
            self.packages.${system}.trevrpc-rust.overrideAttrs {
              dontBuild = true;
              TREVRPC_XRUNTIME_GO = "${crossRuntimeGo}/bin/trevrpc-xruntime-go";
              checkPhase = ''
                cargo test --test cross_runtime --offline -- --nocapture
              '';
              installPhase = ''
                touch $out
              '';
            };

          browser-webtransport =
            let
              browserGoServer = pkgs.buildGoModule (final: {
                pname = "trevrpc-browser-go-server";
                version = "0.1.0";

                src = ./trevrpc-go;
                vendorHash = "sha256-iE1MhGE0XVsNR1bh5ETnKNEdiQafriz1eI9gAZN5uHk=";
                subPackages = [ "examples/greeter_server" ];

                meta.mainProgram = "greeter_server";
              });
              browserLifecycleGoServer = pkgs.buildGoModule (final: {
                pname = "trevrpc-browser-lifecycle-go-server";
                version = "0.1.0";

                src = ./trevrpc-go;
                vendorHash = "sha256-iE1MhGE0XVsNR1bh5ETnKNEdiQafriz1eI9gAZN5uHk=";
                subPackages = [ "cmd/trevrpc-browser-lifecycle-go" ];

                meta.mainProgram = "trevrpc-browser-lifecycle-go";
              });
              browserRustServer = pkgs.rustPlatform.buildRustPackage {
                pname = "trevrpc-browser-rust-server";
                version = "0.1.0";

                src = ./trevrpc-rust;
                cargoLock.lockFile = ./trevrpc-rust/Cargo.lock;
                cargoBuildFlags = [
                  "--example"
                  "greeter_server"
                ];
                doCheck = false;

                installPhase = ''
                  runHook preInstall
                  server=$(find target -path '*/release/examples/greeter_server' -type f -perm -0100 | head -n1)
                  install -Dm755 "$server" $out/bin/greeter_server
                  runHook postInstall
                '';

                meta.mainProgram = "greeter_server";
              };
            in
            self.packages.${system}.trevrpc-js.overrideAttrs {
              dontBuild = true;
              PLAYWRIGHT_BROWSERS_PATH = "${pkgs.playwright-driver.browsers}";
              TREVRPC_BROWSER_GO_SERVER = "${browserGoServer}/bin/greeter_server";
              TREVRPC_BROWSER_LIFECYCLE_GO_SERVER = "${browserLifecycleGoServer}/bin/trevrpc-browser-lifecycle-go";
              TREVRPC_BROWSER_RUST_SERVER = "${browserRustServer}/bin/greeter_server";
              checkPhase = ''
                export HOME=$(mktemp -d)
                for chromium in ${pkgs.playwright-driver.browsers}/chromium-*/chrome-linux*/chrome; do
                  export TREVRPC_BROWSER_CHROMIUM="$chromium"
                  break
                done
                npm run test:browser
              '';
              installPhase = ''
                touch $out
              '';
            };

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

          actions-gh = {
            root = ./.github/workflows;
            filter = file: file.hasExt "yaml";
            packages = with pkgs; [
              action-validator
              zizmor
            ];
            script = ''
              action-validator "$file"
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

          renovate = {
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
