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
            PLAYWRIGHT_BROWSERS_PATH = "${pkgs.playwright-driver.browsers}";
            shellHook = ''
              ${pkgs.shellhook.ref}
              for chromium in "$PLAYWRIGHT_BROWSERS_PATH"/chromium-*/chrome-linux*/chrome; do
                export TREVRPC_BROWSER_CHROMIUM="$chromium"
                break
              done
            '';
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
              playwright-driver.browsers

              # kotlin / android
              jdk25
              gradle_9
              protobuf
              androidenv.androidPkgs.androidsdk

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
              ktlint
              treefmt

              # util
              bumper
              fix-hash
              jq
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
          browser-webtransport-soak = "nix develop -c bash -c 'npm --prefix trevrpc-js ci && npm --prefix trevrpc-js run test:browser:soak'";
          cross-runtime-lifecycle-stress = "nix develop -c bash -c 'mkdir -p target && go build -C trevrpc-go -o ../target/trevrpc-xruntime-go ./cmd/trevrpc-xruntime-go && TREVRPC_XRUNTIME_GO=$PWD/target/trevrpc-xruntime-go cargo test --manifest-path trevrpc-rust/Cargo.toml --test cross_runtime -- --ignored cross_runtime_lifecycle_stress --nocapture'";
          dev = "cargo run --manifest-path trevrpc-rust/Cargo.toml";
          test = "nix flake check";
        };

        # nix build [#...]
        packages = {
          rust-matrix = pkgs.rustPlatform.buildRustPackage (
            final: with pkgs.lib; {
              pname = "trevrpc-rust-matrix";
              version = "0.1.0";

              src = fileset.toSource {
                root = ./.;
                fileset = fileset.unions [
                  ./bench/rust-matrix
                  ./testdata/wire-golden-vectors.txt
                  ./trevrpc-rust
                ];
              };
              sourceRoot = "${final.src.name}/bench/rust-matrix";
              cargoLock.lockFile = ./bench/rust-matrix/Cargo.lock;
              nativeBuildInputs = with pkgs; [ protobuf ];

              installPhase = ''
                runHook preInstall
                peer=$(find target -path '*/release/trevrpc-rust-matrix-peer' -type f -perm -0100 | head -n1)
                reporter=$(find target -path '*/release/trevrpc-rust-matrix-report' -type f -perm -0100 | head -n1)
                install -Dm755 "$peer" $out/bin/trevrpc-rust-matrix-peer
                install -Dm755 "$reporter" $out/bin/trevrpc-rust-matrix-report
                runHook postInstall
              '';

              meta = {
                description = "Controlled Rust RPC benchmark matrix for TrevRPC";
                license = licenses.mit;
                platforms = platforms.linux;
              };
            }
          );

          trevrpc-c = pkgs.stdenv.mkDerivation (
            final: with pkgs.lib; {
              pname = "trevrpc-c";
              version = "0.1.0";

              src = fileset.toSource {
                root = ./.;
                fileset = fileset.unions [
                  ./testdata/wire-golden-vectors.txt
                  ./trevrpc-c
                ];
              };
              sourceRoot = "${final.src.name}/trevrpc-c";

              configurePhase = ''
                runHook preConfigure
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
                cmake --build build
                runHook postBuild
              '';

              checkPhase = ''
                runHook preCheck
                export HOME=$TMPDIR
                clang-format --dry-run --Werror $(find bench examples include src tests tools \( -name '*.c' -o -name '*.h' \))
                clang-tidy --quiet $(find bench examples src tests tools -name '*.c') -- \
                  -x c \
                  -std=c11 \
                  -Iinclude \
                  -Isrc \
                  -Ibuild/protoc-gen-trevrpc-c-protos \
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

              meta = {
                mainProgram = "protoc-gen-trevrpc-c";
                description = "C runtime and code generator for TrevRPC";
                license = licenses.mit;
                platforms = platforms.all;
                homepage = "https://trev.zip/llc/TrevRPC";
                changelog = "https://trev.zip/llc/TrevRPC/releases";
                downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/v${final.version}";
              };
            }
          );

          trevrpc-rust = pkgs.rustPlatform.buildRustPackage (
            final: with pkgs.lib; {
              pname = "trevrpc-rust";
              version = "0.1.0";

              src = fileset.toSource {
                root = ./.;
                fileset = fileset.unions [
                  ./testdata/wire-golden-vectors.txt
                  ./trevrpc-rust
                ];
              };
              sourceRoot = "${final.src.name}/trevrpc-rust";
              cargoLock.lockFile = ./trevrpc-rust/Cargo.lock;
              cargoBuildFlags = [ "--workspace" ];

              nativeCheckInputs = with pkgs; [
                rustfmt
                clippy
              ];
              checkPhase = ''
                cargo fmt --check
                cargo test --workspace --offline
                cargo clippy --workspace --all-targets --offline -- -D warnings
              '';

              meta = {
                mainProgram = "protoc-gen-trevrpc-rust";
                description = "Protobuf over QUIC, HTTP/3 & WebTransport";
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

              src = fileset.toSource {
                root = ./.;
                fileset = fileset.unions [
                  ./testdata/wire-golden-vectors.txt
                  ./trevrpc-go
                ];
              };
              sourceRoot = "${final.src.name}/trevrpc-go";
              vendorHash = "sha256-fRQKsZlO4lK4uJ1KKvNLqTO2F+RvckLz8gV8bNVfaHg=";
              subPackages = [
                "cmd/protoc-gen-trevrpc-go"
              ];

              nativeCheckInputs = with pkgs; [
                go-tools
                gotools
              ];
              checkPhase = ''
                export HOME=$(mktemp -d)
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

              src = fileset.toSource {
                root = ./.;
                fileset = fileset.unions [
                  ./testdata/wire-golden-vectors.txt
                  ./trevrpc-c
                  ./trevrpc-js
                ];
              };
              sourceRoot = "${final.src.name}/trevrpc-js";
              nodejs = pkgs.nodejs_24;

              npmConfigHook = pkgs.importNpmLock.npmConfigHook;
              npmDeps = pkgs.importNpmLock {
                npmRoot = ./trevrpc-js;
              };

              npmBuildScript = "build:native";
              dontUseCmakeConfigure = true;
              NODE_INCLUDE_DIR = "${pkgs.nodejs_24}/include/node";
              PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = "1";

              nativeBuildInputs = with pkgs; [
                cmake
              ];
              buildInputs = with pkgs; [
                libmsquic
              ];

              nativeCheckInputs = with pkgs; [
                clang-tools
                oxfmt
                oxlint
              ];
              checkPhase = ''
                rm -rf build
                oxfmt --check
                oxlint --deny-warnings
                npm run typecheck
                npm run build:native:test
                clang-format --dry-run --Werror native/trevrpc_node.c
                clang-tidy -p build/native native/trevrpc_node.c
                npm test
                npm run build:native
                npm run verify:native:production
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

          trevrpc-kotlin = pkgs.stdenvNoCC.mkDerivation (
            final: with pkgs.lib; {
              pname = "trevrpc-kotlin";
              version = "0.1.0";

              src = fileset.toSource {
                root = ./.;
                fileset = fileset.unions [
                  ./testdata/wire-golden-vectors.txt
                  ./trevrpc-kotlin
                  ./trevrpc-rust/crates/protoc-gen-trevrpc-rust/tests/proto/greeter.proto
                ];
              };
              sourceRoot = "${final.src.name}/trevrpc-kotlin";

              nativeBuildInputs = with pkgs; [
                gradle_9
                jdk25
                makeWrapper
                protobuf
              ];

              mitmCache = pkgs.gradle_9.fetchDeps {
                pkg = final.finalPackage;
                data = ./trevrpc-kotlin/gradle/deps.json;
                bwrapFlags = ''--ro-bind "$PWD" "$PWD" --dir /bin --symlink ${pkgs.runtimeShell} /bin/sh'';
              };
              __darwinAllowLocalNetworking = true;

              gradleFlags = [
                "-Dorg.gradle.java.home=${pkgs.jdk25.home}"
              ];
              gradleBuildTask = [
                "assemble"
                ":examples:installDist"
                ":protoc-gen-trevrpc-kotlin:installDist"
              ];
              gradleUpdateScript = ''
                runHook preBuild
                gradle build
              '';
              doCheck = true;
              gradleCheckTask = "check";

              installPhase = ''
                runHook preInstall
                mkdir -p $out/bin $out/share/trevrpc-kotlin
                cp -R examples/build/install/trevrpc-xruntime-kotlin/* $out/share/trevrpc-kotlin/
                cp -R protoc-gen-trevrpc-kotlin/build/install/protoc-gen-trevrpc-kotlin \
                  $out/share/trevrpc-kotlin/protoc-gen-trevrpc-kotlin
                makeWrapper $out/share/trevrpc-kotlin/bin/trevrpc-xruntime-kotlin \
                  $out/bin/trevrpc-xruntime-kotlin \
                  --set JAVA_HOME ${pkgs.jdk25.home} \
                  --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.jdk25 ]}
                makeWrapper \
                  $out/share/trevrpc-kotlin/protoc-gen-trevrpc-kotlin/bin/protoc-gen-trevrpc-kotlin \
                  $out/bin/protoc-gen-trevrpc-kotlin \
                  --set JAVA_HOME ${pkgs.jdk25.home} \
                  --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.jdk25 ]}
                runHook postInstall
              '';

              meta = {
                mainProgram = "protoc-gen-trevrpc-kotlin";
                description = "Kotlin TrevRPC runtime, transports, and protobuf generator";
                license = licenses.mit;
                platforms = [ "x86_64-linux" ];
                sourceProvenance = with sourceTypes; [
                  fromSource
                  binaryBytecode
                ];
                homepage = "https://trev.zip/llc/TrevRPC";
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
            ktlint
          ];
        };

        # nix flake check
        checks = pkgs.mkChecks {
          rust-matrix = self.packages.${system}.rust-matrix.overrideAttrs {
            installPhase = ''
              touch $out
            '';
          };

          rust-matrix-harness =
            pkgs.runCommand "trevrpc-rust-matrix-harness-check"
              {
                nativeBuildInputs = with pkgs; [
                  jq
                  openssl
                ];
              }
              ''
                PEER=${self.packages.${system}.rust-matrix}/bin/trevrpc-rust-matrix-peer \
                  bash ${./bench/test_rust_matrix_peer.sh}
                REPORTER=${self.packages.${system}.rust-matrix}/bin/trevrpc-rust-matrix-report \
                  bash ${./bench/test_run_rust_matrix_report.sh}
                touch $out
              '';

          c = self.packages.${system}.trevrpc-c.overrideAttrs {
            installPhase = ''
              touch $out
            '';
          };

          c-sanitizers = self.packages.${system}.trevrpc-c.overrideAttrs {
            configurePhase = ''
              runHook preConfigure
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

          kotlin = self.packages.${system}.trevrpc-kotlin.overrideAttrs {
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
                vendorHash = "sha256-fRQKsZlO4lK4uJ1KKvNLqTO2F+RvckLz8gV8bNVfaHg=";
                subPackages = [ "cmd/trevrpc-xruntime-go" ];

                meta.mainProgram = "trevrpc-xruntime-go";
              });
            in
            self.packages.${system}.trevrpc-rust.overrideAttrs {
              dontBuild = true;
              TREVRPC_XRUNTIME_GO = "${crossRuntimeGo}/bin/trevrpc-xruntime-go";
              TREVRPC_XRUNTIME_KOTLIN = "${self.packages.${system}.trevrpc-kotlin}/bin/trevrpc-xruntime-kotlin";
              checkPhase = ''
                cargo test --test cross_runtime --offline -- --nocapture --test-threads=1
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
                vendorHash = "sha256-fRQKsZlO4lK4uJ1KKvNLqTO2F+RvckLz8gV8bNVfaHg=";
                subPackages = [ "examples/greeter_server" ];

                meta.mainProgram = "greeter_server";
              });
              browserLifecycleGoServer = pkgs.buildGoModule (final: {
                pname = "trevrpc-browser-lifecycle-go-server";
                version = "0.1.0";

                src = ./trevrpc-go;
                vendorHash = "sha256-fRQKsZlO4lK4uJ1KKvNLqTO2F+RvckLz8gV8bNVfaHg=";
                subPackages = [ "cmd/trevrpc-browser-lifecycle-go" ];

                meta.mainProgram = "trevrpc-browser-lifecycle-go";
              });
              browserLifecycleRustServer = pkgs.rustPlatform.buildRustPackage (final: {
                pname = "trevrpc-browser-lifecycle-rust-server";
                version = "0.1.0";

                src = pkgs.lib.fileset.toSource {
                  root = ./.;
                  fileset = pkgs.lib.fileset.unions [
                    ./testdata/wire-golden-vectors.txt
                    ./trevrpc-rust
                  ];
                };
                sourceRoot = "${final.src.name}/trevrpc-rust";
                cargoLock.lockFile = ./trevrpc-rust/Cargo.lock;
                cargoBuildFlags = [
                  "--example"
                  "browser_lifecycle_server"
                ];
                doCheck = false;

                installPhase = ''
                  runHook preInstall
                  server=$(find target -path '*/release/examples/browser_lifecycle_server' -type f -perm -0100 | head -n1)
                  install -Dm755 "$server" $out/bin/trevrpc-browser-lifecycle-rust
                  runHook postInstall
                '';

                meta.mainProgram = "trevrpc-browser-lifecycle-rust";
              });
              browserRustServer = pkgs.rustPlatform.buildRustPackage (final: {
                pname = "trevrpc-browser-rust-server";
                version = "0.1.0";

                src = pkgs.lib.fileset.toSource {
                  root = ./.;
                  fileset = pkgs.lib.fileset.unions [
                    ./testdata/wire-golden-vectors.txt
                    ./trevrpc-rust
                  ];
                };
                sourceRoot = "${final.src.name}/trevrpc-rust";
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
              });
            in
            self.packages.${system}.trevrpc-js.overrideAttrs {
              dontBuild = true;
              PLAYWRIGHT_BROWSERS_PATH = "${pkgs.playwright-driver.browsers}";
              TREVRPC_BROWSER = "chromium";
              TREVRPC_BROWSER_GO_SERVER = "${browserGoServer}/bin/greeter_server";
              TREVRPC_BROWSER_LIFECYCLE_GO_SERVER = "${browserLifecycleGoServer}/bin/trevrpc-browser-lifecycle-go";
              TREVRPC_BROWSER_LIFECYCLE_RUST_SERVER = "${browserLifecycleRustServer}/bin/trevrpc-browser-lifecycle-rust";
              TREVRPC_BROWSER_LIFECYCLE_KOTLIN_SERVER = "${
                self.packages.${system}.trevrpc-kotlin
              }/bin/trevrpc-xruntime-kotlin";
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
