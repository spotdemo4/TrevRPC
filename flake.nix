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
              grpc
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
          update-kotlin-deps = {
            packages = [ pkgs.oxfmt ];
            script = ''
              USE_BWRAP=0 ${self.packages.${system}.trevrpc-kotlin.mitmCache.updateScript}
              oxfmt --write trevrpc-kotlin/gradle/deps.json
            '';
          };
        };

        # nix build [#...]
        packages =
          let
            bench = pkgs.callPackage ./bench {
              sourceCommit = self.rev or (self.dirtyRev or "unknown");
              sourceDirty = if self ? rev then "false" else "true";
            };
            c = pkgs.callPackage ./trevrpc-c {
              repoRoot = ./.;
            };
            cBenchPeer = c.override { benchPeer = true; };
            cpp = pkgs.callPackage ./trevrpc-cpp {
              repoRoot = ./.;
              trevrpcC = c;
            };
            cppBenchPeer = cpp.override { benchPeer = true; };
            go = pkgs.callPackage ./trevrpc-go {
              repoRoot = ./.;
            };
            goBenchPeer = go.override { benchPeer = true; };
            js = pkgs.callPackage ./trevrpc-js {
              repoRoot = ./.;
              trevrpcC = c;
            };
            jsBenchPeer = js.override { benchPeer = true; };
            kotlin = pkgs.callPackage ./trevrpc-kotlin {
              repoRoot = ./.;
            };
            kotlinBenchPeer = kotlin.override { benchPeer = true; };
            rust = pkgs.callPackage ./trevrpc-rust {
              repoRoot = ./.;
            };
            rustBenchPeer = rust.override { benchPeer = true; };
          in
          {
            trevrpc-bench = bench;
            trevrpc-c = c;
            trevrpc-c-bench-peer = cBenchPeer;
            trevrpc-cpp = cpp;
            trevrpc-cpp-bench-peer = cppBenchPeer;
            trevrpc-go = go;
            trevrpc-go-bench-peer = goBenchPeer;
            trevrpc-js = js;
            trevrpc-js-bench-peer = jsBenchPeer;
            trevrpc-kotlin = kotlin;
            trevrpc-kotlin-bench-peer = kotlinBenchPeer;
            trevrpc-rust = rust;
            trevrpc-rust-bench-peer = rustBenchPeer;

            trevrpc-bench-suite = pkgs.symlinkJoin {
              name = "trevrpc-bench-suite";
              paths = [
                bench
                cBenchPeer
                cppBenchPeer
                goBenchPeer
                jsBenchPeer
                kotlinBenchPeer
                rustBenchPeer
              ];
              meta.platforms = [ "x86_64-linux" ];
            };
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
          benchmark-controller = self.packages.${system}.trevrpc-bench;

          benchmark-netns-config =
            pkgs.runCommand "trevrpc-benchmark-netns-config"
              {
                nativeBuildInputs = [ self.packages.${system}.trevrpc-bench ];
              }
              ''
                trevrpc-bench validate ${./bench/campaigns/netns-smoke.example.json}
                touch $out
              '';

          c = self.packages.${system}.trevrpc-c;
          c-sanitizers = self.packages.${system}.trevrpc-c.override {
            sanitizers = true;
          };

          cpp = self.packages.${system}.trevrpc-cpp;
          rust = self.packages.${system}.trevrpc-rust;
          go = self.packages.${system}.trevrpc-go;
          js = self.packages.${system}.trevrpc-js;
          kotlin = self.packages.${system}.trevrpc-kotlin;

          benchmark-proto-sync =
            pkgs.runCommand "trevrpc-benchmark-proto-sync"
              {
                nativeBuildInputs = with pkgs; [
                  go
                  protobuf
                  protoc-gen-go
                  protoc-gen-go-grpc
                ];
              }
              ''
                   cmp ${./bench/proto/benchmark.proto} ${./trevrpc-c/bench/proto/benchmark.proto}
                   cmp ${./bench/proto/benchmark.proto} ${./trevrpc-kotlin/bench-peer/src/main/proto/benchmark.proto}
                   cmp ${./bench/proto/benchmark.proto} ${./trevrpc-rust/crates/trevrpc-bench-peer/proto/benchmark.proto}
                mkdir generated
                protoc \
                  --proto_path=${./bench/proto} \
                  --go_out=generated \
                  --go_opt=paths=source_relative \
                  ${./bench/proto}/benchmark.proto
                # The packaged plugin was built with an older Go toolchain, which
                # selects the equivalent pre-TypeFor reflection expression.
                substituteInPlace generated/benchmark.pb.go \
                  --replace-fail 'reflect.TypeOf(x{}).PkgPath()' 'reflect.TypeFor[x]().PkgPath()'
                 cmp generated/benchmark.pb.go ${./trevrpc-go/cmd/trevrpc-bench-peer/benchmarkpb/benchmark.pb.go}
                 protoc \
                   --proto_path=${./bench/proto} \
                   --go-grpc_out=generated \
                   --go-grpc_opt=paths=source_relative \
                   ${./bench/proto}/benchmark.proto
                 substituteInPlace generated/benchmark_grpc.pb.go \
                   --replace-fail 'interface{}' 'any'
                 gofmt -w generated/benchmark_grpc.pb.go
                 cmp generated/benchmark_grpc.pb.go ${./trevrpc-go/cmd/trevrpc-bench-peer/benchmarkpb/benchmark_grpc.pb.go}
                    touch $out
              '';

          benchmark-smoke =
            pkgs.runCommand "trevrpc-benchmark-smoke"
              {
                nativeBuildInputs = [ self.packages.${system}.trevrpc-bench-suite ];
              }
              ''
                trevrpc-bench run ${./bench/campaigns/smoke.example.json} --out run
                test "$(wc -l < run/samples.jsonl)" -eq 4
                test -s run/aggregate.csv
                test -s run/report.md
                test -s run/report.html
                touch $out
              '';

          benchmark-grpc-smoke =
            pkgs.runCommand "trevrpc-benchmark-grpc-smoke"
              {
                nativeBuildInputs = [ self.packages.${system}.trevrpc-bench-suite ];
              }
              ''
                trevrpc-bench run ${./bench/campaigns/grpc-smoke.example.json} --out run
                test "$(wc -l < run/samples.jsonl)" -eq 24
                test -s run/aggregate.csv
                test -s run/report.md
                test -s run/report.html
                touch $out
              '';

          benchmark-peer-capabilities =
            let
              c = self.packages.${system}.trevrpc-c-bench-peer;
              cpp = self.packages.${system}.trevrpc-cpp-bench-peer;
              go = self.packages.${system}.trevrpc-go-bench-peer;
              js = self.packages.${system}.trevrpc-js-bench-peer;
              kotlin = self.packages.${system}.trevrpc-kotlin-bench-peer;
              rust = self.packages.${system}.trevrpc-rust-bench-peer;
            in
            pkgs.runCommand "trevrpc-benchmark-peer-capabilities-check" { nativeBuildInputs = [ pkgs.jq ]; } ''
              check_capabilities() {
                test "$($1 capabilities | jq -r .schema_version)" = 3
                test "$($1 capabilities | jq -r .peer)" = "$2"
                test "$($1 capabilities | jq -c .stacks)" = '["trevrpc_native_quic","grpc_http2"]'
              }
              check_capabilities ${c}/bin/trevrpc-bench-peer-c c
              check_capabilities ${cpp}/bin/trevrpc-bench-peer-cpp cpp
              check_capabilities ${go}/bin/trevrpc-bench-peer-go go
              check_capabilities ${js}/bin/trevrpc-bench-peer-js js
              check_capabilities ${kotlin}/bin/trevrpc-bench-peer-kotlin kotlin
              check_capabilities ${rust}/bin/trevrpc-bench-peer-rust rust
              touch $out
            '';

          consumer-closures-no-grpc =
            let
              c = self.packages.${system}.trevrpc-c;
              cpp = self.packages.${system}.trevrpc-cpp;
              go = self.packages.${system}.trevrpc-go;
              js = self.packages.${system}.trevrpc-js;
              kotlin = self.packages.${system}.trevrpc-kotlin;
              rust = self.packages.${system}.trevrpc-rust;
              consumerClosure = pkgs.closureInfo {
                rootPaths = [
                  c
                  cpp
                  go
                  js
                  kotlin
                  rust
                ];
              };
            in
            pkgs.runCommand "trevrpc-consumer-closures-no-grpc-check" { } ''
              if ${pkgs.gnugrep}/bin/grep -Eiq '(grpc|tonic)' ${consumerClosure}/store-paths; then
                echo "consumer package closure contains a gRPC or Tonic store path" >&2
                exit 1
              fi
              mkdir -p $out
            '';

          cross-runtime =
            let
              crossRuntimeGo = pkgs.buildGoModule (final: {
                pname = "trevrpc-cross-runtime-go";
                version = "0.1.0";

                src = ./trevrpc-go;
                env.GOWORK = "off";
                vendorHash = "sha256-mgF3Ijy2WIM/LxSDr7wDcWa6rgqQ+DSu0V6tgqGWHRo=";
                subPackages = [ "cmd/trevrpc-xruntime-go" ];

                meta.mainProgram = "trevrpc-xruntime-go";
              });
            in
            self.packages.${system}.trevrpc-rust.overrideAttrs {
              dontBuild = true;
              doInstallCheck = false;
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
                env.GOWORK = "off";
                vendorHash = "sha256-mgF3Ijy2WIM/LxSDr7wDcWa6rgqQ+DSu0V6tgqGWHRo=";
                subPackages = [ "examples/greeter_server" ];

                meta.mainProgram = "greeter_server";
              });
              browserLifecycleGoServer = pkgs.buildGoModule (final: {
                pname = "trevrpc-browser-lifecycle-go-server";
                version = "0.1.0";

                src = ./trevrpc-go;
                env.GOWORK = "off";
                vendorHash = "sha256-mgF3Ijy2WIM/LxSDr7wDcWa6rgqQ+DSu0V6tgqGWHRo=";
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
              doInstallCheck = false;
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
