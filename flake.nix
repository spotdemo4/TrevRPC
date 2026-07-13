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
          benchmark = "${self.packages.${system}.trevrpc-bench}/bin/trevrpc-bench";
          browser-webtransport-soak = "nix develop -c bash -c 'npm --prefix trevrpc-js ci && npm --prefix trevrpc-js run test:browser:soak'";
          cross-runtime-lifecycle-stress = "nix develop -c bash -c 'mkdir -p target && go build -C trevrpc-go -o ../target/trevrpc-xruntime-go ./cmd/trevrpc-xruntime-go && TREVRPC_XRUNTIME_GO=$PWD/target/trevrpc-xruntime-go cargo test --manifest-path trevrpc-rust/Cargo.toml --test cross_runtime -- --ignored cross_runtime_lifecycle_stress --nocapture'";
          dev = "cargo run --manifest-path trevrpc-rust/Cargo.toml";
          test = "nix flake check";
          update-kotlin-deps = {
            packages = [ pkgs.oxfmt ];
            script = ''
              ${self.packages.${system}.trevrpc-kotlin.mitmCache.updateScript}
              oxfmt --write trevrpc-kotlin/gradle/deps.json
            '';
          };
        };

        # nix build [#...]
        packages = {
          trevrpc-bench = pkgs.rustPlatform.buildRustPackage (
            final: with pkgs.lib; {
              pname = "trevrpc-bench";
              version = "0.1.0";

              src = fileset.toSource {
                root = ./.;
                fileset = fileset.unions [
                  ./bench/Cargo.lock
                  ./bench/Cargo.toml
                  ./bench/campaigns
                  ./bench/peer-protocol-v1.md
                  ./bench/proto
                  ./bench/src
                ];
              };
              sourceRoot = "${final.src.name}/bench";
              cargoLock.lockFile = ./bench/Cargo.lock;
              nativeBuildInputs = [ pkgs.makeWrapper ];
              postInstall = ''
                wrapProgram $out/bin/trevrpc-bench \
                  --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.openssl ]} \
                  --set TREVRPC_BENCH_SOURCE_COMMIT ${self.rev or (self.dirtyRev or "unknown")} \
                  --set TREVRPC_BENCH_SOURCE_DIRTY ${if self ? rev then "false" else "true"}
              '';

              meta = {
                mainProgram = "trevrpc-bench";
                description = "Cross-language TrevRPC benchmark controller and reporter";
                license = licenses.mit;
                platforms = platforms.linux;
              };
            }
          );

          trevrpc-c = pkgs.stdenv.mkDerivation (
            final: with pkgs.lib; {
              pname = "trevrpc-c";
              version = "0.1.0";
              outputs = [
                "out"
                "dev"
                "lib"
              ];

              src = fileset.toSource {
                root = ./.;
                fileset = fileset.unions [
                  ./bench/proto
                  ./testdata/wire-golden-vectors.txt
                  ./trevrpc-c
                ];
              };
              sourceRoot = "${final.src.name}/trevrpc-c";

              configurePhase = ''
                runHook preConfigure
                cmake -S . -B build \
                  -DCMAKE_BUILD_TYPE=Release \
                  -DCMAKE_INSTALL_BINDIR="$out/bin" \
                  -DCMAKE_INSTALL_INCLUDEDIR="$dev/include" \
                  -DCMAKE_INSTALL_LIBDIR="$lib/lib" \
                  -DCMAKE_INSTALL_LIBEXECDIR="$lib/libexec" \
                  -DTREVRPC_INSTALL_CMAKEDIR="$dev/lib/cmake/trevrpc" \
                  -DTREVRPC_INSTALL_PKGCONFIGDIR="$dev/lib/pkgconfig" \
                  -DTREVRPC_BUILD_BENCHMARKS=ON \
                  -DTREVRPC_BUILD_TESTS=ON
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
                protobufc
              ];
              propagatedBuildInputs = with pkgs; [
                libmsquic
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
                cmake --install build
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

          trevrpc-cpp = pkgs.stdenv.mkDerivation (
            final: with pkgs.lib; {
              pname = "trevrpc-cpp";
              version = "0.1.0";
              outputs = [
                "out"
                "dev"
                "lib"
              ];

              src = fileset.toSource {
                root = ./.;
                fileset = fileset.unions [
                  ./bench/proto
                  ./trevrpc-cpp
                ];
              };
              sourceRoot = "${final.src.name}/trevrpc-cpp";

              nativeBuildInputs = with pkgs; [
                clang-tools
                cmake
                openssl
                protobuf
              ];
              propagatedBuildInputs = with pkgs; [
                self.packages.${system}.trevrpc-c
                protobuf
              ];
              doCheck = true;

              configurePhase = ''
                runHook preConfigure
                cmake -S . -B build \
                  -DCMAKE_BUILD_TYPE=Release \
                  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
                  -DCMAKE_INSTALL_BINDIR="$out/bin" \
                  -DCMAKE_INSTALL_INCLUDEDIR="$dev/include" \
                  -DCMAKE_INSTALL_LIBDIR="$lib/lib" \
                  -DCMAKE_INSTALL_LIBEXECDIR="$lib/libexec" \
                  -DTREVRPC_CPP_INSTALL_CMAKEDIR="$dev/lib/cmake/trevrpc-cpp" \
                  -DTREVRPC_CPP_TREVRPC_C_ROOT= \
                  -DTREVRPC_CPP_BUILD_BENCHMARKS=ON \
                  -DTREVRPC_CPP_BUILD_TESTS=ON \
                  -DTREVRPC_CPP_BUILD_EXAMPLES=ON
                runHook postConfigure
              '';
              buildPhase = ''
                runHook preBuild
                cmake --build build
                runHook postBuild
              '';
              checkPhase = ''
                runHook preCheck
                clang-format --dry-run --Werror $(find . \
                  -path './build' -prune -o \
                  \( -name '*.cpp' -o -name '*.hpp' \) -print)
                clang-tidy --quiet -p build \
                  src/trevrpc.cpp \
                  tools/protoc-gen-trevrpc-cpp/generator.cpp \
                  tools/protoc-gen-trevrpc-cpp/main.cpp \
                  tests/codec_test.cpp \
                  tests/generated_service_test.cpp \
                  tests/generator_options_test.cpp \
                  bench/trevrpc_bench_peer.cpp \
                  examples/greeter/client.cpp \
                  examples/greeter/server.cpp
                ctest --test-dir build --output-on-failure
                cmake --install build
                cmake -S tests/consumer -B build/consumer \
                  -DCMAKE_PREFIX_PATH="$dev"
                cmake --build build/consumer
                runHook postCheck
              '';
              installPhase = ''
                runHook preInstall
                cmake --install build
                runHook postInstall
              '';

              meta = {
                mainProgram = "protoc-gen-trevrpc-cpp";
                description = "C++20 runtime and protobuf code generator for TrevRPC";
                license = licenses.mit;
                platforms = platforms.linux;
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
                  ./bench/proto
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
              installPhase = ''
                runHook preInstall
                generator=$(find target -path '*/release/protoc-gen-trevrpc-rust' -type f -perm -0100 | head -n1)
                peer=$(find target -path '*/release/trevrpc-bench-peer-rust' -type f -perm -0100 | head -n1)
                install -Dm755 "$generator" $out/bin/protoc-gen-trevrpc-rust
                install -Dm755 "$peer" $out/bin/trevrpc-bench-peer-rust
                runHook postInstall
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
                  ./bench/proto
                  ./testdata/wire-golden-vectors.txt
                  ./trevrpc-go
                ];
              };
              sourceRoot = "${final.src.name}/trevrpc-go";
              vendorHash = "sha256-mgF3Ijy2WIM/LxSDr7wDcWa6rgqQ+DSu0V6tgqGWHRo=";
              subPackages = [
                "cmd/protoc-gen-trevrpc-go"
                "cmd/trevrpc-bench-peer"
              ];
              postInstall = ''
                mv $out/bin/trevrpc-bench-peer $out/bin/trevrpc-bench-peer-go
              '';

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
                  ./bench/proto
                  ./testdata/wire-golden-vectors.txt
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
              buildInputs = [
                self.packages.${system}.trevrpc-c
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
                ":bench-peer:installDist"
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
                cp -R bench-peer/build/install/trevrpc-bench-peer-kotlin \
                  $out/share/trevrpc-kotlin/trevrpc-bench-peer-kotlin
                cp -R protoc-gen-trevrpc-kotlin/build/install/protoc-gen-trevrpc-kotlin \
                  $out/share/trevrpc-kotlin/protoc-gen-trevrpc-kotlin
                makeWrapper $out/share/trevrpc-kotlin/bin/trevrpc-xruntime-kotlin \
                  $out/bin/trevrpc-xruntime-kotlin \
                  --set JAVA_HOME ${pkgs.jdk25.home} \
                  --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.jdk25 ]}
                makeWrapper \
                  $out/share/trevrpc-kotlin/trevrpc-bench-peer-kotlin/bin/trevrpc-bench-peer-kotlin \
                  $out/bin/trevrpc-bench-peer-kotlin \
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

          trevrpc-bench-suite = pkgs.symlinkJoin {
            name = "trevrpc-bench-suite";
            paths = [
              self.packages.${system}.trevrpc-bench
              self.packages.${system}.trevrpc-c
              self.packages.${system}.trevrpc-cpp
              self.packages.${system}.trevrpc-go
              self.packages.${system}.trevrpc-js
              self.packages.${system}.trevrpc-kotlin
              self.packages.${system}.trevrpc-rust
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

          benchmark-proto-sync =
            pkgs.runCommand "trevrpc-benchmark-proto-sync"
              {
                nativeBuildInputs = with pkgs; [
                  protobuf
                  protoc-gen-go
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

          benchmark-peer-capabilities =
            let
              c = self.packages.${system}.trevrpc-c;
              cpp = self.packages.${system}.trevrpc-cpp;
              go = self.packages.${system}.trevrpc-go;
              js = self.packages.${system}.trevrpc-js;
              kotlin = self.packages.${system}.trevrpc-kotlin;
              rust = self.packages.${system}.trevrpc-rust;
            in
            pkgs.runCommand "trevrpc-benchmark-peer-capabilities-check" { nativeBuildInputs = [ pkgs.jq ]; } ''
              test "$(${c}/bin/trevrpc-bench-peer-c capabilities | jq -r .peer)" = c
              test "$(${cpp}/bin/trevrpc-bench-peer-cpp capabilities | jq -r .peer)" = cpp
              test "$(${go}/bin/trevrpc-bench-peer-go capabilities | jq -r .peer)" = go
              test "$(${js}/bin/trevrpc-bench-peer-js capabilities | jq -r .peer)" = js
              test "$(${kotlin}/bin/trevrpc-bench-peer-kotlin capabilities | jq -r .peer)" = kotlin
              test "$(${rust}/bin/trevrpc-bench-peer-rust capabilities | jq -r .peer)" = rust
              touch $out
            '';

          c = self.packages.${system}.trevrpc-c.overrideAttrs {
            installPhase = ''
              mkdir -p "$out" "$dev" "$lib"
            '';
          };

          c-sanitizers = self.packages.${system}.trevrpc-c.overrideAttrs {
            configurePhase = ''
              runHook preConfigure
              cmake -S . -B build -DTREVRPC_BUILD_TESTS=ON -DTREVRPC_ENABLE_SANITIZERS=ON
              runHook postConfigure
            '';
            installPhase = ''
              mkdir -p "$out" "$dev" "$lib"
            '';
          };

          cpp = self.packages.${system}.trevrpc-cpp.overrideAttrs {
            installPhase = ''
              mkdir -p "$out" "$dev" "$lib"
            '';
          };

          native-package-outputs =
            let
              c = self.packages.${system}.trevrpc-c;
              cpp = self.packages.${system}.trevrpc-cpp;
              js = self.packages.${system}.trevrpc-js;
            in
            pkgs.runCommand "trevrpc-native-package-outputs-check" { } ''
              test -x ${c}/bin/protoc-gen-trevrpc-c
              test -f ${c.dev}/include/trevrpc_binding.h
              test -f ${c.dev}/lib/cmake/trevrpc/trevrpcConfig.cmake
              test -f ${c.dev}/lib/pkgconfig/trevrpc.pc
              test -f ${c.lib}/lib/libtrevrpc.a
              test ! -e ${c}/include
              test ! -e ${c}/lib

              test "$(${pkgs.pkg-config}/bin/pkg-config \
                --define-prefix \
                --variable=libdir ${c.dev}/lib/pkgconfig/trevrpc.pc)" = "${c.lib}/lib"
              test "$(${pkgs.pkg-config}/bin/pkg-config \
                --define-prefix \
                --variable=includedir ${c.dev}/lib/pkgconfig/trevrpc.pc)" = "${c.dev}/include"

              test -x ${cpp}/bin/protoc-gen-trevrpc-cpp
              test -f ${cpp.dev}/include/trevrpc/trevrpc.hpp
              test -f ${cpp.dev}/lib/cmake/trevrpc-cpp/trevrpc-cppConfig.cmake
              test -f ${cpp.lib}/lib/libtrevrpc_cpp.a
              test ! -e ${cpp}/include
              test ! -e ${cpp}/lib

              ${pkgs.nodejs_24}/bin/node -e \
                'require(process.argv[1])' \
                ${js}/lib/node_modules/trevrpc-js/build/native/trevrpc_native.node

              mkdir -p $out
            '';

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
                vendorHash = "sha256-mgF3Ijy2WIM/LxSDr7wDcWa6rgqQ+DSu0V6tgqGWHRo=";
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
                vendorHash = "sha256-mgF3Ijy2WIM/LxSDr7wDcWa6rgqQ+DSu0V6tgqGWHRo=";
                subPackages = [ "examples/greeter_server" ];

                meta.mainProgram = "greeter_server";
              });
              browserLifecycleGoServer = pkgs.buildGoModule (final: {
                pname = "trevrpc-browser-lifecycle-go-server";
                version = "0.1.0";

                src = ./trevrpc-go;
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
