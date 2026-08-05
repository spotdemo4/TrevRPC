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
          update-go-deps = {
            packages = with pkgs; [
              go
              fix-hash
            ];
            script = ''
              go -C trevrpc-go work sync
              GOWORK=off go -C trevrpc-go mod tidy
              GOWORK=off go -C trevrpc-go/cmd/trevrpc-bench-peer mod tidy
              go -C trevrpc-go work sync
              fix-hash .#trevrpc-go
              fix-hash .#trevrpc-go-bench-peer
            '';
          };

          update-kotlin-deps = {
            packages = with pkgs; [
              jdk25
              oxfmt
            ];
            script = ''
              trevrpc-kotlin/gradlew \
                --project-dir trevrpc-kotlin \
                --no-configuration-cache \
                --refresh-dependencies \
                --write-locks \
                --write-verification-metadata sha256 \
                resolveAndLockAll :core:dokkaGeneratePublicationHtml

              update_script=$(nix build .#trevrpc-kotlin.mitmCache.updateScript --no-link --print-out-paths)
              USE_BWRAP=0 "$update_script"
              oxfmt --write trevrpc-kotlin/gradle/deps.json
            '';
          };
        };

        # nix build [#...]
        packages =
          let
            bench = pkgs.callPackage ./bench {
              repoRoot = ./.;
              sourceCommit = self.rev or (self.dirtyRev or "unversioned");
              sourceDirty = if self ? rev then "false" else "true";
            };
            c = pkgs.callPackage ./trevrpc-c {
              repoRoot = ./.;
            };
            cBenchPeer = c.override { benchPeer = true; };
            cFamilyConformancePeers = pkgs.callPackage ./conformance/adapters/c-family {
              repoRoot = ./.;
            };
            cpp = pkgs.callPackage ./trevrpc-cpp {
              repoRoot = ./.;
              trevrpcC = c;
            };
            cppBenchPeer = cpp.override { benchPeer = true; };
            go = pkgs.callPackage ./trevrpc-go {
              repoRoot = ./.;
            };
            goBenchPeer = go.override { benchPeer = true; };
            goConformancePeer = go.override { conformancePeer = true; };
            nativeLibmsquic =
              (pkgs.callPackage (pkgs.path + "/pkgs/by-name/li/libmsquic/package.nix") {
                fetchFromGitHub =
                  args: pkgs.fetchFromGitHub (builtins.removeAttrs args [ "tag" ] // { rev = args.tag; });
              }).overrideAttrs
                (_: {
                  dontPatchELF = true;
                });
            jsNative =
              if system == "x86_64-linux" then
                pkgs.callPackage ./trevrpc-js/native-package.nix {
                  libmsquic = nativeLibmsquic;
                  repoRoot = ./.;
                }
              else
                null;
            js = pkgs.callPackage ./trevrpc-js {
              repoRoot = ./.;
              trevrpcC = c;
              nativePackage = jsNative;
            };
            jsNpmStage =
              if system == "x86_64-linux" then
                pkgs.callPackage ./trevrpc-js/npm-stage.nix {
                  repoRoot = ./.;
                  trevrpcC = c;
                  trevrpcJs = js;
                  nativePackage = jsNative;
                }
              else
                null;
            jsBenchPeer = js.override { benchPeer = true; };
            jsConformancePeer = js.override { conformancePeer = true; };
            chromiumBenchPeer = pkgs.callPackage ./trevrpc-js/bench-browser {
              repoRoot = ./.;
              trevrpcJs = js;
              trevrpcJsBenchPeer = jsBenchPeer;
            };
            kotlin = pkgs.callPackage ./trevrpc-kotlin {
              repoRoot = ./.;
            };
            kotlinBenchPeer = kotlin.override { benchPeer = true; };
            kotlinConformancePeer = kotlin.override { conformancePeer = true; };
            rust = pkgs.callPackage ./trevrpc-rust {
              repoRoot = ./.;
            };
            rustBenchPeer = rust.override { benchPeer = true; };
            rustConformancePeer = rust.override { conformancePeer = true; };
          in
          {
            trevrpc-bench = bench;
            trevrpc-c = c;
            trevrpc-c-bench-peer = cBenchPeer;
            trevrpc-c-conformance-peer = cFamilyConformancePeers;
            trevrpc-cpp = cpp;
            trevrpc-cpp-bench-peer = cppBenchPeer;
            trevrpc-cpp-conformance-peer = cFamilyConformancePeers;
            trevrpc-go = go;
            trevrpc-go-bench-peer = goBenchPeer;
            trevrpc-go-conformance-peer = goConformancePeer;
            trevrpc-js = js;
            trevrpc-js-bench-peer = jsBenchPeer;
            trevrpc-js-conformance-peer = jsConformancePeer;
            trevrpc-chromium-bench-peer = chromiumBenchPeer;
            trevrpc-kotlin = kotlin;
            trevrpc-kotlin-bench-peer = kotlinBenchPeer;
            trevrpc-kotlin-conformance-peer = kotlinConformancePeer;
            trevrpc-rust = rust;
            trevrpc-rust-bench-peer = rustBenchPeer;
            trevrpc-rust-conformance-peer = rustConformancePeer;

            trevrpc-conformance-suite = pkgs.symlinkJoin {
              name = "trevrpc-conformance-suite";
              paths = [
                bench
                cFamilyConformancePeers
                goConformancePeer
                jsConformancePeer
                kotlinConformancePeer
                rustConformancePeer
              ];
              meta.platforms = [ "x86_64-linux" ];
            };

            trevrpc-bench-suite = pkgs.symlinkJoin {
              name = "trevrpc-bench-suite";
              paths = [
                bench
                cBenchPeer
                cppBenchPeer
                goBenchPeer
                jsBenchPeer
                chromiumBenchPeer
                kotlinBenchPeer
                rustBenchPeer
              ];
              meta.platforms = [ "x86_64-linux" ];
            };
          }
          // pkgs.lib.optionalAttrs (system == "x86_64-linux") {
            trevrpc-js-native-linux-x64-gnu = jsNative;
            trevrpc-js-npm-stage = jsNpmStage;
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

          c = self.packages.${system}.trevrpc-c;
          c-sanitizers = self.packages.${system}.trevrpc-c.override {
            sanitizers = true;
          };
          ${if system == "x86_64-linux" then "c-tsan" else null} =
            self.packages.${system}.trevrpc-c.override
              {
                threadSanitizer = true;
              };
          c-family-sanitizers = self.packages.${system}.trevrpc-c-conformance-peer.override {
            sanitizers = true;
          };

          cpp = self.packages.${system}.trevrpc-cpp;
          cpp-sanitizers = self.packages.${system}.trevrpc-cpp.override {
            sanitizers = true;
            trevrpcC = self.packages.${system}.trevrpc-c.override {
              sanitizers = true;
            };
          };
          rust = self.packages.${system}.trevrpc-rust;
          go = self.packages.${system}.trevrpc-go;
          js = self.packages.${system}.trevrpc-js;
          ${if system == "x86_64-linux" then "js-npm-stage" else null} =
            self.packages.${system}.trevrpc-js-npm-stage;
          kotlin = self.packages.${system}.trevrpc-kotlin;
          kotlin-maven-consumer =
            let
              publicationTests = pkgs.lib.fileset.toSource {
                root = ./trevrpc-kotlin/publication-tests;
                fileset = pkgs.lib.fileset.unions [
                  ./trevrpc-kotlin/publication-tests/maven
                  ./trevrpc-kotlin/publication-tests/proto
                  ./trevrpc-kotlin/publication-tests/sources
                ];
              };
            in
            pkgs.maven.buildMavenPackage {
              pname = "trevrpc-kotlin-maven-consumer";
              version = "0.1.0";
              src = publicationTests;
              sourceRoot = "source/maven";
              mvnHash = "sha256-rImtM+XfS3e5wv9z7COVr+jUo0pPbkui/DsTbjD56Lk=";
              mvnJdk = pkgs.jdk25;
              mvnGoal = "verify";
              mvnParameters = "-Dtrevrpc.repository=file://${self.packages.${system}.trevrpc-kotlin}/share/maven";
              doCheck = false;
              installPhase = ''
                mkdir -p $out
              '';
            };

          benchmark-proto-sync =
            pkgs.runCommand "trevrpc-benchmark-proto-sync"
              {
                nativeBuildInputs = with pkgs; [
                  go
                  protobuf
                  protoc-gen-go
                  protoc-gen-go-grpc
                  self.packages.${system}.trevrpc-go
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
                 protoc \
                   --proto_path=${./bench/proto} \
                   --trevrpc-go_out=generated \
                   --trevrpc-go_opt=paths=source_relative,service_prefix=Native \
                   ${./bench/proto}/benchmark.proto
                 cmp generated/benchmark.trevrpc.go ${./trevrpc-go/cmd/trevrpc-bench-peer/benchmarkpb/benchmark.trevrpc.go}
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
                export TREVRPC_BENCH_SERVER_WORKERS=8
                trevrpc-bench run ${./bench/campaigns/grpc-smoke.example.json} --out run
                test "$(wc -l < run/samples.jsonl)" -eq 24
                test -s run/aggregate.csv
                test -s run/report.md
                test -s run/report.html
                touch $out
              '';

          benchmark-cross-language-smoke =
            pkgs.runCommand "trevrpc-benchmark-cross-language-smoke"
              {
                nativeBuildInputs = [ self.packages.${system}.trevrpc-bench-suite ];
              }
              ''
                export TREVRPC_BENCH_SERVER_WORKERS=8
                trevrpc-bench run ${./bench/campaigns/cross-language-smoke.example.json} --out run
                test "$(wc -l < run/samples.jsonl)" -eq 44
                test -s run/aggregate.csv
                test -s run/report.md
                test -s run/report.html
                touch $out
              '';

          benchmark-webtransport-smoke =
            pkgs.runCommand "trevrpc-benchmark-webtransport-smoke"
              {
                nativeBuildInputs = [ self.packages.${system}.trevrpc-bench-suite ];
              }
              ''
                export HOME=$(mktemp -d)
                export TREVRPC_BENCH_SERVER_WORKERS=8
                trevrpc-bench run ${./bench/campaigns/webtransport-smoke.example.json} --out run
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
              chromium = self.packages.${system}.trevrpc-chromium-bench-peer;
              kotlin = self.packages.${system}.trevrpc-kotlin-bench-peer;
              rust = self.packages.${system}.trevrpc-rust-bench-peer;
            in
            pkgs.runCommand "trevrpc-benchmark-peer-capabilities-check" { nativeBuildInputs = [ pkgs.jq ]; } ''
              check_native_capabilities() {
                test "$($1 capabilities | jq -r .schema_version)" = 4
                test "$($1 capabilities | jq -r .peer)" = "$2"
                test "$($1 capabilities | jq -c '.roles.client | sort')" = '["grpc_http2","trevrpc_native_quic"]'
                test "$($1 capabilities | jq -c '.roles.server | sort')" = '["grpc_http2","trevrpc_native_quic","trevrpc_webtransport"]'
              }
              check_native_capabilities ${c}/bin/trevrpc-bench-peer-c c
              check_native_capabilities ${cpp}/bin/trevrpc-bench-peer-cpp cpp
              check_native_capabilities ${go}/bin/trevrpc-bench-peer-go go
              check_native_capabilities ${js}/bin/trevrpc-bench-peer-js js
              check_native_capabilities ${kotlin}/bin/trevrpc-bench-peer-kotlin kotlin
              check_native_capabilities ${rust}/bin/trevrpc-bench-peer-rust rust
              test "$(${chromium}/bin/trevrpc-bench-peer-chromium capabilities | jq -r .schema_version)" = 4
              test "$(${chromium}/bin/trevrpc-bench-peer-chromium capabilities | jq -r .peer)" = chromium
              test "$(${chromium}/bin/trevrpc-bench-peer-chromium capabilities | jq -c .roles)" = '{"client":["trevrpc_webtransport"]}'
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

          conformance-m3 =
            let
              conformanceSource = pkgs.lib.fileset.toSource {
                root = ./.;
                fileset = pkgs.lib.fileset.unions [
                  ./conformance
                  ./testdata/wire-golden-vectors.txt
                ];
              };
              controller = self.packages.${system}.trevrpc-bench;
              cPeer = self.packages.${system}.trevrpc-c-conformance-peer;
              cppPeer = self.packages.${system}.trevrpc-cpp-conformance-peer;
              goPeer = self.packages.${system}.trevrpc-go-conformance-peer;
              jsPeer = self.packages.${system}.trevrpc-js-conformance-peer;
              kotlinPeer = self.packages.${system}.trevrpc-kotlin-conformance-peer;
              rustPeer = self.packages.${system}.trevrpc-rust-conformance-peer;
            in
            pkgs.runCommand "trevrpc-conformance-m3"
              {
                nativeBuildInputs = [
                  controller
                  pkgs.coreutils
                  pkgs.gnugrep
                  pkgs.jq
                ];
                meta.platforms = [ "x86_64-linux" ];
              }
              ''
                set -euo pipefail
                ulimit -v 6291456
                export JAVA_TOOL_OPTIONS="-Xmx1024m -XX:MaxMetaspaceSize=512m"
                m0_suite=${conformanceSource}/conformance/suites/m0.json
                suite=${conformanceSource}/conformance/suites/m3.json

                trevrpc-conformance validate "$m0_suite" | grep -q '63 cases'
                timeout --kill-after=5s 60s \
                  trevrpc-conformance run "$m0_suite" --out "$TMPDIR/m0" \
                    --peer go=${goPeer}/bin/trevrpc-conformance-go
                test "$(wc -l < "$TMPDIR/m0/results.jsonl")" -eq 63
                jq -e '
                  .peers.go == {total:63,passed:63,allowed:0,failed:0,skipped:0} and
                  .overall == {total:63,passed:63,allowed:0,failed:0,skipped:0} and
                  .clean_shutdown == true
                ' "$TMPDIR/m0/summary.json" >/dev/null

                trevrpc-conformance validate "$suite" | grep -q '90 cases'
                mkdir -p "$out"
                timeout --kill-after=5s 300s \
                  trevrpc-conformance run "$suite" --out "$out" \
                    --peer c=${cPeer}/bin/trevrpc-conformance-c \
                    --peer cpp=${cppPeer}/bin/trevrpc-conformance-cpp \
                    --peer go=${goPeer}/bin/trevrpc-conformance-go \
                    --peer js=${jsPeer}/bin/trevrpc-conformance-js \
                    --peer kotlin=${kotlinPeer}/bin/trevrpc-conformance-kotlin \
                    --peer rust=${rustPeer}/bin/trevrpc-conformance-rust

                test "$(wc -l < "$out/results.jsonl")" -eq 540
                jq -e '
                  .peers.c == {total:90,passed:90,allowed:0,failed:0,skipped:0} and
                  .peers.cpp == {total:90,passed:90,allowed:0,failed:0,skipped:0} and
                  .peers.go == {total:90,passed:90,allowed:0,failed:0,skipped:0} and
                  .peers.js == {total:90,passed:90,allowed:0,failed:0,skipped:0} and
                  .peers.kotlin == {total:90,passed:90,allowed:0,failed:0,skipped:0} and
                  .peers.rust == {total:90,passed:90,allowed:0,failed:0,skipped:0} and
                  .overall == {total:540,passed:540,allowed:0,failed:0,skipped:0} and
                  .clean_shutdown == true
                ' "$out/summary.json" >/dev/null
                jq -e '
                  .source_revision != "unknown" and
                  .limits.max_command_bytes == 262144 and
                  .peer_resolution == "absolute_overrides" and
                  .allowance_policy == "forbid" and
                  .suite_path == "inputs/conformance/suites/m3.json" and
                  .golden_path == "inputs/testdata/wire-golden-vectors.txt" and
                  (.corpus | length) == 6 and
                  (.peers | length) == 6 and
                  all(.peers[];
                    (.executable | startswith("/nix/store/")) and
                    (.sha256 | test("^[0-9a-f]{64}$"))
                  )
                ' "$out/manifest.json" >/dev/null
                while IFS=$'\t' read -r relative expected; do
                  test -f "$out/$relative"
                  test "$(sha256sum "$out/$relative" | cut -d' ' -f1)" = "$expected"
                done < <(
                  jq -r '
                    [.suite_path,.suite_sha256],
                    [.golden_path,.golden_sha256],
                    (.corpus[] | [.path,.sha256]) |
                    @tsv
                  ' "$out/manifest.json"
                )
                trevrpc-conformance validate \
                  "$out/inputs/conformance/suites/m3.json" | grep -q '90 cases'
                while IFS=$'\t' read -r executable expected; do
                  test "$executable" = "$(realpath "$executable")"
                  test -x "$executable"
                  test "$(sha256sum "$executable" | cut -d' ' -f1)" = "$expected"
                done < <(jq -r '.peers[] | [.executable,.sha256] | @tsv' "$out/manifest.json")
              '';
        };
      }
    );
}
