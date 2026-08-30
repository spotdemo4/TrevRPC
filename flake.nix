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
      system: pkgs:
      let
        packageSet =
          let
            cFamilyConformancePeers =
              if pkgs.stdenv.hostPlatform.isLinux then
                pkgs.callPackage ./conformance/adapters/c-family {
                  trevrpcCSrc = ./trevrpc-c;
                  trevrpcCppSrc = ./trevrpc-cpp;
                }
              else
                null;
            c = pkgs.callPackage ./trevrpc-c {
              benchProto = ./bench/proto;
              wireGolden = ./testdata/wire-golden-vectors.txt;
              peerBinaries = pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [
                {
                  package = cFamilyConformancePeers;
                  binary = "trevrpc-conformance-c";
                }
              ];
            };
            cpp = pkgs.callPackage ./trevrpc-cpp {
              benchProto = ./bench/proto;
              trevrpcC = c;
              peerBinaries = pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [
                {
                  package = cFamilyConformancePeers;
                  binary = "trevrpc-conformance-cpp";
                }
              ];
            };
            go = pkgs.callPackage ./trevrpc-go {
              benchProto = ./bench/proto;
              wireGolden = ./testdata/wire-golden-vectors.txt;
            };
            jsPackage = builtins.fromJSON (builtins.readFile ./trevrpc-js/package.json);
            jsPackageManifestWriter = ./trevrpc-js/publication-tests/write-package-manifest.mjs;
            jsNative =
              if system == "x86_64-linux" then
                pkgs.callPackage ./trevrpc-js/npm/native-linux-x64-gnu {
                  sourceTree = ./.;
                  libmsquic = pkgs.libmsquic.overrideAttrs {
                    dontPatchELF = true;
                  };
                  trevrpcCSrc = ./trevrpc-c;
                  jsNativeSrc = ./trevrpc-js/native;
                  jsNativePackageSrc = ./trevrpc-js/npm/native-linux-x64-gnu;
                  jsLicense = ./trevrpc-js/LICENSE;
                  inherit jsPackage;
                  packageManifestWriter = jsPackageManifestWriter;
                  publicationVerifier = ./trevrpc-js/publication-tests/verify.mjs;
                }
              else
                null;
            js = pkgs.callPackage ./trevrpc-js {
              benchProto = ./bench/proto;
              wireGolden = ./testdata/wire-golden-vectors.txt;
              trevrpcC = c;
              inherit jsPackage;
              packageManifestWriter = jsPackageManifestWriter;
              nativePackage = jsNative;
            };
            kotlin = pkgs.callPackage ./trevrpc-kotlin {
              licenseFile = ./LICENSE;
              wireGolden = ./testdata/wire-golden-vectors.txt;
              greeterProto = ./trevrpc-rust/crates/protoc-gen-trevrpc-rust/tests/proto/greeter.proto;
            };
            rust = pkgs.callPackage ./trevrpc-rust {
              benchProto = ./bench/proto;
              wireGolden = ./testdata/wire-golden-vectors.txt;
            };
            bench = pkgs.callPackage ./bench {
              conformanceSrc = ./conformance;
              wireGolden = ./testdata/wire-golden-vectors.txt;
              sourceCommit = self.rev or (self.dirtyRev or "unversioned");
              sourceDirty = if self ? rev then "false" else "true";
            };
            browserBenchPeer = pkgs.callPackage ./trevrpc-js/bench-browser {
              trevrpcJs = js;
            };
            webkitBenchSuite = pkgs.symlinkJoin {
              name = "trevrpc-webkit-bench-suite";
              paths = [
                c
                cpp
                go
                js
                kotlin
                rust
                bench
                browserBenchPeer
              ];
              meta.platforms = [ "aarch64-darwin" ];
            };
          in
          {
            trevrpc-c = c;
            trevrpc-cpp = cpp;
            trevrpc-go = go;
            trevrpc-js = js;
            trevrpc-kotlin = kotlin;
            trevrpc-rust = rust;

            trevrpc-bench = bench;
            trevrpc-browser-bench-peer = browserBenchPeer;
            trevrpc-webkit-bench-suite = webkitBenchSuite;
            trevrpc-bench-suite = pkgs.symlinkJoin {
              name = "trevrpc-bench-suite";
              paths = [
                c
                cpp
                go
                js
                kotlin
                rust
                bench
                browserBenchPeer
              ];
              meta.platforms = [ "x86_64-linux" ];
            };
            trevrpc-conformance-suite = pkgs.symlinkJoin {
              name = "trevrpc-conformance-suite";
              paths = [
                c
                cpp
                go
                js
                kotlin
                rust
                bench
              ];
              meta.platforms = [ "x86_64-linux" ];
            };
          }
          // pkgs.lib.optionalAttrs (system == "x86_64-linux") {
            trevrpc-js-native-linux-x64-gnu = jsNative;
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

              # c
              cmake
              ninja
              gcc
              clang-tools
              openssl
              pkg-config
              protobufc
              libmsquic

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
              protobuf

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
        packages = packageSet;

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
            ruff
          ];
        };

        # nix flake check
        checks =
          let
            linuxBenchSuite = pkgs.symlinkJoin {
              name = "trevrpc-linux-bench-check-suite";
              paths = [
                packageSet.trevrpc-c
                packageSet.trevrpc-cpp
                packageSet.trevrpc-go
                packageSet.trevrpc-js
                packageSet.trevrpc-kotlin
                packageSet.trevrpc-rust
                packageSet.trevrpc-bench
                packageSet.trevrpc-browser-bench-peer
              ];
              meta.platforms = [ "x86_64-linux" ];
            };
          in
          pkgs.mkChecks {
            benchmark-controller = packageSet.trevrpc-bench;

            c = packageSet.trevrpc-c;
            c-sanitizers = packageSet.trevrpc-c.override {
              sanitizers = true;
            };
            ${if system == "x86_64-linux" then "c-tsan" else null} = packageSet.trevrpc-c.override {
              threadSanitizer = true;
            };
            c-family-sanitizers =
              (pkgs.callPackage ./conformance/adapters/c-family {
                trevrpcCSrc = ./trevrpc-c;
                trevrpcCppSrc = ./trevrpc-cpp;
              }).override
                {
                  sanitizers = true;
                };

            cpp = packageSet.trevrpc-cpp;
            cpp-sanitizers = packageSet.trevrpc-cpp.override {
              sanitizers = true;
              trevrpcC = packageSet.trevrpc-c.override {
                sanitizers = true;
              };
            };

            rust = packageSet.trevrpc-rust;

            go = packageSet.trevrpc-go;

            js = packageSet.trevrpc-js;

            kotlin = packageSet.trevrpc-kotlin;

            benchmark-proto-sync =
              pkgs.runCommand "trevrpc-benchmark-proto-sync"
                {
                  nativeBuildInputs = with pkgs; [
                    go
                    protobuf
                    protoc-gen-go
                    packageSet.trevrpc-go
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
                     --trevrpc-go_out=generated \
                     --trevrpc-go_opt=paths=source_relative,service_prefix=Native \
                     ${./bench/proto}/benchmark.proto
                   cmp generated/benchmark.trevrpc.go ${./trevrpc-go/cmd/trevrpc-bench-peer/benchmarkpb/benchmark.trevrpc.go}
                      touch $out
                '';

            benchmark-smoke =
              pkgs.runCommand "trevrpc-benchmark-smoke"
                {
                  nativeBuildInputs = [ linuxBenchSuite ];
                  meta.platforms = [ "x86_64-linux" ];
                }
                ''
                  trevrpc-bench run ${./bench/campaigns/smoke.example.json} --out run
                  test "$(wc -l < run/samples.jsonl)" -eq 4
                  test -s run/aggregate.csv
                  test -s run/report.md
                  test -s run/report.html
                  touch $out
                '';

            benchmark-cross-language-smoke =
              pkgs.runCommand "trevrpc-benchmark-cross-language-smoke"
                {
                  nativeBuildInputs = [ linuxBenchSuite ];
                  meta.platforms = [ "x86_64-linux" ];
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

            benchmark-chromium-smoke =
              pkgs.runCommand "trevrpc-benchmark-chromium-smoke"
                {
                  nativeBuildInputs = [ linuxBenchSuite ];
                  meta.platforms = [ "x86_64-linux" ];
                }
                ''
                  export HOME=$(mktemp -d)
                  export TREVRPC_BENCH_SERVER_WORKERS=8
                  trevrpc-bench run ${./bench/campaigns/chromium-smoke.example.json} --out run
                  test "$(wc -l < run/samples.jsonl)" -eq 24
                  test -s run/aggregate.csv
                  test -s run/report.md
                  test -s run/report.html
                  touch $out
                '';

            benchmark-firefox-smoke =
              pkgs.runCommand "trevrpc-benchmark-firefox-smoke"
                {
                  nativeBuildInputs = [ linuxBenchSuite ];
                  meta.platforms = [ "x86_64-linux" ];
                }
                ''
                  export HOME=$(mktemp -d)
                  export TREVRPC_BENCH_SERVER_WORKERS=8
                  trevrpc-bench run ${./bench/campaigns/firefox-smoke.example.json} --out run
                  test "$(wc -l < run/samples.jsonl)" -eq 24
                  test -s run/aggregate.csv
                  test -s run/report.md
                  test -s run/report.html
                  touch $out
                '';

            benchmark-webkit-smoke =
              pkgs.runCommand "trevrpc-benchmark-webkit-smoke"
                {
                  __darwinAllowLocalNetworking = true;
                  nativeBuildInputs = pkgs.lib.optionals (system == "aarch64-darwin") [
                    packageSet.trevrpc-webkit-bench-suite
                  ];
                  meta.platforms = [ "aarch64-darwin" ];
                }
                ''
                  export HOME=$(mktemp -d)
                  export TREVRPC_BENCH_SERVER_WORKERS=8
                  trevrpc-bench run ${./bench/campaigns/webkit-smoke.example.json} --out run
                  # Rust remains excluded pending an upstream Safari compatibility fix:
                  # https://github.com/hyperium/h3/issues/347
                  test "$(wc -l < run/samples.jsonl)" -eq 20
                  test -s run/aggregate.csv
                  test -s run/report.md
                  test -s run/report.html
                  touch $out
                '';

            benchmark-peer-capabilities =
              let
                c = packageSet.trevrpc-c;
                cpp = packageSet.trevrpc-cpp;
                go = packageSet.trevrpc-go;
                js = packageSet.trevrpc-js;
                kotlin = packageSet.trevrpc-kotlin;
                rust = packageSet.trevrpc-rust;
                browser = packageSet.trevrpc-browser-bench-peer;
              in
              pkgs.runCommand "trevrpc-benchmark-peer-capabilities-check"
                {
                  nativeBuildInputs = [ pkgs.jq ];
                  meta.platforms = [ "x86_64-linux" ];
                }
                ''
                  check_native_capabilities() {
                    test "$($1 capabilities | jq -r .schema_version)" = 4
                    test "$($1 capabilities | jq -r .peer)" = "$2"
                    test "$($1 capabilities | jq -c '.roles.client | sort')" = '["trevrpc_native_quic"]'
                    test "$($1 capabilities | jq -c '.roles.server | sort')" = '["trevrpc_native_quic","trevrpc_webtransport"]'
                  }
                  check_native_capabilities ${c}/bin/trevrpc-bench-peer-c c
                  check_native_capabilities ${cpp}/bin/trevrpc-bench-peer-cpp cpp
                  check_native_capabilities ${go}/bin/trevrpc-bench-peer-go go
                  check_native_capabilities ${js}/bin/trevrpc-bench-peer-js js
                  check_native_capabilities ${kotlin}/bin/trevrpc-bench-peer-kotlin kotlin
                  check_native_capabilities ${rust}/bin/trevrpc-bench-peer-rust rust
                  test "$(${browser}/bin/trevrpc-bench-peer-chromium capabilities | jq -r .schema_version)" = 4
                  test "$(${browser}/bin/trevrpc-bench-peer-chromium capabilities | jq -r .peer)" = chromium
                  test "$(${browser}/bin/trevrpc-bench-peer-chromium capabilities | jq -c .roles)" = '{"client":["trevrpc_webtransport"]}'
                  test "$(${browser}/bin/trevrpc-bench-peer-firefox capabilities | jq -r .schema_version)" = 4
                  test "$(${browser}/bin/trevrpc-bench-peer-firefox capabilities | jq -r .peer)" = firefox
                  test "$(${browser}/bin/trevrpc-bench-peer-firefox capabilities | jq -c .roles)" = '{"client":["trevrpc_webtransport"]}'
                  test "$(${browser}/bin/trevrpc-bench-peer-webkit capabilities | jq -r .schema_version)" = 4
                  test "$(${browser}/bin/trevrpc-bench-peer-webkit capabilities | jq -r .peer)" = webkit
                  test "$(${browser}/bin/trevrpc-bench-peer-webkit capabilities | jq -c .roles)" = '{}'
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

            python = {
              root = ./.;
              filter = file: file.hasExt "py";
              packages = with pkgs; [
                ruff
                basedpyright
              ];
              script = ''
                ruff check
                basedpyright
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
                controller = packageSet.trevrpc-bench;
                cPeer = packageSet.trevrpc-c;
                cppPeer = packageSet.trevrpc-cpp;
                goPeer = packageSet.trevrpc-go;
                jsPeer = packageSet.trevrpc-js;
                kotlinPeer = packageSet.trevrpc-kotlin;
                rustPeer = packageSet.trevrpc-rust;
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
