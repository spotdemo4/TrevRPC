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
    gomod2nix = {
      url = "github:nix-community/gomod2nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-utils.inputs.systems.follows = "systems";
    };
  };

  outputs =
    {
      self,
      trevpkgs,
      gomod2nix,
      ...
    }:
    trevpkgs.libs.mkFlake (
      system: pkgs: {

        # nix develop [#...]
        devShells = {
          default = pkgs.mkShell {
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
            '';
            env = {
              GOTOOLCHAIN = "local";
              RUST_SRC_PATH = pkgs.rustPlatform.rustLibSrc;
              PLAYWRIGHT_BROWSERS_PATH = "${pkgs.playwright-driver.browsers}";
              # The libzstd module that `npm run build` embeds for @trevrpc/trevrpc-js/zstd.
              TREVRPC_ZSTD_WASM = "${self.packages.${system}.trevrpc-js.zstdWasm}/libzstd.wasm";
              FIXED_VERSION = "true";
            };
            packages = with pkgs; [
              # rust
              rustc
              cargo
              rust-analyzer
              rustfmt
              clippy
              cargo-audit

              # go
              go
              gopls
              gotools
              golangci-lint
              protobuf
              benchstat

              # javascript
              nodejs_26
              playwright-driver.browsers
              oxlint
              typescript

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

              vscode-json-languageserver # json
              yaml-language-server # yaml
              tombi # toml
              oxfmt # format

              # nix
              nixd
              nixfmt

              # protobuf
              buf

              # acme (the TLS-ALPN-01 tests)
              pebble

              # util
              treefmt
              bumper
              gomod2nix.packages.${system}.default
              jq
              openssl
            ];
          };

          bump = pkgs.mkShell {
            packages = with pkgs; [
              bumper
            ];
          };

          # The Android interop harness and both servers, for the android workflow. The shell is
          # only offered where the harness builds.
          android = pkgs.mkShell {
            packages = with self.packages.${system}; [
              trevrpc-kotlin.interopAndroid
              trevrpc-go-interop
              trevrpc-rust
            ];
            meta.platforms = self.packages.${system}.trevrpc-kotlin.interopAndroid.meta.platforms;
          };

          # The interop runner and both servers, for the safari workflow.
          safari = pkgs.mkShell {
            packages = with self.packages.${system}; [
              trevrpc-go-interop
              trevrpc-interop
              trevrpc-rust
            ];
          };

          release = pkgs.mkShell {
            packages = with pkgs; [
              flake-release
              # protobuf
              buf
              # rust
              rustc
              cargo
              # protoc, for the build script of prost-protovalidate-types, which cargo publish runs
              # when it verifies trevrpc-protovalidate
              protobuf
              # go
              go
              # javascript
              nodejs_26
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
              gomod2nix.packages.${system}.default
              # javascript
              nodejs_26
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
              nodejs_26
              # kotlin
              osv-scanner
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
            packages = [
              pkgs.go
              gomod2nix.packages.${system}.default
            ];
            script = ''
              for module in trevrpc-go trevrpc-go/oteltrevrpc trevrpc-go/protovalidatetrevrpc trevrpc-go/cmd/trevrpc-interop bench/baseline; do
                go -C "$module" mod tidy
                gomod2nix generate --with-deps --dir "$module"
              done
            '';
          };

          update-kotlin-deps = {
            packages = with pkgs; [
              jdk25
            ];
            script = ''
              export ANDROID_HOME=${self.packages.${system}.trevrpc-kotlin.androidSdk}/libexec/android-sdk
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
            '';
          };

          sync-playwright = {
            packages = [ pkgs.nodejs_26 ];
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
        packages =
          let
            wireGolden = ./testdata/wire-golden-vectors.txt;
            qmuxGolden = ./testdata/qmux-golden-vectors.txt;
            zstdGolden = ./testdata/zstd-golden-vectors.txt;
            interopProto = ./interop/proto;
            interopHarness = ./interop/harness.py;
            trevrpcProto = ./proto;
            licenseFile = ./LICENSE;
          in
          {
            trevrpc-go = pkgs.callPackage ./trevrpc-go {
              inherit
                wireGolden
                qmuxGolden
                zstdGolden
                trevrpcProto
                ;
            };
            trevrpc-go-otel = pkgs.callPackage ./trevrpc-go/oteltrevrpc { };
            trevrpc-go-protovalidate = pkgs.callPackage ./trevrpc-go/protovalidatetrevrpc { };
            # The Go interop and load benchmark executable, a module of its own so that the
            # TrevRPC module does not depend on protovalidate.
            trevrpc-go-interop = pkgs.callPackage ./trevrpc-go/cmd/trevrpc-interop {
              inherit
                zstdGolden
                interopProto
                trevrpcProto
                ;
            };
            trevrpc-js = pkgs.callPackage ./trevrpc-js {
              inherit
                wireGolden
                qmuxGolden
                zstdGolden
                interopProto
                trevrpcProto
                ;
            };
            trevrpc-js-effect = pkgs.callPackage ./trevrpc-js/effect {
              inherit (self.packages.${system}) trevrpc-js;
            };
            trevrpc-js-protovalidate = pkgs.callPackage ./trevrpc-js/protovalidate {
              inherit (self.packages.${system}) trevrpc-js;
              inherit interopProto;
            };
            trevrpc-kotlin = pkgs.callPackage ./trevrpc-kotlin {
              inherit
                wireGolden
                zstdGolden
                interopHarness
                interopProto
                trevrpcProto
                licenseFile
                ;
            };
            trevrpc-rust-otel = pkgs.callPackage ./trevrpc-rust/crates/trevrpc-opentelemetry {
              inherit licenseFile;
            };
            trevrpc-rust-protovalidate = pkgs.callPackage ./trevrpc-rust/crates/trevrpc-protovalidate {
              inherit licenseFile;
            };
            trevrpc-rust = pkgs.callPackage ./trevrpc-rust {
              inherit
                wireGolden
                zstdGolden
                interopProto
                trevrpcProto
                licenseFile
                ;
            };
            # nix run .#trevrpc-interop: the interop matrix; see interop/README.md.
            trevrpc-interop = pkgs.callPackage ./interop {
              inherit (self.packages.${system}) trevrpc-go-interop trevrpc-js trevrpc-rust;
            };
            # nix run .#trevrpc-bench: the load benchmarks; see bench/README.md.
            trevrpc-bench = pkgs.callPackage ./bench {
              inherit (self.packages.${system})
                trevrpc-bench-baseline
                trevrpc-go-interop
                trevrpc-js
                trevrpc-rust
                ;
            };
            # nix run .#trevrpc-bench-micro: the Go and Rust microbenchmarks, from the checkout.
            trevrpc-bench-micro = pkgs.callPackage ./bench/micro.nix { };
            trevrpc-bench-baseline = pkgs.callPackage ./bench/baseline { inherit interopProto; };
          };

        # nix fmt
        formatter = pkgs.treefmt.withConfig {
          configFile = ./treefmt.toml;
          runtimeInputs = with pkgs; [
            rustfmt
            golangci-lint
            go
            nixfmt
            oxfmt
            ktlint
            ruff
            buf
          ];
        };

        # nix flake check
        checks = pkgs.mkChecks {
          inherit (self.packages.${system})
            trevrpc-go
            trevrpc-go-otel
            trevrpc-go-protovalidate
            trevrpc-go-interop
            trevrpc-js
            trevrpc-js-effect
            trevrpc-js-protovalidate
            trevrpc-kotlin
            trevrpc-rust
            trevrpc-bench-baseline
            trevrpc-bench-micro
            ;

          bench = pkgs.callPackage ./bench/check.nix {
            inherit (self.packages.${system}) trevrpc-bench;
          };
          interop = pkgs.callPackage ./interop/check.nix {
            inherit (self.packages.${system}) trevrpc-interop;
          };
          kotlin-interop-android = self.packages.${system}.trevrpc-kotlin.interopAndroid;

          trevrpc-go-lint = {
            src = self.packages.${system}.trevrpc-go;
            packages = with pkgs; [ golangci-lint ];
            script = ''
              golangci-lint run ./...
            '';
          };

          trevrpc-go-otel-lint = {
            src = self.packages.${system}.trevrpc-go-otel;
            packages = with pkgs; [ golangci-lint ];
            script = ''
              golangci-lint run ./...
            '';
          };

          trevrpc-go-interop-lint = {
            src = self.packages.${system}.trevrpc-go-interop;
            packages = with pkgs; [ golangci-lint ];
            script = ''
              golangci-lint run ./...
            '';
          };

          trevrpc-go-protovalidate-lint = {
            src = self.packages.${system}.trevrpc-go-protovalidate;
            packages = with pkgs; [ golangci-lint ];
            script = ''
              golangci-lint run ./...
            '';
          };

          trevrpc-bench-baseline-lint = {
            src = self.packages.${system}.trevrpc-bench-baseline;
            packages = with pkgs; [ golangci-lint ];
            script = ''
              golangci-lint run ./...
            '';
          };

          trevrpc-rust-lint = {
            src = self.packages.${system}.trevrpc-rust;
            packages = with pkgs; [
              rustfmt
              clippy
            ];
            script = ''
              cargo fmt --all --check
              cargo clippy --workspace --all-targets --offline -- -D warnings
              cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
            '';
          };

          trevrpc-js-lint = {
            src = self.packages.${system}.trevrpc-js;
            packages = with pkgs; [
              oxfmt
              oxlint
            ];
            script = ''
              oxfmt --check
              # oxlint's type-aware rules and tsc read the generated interop clients.
              npm run build
              oxlint --deny-warnings
              npm exec -- tsc -p tsconfig.json
            '';
          };

          trevrpc-js-effect-lint = {
            src = self.packages.${system}.trevrpc-js-effect;
            packages = with pkgs; [
              oxfmt
              oxlint
            ];
            script = ''
              oxfmt --check
              # oxlint's type-aware rules and tsc read the generated test client. oxlint is given
              # the package's own directories, so it doesn't follow the link to trevrpc-js.
              npm run build
              npm run lint
              npm exec -- tsc -p tsconfig.json
            '';
          };

          trevrpc-js-protovalidate-lint = {
            src = self.packages.${system}.trevrpc-js-protovalidate;
            packages = with pkgs; [
              oxfmt
              oxlint
            ];
            script = ''
              oxfmt --check
              # oxlint's type-aware rules and tsc read the generated test client. oxlint is given
              # the package's own directories, so it doesn't follow the link to trevrpc-js.
              npm run build
              npm run lint
              npm exec -- tsc -p tsconfig.json
            '';
          };

          # ktlint needs none of the dependencies, so it reads the sources instead of the package.
          trevrpc-kotlin-lint = {
            root = ./trevrpc-kotlin;
            filter = file: file.hasExt "kt" || file.hasExt "kts";
            packages = with pkgs; [
              ktlint
            ];
            script = ''
              ktlint --relative "**/src/**/*.kt" "**/*.gradle.kts"
            '';
          };

          protobuf-lint = {
            root = ./proto;
            filter = file: file.hasExt "proto" || file.hasExt "yaml" || file.hasExt "md";
            packages = with pkgs; [
              buf
            ];
            script = ''
              export BUF_CACHE_DIR=$(mktemp -d)
              buf lint
              buf format --diff --exit-code
              buf build -o /dev/null
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

          shell-lint = {
            root = ./.;
            filter = file: file.hasExt "sh";
            packages = with pkgs; [
              shellcheck
            ];
            script = ''
              shellcheck --external-sources "$file"
            '';
          };

          python-lint = {
            root = ./.;
            filter = file: file.hasExt "py";
            include = [ ./pyproject.toml ];
            packages = with pkgs; [
              ruff
              pyright
            ];
            script = ''
              ruff check
              ruff format --check
              pyright
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

          renovate-fj = {
            root = ./.forgejo;
            files = ./.forgejo/renovate.jsonc;
            packages = with pkgs; [
              renovate
            ];
            script = ''
              renovate-config-validator renovate.jsonc
            '';
          };

          config = {
            root = ./.;
            filter =
              file:
              file.hasExt "json"
              || file.hasExt "jsonc"
              || file.hasExt "yaml"
              || file.hasExt "toml"
              || file.hasExt "md";
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
