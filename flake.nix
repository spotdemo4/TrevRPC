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
            };
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
              jdk25
              oxfmt
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
        packages =
          let
            wireGolden = ./testdata/wire-golden-vectors.txt;
            qmuxGolden = ./testdata/qmux-golden-vectors.txt;
            interopProto = ./interop/proto;
            licenseFile = ./LICENSE;
          in
          {
            trevrpc-go = pkgs.callPackage ./trevrpc-go { inherit wireGolden qmuxGolden interopProto; };
            trevrpc-js = pkgs.callPackage ./trevrpc-js { inherit wireGolden qmuxGolden interopProto; };
            trevrpc-kotlin = pkgs.callPackage ./trevrpc-kotlin { inherit wireGolden interopProto licenseFile; };
            trevrpc-rust = pkgs.callPackage ./trevrpc-rust { inherit wireGolden interopProto; };
            trevrpc-interop = pkgs.callPackage ./interop/runner.nix { };
          };

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
          inherit (self.packages.${system})
            trevrpc-go
            trevrpc-js
            trevrpc-kotlin
            trevrpc-rust
            ;

          interop = pkgs.callPackage ./interop {
            inherit (self.packages.${system})
              trevrpc-go
              trevrpc-interop
              trevrpc-js
              trevrpc-rust
              ;
          };
          kotlin-interop-android = self.packages.${system}.trevrpc-kotlin.interopAndroid;

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
