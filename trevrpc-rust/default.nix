{
  pkgs,
  repoRoot,
}:
let
  package = pkgs.rustPlatform.buildRustPackage (
    final: with pkgs.lib; {
      pname = "trevrpc-rust";
      version = "0.1.0";

      src = fileset.toSource {
        root = repoRoot;
        fileset = fileset.unions [
          (repoRoot + "/bench/proto")
          (repoRoot + "/testdata/wire-golden-vectors.txt")
          ./.
        ];
      };
      sourceRoot = "${final.src.name}/trevrpc-rust";
      cargoLock.lockFile = ./Cargo.lock;
      cargoBuildFlags = [
        "--package"
        "protoc-gen-trevrpc-rust"
      ];

      doCheck = true;
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
        install -Dm755 "$generator" $out/bin/protoc-gen-trevrpc-rust
        runHook postInstall
      '';

      doInstallCheck = true;
      nativeInstallCheckInputs = [ pkgs.gnugrep ];
      installCheckPhase = ''
        runHook preInstallCheck
        ! grep -Eq '^tonic(-prost)?[[:space:]]*=' Cargo.toml
        test ! -e "$out/bin/trevrpc-bench-peer-rust"
        runHook postInstallCheck
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

  benchPeer = package.overrideAttrs (
    old: with pkgs.lib; {
      pname = "trevrpc-rust-bench-peer";
      doInstallCheck = false;
      cargoBuildFlags = [
        "--package"
        "trevrpc-bench-peer"
      ];

      doCheck = true;
      checkPhase = ''
        runHook preCheck
        cargo test --package trevrpc-bench-peer --offline
        runHook postCheck
      '';
      installPhase = ''
        runHook preInstall
        peer=$(find target -path '*/release/trevrpc-bench-peer-rust' -type f -perm -0100 | head -n1)
        install -Dm755 "$peer" $out/bin/trevrpc-bench-peer-rust
        runHook postInstall
      '';

      meta = {
        mainProgram = "trevrpc-bench-peer-rust";
        description = "Rust TrevRPC and gRPC benchmark peer";
        license = licenses.mit;
        platforms = platforms.linux;
      };
    }
  );
in
{
  inherit package benchPeer;
}
