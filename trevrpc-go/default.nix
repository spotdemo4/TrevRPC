{
  pkgs,
  repoRoot,
}:
let
  package = pkgs.buildGoModule (
    final: with pkgs.lib; {
      pname = "trevrpc-go";
      version = "0.1.0";

      src = fileset.toSource {
        root = repoRoot;
        fileset = fileset.unions [
          (repoRoot + "/bench/proto")
          (repoRoot + "/testdata/wire-golden-vectors.txt")
          ./.
        ];
      };
      sourceRoot = "${final.src.name}/trevrpc-go";
      env.GOWORK = "off";
      vendorHash = "sha256-mgF3Ijy2WIM/LxSDr7wDcWa6rgqQ+DSu0V6tgqGWHRo=";
      subPackages = [ "cmd/protoc-gen-trevrpc-go" ];

      doCheck = false;
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

      doInstallCheck = true;
      nativeInstallCheckInputs = [ pkgs.gnugrep ];
      installCheckPhase = ''
        runHook preInstallCheck
        ! grep -q 'google.golang.org/grpc' go.mod
        test ! -e "$out/bin/trevrpc-bench-peer-go"
        runHook postInstallCheck
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

  benchPeer = pkgs.buildGoModule (
    final: with pkgs.lib; {
      pname = "trevrpc-go-bench-peer";
      version = "0.1.0";

      src = fileset.toSource {
        root = repoRoot;
        fileset = fileset.unions [
          (repoRoot + "/bench/proto")
          ./.
        ];
      };
      sourceRoot = "${final.src.name}/trevrpc-go/cmd/trevrpc-bench-peer";
      env.GOWORK = "off";
      vendorHash = "sha256-6vRmNAlXFa8OoR2AAs849+JR6CvNYwSR1aHSEasnqcI=";
      postPatch = ''
        go mod edit -replace=trev.zip/llc/trevrpc/trevrpc-go=../..
      '';
      subPackages = [ "." ];
      postInstall = ''
        mv $out/bin/trevrpc-bench-peer $out/bin/trevrpc-bench-peer-go
      '';

      doCheck = true;
      nativeCheckInputs = with pkgs; [
        go-tools
        gotools
      ];
      checkPhase = ''
        runHook preCheck
        export HOME=$(mktemp -d)
        go test ./...
        go vet ./...
        staticcheck ./...
        modernize ./...
        runHook postCheck
      '';

      meta = {
        mainProgram = "trevrpc-bench-peer-go";
        description = "Go TrevRPC and gRPC benchmark peer";
        license = licenses.mit;
        platforms = platforms.linux;
      };
    }
  );
in
{
  inherit package benchPeer;
}
