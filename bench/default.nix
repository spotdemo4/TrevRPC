{
  pkgs,
  sourceCommit,
  sourceDirty,
}:
pkgs.rustPlatform.buildRustPackage (
  final: with pkgs.lib; {
    pname = "trevrpc-bench";
    version = "0.1.0";

    src = fileset.toSource {
      root = ./.;
      fileset = fileset.unions [
        ./Cargo.lock
        ./Cargo.toml
        ./campaigns
        ./peer-protocol-v2.md
        ./proto
        ./src
      ];
    };
    cargoLock.lockFile = ./Cargo.lock;

    nativeBuildInputs = [ pkgs.makeWrapper ];
    postInstall = ''
      wrapProgram $out/bin/trevrpc-bench \
        --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.openssl ]} \
        --set TREVRPC_BENCH_SOURCE_COMMIT ${sourceCommit} \
        --set TREVRPC_BENCH_SOURCE_DIRTY ${sourceDirty}
    '';

    meta = {
      mainProgram = "trevrpc-bench";
      description = "Cross-language TrevRPC benchmark controller and reporter";
      license = licenses.mit;
      platforms = platforms.linux;
    };
  }
)
