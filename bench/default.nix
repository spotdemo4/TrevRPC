{
  lib,
  makeWrapper,
  openssl,
  rustPlatform,
  sourceCommit,
  sourceDirty,
}:
rustPlatform.buildRustPackage (
  final: with lib; {
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

    nativeBuildInputs = [ makeWrapper ];
    postInstall = ''
      wrapProgram $out/bin/trevrpc-bench \
        --prefix PATH : ${makeBinPath [ openssl ]} \
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
