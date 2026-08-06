{
  lib,
  iproute2,
  makeWrapper,
  openssl,
  rustPlatform,
  conformanceSrc,
  wireGolden,
  sourceCommit,
  sourceDirty,
  util-linux,
}:
rustPlatform.buildRustPackage (
  final: with lib; {
    pname = "trevrpc-bench";
    version = "0.1.3";

    src = fileset.toSource {
      root = ../.;
      fileset = fileset.unions [
        ./Cargo.lock
        ./Cargo.toml
        ./campaigns
        ./peer-protocol-v4.md
        ./proto
        ./src
        conformanceSrc
        wireGolden
      ];
    };
    sourceRoot = "${final.src.name}/bench";
    cargoLock.lockFile = ./Cargo.lock;

    nativeBuildInputs = [ makeWrapper ];
    postInstall = ''
      wrapProgram $out/bin/trevrpc-bench \
        --prefix PATH : ${
          makeBinPath [
            openssl
            iproute2
            util-linux
          ]
        } \
        --set TREVRPC_BENCH_SOURCE_COMMIT ${sourceCommit} \
        --set TREVRPC_BENCH_SOURCE_DIRTY ${sourceDirty}
      wrapProgram $out/bin/trevrpc-conformance \
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
