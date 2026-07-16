{
  lib,
  stdenvNoCC,
  gradle_9,
  jdk25,
  makeWrapper,
  protobuf,
  runtimeShell,
  gnugrep,
  repoRoot,
  benchPeer ? false,
}:
stdenvNoCC.mkDerivation (final: {
  pname = if benchPeer then "trevrpc-kotlin-bench-peer" else "trevrpc-kotlin";
  version = "0.1.0";

  src = lib.fileset.toSource {
    root = repoRoot;
    fileset = lib.fileset.unions [
      (repoRoot + "/testdata/wire-golden-vectors.txt")
      ./.
      (repoRoot + "/trevrpc-rust/crates/protoc-gen-trevrpc-rust/tests/proto/greeter.proto")
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-kotlin";

  nativeBuildInputs = [
    gradle_9
    jdk25
    makeWrapper
    protobuf
  ];

  mitmCache = gradle_9.fetchDeps {
    pkg = final.finalPackage;
    data = ./gradle/deps.json;
    bwrapFlags = ''--ro-bind "$PWD" "$PWD" --dir /bin --symlink ${runtimeShell} /bin/sh'';
  };
  __darwinAllowLocalNetworking = true;

  gradleFlags = [
    "-Dorg.gradle.java.home=${jdk25.home}"
  ];
  gradleBuildTask =
    if benchPeer then
      [ ":bench-peer:installDist" ]
    else
      [
        ":core:assemble"
        ":transport-cronet:assemble"
        ":transport-netty:assemble"
        ":examples:installDist"
        ":protoc-gen-trevrpc-kotlin:installDist"
      ];
  gradleUpdateScript = ''
    runHook preBuild
    gradle build
  '';

  doCheck = true;
  gradleCheckTask = if benchPeer then ":bench-peer:check" else "check";

  installPhase =
    if benchPeer then
      ''
        runHook preInstall
        mkdir -p $out/bin $out/share/trevrpc-kotlin
        cp -R bench-peer/build/install/trevrpc-bench-peer-kotlin \
          $out/share/trevrpc-kotlin/trevrpc-bench-peer-kotlin
        makeWrapper \
          $out/share/trevrpc-kotlin/trevrpc-bench-peer-kotlin/bin/trevrpc-bench-peer-kotlin \
          $out/bin/trevrpc-bench-peer-kotlin \
          --set JAVA_HOME ${jdk25.home} \
          --prefix PATH : ${lib.makeBinPath [ jdk25 ]}
        runHook postInstall
      ''
    else
      ''
        runHook preInstall
        mkdir -p $out/bin $out/share/trevrpc-kotlin
        cp -R examples/build/install/trevrpc-xruntime-kotlin/* $out/share/trevrpc-kotlin/
        cp -R protoc-gen-trevrpc-kotlin/build/install/protoc-gen-trevrpc-kotlin \
          $out/share/trevrpc-kotlin/protoc-gen-trevrpc-kotlin
        makeWrapper $out/share/trevrpc-kotlin/bin/trevrpc-xruntime-kotlin \
          $out/bin/trevrpc-xruntime-kotlin \
          --set JAVA_HOME ${jdk25.home} \
          --prefix PATH : ${lib.makeBinPath [ jdk25 ]}
        makeWrapper \
          $out/share/trevrpc-kotlin/protoc-gen-trevrpc-kotlin/bin/protoc-gen-trevrpc-kotlin \
          $out/bin/protoc-gen-trevrpc-kotlin \
          --set JAVA_HOME ${jdk25.home} \
          --prefix PATH : ${lib.makeBinPath [ jdk25 ]}
        runHook postInstall
      '';

  doInstallCheck = !benchPeer;
  nativeInstallCheckInputs = [ gnugrep ];
  installCheckPhase = ''
    runHook preInstallCheck
    if find "$out" \( -iname '*grpc*' -o -iname '*tonic*' \) -print -quit \
      | grep -q .; then
      echo "Kotlin consumer package contains a gRPC or Tonic artifact" >&2
      exit 1
    fi
    test ! -e "$out/bin/trevrpc-bench-peer-kotlin"
    runHook postInstallCheck
  '';

  meta = {
    license = lib.licenses.mit;
    platforms = [ "x86_64-linux" ];
    sourceProvenance = with lib.sourceTypes; [
      fromSource
      binaryBytecode
    ];
  }
  // lib.optionalAttrs benchPeer {
    mainProgram = "trevrpc-bench-peer-kotlin";
    description = "Kotlin TrevRPC and gRPC benchmark peer";
  }
  // lib.optionalAttrs (!benchPeer) {
    mainProgram = "protoc-gen-trevrpc-kotlin";
    description = "Kotlin TrevRPC runtime, transports, and protobuf generator";
    homepage = "https://trev.zip/llc/TrevRPC";
  };
})
