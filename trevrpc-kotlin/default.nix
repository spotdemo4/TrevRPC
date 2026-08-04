{
  lib,
  stdenvNoCC,
  gradle_9,
  jdk25,
  makeWrapper,
  protobuf,
  runtimeShell,
  gnugrep,
  python3,
  repoRoot,
  benchPeer ? false,
  conformancePeer ? false,
}:
assert !(benchPeer && conformancePeer);
stdenvNoCC.mkDerivation (final: {
  pname =
    if benchPeer then
      "trevrpc-kotlin-bench-peer"
    else if conformancePeer then
      "trevrpc-kotlin-conformance-peer"
    else
      "trevrpc-kotlin";
  version = "0.1.0";

  src = lib.fileset.toSource {
    root = repoRoot;
    fileset = lib.fileset.unions [
      (repoRoot + "/LICENSE")
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
    python3
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
    else if conformancePeer then
      [ ":conformance-peer:installDist" ]
    else
      [
        "stageMavenRepository"
        ":protoc-gen-trevrpc-kotlin:installDist"
      ];
  gradleUpdateScript = ''
    runHook preBuild
    gradle --no-configuration-cache --write-locks \
      resolveAndLockAll \
      :core:dokkaGeneratePublicationHtml
  '';

  doCheck = true;
  preCheck = lib.optionalString (!benchPeer && !conformancePeer) ''
    export TREVRPC_GRADLE_CACHE_SEED="$GRADLE_USER_HOME"
    # Maven POM fallback is covered independently by kotlin-maven-consumer.
    export TREVRPC_GRADLE_METADATA_MODES=gradle
  '';
  gradleCheckTask =
    if benchPeer then
      ":bench-peer:check"
    else if conformancePeer then
      ":conformance-peer:check"
    else
      "check verifyStagedMavenRepository verifyGradleConsumers";

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
    else if conformancePeer then
      ''
        runHook preInstall
        mkdir -p $out/bin $out/share/trevrpc-kotlin
        cp -R conformance-peer/build/install/trevrpc-conformance-kotlin \
          $out/share/trevrpc-kotlin/trevrpc-conformance-kotlin
        makeWrapper \
          $out/share/trevrpc-kotlin/trevrpc-conformance-kotlin/bin/trevrpc-conformance-kotlin \
          $out/bin/trevrpc-conformance-kotlin \
          --set JAVA_HOME ${jdk25.home} \
          --prefix PATH : ${lib.makeBinPath [ jdk25 ]}
        runHook postInstall
      ''
    else
      ''
        runHook preInstall
        mkdir -p $out/bin $out/share/java $out/share/maven $out/share/trevrpc-kotlin
        cp -R build/staging-repository/. $out/share/maven/
        cp build/staging-repository/zip/trev/trevrpc/core/${final.version}/core-${final.version}.jar \
          $out/share/java/
        cp build/staging-repository/zip/trev/trevrpc/transport-cronet/${final.version}/transport-cronet-${final.version}.jar \
          $out/share/java/
        cp build/staging-repository/zip/trev/trevrpc/transport-netty/${final.version}/transport-netty-${final.version}.jar \
          $out/share/java/
        cp -R protoc-gen-trevrpc-kotlin/build/install/protoc-gen-trevrpc-kotlin \
          $out/share/trevrpc-kotlin/protoc-gen-trevrpc-kotlin
        makeWrapper \
          $out/share/trevrpc-kotlin/protoc-gen-trevrpc-kotlin/bin/protoc-gen-trevrpc-kotlin \
          $out/bin/protoc-gen-trevrpc-kotlin \
          --set JAVA_HOME ${jdk25.home} \
          --prefix PATH : ${lib.makeBinPath [ jdk25 ]}
        runHook postInstall
      '';

  doInstallCheck = !benchPeer && !conformancePeer;
  nativeInstallCheckInputs = [ gnugrep ];
  installCheckPhase = ''
    runHook preInstallCheck
    if find "$out" \( -iname '*grpc*' -o -iname '*tonic*' \) -print -quit \
      | grep -q .; then
      echo "Kotlin consumer package contains a gRPC or Tonic artifact" >&2
      exit 1
    fi
    test ! -e "$out/bin/trevrpc-bench-peer-kotlin"
    test ! -e "$out/bin/trevrpc-conformance-kotlin"
    test ! -e "$out/bin/trevrpc-xruntime-kotlin"
    test -f "$out/share/java/core-${final.version}.jar"
    test -f "$out/share/java/transport-cronet-${final.version}.jar"
    test -f "$out/share/java/transport-netty-${final.version}.jar"
    test -x "$out/bin/protoc-gen-trevrpc-kotlin"
    python3 publication-tests/verify_staged_repository.py "$out/share/maven"
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
  // lib.optionalAttrs conformancePeer {
    mainProgram = "trevrpc-conformance-kotlin";
    description = "Kotlin TrevRPC conformance peer";
  }
  // lib.optionalAttrs (!benchPeer && !conformancePeer) {
    mainProgram = "protoc-gen-trevrpc-kotlin";
    description = "Kotlin TrevRPC runtime, transports, and protobuf generator";
    homepage = "https://trev.zip/llc/TrevRPC";
  };
})
