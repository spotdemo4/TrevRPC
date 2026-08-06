{
  lib,
  stdenvNoCC,
  gradle_9,
  jdk25,
  makeWrapper,
  maven,
  protobuf,
  runtimeShell,
  gnugrep,
  python3,
  licenseFile,
  wireGolden,
  greeterProto,
}:
stdenvNoCC.mkDerivation (final: {
  pname = "trevrpc-kotlin";
  version = "0.1.1";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      licenseFile
      wireGolden
      ./.
      greeterProto
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-kotlin";

  nativeBuildInputs = [
    gradle_9
    jdk25
    makeWrapper
    maven
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
  gradleBuildTask = [
    "stageMavenRepository"
    ":protoc-gen-trevrpc-kotlin:installDist"
    ":bench-peer:installDist"
    ":conformance-peer:installDist"
  ];
  gradleUpdateScript = ''
    runHook preBuild
    gradle --no-configuration-cache --write-locks \
      resolveAndLockAll \
      :core:dokkaGeneratePublicationHtml
  '';

  doCheck = true;
  preCheck = ''
    export TREVRPC_GRADLE_CACHE_SEED="$GRADLE_USER_HOME"
    export TREVRPC_GRADLE_METADATA_MODES=gradle
    if [ -d "''${mitmCache:-}" ]; then
      export MAVEN_SETTINGS="$PWD/maven-settings.xml"
      cat > "$MAVEN_SETTINGS" <<EOF
    <?xml version="1.0" encoding="UTF-8"?>
    <settings>
      <mirrors>
        <mirror>
          <id>mitm-central</id>
          <mirrorOf>*,!trevrpc-staging</mirrorOf>
          <url>file://$mitmCache/https/repo.maven.apache.org/maven2</url>
        </mirror>
      </mirrors>
    </settings>
    EOF
    fi
  '';
  gradleCheckTask = "check verifyStagedMavenRepository verifyGradleConsumers verifyMavenConsumers";

  installPhase = ''
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
    cp -R bench-peer/build/install/trevrpc-bench-peer-kotlin \
      $out/share/trevrpc-kotlin/trevrpc-bench-peer-kotlin
    makeWrapper \
      $out/share/trevrpc-kotlin/trevrpc-bench-peer-kotlin/bin/trevrpc-bench-peer-kotlin \
      $out/bin/trevrpc-bench-peer-kotlin \
      --set JAVA_HOME ${jdk25.home} \
      --prefix PATH : ${lib.makeBinPath [ jdk25 ]}
    cp -R conformance-peer/build/install/trevrpc-conformance-kotlin \
      $out/share/trevrpc-kotlin/trevrpc-conformance-kotlin
    makeWrapper \
      $out/share/trevrpc-kotlin/trevrpc-conformance-kotlin/bin/trevrpc-conformance-kotlin \
      $out/bin/trevrpc-conformance-kotlin \
      --set JAVA_HOME ${jdk25.home} \
      --prefix PATH : ${lib.makeBinPath [ jdk25 ]}
    runHook postInstall
  '';

  doInstallCheck = true;
  nativeInstallCheckInputs = [ gnugrep ];
  installCheckPhase = ''
    runHook preInstallCheck
    if find "$out" \( -iname '*grpc*' -o -iname '*tonic*' \) -print -quit \
      | grep -q .; then
      echo "Kotlin consumer package contains a gRPC or Tonic artifact" >&2
      exit 1
    fi
    test -x "$out/bin/trevrpc-bench-peer-kotlin"
    test -x "$out/bin/trevrpc-conformance-kotlin"
    printf 'STOP\n' | "$out/bin/trevrpc-conformance-kotlin" --protocol 1 > peer.out
    grep -q '"event":"ready"' peer.out
    grep -q '"peer":"kotlin"' peer.out
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
    mainProgram = "protoc-gen-trevrpc-kotlin";
    description = "Kotlin TrevRPC runtime, transports, and protobuf generator";
    homepage = "https://trev.zip/llc/TrevRPC";
  };
})
