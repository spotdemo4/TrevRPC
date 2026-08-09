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
  benchmarkOnly ? false,
}:
let
  nettyNativeClassifier =
    if stdenvNoCC.hostPlatform.system == "x86_64-linux" then
      "linux-x86_64"
    else if stdenvNoCC.hostPlatform.system == "aarch64-linux" then
      "linux-aarch_64"
    else if stdenvNoCC.hostPlatform.system == "x86_64-darwin" then
      "osx-x86_64"
    else if stdenvNoCC.hostPlatform.system == "aarch64-darwin" then
      "osx-aarch_64"
    else
      throw "trevrpc-bench-peer-kotlin does not support ${stdenvNoCC.hostPlatform.system}";
in
stdenvNoCC.mkDerivation (final: {
  pname = if benchmarkOnly then "trevrpc-bench-peer-kotlin" else "trevrpc-kotlin";
  version = "0.1.9";

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
    protobuf
    python3
  ]
  ++ lib.optional (!benchmarkOnly) maven;

  mitmCache = gradle_9.fetchDeps {
    pkg = final.finalPackage;
    data = ./gradle/deps.json;
    bwrapFlags = ''--ro-bind "$PWD" "$PWD" --dir /bin --symlink ${runtimeShell} /bin/sh'';
  };
  __darwinAllowLocalNetworking = true;

  gradleFlags = [
    "-Dorg.gradle.java.home=${jdk25.home}"
  ]
  ++ lib.optionals benchmarkOnly [
    "-PtrevrpcNettyNativeClassifier=${nettyNativeClassifier}"
    "-PtrevrpcProtocPath=${protobuf}/bin/protoc"
  ];
  gradleBuildTask =
    if benchmarkOnly then
      [ ":bench-peer:installDist" ]
    else
      [
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
  preCheck = lib.optionalString (!benchmarkOnly) ''
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
  gradleCheckTask =
    if benchmarkOnly then
      ":bench-peer:check"
    else
      "check verifyStagedMavenRepository verifyGradleConsumers verifyMavenConsumers";

  installPhase =
    if benchmarkOnly then
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
  nativeInstallCheckInputs = lib.optional (!benchmarkOnly) gnugrep;
  installCheckPhase =
    if benchmarkOnly then
      ''
        runHook preInstallCheck
        test -x "$out/bin/trevrpc-bench-peer-kotlin"
        "$out/bin/trevrpc-bench-peer-kotlin" capabilities > capabilities.json
        python3 - <<'PY'
        import json

        with open("capabilities.json", encoding="utf-8") as stream:
            capabilities = json.load(stream)
        assert capabilities["schema_version"] == 4
        assert capabilities["peer"] == "kotlin"
        assert capabilities["roles"] == {
            "client": ["trevrpc_native_quic"],
            "server": ["trevrpc_native_quic", "trevrpc_webtransport"],
        }
        PY
        set -- "$out/share/trevrpc-kotlin/trevrpc-bench-peer-kotlin/lib"/netty-codec-native-quic-*.jar
        test "$#" -eq 1
        test -f "$1"
        case "$(basename "$1")" in
          *-${nettyNativeClassifier}.jar) ;;
          *)
            echo "Expected Netty QUIC classifier ${nettyNativeClassifier}, found $(basename "$1")" >&2
            exit 1
            ;;
        esac
        runHook postInstallCheck
      ''
    else
      ''
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
    platforms =
      if benchmarkOnly then
        [
          "x86_64-linux"
          "aarch64-darwin"
        ]
      else
        [ "x86_64-linux" ];
    sourceProvenance = with lib.sourceTypes; [
      fromSource
      binaryBytecode
    ];
    mainProgram = if benchmarkOnly then "trevrpc-bench-peer-kotlin" else "protoc-gen-trevrpc-kotlin";
    description =
      if benchmarkOnly then
        "Kotlin TrevRPC benchmark peer"
      else
        "Kotlin TrevRPC runtime, transports, and protobuf generator";
    homepage = "https://trev.zip/llc/TrevRPC";
  };
})
