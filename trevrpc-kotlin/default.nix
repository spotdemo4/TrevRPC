{
  androidenv,
  callPackage,
  gradle-packages,
  lib,
  stdenvNoCC,
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
  trevrpcBench,
}:
let
  gradleWrapperLines = lib.splitString "\n" (
    builtins.readFile ./gradle/wrapper/gradle-wrapper.properties
  );
  readGradleWrapperProperty =
    name:
    let
      prefix = "${name}=";
      values = map (line: lib.removePrefix prefix line) (
        builtins.filter (lib.hasPrefix prefix) gradleWrapperLines
      );
    in
    if builtins.length values == 1 then
      builtins.head values
    else
      throw "expected exactly one ${name} in gradle-wrapper.properties";
  gradleDistributionUrl = lib.replaceStrings [ "\\:" ] [ ":" ] (
    readGradleWrapperProperty "distributionUrl"
  );
  gradleVersionMatch = builtins.match "https://services[.]gradle[.]org/distributions/gradle-(9[.][0-9]+[.][0-9]+)-bin[.]zip" gradleDistributionUrl;
  gradleVersion =
    if gradleVersionMatch == null then
      throw "gradle-wrapper.properties must use an official Gradle 9 binary distribution"
    else
      builtins.head gradleVersionMatch;
  gradleDistributionSha256 = readGradleWrapperProperty "distributionSha256Sum";
  gradleHash =
    if builtins.match "[0-9a-f]{64}" gradleDistributionSha256 == null then
      throw "distributionSha256Sum in gradle-wrapper.properties must be a lowercase SHA-256 hash"
    else
      builtins.convertHash {
        hash = gradleDistributionSha256;
        hashAlgo = "sha256";
        toHashFormat = "sri";
      };
  gradle9 =
    (gradle-packages.mkGradle {
      version = gradleVersion;
      hash = gradleHash;
      defaultJava = jdk25;
    }).wrapped;
  benchPeerCronet =
    if stdenvNoCC.hostPlatform.system == "x86_64-linux" then
      let
        androidSdk =
          (androidenv.composeAndroidPackages {
            platformVersions = [
              "33"
              "36"
            ];
            buildToolsVersions = [ "37.0.0" ];
            includeEmulator = true;
            includeSystemImages = true;
            systemImageTypes = [ "google_apis" ];
            abiVersions = [ "x86_64" ];
          }).androidsdk;
      in
      callPackage ./bench-peer-cronet {
        inherit androidSdk licenseFile trevrpcBench;
        androidRunner = ../bench/ci/run-android-smoke-cell.py;
        gradle_9 = gradle9;
      }
    else
      null;
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
      throw "trevrpc-kotlin does not support ${stdenvNoCC.hostPlatform.system}";
in
stdenvNoCC.mkDerivation (final: {
  pname = "trevrpc-kotlin";
  version = "0.3.4";
  passthru.benchPeerCronet = benchPeerCronet;

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
    gradle9
    jdk25
    makeWrapper
    maven
    protobuf
    python3
  ];

  mitmCache = gradle9.fetchDeps {
    pkg = final.finalPackage;
    data = ./gradle/deps.json;
    bwrapFlags = ''--ro-bind "$PWD" "$PWD" --dir /bin --symlink ${runtimeShell} /bin/sh'';
  };
  __darwinAllowLocalNetworking = true;

  gradleFlags = [
    "-Dorg.gradle.java.home=${jdk25.home}"
    "-PtrevrpcProtocPath=${protobuf}/bin/protoc"
  ];
  gradleBuildTask = [
    "stageMavenRepository"
    ":protoc-gen-trevrpc-kotlin:installDist"
    ":bench-peer-netty:installDist"
    ":conformance-peer:installDist"
  ];
  gradleUpdateScript = ''
    runHook preBuild
    if [ -n "''${MITM_CACHE_ADDRESS:-}" ]; then
      maven_trust_store=$(mktemp)
      rm "$maven_trust_store"
      keytool \
        -importcert \
        -noprompt \
        -alias trevrpc-mitm-cache \
        -file "$MITM_CACHE_CA" \
        -keystore "$maven_trust_store" \
        -storepass changeit
      export MAVEN_OPTS="''${MAVEN_OPTS:-} \
        -Djavax.net.ssl.trustStore=$maven_trust_store \
        -Djavax.net.ssl.trustStorePassword=changeit \
        -Dhttp.proxyHost=$MITM_CACHE_HOST \
        -Dhttp.proxyPort=$MITM_CACHE_PORT \
        -Dhttps.proxyHost=$MITM_CACHE_HOST \
        -Dhttps.proxyPort=$MITM_CACHE_PORT"
      export MAVEN_SETTINGS=$(mktemp)
      cat > "$MAVEN_SETTINGS" <<EOF
    <?xml version="1.0" encoding="UTF-8"?>
    <settings>
      <proxies>
        <proxy>
          <id>trevrpc-mitm-cache-http</id>
          <active>true</active>
          <protocol>http</protocol>
          <host>$MITM_CACHE_HOST</host>
          <port>$MITM_CACHE_PORT</port>
          <nonProxyHosts>127.0.0.1|localhost</nonProxyHosts>
        </proxy>
        <proxy>
          <id>trevrpc-mitm-cache-https</id>
          <active>true</active>
          <protocol>https</protocol>
          <host>$MITM_CACHE_HOST</host>
          <port>$MITM_CACHE_PORT</port>
          <nonProxyHosts>127.0.0.1|localhost</nonProxyHosts>
        </proxy>
      </proxies>
    </settings>
    EOF
    fi
    gradle --no-configuration-cache --write-locks populateNixDependencyCache
  '';

  doCheck = true;
  preCheck = ''
    export TREVRPC_GRADLE_CACHE_SEED="$GRADLE_USER_HOME"
    export TREVRPC_GRADLE_METADATA_MODES=gradle
    if [ -d "''${mitmCache:-}" ]; then
      export TREVRPC_GRADLE_MAVEN_CENTRAL="file://$mitmCache/https/repo.maven.apache.org/maven2"
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
  gradleCheckTask = "check verifyPublicationConsumers";

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
    cp -R bench-peer-netty/build/install/trevrpc-bench-peer-kotlin-netty \
      $out/share/trevrpc-kotlin/trevrpc-bench-peer-kotlin-netty
    makeWrapper \
      $out/share/trevrpc-kotlin/trevrpc-bench-peer-kotlin-netty/bin/trevrpc-bench-peer-kotlin-netty \
      $out/bin/trevrpc-bench-peer-kotlin-netty \
      --set JAVA_HOME ${jdk25.home} \
      --prefix PATH : ${lib.makeBinPath [ jdk25 ]}
    ln -s trevrpc-bench-peer-kotlin-netty $out/bin/trevrpc-bench-peer-kotlin
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
    test -x "$out/bin/trevrpc-bench-peer-kotlin-netty"
    "$out/bin/trevrpc-bench-peer-kotlin-netty" capabilities > capabilities.json
    python3 - <<'PY'
    import json

    with open("capabilities.json", encoding="utf-8") as stream:
        capabilities = json.load(stream)
    assert capabilities["schema_version"] == 5
    assert capabilities["peer"] == "kotlin"
    assert capabilities["roles"] == {
        "client": ["trevrpc_native_quic"],
        "server": ["trevrpc_native_quic", "trevrpc_http3", "trevrpc_webtransport"],
    }
    PY
    set -- "$out/share/trevrpc-kotlin/trevrpc-bench-peer-kotlin-netty/lib"/netty-codec-native-quic-*-${nettyNativeClassifier}.jar
    test "$#" -eq 1
    test -f "$1"
    test -x "$out/bin/trevrpc-conformance-kotlin"
    printf 'STOP\n' | "$out/bin/trevrpc-conformance-kotlin" --protocol 1 > peer.out
    grep -q '"event":"ready"' peer.out
    grep -q '"peer":"kotlin"' peer.out
    test ! -e "$out/bin/trevrpc-xruntime-kotlin"
    test -f "$out/share/java/core-${final.version}.jar"
    test -f "$out/share/java/transport-cronet-${final.version}.jar"
    test -f "$out/share/java/transport-netty-${final.version}.jar"
    test -x "$out/bin/protoc-gen-trevrpc-kotlin"
    runHook postInstallCheck
  '';

  meta = {
    license = lib.licenses.mit;
    platforms = [
      "x86_64-linux"
      "aarch64-darwin"
    ];
    sourceProvenance = with lib.sourceTypes; [
      fromSource
      binaryBytecode
    ];
    mainProgram = "protoc-gen-trevrpc-kotlin";
    description = "Kotlin TrevRPC runtime, transports, and protobuf generator";
    homepage = "https://trev.zip/llc/TrevRPC";
  };
})
