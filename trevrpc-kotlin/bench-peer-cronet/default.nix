{
  androidRunner,
  androidSdk,
  gradle_9,
  jdk25,
  lib,
  licenseFile,
  makeWrapper,
  maven,
  openssl,
  protobuf,
  python3,
  runtimeShell,
  stdenvNoCC,
  trevrpcBench,
}:
stdenvNoCC.mkDerivation (final: {
  pname = "trevrpc-kotlin-bench-peer-cronet";
  version = "0.3.4";

  src = lib.fileset.toSource {
    root = ../../.;
    fileset = lib.fileset.unions [
      licenseFile
      ../.
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-kotlin";

  nativeBuildInputs = [
    androidSdk
    gradle_9
    jdk25
    makeWrapper
    maven
    protobuf
    python3
  ];

  mitmCache = gradle_9.fetchDeps {
    pkg = final.finalPackage;
    data = ../gradle/deps.json;
    bwrapFlags = ''--ro-bind "$PWD" "$PWD" --dir /bin --symlink ${runtimeShell} /bin/sh'';
  };

  gradleFlags = [
    "-Dorg.gradle.java.home=${jdk25.home}"
    "-Dorg.gradle.project.android.aapt2FromMavenOverride=${androidSdk}/libexec/android-sdk/build-tools/37.0.0/aapt2"
    "-PtrevrpcCronetBenchPeer=true"
    "-PtrevrpcProtocPath=${protobuf}/bin/protoc"
  ];
  gradleBuildTask = [
    ":bench-peer-cronet:assembleDebug"
    ":bench-peer-cronet:assembleDebugAndroidTest"
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
    export ANDROID_HOME=${androidSdk}/libexec/android-sdk
    export ANDROID_SDK_ROOT=$ANDROID_HOME
    gradle --no-configuration-cache --write-locks \
      -PtrevrpcCronetBenchPeer=true \
      populateNixDependencyCache \
      :bench-peer-cronet:assembleDebug \
      :bench-peer-cronet:assembleDebugAndroidTest
  '';

  preBuild = ''
    export ANDROID_HOME=${androidSdk}/libexec/android-sdk
    export ANDROID_SDK_ROOT=$ANDROID_HOME
  '';

  installPhase = ''
    runHook preInstall
    peer=$out/share/trevrpc-kotlin/bench-peer-cronet
    mkdir -p $out/bin "$peer"
    cp bench-peer-cronet/build/outputs/apk/debug/bench-peer-cronet-debug.apk \
      "$peer/app.apk"
    cp bench-peer-cronet/build/outputs/apk/androidTest/debug/bench-peer-cronet-debug-androidTest.apk \
      "$peer/test.apk"
    cp ${androidRunner} "$peer/run.py"
    makeWrapper ${python3}/bin/python3 $out/bin/trevrpc-bench-peer-kotlin-cronet \
      --add-flags "$peer/run.py" \
      --add-flags "--app-apk $peer/app.apk" \
      --add-flags "--test-apk $peer/test.apk" \
      --set ANDROID_HOME ${androidSdk}/libexec/android-sdk \
      --set ANDROID_SDK_ROOT ${androidSdk}/libexec/android-sdk \
      --prefix PATH : ${
        lib.makeBinPath [
          androidSdk
          openssl
          trevrpcBench
        ]
      }
    runHook postInstall
  '';

  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck
    test -x $out/bin/trevrpc-bench-peer-kotlin-cronet
    test -f $out/share/trevrpc-kotlin/bench-peer-cronet/app.apk
    test -f $out/share/trevrpc-kotlin/bench-peer-cronet/test.apk
    $out/bin/trevrpc-bench-peer-kotlin-cronet --help >/dev/null
    runHook postInstallCheck
  '';

  meta = {
    description = "Android Cronet interoperability benchmark peer";
    license = lib.licenses.mit;
    platforms = [ "x86_64-linux" ];
    sourceProvenance = with lib.sourceTypes; [
      fromSource
      binaryBytecode
      binaryNativeCode
    ];
    mainProgram = "trevrpc-bench-peer-kotlin-cronet";
  };
})
