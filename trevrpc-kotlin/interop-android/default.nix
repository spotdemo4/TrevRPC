{
  androidSdk,
  gradle_9,
  interopHarness,
  interopProto,
  jdk25,
  lib,
  licenseFile,
  makeWrapper,
  openssl,
  protobuf,
  python3,
  runtimeShell,
  stdenvNoCC,
  trevrpcProto,
  wireGolden,
}:
stdenvNoCC.mkDerivation (final: {
  pname = "trevrpc-kotlin-interop-android";
  version = "1.1.0";

  src = lib.fileset.toSource {
    root = ../../.;
    fileset = lib.fileset.unions [
      licenseFile
      wireGolden
      interopHarness
      interopProto
      trevrpcProto
      ../.
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-kotlin";

  nativeBuildInputs = [
    androidSdk
    gradle_9
    jdk25
    makeWrapper
    openssl
    protobuf
    python3
  ];

  # Shares the dependency state with the parent package: the update script resolves every module.
  mitmCache = gradle_9.fetchDeps {
    pkg = final.finalPackage;
    data = ../gradle/deps.json;
    bwrapFlags = ''--ro-bind "$PWD" "$PWD" --dir /bin --symlink ${runtimeShell} /bin/sh'';
  };

  gradleFlags = [
    "-Dorg.gradle.java.home=${jdk25.home}"
    "-Dorg.gradle.project.android.aapt2FromMavenOverride=${androidSdk}/libexec/android-sdk/build-tools/37.0.0/aapt2"
    "-PtrevrpcAndroidInterop=true"
  ];
  gradleBuildTask = [
    ":interop-android:assembleDebug"
    ":interop-android:assembleDebugAndroidTest"
  ];
  gradleUpdateScript = ''
    runHook preBuild
    export ANDROID_HOME=${androidSdk}/libexec/android-sdk
    export ANDROID_SDK_ROOT=$ANDROID_HOME
    gradle --no-configuration-cache --write-locks \
      -PtrevrpcAndroidInterop=true \
      updateGradleDependencyState \
      stageMavenRepository \
      :core:test \
      :transport-cronet:test \
      :protoc-gen-trevrpc-kotlin:test \
      :interop-android:assembleDebug \
      :interop-android:assembleDebugAndroidTest
  '';

  preBuild = ''
    export ANDROID_HOME=${androidSdk}/libexec/android-sdk
    export ANDROID_SDK_ROOT=$ANDROID_HOME
  '';

  installPhase = ''
    runHook preInstall
    share=$out/share/trevrpc-kotlin/interop-android
    mkdir -p $out/bin "$share"
    cp interop-android/build/outputs/apk/debug/interop-android-debug.apk "$share/app.apk"
    cp interop-android/build/outputs/apk/androidTest/debug/interop-android-debug-androidTest.apk \
      "$share/test.apk"
    cp interop-android/run.py "$share/run.py"
    # run.py imports the shared harness from ../../interop, as in the checkout.
    install -Dm644 ../interop/harness.py $out/share/interop/harness.py
    makeWrapper ${python3}/bin/python3 $out/bin/trevrpc-interop-android \
      --add-flags "-I $share/run.py" \
      --add-flags "--app-apk $share/app.apk" \
      --add-flags "--test-apk $share/test.apk" \
      --set ANDROID_HOME ${androidSdk}/libexec/android-sdk \
      --set ANDROID_SDK_ROOT ${androidSdk}/libexec/android-sdk \
      --prefix PATH : ${
        lib.makeBinPath [
          androidSdk
          openssl
        ]
      }
    runHook postInstall
  '';

  # The app's only tests are instrumented and need an emulator, and the parent package already runs
  # every host test, so the Gradle check is skipped. doCheck stays on so this is the same derivation
  # as the flake check, and the android workflow and the flake check share one cached build.
  doCheck = true;
  dontUseGradleCheck = true;

  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck
    test -f $out/share/trevrpc-kotlin/interop-android/app.apk
    test -f $out/share/trevrpc-kotlin/interop-android/test.apk
    $out/bin/trevrpc-interop-android --help >/dev/null
    runHook postInstallCheck
  '';

  meta = {
    description = "Runs the TrevRPC interop scenarios from an Android emulator over Cronet HTTP/3";
    license = lib.licenses.mit;
    platforms = [ "x86_64-linux" ];
    sourceProvenance = with lib.sourceTypes; [
      fromSource
      binaryBytecode
      binaryNativeCode
    ];
    mainProgram = "trevrpc-interop-android";
  };
})
