{
  androidenv,
  callPackage,
  gnugrep,
  gradle-packages,
  jdk25,
  lib,
  licenseFile,
  makeWrapper,
  protobuf,
  python3,
  openssl,
  runtimeShell,
  stdenvNoCC,
  interopProto,
  wireGolden,
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

  # The emulator needs KVM and an x86_64 host.
  interopAndroid =
    if stdenvNoCC.hostPlatform.system == "x86_64-linux" then
      callPackage ./interop-android {
        inherit
          interopProto
          licenseFile
          wireGolden
          ;
        gradle_9 = gradle9;
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
      }
    else
      null;
in
stdenvNoCC.mkDerivation (final: {
  pname = "trevrpc-kotlin";
  version = "0.3.4";
  passthru.interopAndroid = interopAndroid;

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      licenseFile
      wireGolden
      interopProto
      ./.
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-kotlin";

  nativeBuildInputs = [
    gradle9
    jdk25
    makeWrapper
    openssl
    protobuf
    python3
  ];

  mitmCache = gradle9.fetchDeps {
    pkg = final.finalPackage;
    data = ./gradle/deps.json;
    bwrapFlags = ''--ro-bind "$PWD" "$PWD" --dir /bin --symlink ${runtimeShell} /bin/sh'';
  };
  __darwinAllowLocalNetworking = true;

  gradleFlags = [ "-Dorg.gradle.java.home=${jdk25.home}" ];
  gradleBuildTask = [
    "stageMavenRepository"
    ":protoc-gen-trevrpc-kotlin:installDist"
  ];
  gradleUpdateScript = ''
    runHook preBuild
    gradle --no-configuration-cache --write-locks \
      updateGradleDependencyState \
      stageMavenRepository \
      :core:test \
      :transport-cronet:test \
      :protoc-gen-trevrpc-kotlin:test
  '';

  doCheck = true;
  gradleCheckTask = "check verifyStagedMavenRepository";
  # The Android interop runner is plain Python; its host-side logic is tested here so it is
  # covered on every platform, not only where the emulator can run.
  postCheck = ''
    python3 -I -m unittest discover -s interop-android
  '';

  installPhase = ''
    runHook preInstall
    mkdir -p $out/bin $out/share/java $out/share/maven $out/share/trevrpc-kotlin
    cp -R build/staging-repository/. $out/share/maven/
    for module in core transport-cronet; do
      cp build/staging-repository/zip/trev/trevrpc/$module/${final.version}/$module-${final.version}.jar \
        $out/share/java/
    done
    cp -R protoc-gen-trevrpc-kotlin/build/install/protoc-gen-trevrpc-kotlin \
      $out/share/trevrpc-kotlin/protoc-gen-trevrpc-kotlin
    makeWrapper \
      $out/share/trevrpc-kotlin/protoc-gen-trevrpc-kotlin/bin/protoc-gen-trevrpc-kotlin \
      $out/bin/protoc-gen-trevrpc-kotlin \
      --set JAVA_HOME ${jdk25.home} \
      --prefix PATH : ${lib.makeBinPath [ jdk25 ]}
    runHook postInstall
  '';

  doInstallCheck = true;
  nativeInstallCheckInputs = [ gnugrep ];
  installCheckPhase = ''
    runHook preInstallCheck
    if find "$out" \( -iname '*grpc*' -o -iname '*tonic*' -o -iname '*netty*' \) -print -quit \
      | grep -q .; then
      echo "Kotlin package contains a gRPC, Tonic, or Netty artifact" >&2
      exit 1
    fi
    test -f "$out/share/java/core-${final.version}.jar"
    test -f "$out/share/java/transport-cronet-${final.version}.jar"
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
    description = "Kotlin TrevRPC client runtime, Cronet HTTP/3 transport, and protobuf generator";
    homepage = "https://trev.zip/llc/TrevRPC";
  };
})
