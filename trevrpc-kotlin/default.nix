{
  pkgs,
  repoRoot,
}:
let
  package = pkgs.stdenvNoCC.mkDerivation (
    final: with pkgs.lib; {
      pname = "trevrpc-kotlin";
      version = "0.1.0";

      src = fileset.toSource {
        root = repoRoot;
        fileset = fileset.unions [
          (repoRoot + "/testdata/wire-golden-vectors.txt")
          ./.
          (repoRoot + "/trevrpc-rust/crates/protoc-gen-trevrpc-rust/tests/proto/greeter.proto")
        ];
      };
      sourceRoot = "${final.src.name}/trevrpc-kotlin";

      nativeBuildInputs = with pkgs; [
        gradle_9
        jdk25
        makeWrapper
        protobuf
      ];

      mitmCache = pkgs.gradle_9.fetchDeps {
        pkg = final.finalPackage;
        data = ./gradle/deps.json;
        bwrapFlags = ''--ro-bind "$PWD" "$PWD" --dir /bin --symlink ${pkgs.runtimeShell} /bin/sh'';
      };
      __darwinAllowLocalNetworking = true;

      gradleFlags = [
        "-Dorg.gradle.java.home=${pkgs.jdk25.home}"
      ];
      gradleBuildTask = [
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

      doCheck = false;
      gradleCheckTask = "check";

      installPhase = ''
        runHook preInstall
        mkdir -p $out/bin $out/share/trevrpc-kotlin
        cp -R examples/build/install/trevrpc-xruntime-kotlin/* $out/share/trevrpc-kotlin/
        cp -R protoc-gen-trevrpc-kotlin/build/install/protoc-gen-trevrpc-kotlin \
          $out/share/trevrpc-kotlin/protoc-gen-trevrpc-kotlin
        makeWrapper $out/share/trevrpc-kotlin/bin/trevrpc-xruntime-kotlin \
          $out/bin/trevrpc-xruntime-kotlin \
          --set JAVA_HOME ${pkgs.jdk25.home} \
          --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.jdk25 ]}
        makeWrapper \
          $out/share/trevrpc-kotlin/protoc-gen-trevrpc-kotlin/bin/protoc-gen-trevrpc-kotlin \
          $out/bin/protoc-gen-trevrpc-kotlin \
          --set JAVA_HOME ${pkgs.jdk25.home} \
          --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.jdk25 ]}
        runHook postInstall
      '';

      doInstallCheck = true;
      nativeInstallCheckInputs = [ pkgs.gnugrep ];
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
        mainProgram = "protoc-gen-trevrpc-kotlin";
        description = "Kotlin TrevRPC runtime, transports, and protobuf generator";
        license = licenses.mit;
        platforms = [ "x86_64-linux" ];
        sourceProvenance = with sourceTypes; [
          fromSource
          binaryBytecode
        ];
        homepage = "https://trev.zip/llc/TrevRPC";
      };
    }
  );

  benchPeer = package.overrideAttrs (
    old: with pkgs.lib; {
      pname = "trevrpc-kotlin-bench-peer";
      doInstallCheck = false;
      gradleBuildTask = [ ":bench-peer:installDist" ];

      doCheck = true;
      gradleCheckTask = ":bench-peer:check";
      installPhase = ''
        runHook preInstall
        mkdir -p $out/bin $out/share/trevrpc-kotlin
        cp -R bench-peer/build/install/trevrpc-bench-peer-kotlin \
          $out/share/trevrpc-kotlin/trevrpc-bench-peer-kotlin
        makeWrapper \
          $out/share/trevrpc-kotlin/trevrpc-bench-peer-kotlin/bin/trevrpc-bench-peer-kotlin \
          $out/bin/trevrpc-bench-peer-kotlin \
          --set JAVA_HOME ${pkgs.jdk25.home} \
          --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.jdk25 ]}
        runHook postInstall
      '';

      meta = {
        mainProgram = "trevrpc-bench-peer-kotlin";
        description = "Kotlin TrevRPC and gRPC benchmark peer";
        license = licenses.mit;
        platforms = [ "x86_64-linux" ];
        sourceProvenance = with sourceTypes; [
          fromSource
          binaryBytecode
        ];
      };
    }
  );
in
{
  inherit package benchPeer;
}
