{
  buildPackages,
  coreutils,
  lib,
  libmsquic,
  openssl,
  stdenv,
}:
let
  isStatic = stdenv.hostPlatform.isStatic;
  usesExternalOpenSSL = stdenv.hostPlatform.isLinux;
  msquicPatch = ../patches/trevrpc-msquic-reset-at.patch;
  opensslStatic = (openssl.override { static = true; }).overrideAttrs (previous: {
    # Nixpkgs' static OpenSSL build puts OPENSSLDIR in a separate output.
    # That output's store path is compiled into libcrypto.a. Use the host's
    # conventional configuration directory instead so the archive is usable
    # outside Nix and across rebuilds/targets.
    configureFlags =
      map (
        flag: if flag == "--openssldir=/.$(etc)/etc/ssl" then "--openssldir=/etc/ssl" else flag
      ) previous.configureFlags
      # The native MsQuic DSO embeds these archives, so its objects must be
      # position-independent. Static target archives keep their existing flags.
      ++ lib.optional (!isStatic) "CFLAGS=-fPIC";
    outputs = [
      "dev"
      "out"
    ];
    postConfigure = (previous.postConfigure or "") + ''
      substituteInPlace Makefile --replace-fail \
        'MODULESDIR=$(libdir)/ossl-modules' \
        'MODULESDIR=/etc/ssl/ossl-modules'
    '';
    installPhase = ''
      runHook preInstall
      make install_sw \
        OPENSSLDIR="$out/etc/ssl" \
        ENGINESDIR="$out/etc/ssl/engines-3" \
        MODULESDIR="$out/etc/ssl/ossl-modules"
      runHook postInstall
    '';
    postInstall = ''
      remove-references-to -t $out $out/lib/*.a
      mkdir -p $dev
      mv $out/include $dev/include
      rm -rf $out/bin $out/etc $out/lib/cmake $out/lib/pkgconfig
    '';
  });
  provenance = builtins.toJSON {
    package = "libmsquic";
    version = libmsquic.version;
    source = {
      url = libmsquic.src.url;
      rev = libmsquic.src.rev;
      hash = libmsquic.src.hash;
    };
    patch = {
      path = "nix/patches/trevrpc-msquic-reset-at.patch";
      storePath = "${msquicPatch}";
      sha256 = builtins.hashFile "sha256" msquicPatch;
      applications = 1;
    };
    cmake = {
      QUIC_BUILD_SHARED = false;
      QUIC_TLS_LIB = "openssl";
      QUIC_USE_EXTERNAL_OPENSSL = true;
      QUIC_ENABLE_LOGGING = false;
    };
    tls = {
      provider = "openssl";
      version = opensslStatic.version;
      static = true;
      portable = true;
      runtimePaths = {
        opensslDir = "/etc/ssl";
        enginesDir = "/etc/ssl/engines-3";
        modulesDir = "/etc/ssl/ossl-modules";
        certificateFile = "/etc/ssl/certs/ca-certificates.crt";
      };
    };
    portability = {
      archivePathPolicy = "reject-nix-store-and-build-paths";
      targetIndependentRuntimePaths = true;
    };
    darwin = {
      status = "evaluation-only";
      blocker = "requires an aarch64-darwin builder with an Apple SDK";
    };
    archive = "lib/libmsquic.a";
  };
in
libmsquic.overrideAttrs (
  previous:
  {
    patches = (previous.patches or [ ]) ++ [ msquicPatch ];
    cmakeFlags =
      (previous.cmakeFlags or [ ])
      ++ lib.optional (
        stdenv.hostPlatform.isLinux && stdenv.hostPlatform.isAarch64
      ) "-DCMAKE_TARGET_ARCHITECTURE=arm64"
      ++ lib.optionals usesExternalOpenSSL [
        "-DQUIC_TLS_LIB=openssl"
        "-DQUIC_USE_EXTERNAL_OPENSSL=ON"
        "-DQUIC_OPENSSL_INCLUDE_DIR=${opensslStatic.dev}/include"
        "-DQUIC_OPENSSL_LIB_DIR=${opensslStatic.out}/lib"
      ]
      ++ lib.optionals isStatic [
        # Keep compiler provenance useful without embedding Nix's ephemeral
        # build directory in the distributable archive.
        "-DCMAKE_C_FLAGS=-ffile-prefix-map=/build/source=/usr/src/msquic"
        "-DCMAKE_CXX_FLAGS=-ffile-prefix-map=/build/source=/usr/src/msquic"
        "-DQUIC_BUILD_SHARED=OFF"
        "-DQUIC_ENABLE_LOGGING=OFF"
        "-DQUIC_BUILD_TOOLS=OFF"
        "-DQUIC_BUILD_TEST=OFF"
        "-DQUIC_BUILD_PERF=OFF"
      ];
    buildInputs =
      lib.filter (input: (input.pname or "") != "lttng-tools") (previous.buildInputs or [ ])
      ++ lib.optionals usesExternalOpenSSL [
        opensslStatic.dev
        opensslStatic.out
      ];
    propagatedBuildInputs = lib.filter (input: (input.pname or "") != "lttng-tools") (
      previous.propagatedBuildInputs or [ ]
    );
    postUnpack = builtins.replaceStrings [ "${coreutils}" ] [ "${buildPackages.coreutils}" ] (
      previous.postUnpack or ""
    );
    dontPatchELF = true;
    passthru =
      (previous.passthru or { })
      // {
        trevrpcResetStreamAtPatch = msquicPatch;
      }
      // lib.optionalAttrs usesExternalOpenSSL {
        msquicOpenSSL = opensslStatic;
        msquicOpenSSLLinkage = if isStatic then "static-archive" else "embedded-static";
        msquicTlsProvider = "openssl";
      }
      // lib.optionalAttrs isStatic {
        msquicStaticArchive = "lib/libmsquic.a";
        msquicStaticOpenSSL = opensslStatic;
        msquicStaticOpenSSLRuntimePaths = {
          opensslDir = "/etc/ssl";
          enginesDir = "/etc/ssl/engines-3";
          modulesDir = "/etc/ssl/ossl-modules";
        };
        msquicStaticDarwinStatus = {
          status = "evaluation-only";
          blocker = "requires an aarch64-darwin builder with an Apple SDK";
        };
        trevrpcResetStreamAtPatchApplications = 1;
      };
    meta =
      (previous.meta or { })
      // lib.optionalAttrs usesExternalOpenSSL {
        license = with lib.licenses; [
          mit
          asl20
        ];
      }
      // lib.optionalAttrs isStatic {
        description = "Patched MsQuic 2.6.0 monolithic static archive with static OpenSSL";
      };
  }
  // lib.optionalAttrs isStatic {
    postInstall = (previous.postInstall or "") + ''
      mkdir -p "$out/share/msquic"
      cat > "$out/share/msquic/provenance.json" <<'EOF'
      ${provenance}
      EOF
    '';
  }
)
