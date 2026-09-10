{
  lib,
  stdenv,
  pkgsBuildBuild,
  file,
  libmsquic,
}:
let
  canExecute = stdenv.buildPlatform.canExecute stdenv.hostPlatform;
in
assert lib.assertMsg stdenv.hostPlatform.isLinux
  "msquic-native check requires a Linux host platform";
assert lib.assertMsg (
  !stdenv.hostPlatform.isStatic
) "msquic-native check requires a non-static host platform";
assert lib.assertMsg (
  (libmsquic.msquicTlsProvider or null) == "openssl"
) "msquic-native check requires the OpenSSL TLS provider";
assert lib.assertMsg (
  (libmsquic.msquicOpenSSLLinkage or null) == "embedded-static"
) "msquic-native check requires statically embedded OpenSSL";
stdenv.mkDerivation {
  pname = "trevrpc-msquic-native-check";
  version = libmsquic.version;
  dontUnpack = true;
  strictDeps = true;
  nativeBuildInputs = [
    pkgsBuildBuild.binutils-unwrapped
    file
  ];

  buildPhase = ''
    runHook preBuild
    set -eu

    library=${libmsquic}/lib/libmsquic.so.2
    test -s "$library"
    file -Lb "$library" | grep -F 'ELF 64-bit LSB shared object'

    dynamic=$(readelf -d "$library")
    if printf '%s\n' "$dynamic" | grep -Eq 'Shared library: \[lib(ssl|crypto)\.so'; then
      echo "native MsQuic unexpectedly depends on a shared OpenSSL library" >&2
      exit 1
    fi

    strings -a "$library" | grep -F 'OpenSSL ${libmsquic.msquicOpenSSL.version}'
    if strings -a "$library" | grep -Fq '${libmsquic.msquicOpenSSL.out}'; then
      echo "native MsQuic contains its build-time OpenSSL store path" >&2
      exit 1
    fi

    symbols=$("$NM" -D --defined-only "$library" 2>/dev/null || true)
    for symbol in MsQuicOpenVersion MsQuicClose; do
      printf '%s\n' "$symbols" | grep -Eq "[[:space:]]$symbol(@@[^[:space:]]+)?$"
    done
    if printf '%s\n' "$symbols" | grep -Eq \
      '[[:space:]](ASYNC|BIO|BN|CRYPTO|EC|ERR|EVP|OPENSSL|OSSL|PEM|PKCS|RAND|RSA|SSL|X509)_[A-Za-z0-9_]+'; then
      echo "native MsQuic unexpectedly exports embedded OpenSSL symbols" >&2
      exit 1
    fi

    cat > msquic-native-consumer.c <<'EOF'
    #include <msquic.h>

    int
    main(void)
    {
        const QUIC_API_TABLE* api = NULL;
        if (MsQuicOpen2(&api) != QUIC_STATUS_SUCCESS) {
            return 1;
        }
        MsQuicClose(api);
        return 0;
    }
    EOF
    $CC -std=c11 -Wall -Wextra -Werror \
      -I${libmsquic}/include msquic-native-consumer.c \
      -L${libmsquic}/lib -Wl,-rpath,${libmsquic}/lib \
      -lmsquic -ldl -pthread \
      -o msquic-native-consumer
    if ${lib.boolToString canExecute}; then
      ./msquic-native-consumer
    fi

    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall
    touch $out
    runHook postInstall
  '';

  passthru = {
    msquicProvider = libmsquic;
    opensslProvider = libmsquic.msquicOpenSSL;
  };

  meta = {
    description = "Focused shared-library and embedded-OpenSSL check for patched MsQuic";
    license = with lib.licenses; [
      mit
      asl20
    ];
    platforms = [
      "x86_64-linux"
      "aarch64-linux"
    ];
  };
}
