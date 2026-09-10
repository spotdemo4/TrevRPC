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
assert lib.assertMsg stdenv.hostPlatform.isStatic
  "msquic-static check requires a static host platform";
stdenv.mkDerivation {
  pname = "trevrpc-msquic-static-check";
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

    archive=${libmsquic}/lib/libmsquic.a
    test -s "$archive"
    test "$(file -b "$archive")" = "current ar archive"

    mkdir members
    (
      cd members
      "$AR" x "$archive"
    )
    test -n "$(find members -type f -name '*.o' -print -quit)"

    case ${stdenv.hostPlatform.system} in
      x86_64-linux)
        expected_object_format='ELF 64-bit LSB relocatable, x86-64'
        ;;
      aarch64-linux)
        expected_object_format='ELF 64-bit LSB relocatable, ARM aarch64'
        ;;
      *)
        echo "unsupported static archive check target: ${stdenv.hostPlatform.system}" >&2
        exit 1
        ;;
    esac
    while IFS= read -r object; do
      file -b "$object" | grep -F "$expected_object_format"
    done < <(find members -type f -name '*.o' -print)

    reject_path_leaks() {
      file=$1
      for forbidden in /nix/store /build; do
        if strings -a "$file" | grep -Fq "$forbidden"; then
          echo "forbidden path '$forbidden' found in $file" >&2
          exit 1
        fi
      done
    }
    reject_path_leaks "$archive"
    while IFS= read -r object; do
      reject_path_leaks "$object"
    done < <(find members -type f -name '*.o' -print)

    symbols=$("$NM" -g --defined-only "$archive" 2>/dev/null || true)
    for symbol in MsQuicOpenVersion MsQuicClose MsQuicSetParam MsQuicStreamSend; do
      printf '%s\n' "$symbols" | grep -Eq "[[:space:]]$symbol$"
    done

    test ! -e ${libmsquic}/lib/libmsquic.so
    test ! -e ${libmsquic}/lib/libmsquic.so.2
    test -s ${libmsquic}/share/msquic/provenance.json
    grep -F '"package":"libmsquic"' ${libmsquic}/share/msquic/provenance.json
    grep -F '"applications":1' ${libmsquic}/share/msquic/provenance.json
    grep -F '"archive":"lib/libmsquic.a"' ${libmsquic}/share/msquic/provenance.json
    grep -F '"portable":true' ${libmsquic}/share/msquic/provenance.json
    grep -F '"targetIndependentRuntimePaths":true' ${libmsquic}/share/msquic/provenance.json
    grep -F '"archivePathPolicy":"reject-nix-store-and-build-paths"' \
      ${libmsquic}/share/msquic/provenance.json
    grep -F '"opensslDir":"/etc/ssl"' ${libmsquic}/share/msquic/provenance.json
    grep -F '"certificateFile":"/etc/ssl/certs/ca-certificates.crt"' \
      ${libmsquic}/share/msquic/provenance.json
    grep -F '"status":"evaluation-only"' ${libmsquic}/share/msquic/provenance.json
    grep -F 'aarch64-darwin builder with an Apple SDK' ${libmsquic}/share/msquic/provenance.json

    cat > msquic-static-consumer.c <<'EOF'
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
    $CC -static -std=c11 -I${libmsquic}/include msquic-static-consumer.c \
      ${libmsquic}/lib/libmsquic.a -ldl -pthread -lm \
      -o msquic-static-consumer
    reject_path_leaks msquic-static-consumer
    file -b msquic-static-consumer | grep -F 'statically linked'
    if readelf -l msquic-static-consumer | grep -Eq '(^|[[:space:]])INTERP([[:space:]]|$)'; then
      echo "static consumer unexpectedly has an interpreter" >&2
      exit 1
    fi
    if readelf -d msquic-static-consumer | grep -Fq '(NEEDED)'; then
      echo "static consumer unexpectedly has dynamic dependencies" >&2
      exit 1
    fi
    if ${lib.boolToString canExecute}; then
      ./msquic-static-consumer
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
  };

  meta = {
    description = "Focused archive, symbol, architecture, and static-link check for patched MsQuic";
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
