{
  lib,
  stdenv,
  pkgsBuildBuild,
  file,
  libmsquicStatic,
}:
let
  canExecute = stdenv.buildPlatform.canExecute stdenv.hostPlatform;
in
stdenv.mkDerivation {
  pname = "trevrpc-msquic-static-check";
  version = libmsquicStatic.version;
  dontUnpack = true;
  strictDeps = true;
  nativeBuildInputs = [
    pkgsBuildBuild.binutils-unwrapped
    file
  ];

  buildPhase = ''
    runHook preBuild
    set -eu

    archive=${libmsquicStatic}/lib/libmsquic.a
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
        dynamic_linker=/lib64/ld-linux-x86-64.so.2
        runtime_loader=${stdenv.cc.libc}/lib/ld-linux-x86-64.so.2
        ;;
      aarch64-linux)
        expected_object_format='ELF 64-bit LSB relocatable, ARM aarch64'
        dynamic_linker=/lib/ld-linux-aarch64.so.1
        runtime_loader=${stdenv.cc.libc}/lib/ld-linux-aarch64.so.1
        ;;
      *)
        echo "unsupported static archive check target: ${stdenv.hostPlatform.system}" >&2
        exit 1
        ;;
    esac
    runtime_library_path=${stdenv.cc.libc}/lib:${stdenv.cc.cc.lib}/lib
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

    test ! -e ${libmsquicStatic}/lib/libmsquic.so
    test ! -e ${libmsquicStatic}/lib/libmsquic.so.2
    test -s ${libmsquicStatic}/share/msquic/provenance.json
    grep -F '"applications":1' ${libmsquicStatic}/share/msquic/provenance.json
    grep -F '"archive":"lib/libmsquic.a"' ${libmsquicStatic}/share/msquic/provenance.json
    grep -F '"portable":true' ${libmsquicStatic}/share/msquic/provenance.json
    grep -F '"targetIndependentRuntimePaths":true' ${libmsquicStatic}/share/msquic/provenance.json
    grep -F '"archivePathPolicy":"reject-nix-store-and-build-paths"' \
      ${libmsquicStatic}/share/msquic/provenance.json
    grep -F '"opensslDir":"/etc/ssl"' ${libmsquicStatic}/share/msquic/provenance.json
    grep -F '"certificateFile":"/etc/ssl/certs/ca-certificates.crt"' \
      ${libmsquicStatic}/share/msquic/provenance.json
    grep -F '"status":"evaluation-only"' ${libmsquicStatic}/share/msquic/provenance.json
    grep -F 'aarch64-darwin builder with an Apple SDK' ${libmsquicStatic}/share/msquic/provenance.json

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
    $CC -std=c11 -I${libmsquicStatic}/include msquic-static-consumer.c \
      ${libmsquicStatic}/lib/libmsquic.a -ldl -pthread -lm \
      -Wl,--no-as-needed -Wl,--dynamic-linker="$dynamic_linker" \
      -o msquic-static-consumer
    for forbidden in /nix/store /build; do
      if strings -a msquic-static-consumer | grep -Fq "$forbidden"; then
        echo "forbidden path '$forbidden' found in final consumer" >&2
        exit 1
      fi
    done
    dynamic_dependencies=$(readelf -d msquic-static-consumer)
    printf '%s\n' "$dynamic_dependencies" | grep -Eq 'lib(msquic|ssl|crypto)' && exit 1 || true
    if ${lib.boolToString canExecute}; then
      ldd_dependencies=$("$runtime_loader" --library-path "$runtime_library_path" --list "$PWD/msquic-static-consumer")
      printf '%s\n' "$ldd_dependencies" | grep -Eq 'lib(msquic|ssl|crypto)' && exit 1 || true
      "$runtime_loader" --library-path "$runtime_library_path" "$PWD/msquic-static-consumer"
    fi

    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall
    touch $out
    runHook postInstall
  '';

  passthru = {
    msquicProvider = libmsquicStatic;
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
