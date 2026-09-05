#!/bin/sh
set -eu

if [ "$#" -ne 3 ]; then
    echo "usage: $0 <prefix> <rpc|rpc-msquic> <source-root>" >&2
    exit 2
fi

prefix=$1
mode=$2
source_root=$3
libdir=$prefix/lib
expected=$(mktemp)
actual=$(mktemp)
definitions=$(mktemp)
undefined=$(mktemp)
trap 'rm -f "$expected" "$actual" "$definitions" "$undefined"' EXIT

case "$mode" in
rpc)
    cat >"$expected" <<'EOF'
libtrevrpc_rpc.a
trevrpc/rpc-abi1/libtrevrpc_rpc_private_core.a
EOF
    check_msquic=false
    ;;
rpc-msquic)
    cat >"$expected" <<'EOF'
libtrevrpc_engine.a
libtrevrpc_engine_msquic.a
libtrevrpc_engine_msquic_support.a
libtrevrpc_msquic_api_owner.a
libtrevrpc_rpc.a
libtrevrpc_rpc_msquic.a
trevrpc/rpc-abi1/libtrevrpc_rpc_private_core.a
trevrpc/rpc-abi1/libtrevrpc_rpc_private_msquic_native.a
trevrpc/rpc-abi1/libtrevrpc_rpc_private_protocol.a
EOF
    check_msquic=true
    ;;
*)
    echo "unknown installed-closure mode: $mode" >&2
    exit 2
    ;;
esac

find "$libdir" -type f -name '*.a' -printf '%P\n' | sort >"$actual"
if ! diff -u "$expected" "$actual"; then
    echo "installed RPC archive inventory differs from the allowlist" >&2
    exit 1
fi

if grep -R -E 'add_library\(trevrpc::(trevrpc_core|trevrpc_protocol_core|trevrpc_msquic_native_core|trevrpc_rpc_private_[A-Za-z0-9_]+)' \
    "$libdir/cmake"; then
    echo "private RPC implementation target is publicly imported" >&2
    exit 1
fi

cmake \
    -DTREVRPC_NM="$(command -v nm)" \
    -DTREVRPC_ARCHIVES="$libdir/libtrevrpc_rpc.a" \
    -DTREVRPC_PUBLIC_HEADERS="$prefix/include/trevrpc_rpc.h" \
    -P "$source_root/tests/rpc-abi1/check_symbols.cmake"
if [ "$check_msquic" = true ]; then
    cmake \
        -DTREVRPC_NM="$(command -v nm)" \
        -DTREVRPC_ARCHIVES="$libdir/libtrevrpc_rpc_msquic.a" \
        -DTREVRPC_PUBLIC_HEADERS="$prefix/include/trevrpc_rpc_msquic.h" \
        -P "$source_root/tests/rpc-abi1/check_msquic_symbols.cmake"
fi

find "$libdir" -type f -name '*.a' -print0 |
    xargs -0 nm -g --defined-only -P |
    awk '$1 ~ /^trevrpc_[A-Za-z0-9_]+$/ { print $1 }' |
    sort -u >"$definitions"
find "$libdir" -type f -name '*.a' -print0 |
    xargs -0 nm -g -u -P |
    awk '$1 ~ /^trevrpc_[A-Za-z0-9_]+$/ { print $1 }' |
    sort -u >"$undefined"

while IFS= read -r symbol; do
    if ! grep -Fxq "$symbol" "$definitions"; then
        echo "installed RPC archive closure does not define $symbol" >&2
        exit 1
    fi
done <"$undefined"
