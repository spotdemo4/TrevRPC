#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
MATRIX_ROOT="$ROOT/bench/rust-matrix"
PEER_WAS_SET=${PEER+x}
PEER=${PEER:-$MATRIX_ROOT/target/debug/trevrpc-rust-matrix-peer}
TMP_DIR=$(mktemp -d)
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if [[ -z "$PEER_WAS_SET" ]]; then
    cargo build --quiet --manifest-path "$MATRIX_ROOT/Cargo.toml" --bin trevrpc-rust-matrix-peer
fi
openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$TMP_DIR/ca-key.pem" -out "$TMP_DIR/ca.pem" -days 1 \
    -subj '/CN=TrevRPC Benchmark CA' \
    -addext basicConstraints=critical,CA:TRUE \
    -addext keyUsage=critical,keyCertSign,cRLSign \
    >"$TMP_DIR/openssl.stdout" 2>"$TMP_DIR/openssl.stderr"
openssl req -newkey rsa:2048 -nodes \
    -keyout "$TMP_DIR/key.pem" -out "$TMP_DIR/server.csr" \
    -subj /CN=localhost -addext subjectAltName=DNS:localhost,IP:127.0.0.1 \
    >"$TMP_DIR/csr.stdout" 2>"$TMP_DIR/csr.stderr"
openssl x509 -req -in "$TMP_DIR/server.csr" \
    -CA "$TMP_DIR/ca.pem" -CAkey "$TMP_DIR/ca-key.pem" -CAcreateserial \
    -out "$TMP_DIR/cert.pem" -days 1 -copy_extensions copyall \
    >"$TMP_DIR/sign.stdout" 2>"$TMP_DIR/sign.stderr"

for stack in trevrpc_quinn grpc_tonic_generated; do
    run_id="self-test-$stack"
    control="$TMP_DIR/$stack.sock"
    ready="$TMP_DIR/$stack-ready.json"
    certificate_sha256=$(sha256sum "$TMP_DIR/cert.pem" | awk '{print $1}')
    config_hash=$("$PEER" hash-config \
        --stack "$stack" \
        --concurrency 1 \
        --warmup-ms 0 \
        --measurement-ms 1 \
        --certificate-sha256 "$certificate_sha256")
    "$PEER" server \
        --stack "$stack" \
        --listen 127.0.0.1:0 \
        --control "$control" \
        --ready-file "$ready" \
        --cert "$TMP_DIR/cert.pem" \
        --key "$TMP_DIR/key.pem" \
        --run-id "$run_id" \
        --config-hash "$config_hash" \
        --concurrency 1 \
        --warmup-ms 0 \
        --measurement-ms 1 \
        --certificate-sha256 "$certificate_sha256" \
        >"$TMP_DIR/$stack-server.stdout" 2> >(tee "$TMP_DIR/$stack-server.stderr" >&2) &
    SERVER_PID=$!
    for _ in $(seq 1 200); do
        [[ -s "$ready" ]] && break
        kill -0 "$SERVER_PID"
        sleep 0.01
    done
    [[ -s "$ready" ]]
    address=$(jq -r '.address' "$ready")
    "$PEER" check-client \
        --stack "$stack" \
        --address "$address" \
        --control "$control" \
        --cert "$TMP_DIR/ca.pem" \
        --identity-cert "$TMP_DIR/cert.pem" \
        --run-id "$run_id" \
        --config-hash "$config_hash" \
        --concurrency 1 \
        --warmup-ms 0 \
        --measurement-ms 1 \
        --repetition 1 \
        --certificate-sha256 "$certificate_sha256" \
        >"$TMP_DIR/$stack-client.stdout" 2> >(tee "$TMP_DIR/$stack-client.stderr" >&2)
    wait "$SERVER_PID"
    SERVER_PID=""
    jq -e --arg stack "$stack" \
        '.schema_version == 1 and .event == "self_test" and .stack == $stack' \
        "$TMP_DIR/$stack-client.stdout" >/dev/null
done

printf 'Rust matrix peer self-tests passed\n'
