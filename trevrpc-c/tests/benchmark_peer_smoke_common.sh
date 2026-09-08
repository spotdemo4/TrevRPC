#!/bin/sh

trevrpc_smoke_cleanup_process() {
    trevrpc_process_label=$1
    trevrpc_process_pid=$2
    trevrpc_known_status=$3
    trevrpc_diagnostics=$4

    if [ -z "$trevrpc_process_pid" ]; then
        if [ "$trevrpc_diagnostics" -ne 0 ] && [ -n "$trevrpc_known_status" ]; then
            printf '%s process exited with status %s\n' "$trevrpc_process_label" "$trevrpc_known_status" >&2
        fi
        return
    fi

    if [ -n "$trevrpc_known_status" ]; then
        trevrpc_process_state=exited
        trevrpc_wait_status=$trevrpc_known_status
    else
        if kill -0 "$trevrpc_process_pid" 2>/dev/null; then
            trevrpc_process_state=active
            kill "$trevrpc_process_pid" 2>/dev/null || true
        else
            trevrpc_process_state=exited
        fi
        set +e
        wait "$trevrpc_process_pid" 2>/dev/null
        trevrpc_wait_status=$?
        set -e
    fi

    if [ "$trevrpc_diagnostics" -ne 0 ]; then
        printf '%s process was %s; wait status %s\n' \
            "$trevrpc_process_label" "$trevrpc_process_state" "$trevrpc_wait_status" >&2
    fi
}

trevrpc_smoke_dump_logs() {
    trevrpc_log_directory=$1
    for trevrpc_log_suffix in out err; do
        for trevrpc_log_file in "$trevrpc_log_directory"/*."$trevrpc_log_suffix"; do
            [ -e "$trevrpc_log_file" ] || continue
            printf '%s\n' "--- ${trevrpc_log_file##*/} ---" >&2
            if [ -s "$trevrpc_log_file" ]; then
                cat "$trevrpc_log_file" >&2
            else
                printf '%s\n' '(empty)' >&2
            fi
        done
    done
}
