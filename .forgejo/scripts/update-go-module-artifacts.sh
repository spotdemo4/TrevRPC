#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 <trevrpc-c|trevrpc-c/provider/msquic>" >&2
  exit 2
fi

module_dir="$1"
case "$module_dir" in
  # Keep the nested provider case before the parent prefix.
  trevrpc-c/provider/msquic)
    module_path="trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2"
    ;;
  trevrpc-c)
    module_path="trev.zip/llc/trevrpc/trevrpc-c"
    ;;
  *)
    echo "unsupported Go module directory: $module_dir" >&2
    exit 2
    ;;
esac

module_file="$module_dir/go.mod"
if [[ -f "$module_file" && "${TREVRPC_SKIP_GO_MOD_TIDY-}" != "1" ]]; then
  # Never let a repository go.work change the module's dependency graph.
  GOWORK=off go -C "$module_dir" mod tidy
fi

# The provider version is intentionally independent from the parent C module.
# A repository-specific bumper can supply this command and choose its version
# source (for example, the pinned MsQuic source revision). No generated archive
# is guessed or rewritten here.
if [[ "$module_dir" == "trevrpc-c/provider/msquic" && -n "${TREVRPC_PROVIDER_VERSION_SOURCE_COMMAND-}" ]]; then
  TREVRPC_MODULE_DIR="$module_dir" \
    TREVRPC_MODULE_PATH="$module_path" \
    bash -c "$TREVRPC_PROVIDER_VERSION_SOURCE_COMMAND"
fi

# Artifact regeneration is an explicit opt-in hook. This keeps Renovate
# portable across targets that cannot build or package the native provider.
if [[ -n "${TREVRPC_ARTIFACT_UPDATE_COMMAND-}" ]]; then
  TREVRPC_MODULE_DIR="$module_dir" \
    TREVRPC_MODULE_PATH="$module_path" \
    bash -c "$TREVRPC_ARTIFACT_UPDATE_COMMAND"
fi
