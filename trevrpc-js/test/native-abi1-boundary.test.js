import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import test from "node:test";

const nativeSourcePath = join(import.meta.dirname, "..", "native", "trevrpc_node.c");
const nativeCMakePath = join(import.meta.dirname, "..", "native", "CMakeLists.txt");

const forbiddenSource = [
  ["removed aggregate public header", /#include\s+[<"]trevrpc\.h[>"]/u],
  ["binding header", /trevrpc_binding/u],
  ["legacy raw API", /trevrpc_raw_/u],
  ["legacy worker implementation", /pthread_|pthread\s*\(/u],
  ["thread-safe function", /napi_(?:create|call|release|acquire|ref|unref)_threadsafe_function/u],
  ["N-API async work", /napi_(?:create|queue|delete|cancel)_async_work/u],
];

const forbiddenCmake = [
  ["removed aggregate runtime option", /TREVRPC_BUILD_RUNTIME/u],
  ["removed aggregate provider option", /TREVRPC_BUILD_MSQUIC/u],
  ["removed WebTransport facade option", /TREVRPC_BUILD_WEBTRANSPORT/u],
  ["removed internal-header install option", /TREVRPC_INSTALL_INTERNAL_HEADERS/u],
  ["removed aggregate CMake install option", /TREVRPC_INSTALL_CMAKEDIR/u],
  ["removed aggregate ABI version", /TREVRPC_C_ABI_VERSION/u],
  ["removed aggregate package lookup", /find_package\s*\(\s*trevrpc(?:\s|\))/u],
  ["removed aggregate imported target", /trevrpc::trevrpc(?:\s|[)"'])/u],
];

const required = [
  "trevrpc_rpc.h",
  "trevrpc_rpc_msquic.h",
  "TREVRPC_RPC_ABI_VERSION",
  "TREVRPC_RPC_MSQUIC_ABI_VERSION",
  "trevrpc_rpc_abi_1_anchor",
  "trevrpc_rpc_msquic_abi_1_anchor",
  "trevrpc_rpc_msquic_create_v1",
  "trevrpc_rpc_runtime_get_wake_source_v1",
  "trevrpc_rpc_runtime_next_event",
  "trevrpc_rpc_event_get_info_v1",
  "trevrpc_rpc_event_release",
  "trevrpc_rpc_runtime_close",
  "trevrpc_rpc_runtime_drain",
  "trevrpc_rpc_runtime_release",
  "uv_poll_init",
  "uv_poll_start",
  "uv_poll_stop",
  "uv_timer_init",
  "uv_timer_start",
  "uv_timer_stop",
  "uv_close",
  "napi_get_instance_data",
  "napi_set_instance_data",
  "napi_add_async_cleanup_hook",
  "napi_remove_async_cleanup_hook",
  "TREVRPC_RPC_WAKE_FLAG_BORROWED",
  "TREVRPC_RPC_WAKE_FLAG_LEVEL_TRIGGERED",
  "EAGAIN",
];

const productionExports = ["connectMsQuic", "createCancellation", "listenMsQuic"];

test("native addon uses only the RPC ABI1 public boundary", async () => {
  const source = await readFile(nativeSourcePath, "utf8");
  const cmake = await readFile(nativeCMakePath, "utf8");

  for (const [name, pattern] of forbiddenSource) {
    assert.doesNotMatch(source, pattern, `${name} remains in ${nativeSourcePath}`);
  }
  for (const [name, pattern] of forbiddenCmake) {
    assert.doesNotMatch(cmake, pattern, `${name} remains in ${nativeCMakePath}`);
  }
  for (const symbol of required) {
    assert.match(
      source,
      new RegExp(`\\b${symbol.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&")}\\b`, "u"),
      `${symbol} is missing`,
    );
  }
  assert.match(cmake, /TREVRPC_BUILD_RPC ON/u);
  assert.match(cmake, /TREVRPC_BUILD_RPC_MSQUIC ON/u);
  assert.match(cmake, /trevrpc_rpc_msquic/u);
  assert.match(cmake, /WIN32/u);
  assert.match(cmake, /POSIX FD wake sources/u);

  const descriptorBlock = source.match(
    /napi_property_descriptor descriptors\[\] = \{(?<body>[\s\S]*?)\n\s*\};/u,
  );
  assert.ok(descriptorBlock?.groups?.body, "production export descriptor block is missing");
  const actualExports = [...descriptorBlock.groups.body.matchAll(/\{"([^"]+)"/gu)].map(
    ([, name]) => name,
  );
  assert.deepEqual(actualExports, productionExports);

  assert.match(source, /for\s*\(\s*;;\s*\)/u, "event drain loop is missing");
  assert.match(source, /result\s*==\s*-EAGAIN/u, "drain-to-EAGAIN handling is missing");
  assert.match(source, /runtime->stopped_seen/u, "STOPPED lifecycle state is missing");
  assert.equal((source.match(/uv_timer_t\s+failure_progress\b/gu) ?? []).length, 1);
  assert.match(source, /uv_unref\(\(uv_handle_t\*\)&runtime->failure_progress\)/u);
});
