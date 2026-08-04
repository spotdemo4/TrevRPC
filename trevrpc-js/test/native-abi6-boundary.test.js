import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import test from "node:test";

const nativeSourcePath = join(import.meta.dirname, "..", "native", "trevrpc_node.c");

const forbidden = [
  ["legacy response type", /\btrevrpc_response\b/u],
  ["legacy stream frame type", /\btrevrpc_stream_frame\b/u],
  ["legacy body-owner field", /\b_body_owner\b/u],
  ["legacy client config", /\btrevrpc_config\b/u],
  ["legacy server config", /\btrevrpc_server_config\b/u],
  ["legacy server options", /\btrevrpc_server_options\b/u],
  ["legacy default config", /trevrpc_default_config/u],
  ["legacy default server config", /trevrpc_default_server_config/u],
  ["legacy default server options", /trevrpc_default_server_options/u],
  ["legacy cancellable unary API", /trevrpc_raw_client_call_request_cancellable/u],
  ["legacy cancellable stream-start API", /trevrpc_raw_client_start_stream_request_cancellable/u],
  ["legacy client connect API", /trevrpc_raw_client_connect_cancellable_with_shutdown_callback/u],
  ["legacy ready receive API", /trevrpc_stream_recv_ready/u],
  ["legacy stream timeout introspection", /trevrpc_stream_wait_timeout_elapsed/u],
  ["legacy stream-frame free", /trevrpc_stream_frame_free/u],
  ["legacy response free", /trevrpc_response_free/u],
  ["legacy server listen", /trevrpc_server_listen\(/u],
  ["legacy server options setter", /trevrpc_server_set_options\(/u],
  ["legacy server shutdown", /trevrpc_server_shutdown/u],
  ["legacy server close", /trevrpc_server_close/u],
  ["legacy server respond", /trevrpc_call_respond\(/u],
  ["legacy server stream finish", /trevrpc_call_finish_stream_with_metadata/u],
];

const required = [
  "trevrpc_client_config_v1_init",
  "trevrpc_raw_client_connect_v1_with_shutdown_callback",
  "trevrpc_server_config_v1_init",
  "trevrpc_server_options_v1_init",
  "trevrpc_server_listen_v1",
  "trevrpc_server_set_options_v1",
  "trevrpc_server_freeze",
  "trevrpc_server_cancel",
  "trevrpc_server_wait_until",
  "trevrpc_server_release",
  "trevrpc_raw_client_call_request_inbound_v1",
  "trevrpc_raw_client_start_stream_request_v1",
  "trevrpc_stream_recv_inbound_ready_since",
  "trevrpc_stream_recv_inbound_ready",
  "trevrpc_inbound_response_get_status",
  "trevrpc_inbound_response_get_message",
  "trevrpc_inbound_response_get_body",
  "trevrpc_inbound_response_metadata_count",
  "trevrpc_inbound_response_metadata_at",
  "trevrpc_inbound_response_take_body",
  "trevrpc_inbound_response_release",
  "trevrpc_inbound_stream_frame_get_kind",
  "trevrpc_inbound_stream_frame_get_status",
  "trevrpc_inbound_stream_frame_get_message",
  "trevrpc_inbound_stream_frame_get_body",
  "trevrpc_inbound_stream_frame_metadata_count",
  "trevrpc_inbound_stream_frame_metadata_at",
  "trevrpc_inbound_stream_frame_take_body",
  "trevrpc_inbound_stream_frame_release",
  "trevrpc_body_owner_get_view",
  "trevrpc_body_owner_release",
  "trevrpc_call_respond_borrowed_v1",
  "trevrpc_call_finish_stream_borrowed_v1",
  "trevrpc_cancellation_retain",
  "trevrpc_cancellation_release",
];

const productionExports = ["createCancellation", "connectMsQuic", "listenMsQuic"];

test("native addon uses the ABI-6 public boundary", async () => {
  const source = await readFile(nativeSourcePath, "utf8");

  assert.match(source, /#include "trevrpc\.h"/u);
  for (const [name, pattern] of forbidden) {
    assert.doesNotMatch(source, pattern, `${name} remains in ${nativeSourcePath}`);
  }
  for (const symbol of required) {
    assert.match(source, new RegExp(`\\b${symbol}\\b`, "u"), `${symbol} is missing`);
  }

  const productionBlock = source.match(
    /napi_property_descriptor exports_desc\[\] = \{(?<body>[\s\S]*?)#ifdef TREVRPC_NODE_TEST_HOOKS/u,
  );
  assert.ok(productionBlock?.groups?.body, "production export descriptor block is missing");
  const actualExports = [...productionBlock.groups.body.matchAll(/\{"([^"]+)"/gu)].map(
    ([, name]) => name,
  );
  assert.deepEqual(actualExports, productionExports);
});
