# Migrating TrevRPC C ABI 5 to ABI 6

TrevRPC C 0.2.0 is an atomic ABI break. It exports C ABI 6 and does not ship ABI-5 declarations,
aliases, archives, targets, or runtime shims. Rebuild every C, C++, Node native, and generated C
consumer together.

The wire protocol is unchanged:

- `TREVRPC_ALPN` remains `"trevrpc/1"`.
- `TREVRPC_WIRE_VERSION` remains `1u`.
- `TREVRPC_C_ABI_VERSION` and `trevrpc_c_abi_version()` are `6u`.

## Configuration

Replace unversioned configuration values and default-returning functions with initialized versioned
structures:

| ABI 5                             | ABI 6                                                  |
| --------------------------------- | ------------------------------------------------------ |
| `trevrpc_config`                  | `trevrpc_client_config_v1`                             |
| `trevrpc_default_config()`        | `trevrpc_client_config_v1_init(&value, sizeof(value))` |
| `trevrpc_server_config`           | `trevrpc_server_config_v1`                             |
| `trevrpc_default_server_config()` | `trevrpc_server_config_v1_init(&value, sizeof(value))` |
| `trevrpc_server_options`          | `trevrpc_server_options_v1`                            |
| `trevrpc_call_options`            | `trevrpc_call_options_v1`                              |

Pass the exact structure size visible to the caller. An unsupported `struct_version` returns
`-ENOTSUP`; undersized layouts return `-EINVAL`.

Connect and operation entry points now carry `_v1` and accept the versioned structures, for example
`trevrpc_channel_connect_v1`, `trevrpc_raw_client_connect_v1`,
`trevrpc_channel_call_request_inbound_v1`, and `trevrpc_raw_client_start_stream_request_v1`.

## Inbound responses and frames

ABI-5 public `trevrpc_response` and `trevrpc_stream_frame` layouts are removed. ABI 6 returns opaque
owned shells:

- `trevrpc_inbound_response`
- `trevrpc_inbound_stream_frame`
- `trevrpc_body_owner`

Use getters rather than direct field access and release shells with
`trevrpc_inbound_response_release` or `trevrpc_inbound_stream_frame_release`.

To preserve a body after shell release, call `trevrpc_inbound_response_take_body` or
`trevrpc_inbound_stream_frame_take_body`. A successful take is one-shot. The returned body owner
retains the original allocator-specific owner and release context, including an interior visible
slice or a zero-length value with a release callback.

The following ABI-5 functions have no compatibility equivalent and must be removed from consumer
code:

- `trevrpc_response_set_*`, `trevrpc_response_reset`, `trevrpc_response_free`
- `trevrpc_stream_frame_set_*`, `trevrpc_stream_frame_reset`, `trevrpc_stream_frame_free`
- `trevrpc_stream_recv`, `trevrpc_stream_recv_ready*`, `trevrpc_stream_recv_batch`

Use `trevrpc_stream_recv_inbound`, `trevrpc_stream_recv_inbound_ready*`, or
`trevrpc_stream_recv_inbound_batch`.

## Borrowed server completion

Construct `trevrpc_response_view_v1` or `trevrpc_status_view_v1` with its initializer, populate
borrowed fields, and call:

- `trevrpc_call_respond_borrowed_v1`
- `trevrpc_call_finish_stream_borrowed_v1`
- `trevrpc_stream_send_status_borrowed_v1`

Referenced storage only needs to remain valid until the function returns.

## Generated bindings

Regenerate every `.trevrpc.c` and `.trevrpc.h` file with the 0.2.0 generator. ABI-6 generated
headers fail compilation against any other C ABI.

Generated unary clients now write a typed result object instead of returning a protobuf pointer
directly. Inspect the result discriminator and reset the object on every exit path. RPC status,
runtime failure, and protobuf decode failure are separate result categories.

Generated stream receives now use a receiver plus owned event. Reset each event before receiving the
next one. Response receivers do not publish terminal status until a clean FIN has been observed and
report missing or trailing terminal frames explicitly.

Generated unary and client-streaming server handlers now receive a synchronous responder callback.
Pass a borrowed protobuf response view to that callback while handler storage is alive. Do not
allocate a protobuf response for generated glue to free. A second responder invocation returns
`-EALREADY`.

Generated service registration validates all handler pointers before registering any route and uses
`trevrpc_server_register_call` exclusively.

## Server lifecycle

Replace `trevrpc_server_shutdown` and `trevrpc_server_close` with the explicit lifecycle:

1. `trevrpc_server_listen_v1`
2. configure options and register handlers
3. `trevrpc_server_freeze`
4. `trevrpc_server_serve`
5. `trevrpc_server_stop` for graceful shutdown, or `trevrpc_server_cancel` to abort
6. `trevrpc_server_wait_until`
7. `trevrpc_server_release`

No configuration or route registration is permitted after freeze.

## Cancellation

Replace `trevrpc_cancellation_free` with `trevrpc_cancellation_release`. Cancellation objects are
reference counted; use `trevrpc_cancellation_retain` when retaining one beyond the current call.
Versioned blocking connect APIs retain a supplied cancellation for the duration of connect.

## Build integration

Require C package version 0.2.0 and ABI 6:

```cmake
find_package(trevrpc 0.2 CONFIG REQUIRED)
if(NOT TREVRPC_C_ABI_VERSION EQUAL 6)
    message(FATAL_ERROR "TrevRPC C ABI 6 is required")
endif()
```

The pkg-config file exports `trevrpc_c_abi_version=6`. The runtime also exports
`trevrpc_c_abi_6_anchor`, allowing compiled bridges to require an ABI-6 link target.

Delete any include of `trevrpc_preview.h`; the preview header and preview version macro no longer
exist.
