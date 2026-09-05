# MsQuic Engine factory ABI 1

This document is the normative contract for the `trevrpc_engine_msquic` provider companion. It is a factory and provider-configuration ABI only. The sole operational transport ABI is [Engine ABI 1](engine-abi-v1.md).

## 1. Public surface

The public header is `trevrpc_engine_msquic.h`. It includes `trevrpc_engine.h` and exports only:

- `TREVRPC_ENGINE_MSQUIC_ABI_VERSION` and `TREVRPC_ENGINE_MSQUIC_STRUCT_VERSION_1`;
- `trevrpc_engine_msquic_config_v1`;
- `trevrpc_engine_msquic_abi_version()`;
- `trevrpc_engine_msquic_abi_1_anchor()`;
- `trevrpc_engine_msquic_config_v1_init()`;
- `trevrpc_engine_msquic_create_v1()`.

There are no public MsQuic-specific adapter, handle, event, receive, wake, diagnostic, command, queue, close, drain, or release types or functions. Earlier provider-prefixed operational names are not aliases. After construction, applications use the provider-neutral `trevrpc_engine_*` API exclusively.

## 2. Version handshake and configuration

`TREVRPC_ENGINE_MSQUIC_ABI_VERSION` is `1`. Compile-time consumers test that macro, runtime consumers call `trevrpc_engine_msquic_abi_version()`, and link-time consumers reference `trevrpc_engine_msquic_abi_1_anchor()`.

`trevrpc_engine_msquic_config_v1` begins with `struct_size` and `struct_version`, followed by `flags`, `reserved0`, and six reserved 64-bit fields. On the supported 64-bit ABI it has size 64 and alignment 8. Every field offset is part of companion ABI 1.

`trevrpc_engine_msquic_config_v1_init()`:

- returns `-EINVAL` for null or undersized storage;
- returns `-EOVERFLOW` when the supplied size exceeds `UINT32_MAX`;
- zeroes the complete caller-supplied extent;
- records the supplied size and structure version 1.

Companion ABI 1 defines no provider-specific configuration options. `flags`, `reserved0`, and every reserved field must remain zero. Larger caller structures are accepted so a later structure version can add configuration without inventing a second operational ABI.

Transport endpoint, queue, object-capacity, receive-budget, frame, and pending-send configuration belongs to `trevrpc_engine_config_v1` and `trevrpc_engine_endpoint_config_v1` in `trevrpc_engine.h`, not to the MsQuic companion structure.

## 3. Factory ownership and failure guarantees

```c
int trevrpc_engine_msquic_create_v1(
    const trevrpc_engine_config_v1* engine_config,
    const trevrpc_engine_msquic_config_v1* provider_config,
    trevrpc_engine** out_engine);
```

The factory validates both versioned configurations, initializes the process-shared MsQuic native support, constructs the MsQuic provider, and attaches it to a provider-neutral Engine ABI 1 instance. On success it transfers one `trevrpc_engine` ownership reference through `out_engine`. On failure it releases any partially acquired provider/native resources and leaves `*out_engine` unchanged.

The factory copies or consumes all configuration needed after it returns; callers retain ownership of the input structures and referenced configuration bytes. The returned engine's handle owner cookie identifies this provider instance, but public handles remain `trevrpc_engine_handle_v1` values.

Choosing and linking a provider factory is required to obtain an operational engine; construction is not part of the provider-neutral Engine ABI surface.

## 4. Post-factory operations

Once `trevrpc_engine_msquic_create_v1()` succeeds, the companion has completed its public role. Applications use the generic functions declared by `trevrpc_engine.h`, including:

- `trevrpc_engine_get_wake_source_v1()` and `trevrpc_engine_next_event()`;
- `trevrpc_engine_event_get_info_v1()` and `trevrpc_engine_event_release()`;
- `trevrpc_engine_receive_get_info_v1()` and `trevrpc_engine_receive_release()`;
- `trevrpc_engine_get_diagnostics_v1()`;
- listener, dial, connection, stream, send, and receive operations;
- `trevrpc_engine_close()`, `trevrpc_engine_drain()`, and `trevrpc_engine_release()`.

The MsQuic provider publishes into the engine's one provider-neutral queue, uses the engine's mandatory reservations, participates in the engine's callback/operation admission gates, reports provider diagnostics into `trevrpc_engine_diagnostics_v1`, and completes shutdown through the engine lifecycle. It does not own a distinct public queue, wake source, state machine, or detached-object lifetime; those semantics are defined only by Engine ABI 1.

## 5. Package and dependencies

The companion package consists of:

- header `trevrpc_engine_msquic.h`;
- static archive `libtrevrpc_engine_msquic.a`;
- CMake target `trevrpc::trevrpc_engine_msquic`;
- pkg-config module `trevrpc_engine_msquic`.

`TREVRPC_BUILD_ENGINE_MSQUIC` follows `TREVRPC_BUILD_ENGINE`, so both default to `ON` for a standalone POSIX build. Set the companion option to `OFF` for an Engine-only package. Enabling it requires `TREVRPC_BUILD_ENGINE=ON`, a POSIX platform with file descriptors and pthreads, and an available `msquic CONFIG` package. The provider shares only the process-wide MsQuic API owner with the temporary internal C ABI 6 compatibility lane; it does not link the legacy `trevrpc_msquic_native_core` or open an independent MsQuic API table.

CMake consumers use:

```cmake
find_package(trevrpc_engine_msquic CONFIG REQUIRED)
target_link_libraries(application PRIVATE trevrpc::trevrpc_engine_msquic)
```

The CMake package finds `Threads`, `msquic CONFIG`, and the exact matching `trevrpc_engine` project version. It exports `TREVRPC_ENGINE_MSQUIC_ABI_VERSION=1`.

The pkg-config module requires the exact matching `trevrpc_engine` version and records the native API-owner, MsQuic, and pthread libraries as private static-link dependencies. It exports `trevrpc_engine_msquic_abi_version=1`.

The companion does not depend on the high-level TrevRPC runtime, WebTransport, H3, protobuf, or binding callbacks.
