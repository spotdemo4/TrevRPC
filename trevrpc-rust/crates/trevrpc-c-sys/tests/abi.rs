#![allow(clippy::cast_possible_truncation, clippy::too_many_lines)]

#[cfg(all(
    any(feature = "system-native", feature = "testing"),
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
use std::ffi::c_int;
#[cfg(all(
    feature = "system-native",
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
use std::io::ErrorKind;
#[cfg(all(
    any(feature = "system-native", feature = "testing"),
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
use std::mem::size_of_val;
use std::mem::{align_of, offset_of, size_of};
#[cfg(all(
    feature = "system-native",
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
use std::time::Duration;
#[cfg(all(
    feature = "system-native",
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
use std::{ptr, thread};

use trevrpc_c_sys::*;

macro_rules! assert_offsets {
    ($type:ty, $($field:ident => $offset:expr),+ $(,)?) => {
        $(assert_eq!(offset_of!($type, $field), $offset);)+
    };
}

#[cfg(all(
    any(feature = "system-native", feature = "testing"),
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
macro_rules! assert_signature {
    ($function:path, $signature:ty) => {{
        let function: $signature = $function;
        std::hint::black_box(function);
    }};
}

#[cfg(target_pointer_width = "64")]
#[test]
fn transport_layouts_match_abi_1() {
    assert_eq!(size_of::<trevrpc_transport_handle_v1>(), 16);
    assert_eq!(align_of::<trevrpc_transport_handle_v1>(), 8);
    assert_eq!(size_of::<trevrpc_transport_config_v1>(), 80);
    assert_eq!(align_of::<trevrpc_transport_config_v1>(), 8);
    assert_eq!(size_of::<trevrpc_transport_endpoint_config_v1>(), 312);
    assert_eq!(align_of::<trevrpc_transport_endpoint_config_v1>(), 8);
    assert_eq!(size_of::<trevrpc_transport_wake_source_v1>(), 48);
    assert_eq!(align_of::<trevrpc_transport_wake_source_v1>(), 8);
    assert_eq!(size_of::<trevrpc_transport_event_info_v1>(), 136);
    assert_eq!(align_of::<trevrpc_transport_event_info_v1>(), 8);
    assert_eq!(size_of::<trevrpc_transport_receive_info_v1>(), 64);
    assert_eq!(align_of::<trevrpc_transport_receive_info_v1>(), 8);
    assert_eq!(size_of::<trevrpc_transport_header_field_v1>(), 32);
    assert_eq!(align_of::<trevrpc_transport_header_field_v1>(), 8);
    assert_eq!(size_of::<trevrpc_transport_admission_info_v1>(), 144);
    assert_eq!(align_of::<trevrpc_transport_admission_info_v1>(), 8);
    assert_eq!(size_of::<trevrpc_transport_event_protocol_info_v1>(), 48);
    assert_eq!(align_of::<trevrpc_transport_event_protocol_info_v1>(), 8);
    assert_eq!(size_of::<trevrpc_transport_diagnostics_v1>(), 208);
    assert_eq!(align_of::<trevrpc_transport_diagnostics_v1>(), 8);
    assert_eq!(size_of::<trevrpc_transport_msquic_config_v1>(), 64);
    assert_eq!(align_of::<trevrpc_transport_msquic_config_v1>(), 8);

    assert_offsets!(
        trevrpc_transport_handle_v1,
        owner => 0,
        slot => 8,
        generation => 12,
    );
    assert_offsets!(
        trevrpc_transport_config_v1,
        struct_size => 0,
        struct_version => 4,
        event_capacity => 8,
        listener_capacity => 12,
        connection_capacity => 16,
        stream_capacity => 20,
        max_receive_owned_count => 24,
        flags => 28,
        max_receive_owned_bytes => 32,
        reserved => 40,
    );
    assert_offsets!(
        trevrpc_transport_endpoint_config_v1,
        struct_size => 0,
        struct_version => 4,
        protocol => 8,
        flags => 12,
        host => 16,
        host_len => 24,
        port => 28,
        peer_bidi_stream_count => 30,
        server_name => 32,
        server_name_len => 40,
        reserved0 => 44,
        alpn => 48,
        alpn_len => 56,
        reserved1 => 60,
        cert_file => 64,
        cert_file_len => 72,
        reserved2 => 76,
        key_file => 80,
        key_file_len => 88,
        reserved3 => 92,
        ca_cert_file => 96,
        ca_cert_file_len => 104,
        reserved4 => 108,
        cert_data => 112,
        cert_data_len => 120,
        key_data => 128,
        key_data_len => 136,
        ca_cert_data => 144,
        ca_cert_data_len => 152,
        path => 160,
        path_len => 168,
        webtransport_profiles => 172,
        origin => 176,
        origin_len => 184,
        max_sessions => 188,
        max_frame_size => 192,
        max_field_section_size => 200,
        max_pending_send_bytes => 208,
        max_pending_receive_bytes => 216,
        max_idle_timeout_ms => 224,
        max_pending_send_count => 232,
        max_pending_receive_count => 236,
        keep_alive_ms => 240,
        stream_recv_window => 244,
        conn_flow_control_window => 248,
        unresolved_stream_count => 252,
        unresolved_stream_bytes => 256,
        unresolved_stream_timeout_ms => 264,
        reserved => 272,
    );
    assert_offsets!(
        trevrpc_transport_wake_source_v1,
        struct_size => 0,
        struct_version => 4,
        kind => 8,
        flags => 12,
        native_handle => 16,
        reserved => 24,
    );
    assert_offsets!(
        trevrpc_transport_event_info_v1,
        struct_size => 0,
        struct_version => 4,
        kind => 8,
        flags => 12,
        status => 16,
        subject_kind => 20,
        sequence => 24,
        subject => 32,
        parent => 48,
        operation_id => 64,
        application_error_code => 72,
        provider_error_code => 80,
        data => 88,
        data_len => 96,
        reserved => 104,
    );
    assert_offsets!(
        trevrpc_transport_receive_info_v1,
        struct_size => 0,
        struct_version => 4,
        flags => 8,
        reserved0 => 12,
        data => 16,
        data_len => 24,
        reserved => 32,
    );
    assert_offsets!(
        trevrpc_transport_header_field_v1,
        name => 0,
        name_len => 8,
        value => 16,
        value_len => 24,
    );
    assert_offsets!(
        trevrpc_transport_admission_info_v1,
        struct_size => 0,
        struct_version => 4,
        protocol => 8,
        reserved0 => 12,
        listener => 16,
        headers => 32,
        header_count => 40,
        method => 48,
        method_len => 56,
        path => 64,
        path_len => 72,
        authority => 80,
        authority_len => 88,
        origin => 96,
        origin_len => 104,
        reserved => 112,
    );
    assert_offsets!(
        trevrpc_transport_event_protocol_info_v1,
        struct_size => 0,
        struct_version => 4,
        protocol => 8,
        reserved0 => 12,
        reserved => 16,
    );
    assert_offsets!(
        trevrpc_transport_diagnostics_v1,
        struct_size => 0,
        struct_version => 4,
        transport_abi_version => 8,
        state => 12,
        terminal_status => 16,
        event_capacity => 20,
        queue_depth => 24,
        ordinary_queue_depth => 28,
        events_enqueued => 32,
        events_dequeued => 40,
        events_rejected => 48,
        receive_owned_count => 56,
        peak_receive_owned_count => 64,
        receive_owned_bytes => 72,
        peak_receive_owned_bytes => 80,
        pending_send_bytes => 88,
        pending_send_count => 96,
        live_listeners => 104,
        live_connections => 112,
        live_streams => 120,
        active_callbacks => 128,
        active_api_calls => 136,
        wake_signals => 144,
        wake_write_eagain => 152,
        wake_failures => 160,
        provider_error_code => 168,
        mandatory_reservations => 176,
        reserved => 184,
    );
    assert_offsets!(
        trevrpc_transport_msquic_config_v1,
        struct_size => 0,
        struct_version => 4,
        flags => 8,
        reserved0 => 12,
        reserved => 16,
    );
}

#[cfg(all(
    feature = "system-native",
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
type Init<T> = unsafe extern "C" fn(*mut T, usize) -> c_int;
#[cfg(all(
    feature = "system-native",
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
type TransportUnary = unsafe extern "C" fn(*mut trevrpc_transport) -> c_int;
#[cfg(all(
    feature = "system-native",
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
type HandleUnary =
    unsafe extern "C" fn(*mut trevrpc_transport, trevrpc_transport_handle_v1) -> c_int;
#[cfg(all(
    feature = "system-native",
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
type HandleCode =
    unsafe extern "C" fn(*mut trevrpc_transport, trevrpc_transport_handle_v1, u64) -> c_int;

#[cfg(all(
    feature = "system-native",
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn assert_public_symbols_link() {
    assert_signature!(trevrpc_transport_abi_version, unsafe extern "C" fn() -> u32);
    assert_signature!(trevrpc_transport_abi_1_anchor, unsafe extern "C" fn());
    assert_signature!(
        trevrpc_transport_config_v1_init,
        Init<trevrpc_transport_config_v1>
    );
    assert_signature!(
        trevrpc_transport_endpoint_config_v1_init,
        Init<trevrpc_transport_endpoint_config_v1>
    );
    assert_signature!(
        trevrpc_transport_wake_source_v1_init,
        Init<trevrpc_transport_wake_source_v1>
    );
    assert_signature!(
        trevrpc_transport_event_info_v1_init,
        Init<trevrpc_transport_event_info_v1>
    );
    assert_signature!(
        trevrpc_transport_receive_info_v1_init,
        Init<trevrpc_transport_receive_info_v1>
    );
    assert_signature!(
        trevrpc_transport_admission_info_v1_init,
        Init<trevrpc_transport_admission_info_v1>
    );
    assert_signature!(
        trevrpc_transport_event_protocol_info_v1_init,
        Init<trevrpc_transport_event_protocol_info_v1>
    );
    assert_signature!(
        trevrpc_transport_diagnostics_v1_init,
        Init<trevrpc_transport_diagnostics_v1>
    );
    assert_signature!(
        trevrpc_transport_get_wake_sources_v1,
        unsafe extern "C" fn(
            *mut trevrpc_transport,
            *mut trevrpc_transport_wake_source_v1,
            usize,
            *mut usize,
        ) -> c_int
    );
    assert_signature!(trevrpc_transport_poll_timeout_ms, TransportUnary);
    assert_signature!(
        trevrpc_transport_next_event,
        unsafe extern "C" fn(*mut trevrpc_transport, *mut *mut trevrpc_transport_event_v1) -> c_int
    );
    assert_signature!(
        trevrpc_transport_event_get_info_v1,
        unsafe extern "C" fn(
            *mut trevrpc_transport,
            *const trevrpc_transport_event_v1,
            *mut trevrpc_transport_event_info_v1,
        ) -> c_int
    );
    assert_signature!(
        trevrpc_transport_event_get_admission_info_v1,
        unsafe extern "C" fn(
            *mut trevrpc_transport,
            *const trevrpc_transport_event_v1,
            *mut trevrpc_transport_admission_info_v1,
        ) -> c_int
    );
    assert_signature!(
        trevrpc_transport_event_get_protocol_info_v1,
        unsafe extern "C" fn(
            *mut trevrpc_transport,
            *const trevrpc_transport_event_v1,
            *mut trevrpc_transport_event_protocol_info_v1,
        ) -> c_int
    );
    assert_signature!(
        trevrpc_transport_admission_respond_v1,
        unsafe extern "C" fn(
            *mut trevrpc_transport,
            *const trevrpc_transport_event_v1,
            u16,
        ) -> c_int
    );
    assert_signature!(
        trevrpc_transport_event_release,
        unsafe extern "C" fn(*mut trevrpc_transport, *mut trevrpc_transport_event_v1)
    );
    assert_signature!(
        trevrpc_transport_receive_get_info_v1,
        unsafe extern "C" fn(
            *mut trevrpc_transport,
            *const trevrpc_transport_receive_v1,
            *mut trevrpc_transport_receive_info_v1,
        ) -> c_int
    );
    assert_signature!(
        trevrpc_transport_receive_release,
        unsafe extern "C" fn(*mut trevrpc_transport, *mut trevrpc_transport_receive_v1)
    );
    assert_signature!(
        trevrpc_transport_get_diagnostics_v1,
        unsafe extern "C" fn(
            *mut trevrpc_transport,
            *mut trevrpc_transport_diagnostics_v1,
        ) -> c_int
    );
    assert_signature!(
        trevrpc_transport_listen_v1,
        unsafe extern "C" fn(
            *mut trevrpc_transport,
            *const trevrpc_transport_endpoint_config_v1,
            *mut trevrpc_transport_handle_v1,
        ) -> c_int
    );
    assert_signature!(
        trevrpc_transport_listener_get_port_v1,
        unsafe extern "C" fn(
            *mut trevrpc_transport,
            trevrpc_transport_handle_v1,
            *mut u16,
        ) -> c_int
    );
    assert_signature!(
        trevrpc_transport_dial_v1,
        unsafe extern "C" fn(
            *mut trevrpc_transport,
            *const trevrpc_transport_endpoint_config_v1,
            u64,
            *mut trevrpc_transport_handle_v1,
        ) -> c_int
    );
    assert_signature!(trevrpc_transport_dial_cancel, HandleUnary);
    assert_signature!(
        trevrpc_transport_connection_open_bidi_stream_v1,
        unsafe extern "C" fn(
            *mut trevrpc_transport,
            trevrpc_transport_handle_v1,
            u64,
            *mut trevrpc_transport_handle_v1,
        ) -> c_int
    );
    assert_signature!(
        trevrpc_transport_stream_send_v1,
        unsafe extern "C" fn(
            *mut trevrpc_transport,
            trevrpc_transport_handle_v1,
            u64,
            *const u8,
            usize,
        ) -> c_int
    );
    assert_signature!(
        trevrpc_transport_stream_receive,
        unsafe extern "C" fn(
            *mut trevrpc_transport,
            trevrpc_transport_handle_v1,
            *mut *mut trevrpc_transport_receive_v1,
        ) -> c_int
    );
    assert_signature!(trevrpc_transport_stream_finish_send, HandleUnary);
    assert_signature!(trevrpc_transport_stream_abort_receive, HandleCode);
    assert_signature!(trevrpc_transport_stream_abort_send, HandleCode);
    assert_signature!(trevrpc_transport_stream_abort, HandleCode);
    assert_signature!(trevrpc_transport_stream_close, HandleUnary);
    assert_signature!(trevrpc_transport_connection_close, HandleCode);
    assert_signature!(trevrpc_transport_listener_close, HandleUnary);
    assert_signature!(
        trevrpc_transport_release_handle,
        unsafe extern "C" fn(*mut trevrpc_transport, trevrpc_transport_handle_v1, u32) -> c_int
    );
    assert_signature!(trevrpc_transport_close, TransportUnary);
    assert_signature!(trevrpc_transport_drain, TransportUnary);
    assert_signature!(trevrpc_transport_release, TransportUnary);
    assert_signature!(
        trevrpc_transport_msquic_abi_version,
        unsafe extern "C" fn() -> u32
    );
    assert_signature!(
        trevrpc_transport_msquic_abi_1_anchor,
        unsafe extern "C" fn()
    );
    assert_signature!(
        trevrpc_transport_msquic_config_v1_init,
        Init<trevrpc_transport_msquic_config_v1>
    );
    assert_signature!(
        trevrpc_transport_msquic_create_v1,
        unsafe extern "C" fn(
            *const trevrpc_transport_config_v1,
            *const trevrpc_transport_msquic_config_v1,
            *mut *mut trevrpc_transport,
        ) -> c_int
    );
}

#[test]
fn public_constants_match_abi_1() {
    assert_eq!(
        [
            TREVRPC_TRANSPORT_ABI_VERSION,
            TREVRPC_TRANSPORT_STRUCT_VERSION_1,
            TREVRPC_TRANSPORT_DEFAULT_EVENT_CAPACITY,
            TREVRPC_TRANSPORT_DEFAULT_LISTENER_CAPACITY,
            TREVRPC_TRANSPORT_DEFAULT_CONNECTION_CAPACITY,
            TREVRPC_TRANSPORT_DEFAULT_STREAM_CAPACITY,
            TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_COUNT,
            TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_SEND_COUNT,
            TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_RECEIVE_COUNT,
            TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_COUNT,
            TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_TIMEOUT_MS,
            TREVRPC_TRANSPORT_MAX_WAKE_SOURCES,
        ],
        [1, 1, 256, 16, 1024, 4096, 4096, 1024, 4096, 64, 5000, 8]
    );
    assert_eq!(
        [
            TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_BYTES,
            TREVRPC_TRANSPORT_DEFAULT_MAX_FRAME_SIZE,
            TREVRPC_TRANSPORT_DEFAULT_MAX_FIELD_SECTION_SIZE,
            TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_SEND_BYTES,
            TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_RECEIVE_BYTES,
            TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_BYTES,
        ],
        [64 << 20, 4 << 20, 64 << 10, 64 << 20, 64 << 20, 64 << 20]
    );
    assert_eq!(
        [
            TREVRPC_TRANSPORT_STATE_RUNNING,
            TREVRPC_TRANSPORT_STATE_STOPPING,
            TREVRPC_TRANSPORT_STATE_STOPPED,
            TREVRPC_TRANSPORT_WAKE_SOURCE_POSIX_FD,
            TREVRPC_TRANSPORT_WAKE_FLAG_BORROWED,
            TREVRPC_TRANSPORT_WAKE_FLAG_LEVEL_TRIGGERED,
            TREVRPC_TRANSPORT_OBJECT_NONE,
            TREVRPC_TRANSPORT_OBJECT_LISTENER,
            TREVRPC_TRANSPORT_OBJECT_CONNECTION,
            TREVRPC_TRANSPORT_OBJECT_STREAM,
        ],
        [0, 1, 2, 1, 1, 2, 0, 1, 2, 3]
    );
    assert_eq!(
        [
            TREVRPC_TRANSPORT_EVENT_DIAGNOSTIC,
            TREVRPC_TRANSPORT_EVENT_STOPPED,
            TREVRPC_TRANSPORT_EVENT_LISTENER_STOPPED,
            TREVRPC_TRANSPORT_EVENT_CONNECTION_READY,
            TREVRPC_TRANSPORT_EVENT_CONNECTION_FAILED,
            TREVRPC_TRANSPORT_EVENT_CONNECTION_CLOSED,
            TREVRPC_TRANSPORT_EVENT_STREAM_READY,
            TREVRPC_TRANSPORT_EVENT_STREAM_FAILED,
            TREVRPC_TRANSPORT_EVENT_STREAM_READABLE,
            TREVRPC_TRANSPORT_EVENT_RECEIVE_FIN,
            TREVRPC_TRANSPORT_EVENT_SEND_COMPLETE,
            TREVRPC_TRANSPORT_EVENT_STREAM_CLOSED,
            TREVRPC_TRANSPORT_EVENT_HTTP3_ADMISSION,
            TREVRPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION,
            TREVRPC_TRANSPORT_EVENT_SEND_STOPPED,
        ],
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
    );
    assert_eq!(
        [
            TREVRPC_TRANSPORT_EVENT_FLAG_FATAL,
            TREVRPC_TRANSPORT_EVENT_FLAG_TERMINAL,
            TREVRPC_TRANSPORT_EVENT_FLAG_CLIENT,
            TREVRPC_TRANSPORT_EVENT_FLAG_SERVER,
            TREVRPC_TRANSPORT_EVENT_FLAG_LOCAL,
            TREVRPC_TRANSPORT_EVENT_FLAG_PEER,
            TREVRPC_TRANSPORT_EVENT_FLAG_PEER_RESET,
            TREVRPC_TRANSPORT_EVENT_FLAG_TRANSPORT_ERROR,
            TREVRPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN,
        ],
        [1, 2, 4, 8, 16, 32, 64, 128, 256]
    );
    assert_eq!(
        [
            TREVRPC_TRANSPORT_ENDPOINT_SKIP_CERTIFICATE_VALIDATION,
            TREVRPC_TRANSPORT_ENDPOINT_DEFER_ADMISSION,
            TREVRPC_TRANSPORT_PROTOCOL_AUTO,
            TREVRPC_TRANSPORT_PROTOCOL_NATIVE,
            TREVRPC_TRANSPORT_PROTOCOL_HTTP3,
            TREVRPC_TRANSPORT_PROTOCOL_WEBTRANSPORT,
            TREVRPC_TRANSPORT_PROTOCOL_MULTIPLEXED,
        ],
        [1, 2, 0, 1, 2, 3, 4]
    );
    assert_eq!(
        [
            TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_DRAFT_02,
            TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_DRAFT_07,
            TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_DRAFT_14,
            TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_DRAFT_15,
            TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_ALL_SUPPORTED,
            TREVRPC_TRANSPORT_RECEIVE_FLAG_NONE,
            TREVRPC_TRANSPORT_MSQUIC_ABI_VERSION,
            TREVRPC_TRANSPORT_MSQUIC_STRUCT_VERSION_1,
        ],
        [1, 2, 4, 8, 15, 0, 1, 1]
    );
}

#[cfg(all(
    feature = "testing",
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[test]
fn testing_provider_symbols_initialize_and_link() {
    assert_eq!(size_of::<trevrpc_transport_testing_config_v1>(), 64);
    assert_eq!(align_of::<trevrpc_transport_testing_config_v1>(), 8);
    assert_eq!(size_of::<trevrpc_transport_testing_event_v1>(), 208);
    assert_eq!(align_of::<trevrpc_transport_testing_event_v1>(), 8);
    assert_eq!(size_of::<trevrpc_transport_testing_receive_v1>(), 72);
    assert_eq!(align_of::<trevrpc_transport_testing_receive_v1>(), 8);
    assert_eq!(size_of::<trevrpc_transport_testing_counters_v1>(), 304);
    assert_eq!(align_of::<trevrpc_transport_testing_counters_v1>(), 8);
    assert_offsets!(
        trevrpc_transport_testing_config_v1,
        struct_size => 0,
        struct_version => 4,
        event_capacity => 8,
        receive_capacity => 12,
        status_capacity => 16,
        listener_port => 20,
        reserved0 => 22,
        reserved1 => 24,
        reserved => 32,
    );
    assert_offsets!(
        trevrpc_transport_testing_event_v1,
        struct_size => 0,
        struct_version => 4,
        kind => 8,
        flags => 12,
        status => 16,
        subject_kind => 20,
        subject => 24,
        parent => 40,
        operation_id => 56,
        application_error_code => 64,
        provider_error_code => 72,
        protocol => 80,
        reserved0 => 84,
        data => 88,
        data_len => 96,
        headers => 104,
        header_count => 112,
        method => 120,
        method_len => 128,
        path => 136,
        path_len => 144,
        authority => 152,
        authority_len => 160,
        origin => 168,
        origin_len => 176,
        reserved => 184,
    );
    assert_offsets!(
        trevrpc_transport_testing_receive_v1,
        struct_size => 0,
        struct_version => 4,
        flags => 8,
        reserved0 => 12,
        stream => 16,
        data => 32,
        data_len => 40,
        reserved => 48,
    );
    assert_offsets!(
        trevrpc_transport_testing_counters_v1,
        struct_size => 0,
        struct_version => 4,
        events_injected => 8,
        events_popped => 16,
        events_released => 24,
        receives_injected => 32,
        receives_popped => 40,
        receives_released => 48,
        admissions_responded => 56,
        operation_calls => 64,
        last_operation_id => 224,
        last_send_operation_id => 232,
        last_dial_operation_id => 240,
        last_open_operation_id => 248,
        last_admission_status => 256,
        last_status => 264,
        last_operation => 268,
        reserved => 272,
    );

    unsafe {
        assert_signature!(
            trevrpc_transport_testing_create_v1,
            unsafe extern "C" fn(
                *const trevrpc_transport_config_v1,
                *const trevrpc_transport_testing_config_v1,
                *mut *mut trevrpc_transport,
                *mut *mut trevrpc_transport_testing_control,
            ) -> c_int
        );
        assert_signature!(
            trevrpc_transport_testing_push_event_v1,
            unsafe extern "C" fn(
                *mut trevrpc_transport_testing_control,
                *const trevrpc_transport_testing_event_v1,
            ) -> c_int
        );
        assert_signature!(
            trevrpc_transport_testing_push_receive_v1,
            unsafe extern "C" fn(
                *mut trevrpc_transport_testing_control,
                *const trevrpc_transport_testing_receive_v1,
            ) -> c_int
        );
        assert_signature!(
            trevrpc_transport_testing_script_status_v1,
            unsafe extern "C" fn(*mut trevrpc_transport_testing_control, i32) -> c_int
        );
        assert_signature!(
            trevrpc_transport_testing_clear_status_script_v1,
            unsafe extern "C" fn(*mut trevrpc_transport_testing_control) -> c_int
        );
        assert_signature!(
            trevrpc_transport_testing_set_checkpoint_v1,
            unsafe extern "C" fn(*mut trevrpc_transport_testing_control, u32, u32) -> c_int
        );
        assert_signature!(
            trevrpc_transport_testing_wait_checkpoint_v1,
            unsafe extern "C" fn(*mut trevrpc_transport_testing_control, u32) -> c_int
        );
        assert_signature!(
            trevrpc_transport_testing_release_checkpoint_v1,
            unsafe extern "C" fn(*mut trevrpc_transport_testing_control, u32) -> c_int
        );
        assert_signature!(
            trevrpc_transport_testing_push_stopped_v1,
            unsafe extern "C" fn(*mut trevrpc_transport_testing_control, i32) -> c_int
        );
        assert_signature!(
            trevrpc_transport_testing_get_counters_v1,
            unsafe extern "C" fn(
                *mut trevrpc_transport_testing_control,
                *mut trevrpc_transport_testing_counters_v1,
            ) -> c_int
        );
        assert_signature!(
            trevrpc_transport_testing_get_wake_fd_v1,
            unsafe extern "C" fn(*mut trevrpc_transport_testing_control, u32) -> c_int
        );
        assert_signature!(
            trevrpc_transport_testing_set_poll_timeout_ms_v1,
            unsafe extern "C" fn(*mut trevrpc_transport_testing_control, i32) -> c_int
        );
        assert_eq!(
            trevrpc_transport_testing_abi_version(),
            TREVRPC_TRANSPORT_TESTING_ABI_VERSION
        );
        trevrpc_transport_testing_abi_1_anchor();

        let mut config = std::mem::zeroed::<trevrpc_transport_testing_config_v1>();
        let mut event = std::mem::zeroed::<trevrpc_transport_testing_event_v1>();
        let mut receive = std::mem::zeroed::<trevrpc_transport_testing_receive_v1>();
        let mut counters = std::mem::zeroed::<trevrpc_transport_testing_counters_v1>();
        assert_eq!(
            trevrpc_transport_testing_config_v1_init(&raw mut config, size_of_val(&config)),
            0
        );
        assert_eq!(
            trevrpc_transport_testing_event_v1_init(&raw mut event, size_of_val(&event)),
            0
        );
        assert_eq!(
            trevrpc_transport_testing_receive_v1_init(&raw mut receive, size_of_val(&receive)),
            0
        );
        assert_eq!(
            trevrpc_transport_testing_counters_v1_init(&raw mut counters, size_of_val(&counters)),
            0
        );
    }
}

#[cfg(all(
    feature = "system-native",
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[test]
fn public_initializers_match_abi_1_defaults() {
    unsafe {
        let mut transport = std::mem::zeroed::<trevrpc_transport_config_v1>();
        let mut endpoint = std::mem::zeroed::<trevrpc_transport_endpoint_config_v1>();
        let mut wake = std::mem::zeroed::<trevrpc_transport_wake_source_v1>();
        let mut event = std::mem::zeroed::<trevrpc_transport_event_info_v1>();
        let mut receive = std::mem::zeroed::<trevrpc_transport_receive_info_v1>();
        let mut admission = std::mem::zeroed::<trevrpc_transport_admission_info_v1>();
        let mut protocol = std::mem::zeroed::<trevrpc_transport_event_protocol_info_v1>();
        let mut diagnostics = std::mem::zeroed::<trevrpc_transport_diagnostics_v1>();
        let mut provider = std::mem::zeroed::<trevrpc_transport_msquic_config_v1>();

        assert_eq!(
            trevrpc_transport_config_v1_init(&raw mut transport, size_of_val(&transport)),
            0
        );
        assert_eq!(
            trevrpc_transport_endpoint_config_v1_init(&raw mut endpoint, size_of_val(&endpoint)),
            0
        );
        assert_eq!(
            trevrpc_transport_wake_source_v1_init(&raw mut wake, size_of_val(&wake)),
            0
        );
        assert_eq!(
            trevrpc_transport_event_info_v1_init(&raw mut event, size_of_val(&event)),
            0
        );
        assert_eq!(
            trevrpc_transport_receive_info_v1_init(&raw mut receive, size_of_val(&receive)),
            0
        );
        assert_eq!(
            trevrpc_transport_admission_info_v1_init(&raw mut admission, size_of_val(&admission)),
            0
        );
        assert_eq!(
            trevrpc_transport_event_protocol_info_v1_init(
                &raw mut protocol,
                size_of_val(&protocol)
            ),
            0
        );
        assert_eq!(
            trevrpc_transport_diagnostics_v1_init(&raw mut diagnostics, size_of_val(&diagnostics)),
            0
        );
        assert_eq!(
            trevrpc_transport_msquic_config_v1_init(&raw mut provider, size_of_val(&provider)),
            0
        );

        assert_eq!(
            transport,
            trevrpc_transport_config_v1 {
                struct_size: 80,
                struct_version: TREVRPC_TRANSPORT_STRUCT_VERSION_1,
                event_capacity: TREVRPC_TRANSPORT_DEFAULT_EVENT_CAPACITY,
                listener_capacity: TREVRPC_TRANSPORT_DEFAULT_LISTENER_CAPACITY,
                connection_capacity: TREVRPC_TRANSPORT_DEFAULT_CONNECTION_CAPACITY,
                stream_capacity: TREVRPC_TRANSPORT_DEFAULT_STREAM_CAPACITY,
                max_receive_owned_count: TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_COUNT,
                flags: 0,
                max_receive_owned_bytes: TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_BYTES,
                reserved: [0; 5],
            }
        );
        assert_eq!(
            endpoint,
            trevrpc_transport_endpoint_config_v1 {
                struct_size: 312,
                struct_version: TREVRPC_TRANSPORT_STRUCT_VERSION_1,
                protocol: TREVRPC_TRANSPORT_PROTOCOL_AUTO,
                flags: 0,
                host: ptr::null(),
                host_len: 0,
                port: 0,
                peer_bidi_stream_count: 100,
                server_name: ptr::null(),
                server_name_len: 0,
                reserved0: 0,
                alpn: ptr::null(),
                alpn_len: 0,
                reserved1: 0,
                cert_file: ptr::null(),
                cert_file_len: 0,
                reserved2: 0,
                key_file: ptr::null(),
                key_file_len: 0,
                reserved3: 0,
                ca_cert_file: ptr::null(),
                ca_cert_file_len: 0,
                reserved4: 0,
                cert_data: ptr::null(),
                cert_data_len: 0,
                key_data: ptr::null(),
                key_data_len: 0,
                ca_cert_data: ptr::null(),
                ca_cert_data_len: 0,
                path: ptr::null(),
                path_len: 0,
                webtransport_profiles: TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_ALL_SUPPORTED,
                origin: ptr::null(),
                origin_len: 0,
                max_sessions: 1,
                max_frame_size: TREVRPC_TRANSPORT_DEFAULT_MAX_FRAME_SIZE,
                max_field_section_size: TREVRPC_TRANSPORT_DEFAULT_MAX_FIELD_SECTION_SIZE,
                max_pending_send_bytes: TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_SEND_BYTES,
                max_pending_receive_bytes: TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_RECEIVE_BYTES,
                max_idle_timeout_ms: 0,
                max_pending_send_count: TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_SEND_COUNT,
                max_pending_receive_count: TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_RECEIVE_COUNT,
                keep_alive_ms: 0,
                stream_recv_window: 0,
                conn_flow_control_window: 0,
                unresolved_stream_count: TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_COUNT,
                unresolved_stream_bytes: TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_BYTES,
                unresolved_stream_timeout_ms: u64::from(
                    TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_TIMEOUT_MS,
                ),
                reserved: [0; 5],
            }
        );
        assert_eq!(
            wake,
            trevrpc_transport_wake_source_v1 {
                struct_size: 48,
                struct_version: TREVRPC_TRANSPORT_STRUCT_VERSION_1,
                kind: 0,
                flags: 0,
                native_handle: 0,
                reserved: [0; 3],
            }
        );
        assert_eq!(
            event,
            trevrpc_transport_event_info_v1 {
                struct_size: 136,
                struct_version: TREVRPC_TRANSPORT_STRUCT_VERSION_1,
                kind: 0,
                flags: 0,
                status: 0,
                subject_kind: 0,
                sequence: 0,
                subject: trevrpc_transport_handle_v1::default(),
                parent: trevrpc_transport_handle_v1::default(),
                operation_id: 0,
                application_error_code: 0,
                provider_error_code: 0,
                data: ptr::null(),
                data_len: 0,
                reserved: [0; 4],
            }
        );
        assert_eq!(
            receive,
            trevrpc_transport_receive_info_v1 {
                struct_size: 64,
                struct_version: TREVRPC_TRANSPORT_STRUCT_VERSION_1,
                flags: 0,
                reserved0: 0,
                data: ptr::null(),
                data_len: 0,
                reserved: [0; 4],
            }
        );
        assert_eq!(
            admission,
            trevrpc_transport_admission_info_v1 {
                struct_size: 144,
                struct_version: TREVRPC_TRANSPORT_STRUCT_VERSION_1,
                protocol: 0,
                reserved0: 0,
                listener: trevrpc_transport_handle_v1::default(),
                headers: ptr::null(),
                header_count: 0,
                method: ptr::null(),
                method_len: 0,
                path: ptr::null(),
                path_len: 0,
                authority: ptr::null(),
                authority_len: 0,
                origin: ptr::null(),
                origin_len: 0,
                reserved: [0; 4],
            }
        );
        assert_eq!(
            protocol,
            trevrpc_transport_event_protocol_info_v1 {
                struct_size: 48,
                struct_version: TREVRPC_TRANSPORT_STRUCT_VERSION_1,
                protocol: 0,
                reserved0: 0,
                reserved: [0; 4],
            }
        );
        assert_eq!(
            diagnostics,
            trevrpc_transport_diagnostics_v1 {
                struct_size: 208,
                struct_version: TREVRPC_TRANSPORT_STRUCT_VERSION_1,
                transport_abi_version: TREVRPC_TRANSPORT_ABI_VERSION,
                state: 0,
                terminal_status: 0,
                event_capacity: 0,
                queue_depth: 0,
                ordinary_queue_depth: 0,
                events_enqueued: 0,
                events_dequeued: 0,
                events_rejected: 0,
                receive_owned_count: 0,
                peak_receive_owned_count: 0,
                receive_owned_bytes: 0,
                peak_receive_owned_bytes: 0,
                pending_send_bytes: 0,
                pending_send_count: 0,
                live_listeners: 0,
                live_connections: 0,
                live_streams: 0,
                active_callbacks: 0,
                active_api_calls: 0,
                wake_signals: 0,
                wake_write_eagain: 0,
                wake_failures: 0,
                provider_error_code: 0,
                mandatory_reservations: 0,
                reserved: [0; 3],
            }
        );
        assert_eq!(
            provider,
            trevrpc_transport_msquic_config_v1 {
                struct_size: 64,
                struct_version: TREVRPC_TRANSPORT_MSQUIC_STRUCT_VERSION_1,
                flags: 0,
                reserved0: 0,
                reserved: [0; 6],
            }
        );

        assert_eq!(
            trevrpc_transport_config_v1_init(&raw mut transport, size_of_val(&transport) - 1),
            -22
        );
        assert_eq!(
            trevrpc_transport_endpoint_config_v1_init(
                &raw mut endpoint,
                size_of_val(&endpoint) - 1
            ),
            -22
        );
        assert_eq!(
            trevrpc_transport_msquic_config_v1_init(&raw mut provider, size_of_val(&provider) - 1),
            -22
        );
    }
}

#[cfg(all(
    feature = "system-native",
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[test]
fn public_symbols_initialize_and_link() {
    assert_public_symbols_link();

    unsafe {
        assert_eq!(
            trevrpc_transport_abi_version(),
            TREVRPC_TRANSPORT_ABI_VERSION
        );
        assert_eq!(
            trevrpc_transport_msquic_abi_version(),
            TREVRPC_TRANSPORT_MSQUIC_ABI_VERSION
        );
        trevrpc_transport_abi_1_anchor();
        trevrpc_transport_msquic_abi_1_anchor();
    }
}

#[cfg(all(
    feature = "system-native",
    any(target_os = "linux", target_os = "macos"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[test]
fn factory_closes_and_releases_cleanly() {
    unsafe {
        let mut transport_config = std::mem::zeroed::<trevrpc_transport_config_v1>();
        let mut provider_config = std::mem::zeroed::<trevrpc_transport_msquic_config_v1>();
        assert_eq!(
            trevrpc_transport_config_v1_init(
                &raw mut transport_config,
                size_of_val(&transport_config)
            ),
            0
        );
        assert_eq!(
            trevrpc_transport_msquic_config_v1_init(
                &raw mut provider_config,
                size_of_val(&provider_config)
            ),
            0
        );

        let mut transport = ptr::null_mut();
        assert_eq!(
            trevrpc_transport_msquic_create_v1(
                &raw const transport_config,
                &raw const provider_config,
                &raw mut transport,
            ),
            0
        );
        assert!(!transport.is_null());
        assert_eq!(trevrpc_transport_close(transport), 0);

        let mut stopped = false;
        for _ in 0..100 {
            loop {
                let mut raw_event = ptr::null_mut();
                let result = trevrpc_transport_next_event(transport, &raw mut raw_event);
                if result != 0 {
                    assert_eq!(
                        std::io::Error::from_raw_os_error(-result).kind(),
                        ErrorKind::WouldBlock
                    );
                    break;
                }
                assert!(!raw_event.is_null());
                let mut info = std::mem::zeroed::<trevrpc_transport_event_info_v1>();
                assert_eq!(
                    trevrpc_transport_event_info_v1_init(&raw mut info, size_of_val(&info)),
                    0
                );
                assert_eq!(
                    trevrpc_transport_event_get_info_v1(transport, raw_event, &raw mut info),
                    0
                );
                stopped |= info.kind == TREVRPC_TRANSPORT_EVENT_STOPPED;
                trevrpc_transport_event_release(transport, raw_event);
            }
            if stopped {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(stopped);
        assert_eq!(trevrpc_transport_drain(transport), 0);
        assert_eq!(trevrpc_transport_release(transport), 0);
    }
}
