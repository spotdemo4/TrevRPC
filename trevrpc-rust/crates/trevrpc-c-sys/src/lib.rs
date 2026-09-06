#![allow(non_camel_case_types)]
#![allow(clippy::missing_safety_doc)]

use std::ffi::{c_char, c_int};

pub const TREVRPC_TRANSPORT_ABI_VERSION: u32 = 1;
pub const TREVRPC_TRANSPORT_STRUCT_VERSION_1: u32 = 1;

pub const TREVRPC_TRANSPORT_DEFAULT_EVENT_CAPACITY: u32 = 256;
pub const TREVRPC_TRANSPORT_DEFAULT_LISTENER_CAPACITY: u32 = 16;
pub const TREVRPC_TRANSPORT_DEFAULT_CONNECTION_CAPACITY: u32 = 1024;
pub const TREVRPC_TRANSPORT_DEFAULT_STREAM_CAPACITY: u32 = 4096;
pub const TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_COUNT: u32 = 4096;
pub const TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_BYTES: u64 = 64 * 1024 * 1024;
pub const TREVRPC_TRANSPORT_DEFAULT_MAX_FRAME_SIZE: u64 = 4 * 1024 * 1024;
pub const TREVRPC_TRANSPORT_DEFAULT_MAX_FIELD_SECTION_SIZE: u64 = 64 * 1024;
pub const TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_SEND_BYTES: u64 = 64 * 1024 * 1024;
pub const TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_SEND_COUNT: u32 = 1024;
pub const TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_RECEIVE_BYTES: u64 = 64 * 1024 * 1024;
pub const TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_RECEIVE_COUNT: u32 = 4096;
pub const TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_COUNT: u32 = 64;
pub const TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_BYTES: u64 = 64 * 1024 * 1024;
pub const TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_TIMEOUT_MS: u32 = 5000;
pub const TREVRPC_TRANSPORT_MAX_WAKE_SOURCES: u32 = 8;

pub const TREVRPC_TRANSPORT_STATE_RUNNING: u32 = 0;
pub const TREVRPC_TRANSPORT_STATE_STOPPING: u32 = 1;
pub const TREVRPC_TRANSPORT_STATE_STOPPED: u32 = 2;

pub const TREVRPC_TRANSPORT_WAKE_SOURCE_POSIX_FD: u32 = 1;
pub const TREVRPC_TRANSPORT_WAKE_FLAG_BORROWED: u32 = 0x1;
pub const TREVRPC_TRANSPORT_WAKE_FLAG_LEVEL_TRIGGERED: u32 = 0x2;

pub const TREVRPC_TRANSPORT_OBJECT_NONE: u32 = 0;
pub const TREVRPC_TRANSPORT_OBJECT_LISTENER: u32 = 1;
pub const TREVRPC_TRANSPORT_OBJECT_CONNECTION: u32 = 2;
pub const TREVRPC_TRANSPORT_OBJECT_STREAM: u32 = 3;

pub const TREVRPC_TRANSPORT_EVENT_DIAGNOSTIC: u32 = 1;
pub const TREVRPC_TRANSPORT_EVENT_STOPPED: u32 = 2;
pub const TREVRPC_TRANSPORT_EVENT_LISTENER_STOPPED: u32 = 3;
pub const TREVRPC_TRANSPORT_EVENT_CONNECTION_READY: u32 = 4;
pub const TREVRPC_TRANSPORT_EVENT_CONNECTION_FAILED: u32 = 5;
pub const TREVRPC_TRANSPORT_EVENT_CONNECTION_CLOSED: u32 = 6;
pub const TREVRPC_TRANSPORT_EVENT_STREAM_READY: u32 = 7;
pub const TREVRPC_TRANSPORT_EVENT_STREAM_FAILED: u32 = 8;
pub const TREVRPC_TRANSPORT_EVENT_STREAM_READABLE: u32 = 9;
pub const TREVRPC_TRANSPORT_EVENT_RECEIVE_FIN: u32 = 10;
pub const TREVRPC_TRANSPORT_EVENT_SEND_COMPLETE: u32 = 11;
pub const TREVRPC_TRANSPORT_EVENT_STREAM_CLOSED: u32 = 12;
pub const TREVRPC_TRANSPORT_EVENT_HTTP3_ADMISSION: u32 = 13;
pub const TREVRPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION: u32 = 14;
pub const TREVRPC_TRANSPORT_EVENT_SEND_STOPPED: u32 = 15;

pub const TREVRPC_TRANSPORT_EVENT_FLAG_FATAL: u32 = 0x0000_0001;
pub const TREVRPC_TRANSPORT_EVENT_FLAG_TERMINAL: u32 = 0x0000_0002;
pub const TREVRPC_TRANSPORT_EVENT_FLAG_CLIENT: u32 = 0x0000_0004;
pub const TREVRPC_TRANSPORT_EVENT_FLAG_SERVER: u32 = 0x0000_0008;
pub const TREVRPC_TRANSPORT_EVENT_FLAG_LOCAL: u32 = 0x0000_0010;
pub const TREVRPC_TRANSPORT_EVENT_FLAG_PEER: u32 = 0x0000_0020;
pub const TREVRPC_TRANSPORT_EVENT_FLAG_PEER_RESET: u32 = 0x0000_0040;
pub const TREVRPC_TRANSPORT_EVENT_FLAG_TRANSPORT_ERROR: u32 = 0x0000_0080;
pub const TREVRPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN: u32 = 0x0000_0100;

pub const TREVRPC_TRANSPORT_ENDPOINT_SKIP_CERTIFICATE_VALIDATION: u32 = 0x0000_0001;
pub const TREVRPC_TRANSPORT_ENDPOINT_DEFER_ADMISSION: u32 = 0x0000_0002;

pub const TREVRPC_TRANSPORT_PROTOCOL_AUTO: u32 = 0;
pub const TREVRPC_TRANSPORT_PROTOCOL_NATIVE: u32 = 1;
pub const TREVRPC_TRANSPORT_PROTOCOL_HTTP3: u32 = 2;
pub const TREVRPC_TRANSPORT_PROTOCOL_WEBTRANSPORT: u32 = 3;
pub const TREVRPC_TRANSPORT_PROTOCOL_MULTIPLEXED: u32 = 4;

pub const TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_DRAFT_02: u32 = 0x0000_0001;
pub const TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_DRAFT_07: u32 = 0x0000_0002;
pub const TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_DRAFT_14: u32 = 0x0000_0004;
pub const TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_DRAFT_15: u32 = 0x0000_0008;
pub const TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_ALL_SUPPORTED: u32 = 0x0000_000f;

pub const TREVRPC_TRANSPORT_RECEIVE_FLAG_NONE: u32 = 0;

pub const TREVRPC_TRANSPORT_MSQUIC_ABI_VERSION: u32 = 1;
pub const TREVRPC_TRANSPORT_MSQUIC_STRUCT_VERSION_1: u32 = 1;

#[repr(C)]
pub struct trevrpc_transport {
    _private: [u8; 0],
}

#[repr(C)]
pub struct trevrpc_transport_event_v1 {
    _private: [u8; 0],
}

#[repr(C)]
pub struct trevrpc_transport_receive_v1 {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct trevrpc_transport_handle_v1 {
    pub owner: u64,
    pub slot: u32,
    pub generation: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct trevrpc_transport_config_v1 {
    pub struct_size: u32,
    pub struct_version: u32,
    pub event_capacity: u32,
    pub listener_capacity: u32,
    pub connection_capacity: u32,
    pub stream_capacity: u32,
    pub max_receive_owned_count: u32,
    pub flags: u32,
    pub max_receive_owned_bytes: u64,
    pub reserved: [u64; 5],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct trevrpc_transport_endpoint_config_v1 {
    pub struct_size: u32,
    pub struct_version: u32,
    pub protocol: u32,
    pub flags: u32,
    pub host: *const c_char,
    pub host_len: u32,
    pub port: u16,
    pub peer_bidi_stream_count: u16,
    pub server_name: *const c_char,
    pub server_name_len: u32,
    pub reserved0: u32,
    pub alpn: *const u8,
    pub alpn_len: u32,
    pub reserved1: u32,
    pub cert_file: *const c_char,
    pub cert_file_len: u32,
    pub reserved2: u32,
    pub key_file: *const c_char,
    pub key_file_len: u32,
    pub reserved3: u32,
    pub ca_cert_file: *const c_char,
    pub ca_cert_file_len: u32,
    pub reserved4: u32,
    pub cert_data: *const u8,
    pub cert_data_len: u64,
    pub key_data: *const u8,
    pub key_data_len: u64,
    pub ca_cert_data: *const u8,
    pub ca_cert_data_len: u64,
    pub path: *const c_char,
    pub path_len: u32,
    pub webtransport_profiles: u32,
    pub origin: *const c_char,
    pub origin_len: u32,
    pub max_sessions: u32,
    pub max_frame_size: u64,
    pub max_field_section_size: u64,
    pub max_pending_send_bytes: u64,
    pub max_pending_receive_bytes: u64,
    pub max_idle_timeout_ms: u64,
    pub max_pending_send_count: u32,
    pub max_pending_receive_count: u32,
    pub keep_alive_ms: u32,
    pub stream_recv_window: u32,
    pub conn_flow_control_window: u32,
    pub unresolved_stream_count: u32,
    pub unresolved_stream_bytes: u64,
    pub unresolved_stream_timeout_ms: u64,
    pub reserved: [u64; 5],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct trevrpc_transport_wake_source_v1 {
    pub struct_size: u32,
    pub struct_version: u32,
    pub kind: u32,
    pub flags: u32,
    pub native_handle: isize,
    pub reserved: [u64; 3],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct trevrpc_transport_event_info_v1 {
    pub struct_size: u32,
    pub struct_version: u32,
    pub kind: u32,
    pub flags: u32,
    pub status: i32,
    pub subject_kind: u32,
    pub sequence: u64,
    pub subject: trevrpc_transport_handle_v1,
    pub parent: trevrpc_transport_handle_v1,
    pub operation_id: u64,
    pub application_error_code: u64,
    pub provider_error_code: u64,
    pub data: *const u8,
    pub data_len: u64,
    pub reserved: [u64; 4],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct trevrpc_transport_receive_info_v1 {
    pub struct_size: u32,
    pub struct_version: u32,
    pub flags: u32,
    pub reserved0: u32,
    pub data: *const u8,
    pub data_len: u64,
    pub reserved: [u64; 4],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct trevrpc_transport_header_field_v1 {
    pub name: *const u8,
    pub name_len: u64,
    pub value: *const u8,
    pub value_len: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct trevrpc_transport_admission_info_v1 {
    pub struct_size: u32,
    pub struct_version: u32,
    pub protocol: u32,
    pub reserved0: u32,
    pub listener: trevrpc_transport_handle_v1,
    pub headers: *const trevrpc_transport_header_field_v1,
    pub header_count: u64,
    pub method: *const u8,
    pub method_len: u64,
    pub path: *const u8,
    pub path_len: u64,
    pub authority: *const u8,
    pub authority_len: u64,
    pub origin: *const u8,
    pub origin_len: u64,
    pub reserved: [u64; 4],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct trevrpc_transport_event_protocol_info_v1 {
    pub struct_size: u32,
    pub struct_version: u32,
    pub protocol: u32,
    pub reserved0: u32,
    pub reserved: [u64; 4],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct trevrpc_transport_diagnostics_v1 {
    pub struct_size: u32,
    pub struct_version: u32,
    pub transport_abi_version: u32,
    pub state: u32,
    pub terminal_status: i32,
    pub event_capacity: u32,
    pub queue_depth: u32,
    pub ordinary_queue_depth: u32,
    pub events_enqueued: u64,
    pub events_dequeued: u64,
    pub events_rejected: u64,
    pub receive_owned_count: u64,
    pub peak_receive_owned_count: u64,
    pub receive_owned_bytes: u64,
    pub peak_receive_owned_bytes: u64,
    pub pending_send_bytes: u64,
    pub pending_send_count: u64,
    pub live_listeners: u64,
    pub live_connections: u64,
    pub live_streams: u64,
    pub active_callbacks: u64,
    pub active_api_calls: u64,
    pub wake_signals: u64,
    pub wake_write_eagain: u64,
    pub wake_failures: u64,
    pub provider_error_code: u64,
    pub mandatory_reservations: u64,
    pub reserved: [u64; 3],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct trevrpc_transport_msquic_config_v1 {
    pub struct_size: u32,
    pub struct_version: u32,
    pub flags: u32,
    pub reserved0: u32,
    pub reserved: [u64; 6],
}

#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_ABI_VERSION: u32 = 1;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_STRUCT_VERSION_1: u32 = 1;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_WAKE_COUNT: u32 = 3;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_LISTEN: u32 = 1;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_LISTENER_GET_PORT: u32 = 2;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_DIAL: u32 = 3;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_DIAL_CANCEL: u32 = 4;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_OPEN_BIDI_STREAM: u32 = 5;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_STREAM_SEND: u32 = 6;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_STREAM_RECEIVE: u32 = 7;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_FINISH_SEND: u32 = 8;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_RECEIVE: u32 = 9;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_SEND: u32 = 10;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_STREAM: u32 = 11;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_STREAM: u32 = 12;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_CONNECTION: u32 = 13;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_LISTENER: u32 = 14;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_RELEASE_HANDLE: u32 = 15;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_TRANSPORT: u32 = 16;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_DRAIN: u32 = 17;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_RELEASE_TRANSPORT: u32 = 18;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_ADMISSION_RESPOND: u32 = 19;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_OPERATION_COUNT: usize = 20;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_POP: u32 = 1;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_SEND_ADMISSION: u32 = 2;
#[cfg(feature = "testing")]
pub const TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_FINISH_SEND_ADMISSION: u32 = 3;

#[cfg(feature = "testing")]
#[repr(C)]
pub struct trevrpc_transport_testing_control {
    _private: [u8; 0],
}

#[cfg(feature = "testing")]
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct trevrpc_transport_testing_config_v1 {
    pub struct_size: u32,
    pub struct_version: u32,
    pub event_capacity: u32,
    pub receive_capacity: u32,
    pub status_capacity: u32,
    pub listener_port: u16,
    pub reserved0: u16,
    pub reserved1: u32,
    pub reserved: [u64; 4],
}

#[cfg(feature = "testing")]
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct trevrpc_transport_testing_event_v1 {
    pub struct_size: u32,
    pub struct_version: u32,
    pub kind: u32,
    pub flags: u32,
    pub status: i32,
    pub subject_kind: u32,
    pub subject: trevrpc_transport_handle_v1,
    pub parent: trevrpc_transport_handle_v1,
    pub operation_id: u64,
    pub application_error_code: u64,
    pub provider_error_code: u64,
    pub protocol: u32,
    pub reserved0: u32,
    pub data: *const u8,
    pub data_len: u64,
    pub headers: *const trevrpc_transport_header_field_v1,
    pub header_count: u64,
    pub method: *const u8,
    pub method_len: u64,
    pub path: *const u8,
    pub path_len: u64,
    pub authority: *const u8,
    pub authority_len: u64,
    pub origin: *const u8,
    pub origin_len: u64,
    pub reserved: [u64; 3],
}

#[cfg(feature = "testing")]
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct trevrpc_transport_testing_receive_v1 {
    pub struct_size: u32,
    pub struct_version: u32,
    pub flags: u32,
    pub reserved0: u32,
    pub stream: trevrpc_transport_handle_v1,
    pub data: *const u8,
    pub data_len: u64,
    pub reserved: [u64; 3],
}

#[cfg(feature = "testing")]
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct trevrpc_transport_testing_counters_v1 {
    pub struct_size: u32,
    pub struct_version: u32,
    pub events_injected: u64,
    pub events_popped: u64,
    pub events_released: u64,
    pub receives_injected: u64,
    pub receives_popped: u64,
    pub receives_released: u64,
    pub admissions_responded: u64,
    pub operation_calls: [u64; TREVRPC_TRANSPORT_TESTING_OPERATION_COUNT],
    pub last_operation_id: u64,
    pub last_send_operation_id: u64,
    pub last_dial_operation_id: u64,
    pub last_open_operation_id: u64,
    pub last_admission_status: u64,
    pub last_status: i32,
    pub last_operation: u32,
    pub reserved: [u64; 4],
}

unsafe extern "C" {
    pub fn trevrpc_transport_abi_version() -> u32;
    pub fn trevrpc_transport_abi_1_anchor();

    pub fn trevrpc_transport_config_v1_init(
        config: *mut trevrpc_transport_config_v1,
        struct_size: usize,
    ) -> c_int;
    pub fn trevrpc_transport_endpoint_config_v1_init(
        config: *mut trevrpc_transport_endpoint_config_v1,
        struct_size: usize,
    ) -> c_int;
    pub fn trevrpc_transport_wake_source_v1_init(
        wake_source: *mut trevrpc_transport_wake_source_v1,
        struct_size: usize,
    ) -> c_int;
    pub fn trevrpc_transport_event_info_v1_init(
        info: *mut trevrpc_transport_event_info_v1,
        struct_size: usize,
    ) -> c_int;
    pub fn trevrpc_transport_receive_info_v1_init(
        info: *mut trevrpc_transport_receive_info_v1,
        struct_size: usize,
    ) -> c_int;
    pub fn trevrpc_transport_admission_info_v1_init(
        info: *mut trevrpc_transport_admission_info_v1,
        struct_size: usize,
    ) -> c_int;
    pub fn trevrpc_transport_event_protocol_info_v1_init(
        info: *mut trevrpc_transport_event_protocol_info_v1,
        struct_size: usize,
    ) -> c_int;
    pub fn trevrpc_transport_diagnostics_v1_init(
        diagnostics: *mut trevrpc_transport_diagnostics_v1,
        struct_size: usize,
    ) -> c_int;

    pub fn trevrpc_transport_get_wake_sources_v1(
        transport: *mut trevrpc_transport,
        wake_sources: *mut trevrpc_transport_wake_source_v1,
        capacity: usize,
        out_count: *mut usize,
    ) -> c_int;
    pub fn trevrpc_transport_poll_timeout_ms(transport: *mut trevrpc_transport) -> c_int;
    pub fn trevrpc_transport_next_event(
        transport: *mut trevrpc_transport,
        out_event: *mut *mut trevrpc_transport_event_v1,
    ) -> c_int;
    pub fn trevrpc_transport_event_get_info_v1(
        transport: *mut trevrpc_transport,
        event: *const trevrpc_transport_event_v1,
        info: *mut trevrpc_transport_event_info_v1,
    ) -> c_int;
    pub fn trevrpc_transport_event_get_admission_info_v1(
        transport: *mut trevrpc_transport,
        event: *const trevrpc_transport_event_v1,
        info: *mut trevrpc_transport_admission_info_v1,
    ) -> c_int;
    pub fn trevrpc_transport_event_get_protocol_info_v1(
        transport: *mut trevrpc_transport,
        event: *const trevrpc_transport_event_v1,
        info: *mut trevrpc_transport_event_protocol_info_v1,
    ) -> c_int;
    pub fn trevrpc_transport_admission_respond_v1(
        transport: *mut trevrpc_transport,
        event: *const trevrpc_transport_event_v1,
        http_status: u16,
    ) -> c_int;
    pub fn trevrpc_transport_event_release(
        transport: *mut trevrpc_transport,
        event: *mut trevrpc_transport_event_v1,
    );
    pub fn trevrpc_transport_receive_get_info_v1(
        transport: *mut trevrpc_transport,
        receive: *const trevrpc_transport_receive_v1,
        info: *mut trevrpc_transport_receive_info_v1,
    ) -> c_int;
    pub fn trevrpc_transport_receive_release(
        transport: *mut trevrpc_transport,
        receive: *mut trevrpc_transport_receive_v1,
    );
    pub fn trevrpc_transport_get_diagnostics_v1(
        transport: *mut trevrpc_transport,
        diagnostics: *mut trevrpc_transport_diagnostics_v1,
    ) -> c_int;

    pub fn trevrpc_transport_listen_v1(
        transport: *mut trevrpc_transport,
        config: *const trevrpc_transport_endpoint_config_v1,
        out_listener: *mut trevrpc_transport_handle_v1,
    ) -> c_int;
    pub fn trevrpc_transport_listener_get_port_v1(
        transport: *mut trevrpc_transport,
        listener: trevrpc_transport_handle_v1,
        out_port: *mut u16,
    ) -> c_int;
    pub fn trevrpc_transport_dial_v1(
        transport: *mut trevrpc_transport,
        config: *const trevrpc_transport_endpoint_config_v1,
        operation_id: u64,
        out_connection: *mut trevrpc_transport_handle_v1,
    ) -> c_int;
    pub fn trevrpc_transport_dial_cancel(
        transport: *mut trevrpc_transport,
        connection: trevrpc_transport_handle_v1,
    ) -> c_int;
    pub fn trevrpc_transport_connection_open_bidi_stream_v1(
        transport: *mut trevrpc_transport,
        connection: trevrpc_transport_handle_v1,
        operation_id: u64,
        out_stream: *mut trevrpc_transport_handle_v1,
    ) -> c_int;
    pub fn trevrpc_transport_stream_send_v1(
        transport: *mut trevrpc_transport,
        stream: trevrpc_transport_handle_v1,
        operation_id: u64,
        data: *const u8,
        data_len: usize,
    ) -> c_int;
    pub fn trevrpc_transport_stream_receive(
        transport: *mut trevrpc_transport,
        stream: trevrpc_transport_handle_v1,
        out_receive: *mut *mut trevrpc_transport_receive_v1,
    ) -> c_int;
    pub fn trevrpc_transport_stream_finish_send(
        transport: *mut trevrpc_transport,
        stream: trevrpc_transport_handle_v1,
    ) -> c_int;
    pub fn trevrpc_transport_stream_abort_receive(
        transport: *mut trevrpc_transport,
        stream: trevrpc_transport_handle_v1,
        application_error_code: u64,
    ) -> c_int;
    pub fn trevrpc_transport_stream_abort_send(
        transport: *mut trevrpc_transport,
        stream: trevrpc_transport_handle_v1,
        application_error_code: u64,
    ) -> c_int;
    pub fn trevrpc_transport_stream_abort(
        transport: *mut trevrpc_transport,
        stream: trevrpc_transport_handle_v1,
        application_error_code: u64,
    ) -> c_int;
    pub fn trevrpc_transport_stream_close(
        transport: *mut trevrpc_transport,
        stream: trevrpc_transport_handle_v1,
    ) -> c_int;
    pub fn trevrpc_transport_connection_close(
        transport: *mut trevrpc_transport,
        connection: trevrpc_transport_handle_v1,
        application_error_code: u64,
    ) -> c_int;
    pub fn trevrpc_transport_listener_close(
        transport: *mut trevrpc_transport,
        listener: trevrpc_transport_handle_v1,
    ) -> c_int;
    pub fn trevrpc_transport_release_handle(
        transport: *mut trevrpc_transport,
        handle: trevrpc_transport_handle_v1,
        object_kind: u32,
    ) -> c_int;
    pub fn trevrpc_transport_close(transport: *mut trevrpc_transport) -> c_int;
    pub fn trevrpc_transport_drain(transport: *mut trevrpc_transport) -> c_int;
    pub fn trevrpc_transport_release(transport: *mut trevrpc_transport) -> c_int;

    pub fn trevrpc_transport_msquic_abi_version() -> u32;
    pub fn trevrpc_transport_msquic_abi_1_anchor();
    pub fn trevrpc_transport_msquic_config_v1_init(
        config: *mut trevrpc_transport_msquic_config_v1,
        struct_size: usize,
    ) -> c_int;
    pub fn trevrpc_transport_msquic_create_v1(
        transport_config: *const trevrpc_transport_config_v1,
        provider_config: *const trevrpc_transport_msquic_config_v1,
        out_transport: *mut *mut trevrpc_transport,
    ) -> c_int;

    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_abi_version() -> u32;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_abi_1_anchor();
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_config_v1_init(
        config: *mut trevrpc_transport_testing_config_v1,
        struct_size: usize,
    ) -> c_int;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_event_v1_init(
        event: *mut trevrpc_transport_testing_event_v1,
        struct_size: usize,
    ) -> c_int;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_receive_v1_init(
        receive: *mut trevrpc_transport_testing_receive_v1,
        struct_size: usize,
    ) -> c_int;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_counters_v1_init(
        counters: *mut trevrpc_transport_testing_counters_v1,
        struct_size: usize,
    ) -> c_int;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_create_v1(
        transport_config: *const trevrpc_transport_config_v1,
        provider_config: *const trevrpc_transport_testing_config_v1,
        out_transport: *mut *mut trevrpc_transport,
        out_control: *mut *mut trevrpc_transport_testing_control,
    ) -> c_int;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_push_event_v1(
        control: *mut trevrpc_transport_testing_control,
        event: *const trevrpc_transport_testing_event_v1,
    ) -> c_int;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_push_receive_v1(
        control: *mut trevrpc_transport_testing_control,
        receive: *const trevrpc_transport_testing_receive_v1,
    ) -> c_int;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_script_status_v1(
        control: *mut trevrpc_transport_testing_control,
        status: i32,
    ) -> c_int;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_clear_status_script_v1(
        control: *mut trevrpc_transport_testing_control,
    ) -> c_int;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_set_checkpoint_v1(
        control: *mut trevrpc_transport_testing_control,
        checkpoint: u32,
        enabled: u32,
    ) -> c_int;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_wait_checkpoint_v1(
        control: *mut trevrpc_transport_testing_control,
        checkpoint: u32,
    ) -> c_int;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_release_checkpoint_v1(
        control: *mut trevrpc_transport_testing_control,
        checkpoint: u32,
    ) -> c_int;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_push_stopped_v1(
        control: *mut trevrpc_transport_testing_control,
        status: i32,
    ) -> c_int;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_get_counters_v1(
        control: *mut trevrpc_transport_testing_control,
        counters: *mut trevrpc_transport_testing_counters_v1,
    ) -> c_int;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_get_wake_fd_v1(
        control: *mut trevrpc_transport_testing_control,
        index: u32,
    ) -> c_int;
    #[cfg(feature = "testing")]
    pub fn trevrpc_transport_testing_set_poll_timeout_ms_v1(
        control: *mut trevrpc_transport_testing_control,
        timeout_ms: i32,
    ) -> c_int;
}
