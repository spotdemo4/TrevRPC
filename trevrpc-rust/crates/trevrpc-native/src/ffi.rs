use std::ffi::c_char;
use std::os::fd::RawFd;
use std::ptr::{self, NonNull};
use std::time::Duration;

use trevrpc_c_sys as sys;

use crate::error::ErrorOrigin;
use crate::{CloseReason, EndpointConfig, MsQuicConfig, NativeError, Protocol, TransportConfig};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Handle(sys::trevrpc_transport_handle_v1);

impl std::hash::Hash for Handle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.owner.hash(state);
        self.0.slot.hash(state);
        self.0.generation.hash(state);
    }
}

impl Handle {
    pub(crate) const fn owner(self) -> u64 {
        self.0.owner
    }
    pub(crate) const fn slot(self) -> u32 {
        self.0.slot
    }
    pub(crate) const fn generation(self) -> u32 {
        self.0.generation
    }
    const fn raw(self) -> sys::trevrpc_transport_handle_v1 {
        self.0
    }
}

impl From<sys::trevrpc_transport_handle_v1> for Handle {
    fn from(value: sys::trevrpc_transport_handle_v1) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ObjectKind {
    Listener,
    Connection,
    Stream,
}
impl ObjectKind {
    pub(crate) const fn from_raw(value: u32) -> Option<Self> {
        match value {
            sys::TREVRPC_TRANSPORT_OBJECT_LISTENER => Some(Self::Listener),
            sys::TREVRPC_TRANSPORT_OBJECT_CONNECTION => Some(Self::Connection),
            sys::TREVRPC_TRANSPORT_OBJECT_STREAM => Some(Self::Stream),
            _ => None,
        }
    }

    const fn raw(self) -> u32 {
        match self {
            Self::Listener => sys::TREVRPC_TRANSPORT_OBJECT_LISTENER,
            Self::Connection => sys::TREVRPC_TRANSPORT_OBJECT_CONNECTION,
            Self::Stream => sys::TREVRPC_TRANSPORT_OBJECT_STREAM,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EventKind {
    Diagnostic,
    Stopped,
    ListenerStopped,
    ConnectionReady,
    ConnectionFailed,
    ConnectionClosed,
    StreamReady,
    StreamFailed,
    StreamReadable,
    ReceiveFin,
    SendComplete,
    StreamClosed,
    SendStopped,
    Http3Admission,
    WebTransportAdmission,
    Unknown,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct EventInfo {
    pub kind: EventKind,
    pub status: i32,
    pub subject_kind: u32,
    pub sequence: u64,
    pub subject: Handle,
    pub parent: Handle,
    pub operation_id: u64,
    pub application_error_code: u64,
    pub provider_error_code: u64,
    pub protocol: Option<Protocol>,
    flags: u32,
}

impl EventInfo {
    pub(crate) fn close_reason(self) -> CloseReason {
        let peer = self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_PEER != 0;
        let local = self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_LOCAL != 0;
        let peer_reset = self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_PEER_RESET != 0;
        if self.status == 0 {
            if peer {
                CloseReason::Peer {
                    code: self.application_error_code,
                    peer_reset,
                }
            } else if local {
                CloseReason::Local {
                    code: self.application_error_code,
                }
            } else {
                CloseReason::Clean
            }
        } else if self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_TRANSPORT_ERROR != 0 {
            CloseReason::Transport {
                status: self.status,
                application_code: self.application_error_code,
                provider_code: self.provider_error_code,
                peer,
                local,
                peer_reset,
            }
        } else {
            CloseReason::Failed {
                status: self.status,
                application_code: self.application_error_code,
                provider_code: self.provider_error_code,
                peer,
                local,
                peer_reset,
            }
        }
    }

    pub(crate) fn error(self) -> NativeError {
        let origin = if self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_TRANSPORT_ERROR != 0 {
            ErrorOrigin::Transport
        } else if self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_PEER != 0 {
            ErrorOrigin::Peer
        } else if self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_LOCAL != 0 {
            ErrorOrigin::Local
        } else {
            ErrorOrigin::Unknown
        };
        NativeError::event(
            self.status,
            origin,
            self.application_error_code,
            self.provider_error_code,
            self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_PEER_RESET != 0,
        )
    }

    pub(crate) fn send_stop_reason(self) -> CloseReason {
        if self.peer() {
            CloseReason::Peer {
                code: self.application_error_code,
                peer_reset: self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_PEER_RESET != 0,
            }
        } else {
            self.close_reason()
        }
    }

    pub(crate) fn clean_fin(self) -> bool {
        self.status == 0 && self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN != 0
    }

    pub(crate) fn fatal(self) -> bool {
        self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_FATAL != 0
    }

    pub(crate) fn peer(self) -> bool {
        self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_PEER != 0
    }

    pub(crate) fn server(self) -> bool {
        self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_SERVER != 0
    }

    pub(crate) fn has_invalid_transport_error(self) -> bool {
        self.status == 0 && self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_TRANSPORT_ERROR != 0
    }

    pub(crate) fn has_conflicting_origin(self) -> bool {
        self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_LOCAL != 0
            && self.flags & sys::TREVRPC_TRANSPORT_EVENT_FLAG_PEER != 0
    }

    pub(crate) fn subject_kind_is_valid(self) -> bool {
        let expected = match self.kind {
            EventKind::Stopped | EventKind::Http3Admission | EventKind::WebTransportAdmission => {
                Some(sys::TREVRPC_TRANSPORT_OBJECT_NONE)
            }
            EventKind::ListenerStopped => Some(sys::TREVRPC_TRANSPORT_OBJECT_LISTENER),
            EventKind::ConnectionReady
            | EventKind::ConnectionFailed
            | EventKind::ConnectionClosed => Some(sys::TREVRPC_TRANSPORT_OBJECT_CONNECTION),
            EventKind::StreamReady
            | EventKind::StreamFailed
            | EventKind::StreamReadable
            | EventKind::ReceiveFin
            | EventKind::SendComplete
            | EventKind::StreamClosed
            | EventKind::SendStopped => Some(sys::TREVRPC_TRANSPORT_OBJECT_STREAM),
            EventKind::Diagnostic | EventKind::Unknown => None,
        };
        expected.is_none_or(|expected| self.subject_kind == expected)
    }
}

pub(crate) struct AdmissionOwned {
    pub protocol: Protocol,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub method: Vec<u8>,
    pub path: Vec<u8>,
    pub authority: Vec<u8>,
    pub origin: Vec<u8>,
    pub listener: Handle,
}

pub(crate) struct RawEvent(NonNull<sys::trevrpc_transport_event_v1>);
// SAFETY: retained events are accessed only by the single owner task. Send only
// permits that task to migrate between Tokio worker threads.
unsafe impl Send for RawEvent {}

pub(crate) struct RawTransport {
    pointer: Option<NonNull<sys::trevrpc_transport>>,
    #[cfg(feature = "testing")]
    testing_control: Option<TestingControl>,
}
// SAFETY: the raw transport is created, used, and released exclusively by one
// owner task and is never dereferenced concurrently.
unsafe impl Send for RawTransport {}

#[cfg(feature = "testing")]
struct TestingControlState {
    pointer: std::sync::RwLock<Option<NonNull<sys::trevrpc_transport_testing_control>>>,
}

#[cfg(feature = "testing")]
unsafe impl Send for TestingControlState {}
#[cfg(feature = "testing")]
unsafe impl Sync for TestingControlState {}

#[cfg(feature = "testing")]
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct TestingControl {
    state: std::sync::Arc<TestingControlState>,
}

#[cfg(feature = "testing")]
pub(crate) struct TestingEvent {
    pub(crate) kind: u32,
    pub(crate) flags: u32,
    pub(crate) status: i32,
    pub(crate) subject_kind: u32,
    pub(crate) subject: Handle,
    pub(crate) parent: Handle,
    pub(crate) operation_id: u64,
}

#[cfg(feature = "testing")]
pub(crate) struct TestingAdmission<'a> {
    pub(crate) kind: u32,
    pub(crate) protocol: Protocol,
    pub(crate) listener: Handle,
    pub(crate) headers: &'a [(&'a [u8], &'a [u8])],
    pub(crate) method: &'a [u8],
    pub(crate) path: &'a [u8],
    pub(crate) authority: &'a [u8],
    pub(crate) origin: &'a [u8],
}

#[cfg(feature = "testing")]
#[allow(dead_code)]
impl TestingControl {
    fn with_pointer<T>(
        &self,
        operation: impl FnOnce(*mut sys::trevrpc_transport_testing_control) -> crate::Result<T>,
    ) -> crate::Result<T> {
        let pointer = self
            .state
            .pointer
            .read()
            .map_err(|_| NativeError::message("testing control lock poisoned"))?;
        let pointer = pointer
            .as_ref()
            .ok_or_else(|| NativeError::message("testing transport has been released"))?;
        operation(pointer.as_ptr())
    }

    fn release_transport(&self, transport: *mut sys::trevrpc_transport) -> crate::Result<()> {
        let mut pointer = self
            .state
            .pointer
            .write()
            .map_err(|_| NativeError::message("testing control lock poisoned"))?;
        let status = unsafe { sys::trevrpc_transport_release(transport) };
        if status == 0 {
            *pointer = None;
            Ok(())
        } else {
            Err(NativeError::status(status))
        }
    }

    pub(crate) fn script_status(&self, status: i32) -> crate::Result<()> {
        self.with_pointer(|pointer| {
            result(unsafe { sys::trevrpc_transport_testing_script_status_v1(pointer, status) })
        })
    }

    pub(crate) fn clear_status_script(&self) -> crate::Result<()> {
        self.with_pointer(|pointer| {
            result(unsafe { sys::trevrpc_transport_testing_clear_status_script_v1(pointer) })
        })
    }

    pub(crate) fn set_checkpoint(&self, checkpoint: u32, enabled: bool) -> crate::Result<()> {
        self.with_pointer(|pointer| {
            result(unsafe {
                sys::trevrpc_transport_testing_set_checkpoint_v1(
                    pointer,
                    checkpoint,
                    u32::from(enabled),
                )
            })
        })
    }

    pub(crate) fn wait_checkpoint(&self, checkpoint: u32) -> crate::Result<()> {
        self.with_pointer(|pointer| {
            result(unsafe {
                sys::trevrpc_transport_testing_wait_checkpoint_v1(pointer, checkpoint)
            })
        })
    }

    pub(crate) fn release_checkpoint(&self, checkpoint: u32) -> crate::Result<()> {
        self.with_pointer(|pointer| {
            result(unsafe {
                sys::trevrpc_transport_testing_release_checkpoint_v1(pointer, checkpoint)
            })
        })
    }

    pub(crate) fn set_poll_timeout_ms(&self, timeout_ms: i32) -> crate::Result<()> {
        self.with_pointer(|pointer| {
            result(unsafe {
                sys::trevrpc_transport_testing_set_poll_timeout_ms_v1(pointer, timeout_ms)
            })
        })
    }

    pub(crate) fn push_stopped(&self, status: i32) -> crate::Result<()> {
        self.with_pointer(|pointer| {
            result(unsafe { sys::trevrpc_transport_testing_push_stopped_v1(pointer, status) })
        })
    }

    pub(crate) fn push_event(
        &self,
        event: &sys::trevrpc_transport_testing_event_v1,
    ) -> crate::Result<()> {
        self.with_pointer(|pointer| {
            result(unsafe {
                sys::trevrpc_transport_testing_push_event_v1(pointer, &raw const *event)
            })
        })
    }

    pub(crate) fn push_receive(
        &self,
        receive: &sys::trevrpc_transport_testing_receive_v1,
    ) -> crate::Result<()> {
        self.with_pointer(|pointer| {
            result(unsafe {
                sys::trevrpc_transport_testing_push_receive_v1(pointer, &raw const *receive)
            })
        })
    }

    pub(crate) fn push_simple_event(&self, specification: &TestingEvent) -> crate::Result<()> {
        let mut event = std::mem::MaybeUninit::<sys::trevrpc_transport_testing_event_v1>::zeroed();
        result(unsafe {
            sys::trevrpc_transport_testing_event_v1_init(
                event.as_mut_ptr(),
                std::mem::size_of::<sys::trevrpc_transport_testing_event_v1>(),
            )
        })?;
        let mut event = unsafe { event.assume_init() };
        event.kind = specification.kind;
        event.flags = specification.flags;
        event.status = specification.status;
        event.subject_kind = specification.subject_kind;
        event.subject = specification.subject.raw();
        event.parent = specification.parent.raw();
        event.operation_id = specification.operation_id;
        event.protocol = sys::TREVRPC_TRANSPORT_PROTOCOL_NATIVE;
        self.push_event(&event)
    }

    pub(crate) fn push_admission(&self, specification: &TestingAdmission<'_>) -> crate::Result<()> {
        let mut event = std::mem::MaybeUninit::<sys::trevrpc_transport_testing_event_v1>::zeroed();
        result(unsafe {
            sys::trevrpc_transport_testing_event_v1_init(
                event.as_mut_ptr(),
                std::mem::size_of::<sys::trevrpc_transport_testing_event_v1>(),
            )
        })?;
        let headers = specification
            .headers
            .iter()
            .map(|(name, value)| {
                Ok(sys::trevrpc_transport_header_field_v1 {
                    name: byte_pointer(name),
                    name_len: u64_len(name, "testing admission header name")?,
                    value: byte_pointer(value),
                    value_len: u64_len(value, "testing admission header value")?,
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let mut event = unsafe { event.assume_init() };
        event.kind = specification.kind;
        event.flags =
            sys::TREVRPC_TRANSPORT_EVENT_FLAG_SERVER | sys::TREVRPC_TRANSPORT_EVENT_FLAG_PEER;
        event.subject_kind = sys::TREVRPC_TRANSPORT_OBJECT_NONE;
        event.parent = specification.listener.raw();
        event.protocol = specification.protocol.raw();
        event.headers = headers.as_ptr();
        event.header_count = u64::try_from(headers.len()).map_err(|_| {
            NativeError::message("testing admission header count exceeds UINT64_MAX")
        })?;
        event.method = byte_pointer(specification.method);
        event.method_len = u64_len(specification.method, "testing admission method")?;
        event.path = byte_pointer(specification.path);
        event.path_len = u64_len(specification.path, "testing admission path")?;
        event.authority = byte_pointer(specification.authority);
        event.authority_len = u64_len(specification.authority, "testing admission authority")?;
        event.origin = byte_pointer(specification.origin);
        event.origin_len = u64_len(specification.origin, "testing admission origin")?;
        self.push_event(&event)
    }

    pub(crate) fn push_receive_data(&self, stream: Handle, data: &[u8]) -> crate::Result<()> {
        let mut receive =
            std::mem::MaybeUninit::<sys::trevrpc_transport_testing_receive_v1>::zeroed();
        result(unsafe {
            sys::trevrpc_transport_testing_receive_v1_init(
                receive.as_mut_ptr(),
                std::mem::size_of::<sys::trevrpc_transport_testing_receive_v1>(),
            )
        })?;
        let mut receive = unsafe { receive.assume_init() };
        receive.stream = stream.raw();
        receive.data = data.as_ptr();
        receive.data_len = u64_len(data, "testing receive body")?;
        self.push_receive(&receive)
    }

    pub(crate) fn counters(&self) -> crate::Result<sys::trevrpc_transport_testing_counters_v1> {
        let mut counters =
            std::mem::MaybeUninit::<sys::trevrpc_transport_testing_counters_v1>::zeroed();
        result(unsafe {
            sys::trevrpc_transport_testing_counters_v1_init(
                counters.as_mut_ptr(),
                std::mem::size_of::<sys::trevrpc_transport_testing_counters_v1>(),
            )
        })?;
        let mut counters = unsafe { counters.assume_init() };
        self.with_pointer(|pointer| {
            result(unsafe {
                sys::trevrpc_transport_testing_get_counters_v1(pointer, &raw mut counters)
            })
        })?;
        Ok(counters)
    }

    pub(crate) fn wake_fd(&self, index: u32) -> crate::Result<RawFd> {
        self.with_pointer(|pointer| {
            let fd = unsafe { sys::trevrpc_transport_testing_get_wake_fd_v1(pointer, index) };
            if fd < 0 {
                Err(NativeError::status(fd))
            } else {
                Ok(fd)
            }
        })
    }
}

struct EndpointBuffers {
    host: Vec<u8>,
    server_name: Vec<u8>,
    alpn: Vec<u8>,
    cert_file: Vec<u8>,
    key_file: Vec<u8>,
    ca_cert_file: Vec<u8>,
    cert_data: Vec<u8>,
    key_data: Vec<u8>,
    ca_cert_data: Vec<u8>,
    path: Vec<u8>,
    origin: Vec<u8>,
}

pub(crate) struct EndpointFfi {
    raw: sys::trevrpc_transport_endpoint_config_v1,
    _buffers: EndpointBuffers,
}

fn result(status: i32) -> crate::Result<()> {
    if status == 0 {
        Ok(())
    } else {
        Err(NativeError::status(status))
    }
}
fn u32_len(value: &[u8], field: &str) -> crate::Result<u32> {
    u32::try_from(value.len())
        .map_err(|_| NativeError::message(format!("{field} exceeds UINT32_MAX")))
}
fn u64_len(value: &[u8], field: &str) -> crate::Result<u64> {
    u64::try_from(value.len())
        .map_err(|_| NativeError::message(format!("{field} exceeds UINT64_MAX")))
}
fn byte_pointer(value: &[u8]) -> *const u8 {
    if value.is_empty() {
        ptr::null()
    } else {
        value.as_ptr()
    }
}
fn char_pointer(value: &[u8]) -> *const c_char {
    byte_pointer(value).cast()
}
fn optional_bytes(value: Option<&String>) -> Vec<u8> {
    value.map_or_else(Vec::new, |value| value.as_bytes().to_vec())
}

fn copy_bytes(pointer: *const u8, length: u64, field: &str) -> crate::Result<Vec<u8>> {
    let length = usize::try_from(length)
        .map_err(|_| NativeError::message(format!("{field} exceeds SIZE_MAX")))?;
    if length == 0 {
        Ok(Vec::new())
    } else if pointer.is_null() {
        Err(NativeError::message(format!(
            "{field} has nonzero length with null pointer"
        )))
    } else {
        Ok(unsafe { std::slice::from_raw_parts(pointer, length) }.to_vec())
    }
}

pub(crate) fn endpoint(config: &EndpointConfig) -> crate::Result<EndpointFfi> {
    let buffers = EndpointBuffers {
        host: config.host.as_bytes().to_vec(),
        server_name: optional_bytes(config.server_name.as_ref()),
        alpn: config.alpn.clone(),
        cert_file: optional_bytes(config.cert_file.as_ref()),
        key_file: optional_bytes(config.key_file.as_ref()),
        ca_cert_file: optional_bytes(config.ca_cert_file.as_ref()),
        cert_data: config.cert_data.clone(),
        key_data: config.key_data.clone(),
        ca_cert_data: config.ca_cert_data.clone(),
        path: optional_bytes(config.path.as_ref()),
        origin: optional_bytes(config.origin.as_ref()),
    };
    let mut raw = std::mem::MaybeUninit::<sys::trevrpc_transport_endpoint_config_v1>::zeroed();
    result(unsafe {
        sys::trevrpc_transport_endpoint_config_v1_init(
            raw.as_mut_ptr(),
            std::mem::size_of::<sys::trevrpc_transport_endpoint_config_v1>(),
        )
    })?;
    let mut raw = unsafe { raw.assume_init() };
    raw.protocol = config.protocol.raw();
    raw.flags = (if config.skip_certificate_validation {
        sys::TREVRPC_TRANSPORT_ENDPOINT_SKIP_CERTIFICATE_VALIDATION
    } else {
        0
    }) | (if config.defer_admission {
        sys::TREVRPC_TRANSPORT_ENDPOINT_DEFER_ADMISSION
    } else {
        0
    });
    raw.host = char_pointer(&buffers.host);
    raw.host_len = u32_len(&buffers.host, "host")?;
    raw.port = config.port;
    raw.peer_bidi_stream_count = config.peer_bidi_stream_count;
    raw.server_name = char_pointer(&buffers.server_name);
    raw.server_name_len = u32_len(&buffers.server_name, "server name")?;
    raw.alpn = byte_pointer(&buffers.alpn);
    raw.alpn_len = u32_len(&buffers.alpn, "ALPN")?;
    raw.cert_file = char_pointer(&buffers.cert_file);
    raw.cert_file_len = u32_len(&buffers.cert_file, "certificate path")?;
    raw.key_file = char_pointer(&buffers.key_file);
    raw.key_file_len = u32_len(&buffers.key_file, "key path")?;
    raw.ca_cert_file = char_pointer(&buffers.ca_cert_file);
    raw.ca_cert_file_len = u32_len(&buffers.ca_cert_file, "CA certificate path")?;
    raw.cert_data = byte_pointer(&buffers.cert_data);
    raw.cert_data_len = u64_len(&buffers.cert_data, "certificate data")?;
    raw.key_data = byte_pointer(&buffers.key_data);
    raw.key_data_len = u64_len(&buffers.key_data, "key data")?;
    raw.ca_cert_data = byte_pointer(&buffers.ca_cert_data);
    raw.ca_cert_data_len = u64_len(&buffers.ca_cert_data, "CA certificate data")?;
    raw.path = char_pointer(&buffers.path);
    raw.path_len = u32_len(&buffers.path, "path")?;
    raw.webtransport_profiles = config.webtransport_profiles;
    raw.origin = char_pointer(&buffers.origin);
    raw.origin_len = u32_len(&buffers.origin, "origin")?;
    raw.max_sessions = config.max_sessions;
    raw.max_frame_size = config.max_frame_size;
    raw.max_field_section_size = config.max_field_section_size;
    raw.max_pending_send_bytes = config.max_pending_send_bytes;
    raw.max_pending_receive_bytes = config.max_pending_receive_bytes;
    raw.max_idle_timeout_ms = config.max_idle_timeout_ms;
    raw.max_pending_send_count = config.max_pending_send_count;
    raw.max_pending_receive_count = config.max_pending_receive_count;
    raw.keep_alive_ms = config.keep_alive_ms;
    raw.stream_recv_window = config.stream_recv_window;
    raw.conn_flow_control_window = config.conn_flow_control_window;
    raw.unresolved_stream_count = config.unresolved_stream_count;
    raw.unresolved_stream_bytes = config.unresolved_stream_bytes;
    raw.unresolved_stream_timeout_ms = config.unresolved_stream_timeout_ms;
    Ok(EndpointFfi {
        raw,
        _buffers: buffers,
    })
}

impl RawTransport {
    pub(crate) fn create(
        config: &TransportConfig,
        provider_config: &MsQuicConfig,
    ) -> crate::Result<Self> {
        config.validate()?;
        let mut transport = std::mem::MaybeUninit::<sys::trevrpc_transport_config_v1>::zeroed();
        result(unsafe {
            sys::trevrpc_transport_config_v1_init(
                transport.as_mut_ptr(),
                std::mem::size_of::<sys::trevrpc_transport_config_v1>(),
            )
        })?;
        let mut transport = unsafe { transport.assume_init() };
        transport.event_capacity = config.event_capacity;
        transport.listener_capacity = config.listener_capacity;
        transport.connection_capacity = config.connection_capacity;
        transport.stream_capacity = config.stream_capacity;
        transport.max_receive_owned_count = config.max_receive_owned_count;
        transport.max_receive_owned_bytes = config.max_receive_owned_bytes;
        let mut provider =
            std::mem::MaybeUninit::<sys::trevrpc_transport_msquic_config_v1>::zeroed();
        result(unsafe {
            sys::trevrpc_transport_msquic_config_v1_init(
                provider.as_mut_ptr(),
                std::mem::size_of::<sys::trevrpc_transport_msquic_config_v1>(),
            )
        })?;
        let mut provider = unsafe { provider.assume_init() };
        provider.flags = provider_config.flags;
        let mut pointer = ptr::null_mut();
        result(unsafe {
            sys::trevrpc_transport_msquic_create_v1(
                &raw const transport,
                &raw const provider,
                &raw mut pointer,
            )
        })?;
        Ok(Self {
            pointer: Some(NonNull::new(pointer).ok_or_else(|| {
                NativeError::message("native transport factory returned a null pointer")
            })?),
            #[cfg(feature = "testing")]
            testing_control: None,
        })
    }

    #[cfg(feature = "testing")]
    #[allow(dead_code)]
    pub(crate) fn create_testing(
        config: &TransportConfig,
    ) -> crate::Result<(Self, TestingControl)> {
        config.validate()?;
        let mut transport = std::mem::MaybeUninit::<sys::trevrpc_transport_config_v1>::zeroed();
        result(unsafe {
            sys::trevrpc_transport_config_v1_init(
                transport.as_mut_ptr(),
                std::mem::size_of::<sys::trevrpc_transport_config_v1>(),
            )
        })?;
        let mut transport = unsafe { transport.assume_init() };
        transport.event_capacity = config.event_capacity;
        transport.listener_capacity = config.listener_capacity;
        transport.connection_capacity = config.connection_capacity;
        transport.stream_capacity = config.stream_capacity;
        transport.max_receive_owned_count = config.max_receive_owned_count;
        transport.max_receive_owned_bytes = config.max_receive_owned_bytes;

        let mut provider =
            std::mem::MaybeUninit::<sys::trevrpc_transport_testing_config_v1>::zeroed();
        result(unsafe {
            sys::trevrpc_transport_testing_config_v1_init(
                provider.as_mut_ptr(),
                std::mem::size_of::<sys::trevrpc_transport_testing_config_v1>(),
            )
        })?;
        let provider = unsafe { provider.assume_init() };
        let mut pointer = ptr::null_mut();
        let mut control = ptr::null_mut();
        result(unsafe {
            sys::trevrpc_transport_testing_create_v1(
                &raw const transport,
                &raw const provider,
                &raw mut pointer,
                &raw mut control,
            )
        })?;
        let pointer = NonNull::new(pointer).ok_or_else(|| {
            NativeError::message("testing transport factory returned a null pointer")
        })?;
        let control = NonNull::new(control).ok_or_else(|| {
            NativeError::message("testing transport factory returned a null control")
        })?;
        let control = TestingControl {
            state: std::sync::Arc::new(TestingControlState {
                pointer: std::sync::RwLock::new(Some(control)),
            }),
        };
        Ok((
            Self {
                pointer: Some(pointer),
                testing_control: Some(control.clone()),
            },
            control,
        ))
    }

    fn pointer(&self) -> *mut sys::trevrpc_transport {
        self.pointer
            .expect("native transport used after successful release")
            .as_ptr()
    }
    pub(crate) fn wake_fds(&self) -> crate::Result<Vec<RawFd>> {
        let capacity = usize::try_from(sys::TREVRPC_TRANSPORT_MAX_WAKE_SOURCES)
            .expect("wake limit fits usize");
        let mut sources = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            let mut source =
                std::mem::MaybeUninit::<sys::trevrpc_transport_wake_source_v1>::zeroed();
            result(unsafe {
                sys::trevrpc_transport_wake_source_v1_init(
                    source.as_mut_ptr(),
                    std::mem::size_of::<sys::trevrpc_transport_wake_source_v1>(),
                )
            })?;
            sources.push(unsafe { source.assume_init() });
        }
        let mut count = 0;
        result(unsafe {
            sys::trevrpc_transport_get_wake_sources_v1(
                self.pointer(),
                sources.as_mut_ptr(),
                sources.len(),
                &raw mut count,
            )
        })?;
        if count > sources.len() {
            return Err(NativeError::message(
                "native transport returned more wake sources than the supplied capacity",
            ));
        }
        sources.truncate(count);
        if sources.is_empty() {
            return Err(NativeError::message(
                "native transport did not provide any wake sources",
            ));
        }
        sources
            .into_iter()
            .map(|source| {
                if source.kind != sys::TREVRPC_TRANSPORT_WAKE_SOURCE_POSIX_FD {
                    return Err(NativeError::message(
                        "native transport returned an unsupported wake source kind",
                    ));
                }
                let required = sys::TREVRPC_TRANSPORT_WAKE_FLAG_BORROWED
                    | sys::TREVRPC_TRANSPORT_WAKE_FLAG_LEVEL_TRIGGERED;
                if source.flags & required != required {
                    return Err(NativeError::message(
                        "native transport wake source is not borrowed and level-triggered",
                    ));
                }
                i32::try_from(source.native_handle)
                    .map_err(|_| NativeError::message("native wake descriptor does not fit RawFd"))
            })
            .collect()
    }
    pub(crate) fn poll_timeout(&self) -> Option<Duration> {
        let value = unsafe { sys::trevrpc_transport_poll_timeout_ms(self.pointer()) };
        u64::try_from(value).ok().map(Duration::from_millis)
    }
    pub(crate) fn listen(&self, endpoint: &EndpointFfi) -> crate::Result<Handle> {
        let mut handle = sys::trevrpc_transport_handle_v1::default();
        result(unsafe {
            sys::trevrpc_transport_listen_v1(
                self.pointer(),
                &raw const endpoint.raw,
                &raw mut handle,
            )
        })?;
        Ok(handle.into())
    }
    pub(crate) fn listener_port(&self, handle: Handle) -> crate::Result<u16> {
        let mut port = 0;
        result(unsafe {
            sys::trevrpc_transport_listener_get_port_v1(self.pointer(), handle.raw(), &raw mut port)
        })?;
        Ok(port)
    }
    pub(crate) fn dial(&self, endpoint: &EndpointFfi, operation_id: u64) -> crate::Result<Handle> {
        let mut handle = sys::trevrpc_transport_handle_v1::default();
        result(unsafe {
            sys::trevrpc_transport_dial_v1(
                self.pointer(),
                &raw const endpoint.raw,
                operation_id,
                &raw mut handle,
            )
        })?;
        Ok(handle.into())
    }
    pub(crate) fn cancel_dial(&self, connection: Handle) -> crate::Result<()> {
        result(unsafe { sys::trevrpc_transport_dial_cancel(self.pointer(), connection.raw()) })
    }
    pub(crate) fn open_bi(&self, connection: Handle, operation_id: u64) -> crate::Result<Handle> {
        let mut handle = sys::trevrpc_transport_handle_v1::default();
        result(unsafe {
            sys::trevrpc_transport_connection_open_bidi_stream_v1(
                self.pointer(),
                connection.raw(),
                operation_id,
                &raw mut handle,
            )
        })?;
        Ok(handle.into())
    }
    pub(crate) fn send(&self, stream: Handle, operation_id: u64, data: &[u8]) -> crate::Result<()> {
        result(unsafe {
            sys::trevrpc_transport_stream_send_v1(
                self.pointer(),
                stream.raw(),
                operation_id,
                byte_pointer(data),
                data.len(),
            )
        })
    }
    pub(crate) fn receive(&self, stream: Handle) -> crate::Result<Vec<u8>> {
        let mut receive = ptr::null_mut();
        result(unsafe {
            sys::trevrpc_transport_stream_receive(self.pointer(), stream.raw(), &raw mut receive)
        })?;
        let receive = NonNull::new(receive)
            .ok_or_else(|| NativeError::message("native receive succeeded with a null object"))?;
        let mut info = std::mem::MaybeUninit::<sys::trevrpc_transport_receive_info_v1>::zeroed();
        let initialized = result(unsafe {
            sys::trevrpc_transport_receive_info_v1_init(
                info.as_mut_ptr(),
                std::mem::size_of::<sys::trevrpc_transport_receive_info_v1>(),
            )
        });
        let copied = initialized.and_then(|()| {
            let mut info = unsafe { info.assume_init() };
            result(unsafe {
                sys::trevrpc_transport_receive_get_info_v1(
                    self.pointer(),
                    receive.as_ptr(),
                    &raw mut info,
                )
            })?;
            let length = usize::try_from(info.data_len)
                .map_err(|_| NativeError::message("receive length exceeds SIZE_MAX"))?;
            if length == 0 {
                Ok(Vec::new())
            } else if info.data.is_null() {
                Err(NativeError::message(
                    "receive has nonzero length with null data",
                ))
            } else {
                Ok(unsafe { std::slice::from_raw_parts(info.data, length) }.to_vec())
            }
        });
        unsafe { sys::trevrpc_transport_receive_release(self.pointer(), receive.as_ptr()) };
        copied
    }
    pub(crate) fn finish_send(&self, stream: Handle) -> crate::Result<()> {
        result(unsafe { sys::trevrpc_transport_stream_finish_send(self.pointer(), stream.raw()) })
    }
    pub(crate) fn abort_receive(&self, stream: Handle, code: u64) -> crate::Result<()> {
        result(unsafe {
            sys::trevrpc_transport_stream_abort_receive(self.pointer(), stream.raw(), code)
        })
    }
    pub(crate) fn abort_send(&self, stream: Handle, code: u64) -> crate::Result<()> {
        result(unsafe {
            sys::trevrpc_transport_stream_abort_send(self.pointer(), stream.raw(), code)
        })
    }
    pub(crate) fn abort_stream(&self, stream: Handle, code: u64) -> crate::Result<()> {
        result(unsafe { sys::trevrpc_transport_stream_abort(self.pointer(), stream.raw(), code) })
    }
    pub(crate) fn close_stream(&self, stream: Handle) -> crate::Result<()> {
        result(unsafe { sys::trevrpc_transport_stream_close(self.pointer(), stream.raw()) })
    }
    pub(crate) fn close_connection(&self, handle: Handle, code: u64) -> crate::Result<()> {
        result(unsafe {
            sys::trevrpc_transport_connection_close(self.pointer(), handle.raw(), code)
        })
    }
    pub(crate) fn close_listener(&self, handle: Handle) -> crate::Result<()> {
        result(unsafe { sys::trevrpc_transport_listener_close(self.pointer(), handle.raw()) })
    }
    pub(crate) fn release_handle(&self, handle: Handle, kind: ObjectKind) -> crate::Result<()> {
        result(unsafe {
            sys::trevrpc_transport_release_handle(self.pointer(), handle.raw(), kind.raw())
        })
    }
    pub(crate) fn close(&self) -> crate::Result<()> {
        result(unsafe { sys::trevrpc_transport_close(self.pointer()) })
    }
    pub(crate) fn drain(&self) -> crate::Result<()> {
        result(unsafe { sys::trevrpc_transport_drain(self.pointer()) })
    }
    pub(crate) fn release(&mut self) -> crate::Result<()> {
        #[cfg(feature = "testing")]
        let released = if let Some(control) = &self.testing_control {
            control.release_transport(self.pointer())
        } else {
            result(unsafe { sys::trevrpc_transport_release(self.pointer()) })
        };
        #[cfg(not(feature = "testing"))]
        let released = result(unsafe { sys::trevrpc_transport_release(self.pointer()) });
        released?;
        self.pointer = None;
        #[cfg(feature = "testing")]
        {
            self.testing_control = None;
        }
        Ok(())
    }
    pub(crate) const fn released(&self) -> bool {
        self.pointer.is_none()
    }
    pub(crate) fn take(&mut self) -> Option<Self> {
        self.pointer.take().map(|pointer| Self {
            pointer: Some(pointer),
            #[cfg(feature = "testing")]
            testing_control: self.testing_control.take(),
        })
    }
    pub(crate) fn next_event(&self) -> crate::Result<Option<RawEvent>> {
        let mut event = ptr::null_mut();
        let status = unsafe { sys::trevrpc_transport_next_event(self.pointer(), &raw mut event) };
        if status == 0 {
            return NonNull::new(event)
                .map(RawEvent)
                .map(Some)
                .ok_or_else(|| NativeError::message("event dequeue succeeded with null event"));
        }
        let error = NativeError::status(status);
        if error.is_would_block() {
            Ok(None)
        } else {
            Err(error)
        }
    }
    pub(crate) fn event_info(&self, event: &RawEvent) -> crate::Result<EventInfo> {
        let mut info = std::mem::MaybeUninit::<sys::trevrpc_transport_event_info_v1>::zeroed();
        result(unsafe {
            sys::trevrpc_transport_event_info_v1_init(
                info.as_mut_ptr(),
                std::mem::size_of::<sys::trevrpc_transport_event_info_v1>(),
            )
        })?;
        let mut info = unsafe { info.assume_init() };
        result(unsafe {
            sys::trevrpc_transport_event_get_info_v1(
                self.pointer(),
                event.0.as_ptr(),
                &raw mut info,
            )
        })?;
        let kind = match info.kind {
            sys::TREVRPC_TRANSPORT_EVENT_DIAGNOSTIC => EventKind::Diagnostic,
            sys::TREVRPC_TRANSPORT_EVENT_STOPPED => EventKind::Stopped,
            sys::TREVRPC_TRANSPORT_EVENT_LISTENER_STOPPED => EventKind::ListenerStopped,
            sys::TREVRPC_TRANSPORT_EVENT_CONNECTION_READY => EventKind::ConnectionReady,
            sys::TREVRPC_TRANSPORT_EVENT_CONNECTION_FAILED => EventKind::ConnectionFailed,
            sys::TREVRPC_TRANSPORT_EVENT_CONNECTION_CLOSED => EventKind::ConnectionClosed,
            sys::TREVRPC_TRANSPORT_EVENT_STREAM_READY => EventKind::StreamReady,
            sys::TREVRPC_TRANSPORT_EVENT_STREAM_FAILED => EventKind::StreamFailed,
            sys::TREVRPC_TRANSPORT_EVENT_STREAM_READABLE => EventKind::StreamReadable,
            sys::TREVRPC_TRANSPORT_EVENT_RECEIVE_FIN => EventKind::ReceiveFin,
            sys::TREVRPC_TRANSPORT_EVENT_SEND_COMPLETE => EventKind::SendComplete,
            sys::TREVRPC_TRANSPORT_EVENT_STREAM_CLOSED => EventKind::StreamClosed,
            sys::TREVRPC_TRANSPORT_EVENT_SEND_STOPPED => EventKind::SendStopped,
            sys::TREVRPC_TRANSPORT_EVENT_HTTP3_ADMISSION => EventKind::Http3Admission,
            sys::TREVRPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION => EventKind::WebTransportAdmission,
            _ => EventKind::Unknown,
        };
        let protocol = if info.subject_kind == sys::TREVRPC_TRANSPORT_OBJECT_NONE {
            None
        } else {
            let mut protocol =
                std::mem::MaybeUninit::<sys::trevrpc_transport_event_protocol_info_v1>::zeroed();
            result(unsafe {
                sys::trevrpc_transport_event_protocol_info_v1_init(
                    protocol.as_mut_ptr(),
                    std::mem::size_of::<sys::trevrpc_transport_event_protocol_info_v1>(),
                )
            })?;
            let mut protocol = unsafe { protocol.assume_init() };
            result(unsafe {
                sys::trevrpc_transport_event_get_protocol_info_v1(
                    self.pointer(),
                    event.0.as_ptr(),
                    &raw mut protocol,
                )
            })?;
            Some(
                Protocol::from_raw(protocol.protocol)
                    .ok_or_else(|| NativeError::message("unknown event protocol"))?,
            )
        };
        Ok(EventInfo {
            kind,
            status: info.status,
            subject_kind: info.subject_kind,
            sequence: info.sequence,
            subject: info.subject.into(),
            parent: info.parent.into(),
            operation_id: info.operation_id,
            application_error_code: info.application_error_code,
            provider_error_code: info.provider_error_code,
            protocol,
            flags: info.flags,
        })
    }
    pub(crate) fn admission_owned(&self, event: &RawEvent) -> crate::Result<AdmissionOwned> {
        let mut info = std::mem::MaybeUninit::<sys::trevrpc_transport_admission_info_v1>::zeroed();
        result(unsafe {
            sys::trevrpc_transport_admission_info_v1_init(
                info.as_mut_ptr(),
                std::mem::size_of::<sys::trevrpc_transport_admission_info_v1>(),
            )
        })?;
        let mut info = unsafe { info.assume_init() };
        result(unsafe {
            sys::trevrpc_transport_event_get_admission_info_v1(
                self.pointer(),
                event.0.as_ptr(),
                &raw mut info,
            )
        })?;
        let count = usize::try_from(info.header_count)
            .map_err(|_| NativeError::message("header count exceeds SIZE_MAX"))?;
        let raw_headers = if count == 0 {
            &[][..]
        } else if info.headers.is_null() {
            return Err(NativeError::message(
                "nonzero header count with null pointer",
            ));
        } else {
            unsafe { std::slice::from_raw_parts(info.headers, count) }
        };
        let mut headers = Vec::with_capacity(count);
        for header in raw_headers {
            headers.push((
                copy_bytes(header.name, header.name_len, "header name")?,
                copy_bytes(header.value, header.value_len, "header value")?,
            ));
        }
        Ok(AdmissionOwned {
            protocol: Protocol::from_raw(info.protocol)
                .ok_or_else(|| NativeError::message("unknown admission protocol"))?,
            headers,
            method: copy_bytes(info.method, info.method_len, "method")?,
            path: copy_bytes(info.path, info.path_len, "path")?,
            authority: copy_bytes(info.authority, info.authority_len, "authority")?,
            origin: copy_bytes(info.origin, info.origin_len, "origin")?,
            listener: info.listener.into(),
        })
    }
    pub(crate) fn respond_admission(&self, event: &RawEvent, status: u16) -> crate::Result<()> {
        result(unsafe {
            sys::trevrpc_transport_admission_respond_v1(self.pointer(), event.0.as_ptr(), status)
        })
    }
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn release_event(&self, event: RawEvent) {
        unsafe { sys::trevrpc_transport_event_release(self.pointer(), event.0.as_ptr()) };
    }
    pub(crate) fn diagnostics(&self) -> crate::Result<RawDiagnostics> {
        let mut value = std::mem::MaybeUninit::<sys::trevrpc_transport_diagnostics_v1>::zeroed();
        result(unsafe {
            sys::trevrpc_transport_diagnostics_v1_init(
                value.as_mut_ptr(),
                std::mem::size_of::<sys::trevrpc_transport_diagnostics_v1>(),
            )
        })?;
        let mut value = unsafe { value.assume_init() };
        result(unsafe {
            sys::trevrpc_transport_get_diagnostics_v1(self.pointer(), &raw mut value)
        })?;
        Ok(RawDiagnostics {
            state: value.state,
            terminal_status: value.terminal_status,
            queue_depth: value.queue_depth,
            ordinary_queue_depth: value.ordinary_queue_depth,
            live_listeners: value.live_listeners,
            live_connections: value.live_connections,
            live_streams: value.live_streams,
            pending_send_count: value.pending_send_count,
            pending_send_bytes: value.pending_send_bytes,
            receive_owned_count: value.receive_owned_count,
            receive_owned_bytes: value.receive_owned_bytes,
            events_enqueued: value.events_enqueued,
            events_dequeued: value.events_dequeued,
            events_rejected: value.events_rejected,
            provider_error_code: value.provider_error_code,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RawDiagnostics {
    pub state: u32,
    pub terminal_status: i32,
    pub queue_depth: u32,
    pub ordinary_queue_depth: u32,
    pub live_listeners: u64,
    pub live_connections: u64,
    pub live_streams: u64,
    pub pending_send_count: u64,
    pub pending_send_bytes: u64,
    pub receive_owned_count: u64,
    pub receive_owned_bytes: u64,
    pub events_enqueued: u64,
    pub events_dequeued: u64,
    pub events_rejected: u64,
    pub provider_error_code: u64,
}
