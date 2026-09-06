use trevrpc_c_sys as sys;

/// Protocol carried by a native endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Protocol {
    Auto,
    Native,
    Http3,
    WebTransport,
    Multiplexed,
}

impl Protocol {
    pub(crate) const fn raw(self) -> u32 {
        match self {
            Self::Auto => sys::TREVRPC_TRANSPORT_PROTOCOL_AUTO,
            Self::Native => sys::TREVRPC_TRANSPORT_PROTOCOL_NATIVE,
            Self::Http3 => sys::TREVRPC_TRANSPORT_PROTOCOL_HTTP3,
            Self::WebTransport => sys::TREVRPC_TRANSPORT_PROTOCOL_WEBTRANSPORT,
            Self::Multiplexed => sys::TREVRPC_TRANSPORT_PROTOCOL_MULTIPLEXED,
        }
    }

    pub(crate) const fn from_raw(value: u32) -> Option<Self> {
        match value {
            sys::TREVRPC_TRANSPORT_PROTOCOL_AUTO => Some(Self::Auto),
            sys::TREVRPC_TRANSPORT_PROTOCOL_NATIVE => Some(Self::Native),
            sys::TREVRPC_TRANSPORT_PROTOCOL_HTTP3 => Some(Self::Http3),
            sys::TREVRPC_TRANSPORT_PROTOCOL_WEBTRANSPORT => Some(Self::WebTransport),
            sys::TREVRPC_TRANSPORT_PROTOCOL_MULTIPLEXED => Some(Self::Multiplexed),
            _ => None,
        }
    }
}

/// Capacity limits for one native transport and its Rust driver.
#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub event_capacity: u32,
    pub listener_capacity: u32,
    pub connection_capacity: u32,
    pub stream_capacity: u32,
    pub max_receive_owned_count: u32,
    pub max_receive_owned_bytes: u64,
    pub command_capacity: usize,
    pub delivery_capacity: usize,
}

impl TransportConfig {
    pub(crate) fn validate(&self) -> crate::Result<()> {
        if self.command_capacity == 0 || self.delivery_capacity == 0 {
            return Err(crate::NativeError::message(
                "driver command and delivery capacities must be nonzero",
            ));
        }
        Ok(())
    }
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            event_capacity: sys::TREVRPC_TRANSPORT_DEFAULT_EVENT_CAPACITY,
            listener_capacity: sys::TREVRPC_TRANSPORT_DEFAULT_LISTENER_CAPACITY,
            connection_capacity: sys::TREVRPC_TRANSPORT_DEFAULT_CONNECTION_CAPACITY,
            stream_capacity: sys::TREVRPC_TRANSPORT_DEFAULT_STREAM_CAPACITY,
            max_receive_owned_count: sys::TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_COUNT,
            max_receive_owned_bytes: sys::TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_BYTES,
            command_capacity: 256,
            delivery_capacity: 256,
        }
    }
}

/// Native listener or dial configuration.
#[derive(Clone, Debug)]
pub struct EndpointConfig {
    pub protocol: Protocol,
    pub host: String,
    pub port: u16,
    pub peer_bidi_stream_count: u16,
    pub server_name: Option<String>,
    pub alpn: Vec<u8>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub ca_cert_file: Option<String>,
    pub cert_data: Vec<u8>,
    pub key_data: Vec<u8>,
    pub ca_cert_data: Vec<u8>,
    pub path: Option<String>,
    pub origin: Option<String>,
    pub webtransport_profiles: u32,
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
    pub skip_certificate_validation: bool,
    pub defer_admission: bool,
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            protocol: Protocol::Auto,
            host: String::new(),
            port: 0,
            peer_bidi_stream_count: 100,
            server_name: None,
            alpn: Vec::new(),
            cert_file: None,
            key_file: None,
            ca_cert_file: None,
            cert_data: Vec::new(),
            key_data: Vec::new(),
            ca_cert_data: Vec::new(),
            path: None,
            origin: None,
            webtransport_profiles: sys::TREVRPC_TRANSPORT_WEBTRANSPORT_PROFILE_ALL_SUPPORTED,
            max_sessions: 1,
            max_frame_size: sys::TREVRPC_TRANSPORT_DEFAULT_MAX_FRAME_SIZE,
            max_field_section_size: sys::TREVRPC_TRANSPORT_DEFAULT_MAX_FIELD_SECTION_SIZE,
            max_pending_send_bytes: sys::TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_SEND_BYTES,
            max_pending_receive_bytes: sys::TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_RECEIVE_BYTES,
            max_idle_timeout_ms: 0,
            max_pending_send_count: sys::TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_SEND_COUNT,
            max_pending_receive_count: sys::TREVRPC_TRANSPORT_DEFAULT_MAX_PENDING_RECEIVE_COUNT,
            keep_alive_ms: 0,
            stream_recv_window: 0,
            conn_flow_control_window: 0,
            unresolved_stream_count: sys::TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_COUNT,
            unresolved_stream_bytes: sys::TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_BYTES,
            unresolved_stream_timeout_ms: u64::from(
                sys::TREVRPC_TRANSPORT_DEFAULT_UNRESOLVED_STREAM_TIMEOUT_MS,
            ),
            skip_certificate_validation: false,
            defer_admission: false,
        }
    }
}

/// Reserved provider configuration for the `MsQuic` companion factory.
#[derive(Clone, Debug, Default)]
pub struct MsQuicConfig {
    pub flags: u32,
}
