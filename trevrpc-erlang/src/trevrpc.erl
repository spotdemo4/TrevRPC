%% @doc TrevRPC protocol identifiers and defaults shared by every transport.
%%
%% See the wiki's Protocol and Wire Format page for what each identifier
%% names.
-module(trevrpc).

-export([
    alpn/0,
    qmux_alpn/0,
    http3_alpn/0,
    webtransport_protocol/0,
    content_type/0,
    default_max_frame_size/0
]).

%% @doc The TLS ALPN of native TrevRPC over QUIC, wire version 1.
-spec alpn() -> binary().
alpn() -> <<"trevrpc/1">>.

%% @doc The TLS ALPN of TrevRPC over QMux over TLS, and the WebSocket
%% subprotocol of TrevRPC over QMux over WebSocket.
-spec qmux_alpn() -> binary().
qmux_alpn() -> <<"qmux-02.trevrpc-1">>.

%% @doc The TLS ALPN of HTTP/3, which ordinary HTTP and WebTransport use.
-spec http3_alpn() -> binary().
http3_alpn() -> <<"h3">>.

%% @doc The WebTransport protocol (WT-Protocol) of wire version 1.
-spec webtransport_protocol() -> binary().
webtransport_protocol() -> <<"trevrpc-1">>.

%% @doc The media type of the ordinary HTTP mapping, wire version 1.
-spec content_type() -> binary().
content_type() -> <<"application/trevrpc">>.

%% @doc The default limit on a frame's payload, 4 MiB.
-spec default_max_frame_size() -> pos_integer().
default_max_frame_size() -> 4194304.
