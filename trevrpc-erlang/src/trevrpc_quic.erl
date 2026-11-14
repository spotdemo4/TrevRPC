%% @doc erlang_quic options from TrevRPC's: credentials and the receive
%% windows derived from the frame size limit.
%%
%% A stream's receive window holds one whole frame, the max frame size plus
%% the 16 bytes of the largest frame header. The connection's window holds
%% one frame per concurrent stream, capped at 16 MiB, and never below one
%% frame, as the Go and Rust runtimes size theirs.
-module(trevrpc_quic).

-export([credentials/1, stream_window/1, connection_window/2, transport_opts/3]).

-define(MAX_CONNECTION_WINDOW, 16777216).
-define(MAX_FRAME_HEADER, 16).

%% @doc The certificate, chain and key for TLS, from `cert', `cert_chain'
%% and `key' (DER certificates and a decoded key), or from `certfile' and
%% `keyfile' (PEM files; the certificate file may hold the chain after the
%% certificate).
-spec credentials(map()) -> {ok, map()} | {error, term()}.
credentials(#{cert := Cert, key := Key} = Opts) ->
    {ok, #{cert => Cert, key => Key, cert_chain => maps:get(cert_chain, Opts, [])}};
credentials(#{certfile := CertFile, keyfile := KeyFile}) ->
    maybe
        {ok, [Cert | Chain]} ?= read_certificates(CertFile),
        {ok, Key} ?= read_key(KeyFile),
        {ok, #{cert => Cert, key => Key, cert_chain => Chain}}
    end;
credentials(_Opts) ->
    {error, no_certificate}.

read_certificates(Path) ->
    case read_pem(Path) of
        {ok, Entries} ->
            case [Der || {'Certificate', Der, not_encrypted} <- Entries] of
                [] -> {error, {no_certificate, Path}};
                Certs -> {ok, Certs}
            end;
        Error ->
            Error
    end.

read_key(Path) ->
    case read_pem(Path) of
        {ok, Entries} ->
            case [Entry || {Type, _, not_encrypted} = Entry <- Entries, is_key(Type)] of
                [Entry | _] -> {ok, public_key:pem_entry_decode(Entry)};
                [] -> {error, {no_private_key, Path}}
            end;
        Error ->
            Error
    end.

read_pem(Path) ->
    case file:read_file(Path) of
        {ok, Pem} -> {ok, public_key:pem_decode(Pem)};
        {error, Reason} -> {error, {Reason, Path}}
    end.

is_key('RSAPrivateKey') -> true;
is_key('ECPrivateKey') -> true;
is_key('PrivateKeyInfo') -> true;
is_key(_) -> false.

%% @doc A stream's receive window: one whole frame.
-spec stream_window(pos_integer()) -> pos_integer().
stream_window(MaxFrameSize) when is_integer(MaxFrameSize), MaxFrameSize > 0 ->
    MaxFrameSize + ?MAX_FRAME_HEADER.

%% @doc A connection's receive window for `Streams' concurrent streams.
-spec connection_window(pos_integer(), pos_integer()) -> pos_integer().
connection_window(MaxFrameSize, Streams) when is_integer(Streams), Streams > 0 ->
    Stream = stream_window(MaxFrameSize),
    max(Stream, min(?MAX_CONNECTION_WINDOW, Stream * Streams)).

%% @doc The transport options of a connection: its receive windows, idle
%% timeout (with keep-alives at half of it, so a healthy connection stays
%% open), and how many concurrent bidirectional streams the peer may open.
-spec transport_opts(pos_integer(), pos_integer(), pos_integer() | infinity) -> map().
transport_opts(MaxFrameSize, Streams, IdleTimeout) ->
    StreamWindow = stream_window(MaxFrameSize),
    ConnectionWindow = connection_window(MaxFrameSize, Streams),
    Idle =
        case IdleTimeout of
            infinity -> #{idle_timeout => 0};
            _ -> #{idle_timeout => IdleTimeout, keep_alive_interval => max(1, IdleTimeout div 2)}
        end,
    Idle#{
        alpn => [trevrpc:alpn()],
        max_streams_bidi => Streams,
        max_streams_uni => 0,
        max_stream_data_bidi_local => StreamWindow,
        max_stream_data_bidi_remote => StreamWindow,
        max_stream_data_uni => 0,
        max_data => ConnectionWindow,
        max_receive_window => ConnectionWindow,
        notify_streams_available => true
    }.
