%% @private A connection of any transport: native QUIC (an erlang_quic
%% connection), QMux (a `qmux' session), HTTP/3 (a `quic_h3'
%% connection), HTTP/2 (an erlang_h2 connection) or HTTP/1.1 (an erlang_h1
%% connection), or a WebTransport session (see {@link trevrpc_webtransport}).
%%
%% Each carries bidirectional streams with flow control, and sends its owner
%% events tagged `quic', `qmux', `quic_h3', `h2', `h1' or
%% `trevrpc_wt'. HTTP/1.1 carries no RPCs, only the requests of the HTTP
%% fallback, one at a time, and has no stream reset: resetting a request
%% whose response has not ended closes the connection.
%% QUIC, QMux and WebTransport streams are the RPCs' own streams; HTTP request streams carry one RPC in
%% their request and response bodies, on streams the client opened, and are
%% cancelled with the HTTP codes (`H3_REQUEST_CANCELLED', `CANCEL') instead
%% of TrevRPC's. HTTP/2 has no STOP_SENDING: a stream whose reading stops is
%% consumed until the client finishes or resets it. These functions never
%% raise: a connection that is gone reports `{error, closed}'.
-module(trevrpc_transport).

-export([
    pid/1,
    kind/1,
    tag/1,
    send_response/4,
    respond/5,
    send_data/4,
    send_data/5,
    send_data_async/4,
    consume/3,
    reset_stream/3,
    stop_sending/3,
    manual_flow_control/2,
    hold_streams/1,
    track_flow_control/3,
    forget/2,
    notify_send_ready/1,
    open_stream/1,
    send_datagram/2,
    datagram_max_size/1,
    goaway/1,
    close/2
]).

-export_type([t/0, kind/0]).

-type kind() :: quic | qmux | h3 | h2 | h1 | wt.

%% A call on a connection that may be gone.
-define(SAFE(Call),
    try
        Call
    catch
        _:_ -> {error, closed}
    end
).

%% A QMux connection is the session and its send queue; an HTTP/3
%% connection is the `quic_h3' process and its QUIC connection; a
%% WebTransport session is the session's process, its QUIC connection, and
%% the size of the quarter stream ID that prefixes its datagrams.
-type t() ::
    {quic, pid()}
    | {qmux, pid(), qmux:send_queue()}
    | {h3, pid(), pid()}
    | {h2, pid()}
    | {h1, pid()}
    | {wt, pid(), pid(), pos_integer()}.

-define(H3_NO_ERROR, 16#100).
-define(H3_REQUEST_CANCELLED, 16#10c).
%% The most bytes an erlang_h1 write takes (see h1_send/4).
-define(H1_CHUNK, 65536).

-spec pid(t()) -> pid().
pid(Transport) -> element(2, Transport).

-spec kind(t()) -> kind().
kind(Transport) -> element(1, Transport).

%% @doc The tag of the events the connection sends its owner.
-spec tag(t()) -> quic | qmux | quic_h3 | h2 | h1 | trevrpc_wt.
tag({quic, _}) -> quic;
tag({qmux, _, _}) -> qmux;
tag({h3, _, _}) -> quic_h3;
tag({h2, _}) -> h2;
tag({h1, _}) -> h1;
tag({wt, _, _, _}) -> trevrpc_wt.

%% @doc Writes the response head of HTTP request `Id', whose body follows
%% with {@link send_data/5}. quic_h3 holds a response's HEADERS back until
%% its first DATA, so an empty one follows them at once: the client hears
%% of the response right away, as from Go and Rust.
-spec send_response(t(), non_neg_integer(), 100..599, [{binary(), iodata()}]) ->
    ok | {error, term()}.
send_response({h3, H3, _Quic}, Id, Status, Headers) ->
    case ?SAFE(quic_h3:send_response(H3, Id, Status, Headers)) of
        ok ->
            _ = ?SAFE(quic_h3:send_data(H3, Id, <<>>, false)),
            ok;
        {error, _} = Error ->
            Error
    end;
send_response({h2, Conn}, Id, Status, Headers) ->
    ?SAFE(h2:send_response(Conn, Id, Status, Headers));
send_response({h1, Conn}, Id, Status, Headers) ->
    ?SAFE(h1_connection:send_response(Conn, Id, Status, Headers)).

%% @doc Writes the whole response of HTTP request `Id', which ends its
%% sending side.
-spec respond(t(), non_neg_integer(), 100..599, [{binary(), iodata()}], iodata()) ->
    ok | {error, term()}.
respond({h3, H3, _Quic}, Id, Status, Headers, Body) ->
    ?SAFE(quic_h3:respond(H3, Id, Status, Headers, iolist_to_binary(Body)));
respond({h2, Conn}, Id, Status, Headers, Body) ->
    ?SAFE(h2:respond(Conn, Id, Status, Headers, iolist_to_binary(Body)));
respond({h1, Conn}, Id, Status, Headers, Body) ->
    Size = iolist_size(Body),
    Framed =
        lists:keymember(<<"content-length">>, 1, Headers) orelse
            lists:keymember(<<"transfer-encoding">>, 1, Headers),
    case Size > ?H1_CHUNK of
        false ->
            ?SAFE(h1_connection:respond(Conn, Id, Status, Headers, Body));
        true ->
            Length = [{<<"content-length">>, integer_to_binary(Size)} || not Framed],
            case ?SAFE(h1_connection:send_response(Conn, Id, Status, Length ++ Headers)) of
                ok -> h1_send(Conn, Id, iolist_to_binary(Body), true);
                {error, _} = Error -> Error
            end
    end.

%% @doc Writes `Data', finishing the stream with `Fin', waiting as long as
%% it takes over HTTP/2.
-spec send_data(t(), non_neg_integer(), iodata(), boolean()) -> ok | {error, term()}.
send_data(Transport, Id, Data, Fin) ->
    send_data(Transport, Id, Data, Fin, infinity).

%% @doc Writes `Data', finishing the stream with `Fin'. A full send queue
%% returns `{error, send_queue_full}' at once, except over HTTP/2, which
%% waits up to `Timeout' milliseconds for the peer's window and returns
%% `{error, timeout}' after, and over WebTransport, which waits as long for
%% the session to take the write. quic_h3 and erlang_h2 take a binary only (they
%% size and frame the payload whole), so the HTTP writes copy `Data' into
%% one.
-spec send_data(t(), non_neg_integer(), iodata(), boolean(), timeout()) ->
    ok | {error, term()}.
send_data({quic, Conn}, Id, Data, Fin, _Timeout) ->
    ?SAFE(quic:send_data(Conn, Id, Data, Fin));
send_data({qmux, Session, Queue}, Id, Data, Fin, _Timeout) ->
    qmux:send_data(Session, Queue, Id, Data, Fin);
send_data({h3, H3, _Quic}, Id, Data, Fin, _Timeout) ->
    ?SAFE(quic_h3:send_data(H3, Id, iolist_to_binary(Data), Fin));
send_data({h2, Conn}, Id, Data, Fin, Timeout) ->
    ?SAFE(h2:send_data(Conn, Id, iolist_to_binary(Data), Fin, #{block => Timeout}));
send_data({h1, Conn}, Id, Data, Fin, _Timeout) ->
    %% erlang_h1 writes to the socket before it returns.
    h1_send(Conn, Id, iolist_to_binary(Data), Fin);
send_data({wt, Session, _Quic, _Prefix}, Id, Data, Fin, Timeout) ->
    trevrpc_webtransport:send(Session, Id, Data, Fin, Timeout).

%% @doc Queues data without waiting and without the send queue's limit.
%% quic_h3 has no such write: when it refuses one, the caller is sent
%% `{quic_h3, H3, {send_ready, Id}}' once the queue drained, to write it
%% again then.
-spec send_data_async(t(), non_neg_integer(), iodata(), boolean()) ->
    ok | {error, send_queue_full}.
send_data_async({quic, Conn}, Id, Data, Fin) ->
    _ = ?SAFE(quic:send_data_async(Conn, Id, Data, Fin)),
    ok;
send_data_async({qmux, Session, Queue}, Id, Data, Fin) ->
    qmux:send_data_async(Session, Queue, Id, Data, Fin);
send_data_async({h3, H3, _Quic}, Id, Data, Fin) ->
    case ?SAFE(quic_h3:send_data(H3, Id, iolist_to_binary(Data), Fin)) of
        {error, send_queue_full} -> {error, send_queue_full};
        _ -> ok
    end;
send_data_async({h2, Conn}, Id, Data, Fin) ->
    %% HTTP/2 buffers what the peer's window does not take yet.
    _ = ?SAFE(h2:send_data(Conn, Id, iolist_to_binary(Data), Fin)),
    ok;
send_data_async({h1, Conn}, Id, Data, Fin) ->
    _ = h1_send(Conn, Id, iolist_to_binary(Data), Fin),
    ok;
send_data_async({wt, Session, _Quic, _Prefix}, Id, Data, Fin) ->
    %% The session queues what its send queue does not take yet.
    trevrpc_webtransport:send_async(Session, Id, Data, Fin).

%% An HTTP/1.1 write in parts of at most ?H1_CHUNK bytes. erlang_h1's calls
%% have no timeout: each waits as long as the socket does, and a client that
%% stops reading holds it until the kernel drops the connection after the
%% listener's TCP_USER_TIMEOUT. The parts let the connection's process take
%% its other calls between them rather than after the whole body.
h1_send(Conn, Id, <<Part:?H1_CHUNK/binary, Rest/binary>>, Fin) when Rest =/= <<>> ->
    case ?SAFE(h1_connection:send_data(Conn, Id, Part, false)) of
        ok -> h1_send(Conn, Id, Rest, Fin);
        {error, _} = Error -> Error
    end;
h1_send(Conn, Id, Data, Fin) ->
    ?SAFE(h1_connection:send_data(Conn, Id, Data, Fin)).

-spec consume(t(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
consume({quic, Conn}, Id, Bytes) ->
    ?SAFE(quic:consume(Conn, Id, Bytes));
consume({qmux, Session, _}, Id, Bytes) ->
    qmux:consume(Session, Id, Bytes);
consume({h3, H3, _Quic}, Id, Bytes) ->
    ?SAFE(quic_h3:consume(H3, Id, Bytes));
consume({h2, Conn}, Id, Bytes) ->
    ?SAFE(h2:consume(Conn, Id, Bytes));
consume({h1, Conn}, Id, Bytes) ->
    ?SAFE(h1_connection:consume(Conn, Id, Bytes));
consume({wt, Session, _Quic, _Prefix}, Id, Bytes) ->
    trevrpc_webtransport:consume(Session, Id, Bytes).

%% @doc Resets the sending side. An HTTP request stream is cancelled
%% whole, its receiving side as well.
-spec reset_stream(t(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
reset_stream({quic, Conn}, Id, Code) ->
    ?SAFE(quic:reset_stream(Conn, Id, Code));
reset_stream({qmux, Session, _}, Id, Code) ->
    qmux:reset_stream(Session, Id, Code);
reset_stream({h3, H3, _Quic}, Id, _Code) ->
    ?SAFE(quic_h3:cancel(H3, Id, ?H3_REQUEST_CANCELLED));
reset_stream({h2, Conn}, Id, _Code) ->
    ?SAFE(h2:cancel(Conn, Id, cancel));
reset_stream({h1, Conn}, Id, _Code) ->
    %% Closes the connection if the response has not ended.
    ?SAFE(h1_connection:cancel_stream(Conn, Id));
reset_stream({wt, Session, _Quic, _Prefix}, Id, Code) ->
    trevrpc_webtransport:reset_stream(Session, Id, Code).

%% @doc Stops the receiving side. HTTP/2 and HTTP/1.1 cannot: the
%% connection consumes what still arrives.
-spec stop_sending(t(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
stop_sending({quic, Conn}, Id, Code) ->
    ?SAFE(quic:stop_sending(Conn, Id, Code));
stop_sending({qmux, Session, _}, Id, Code) ->
    qmux:stop_sending(Session, Id, Code);
stop_sending({h3, _H3, Quic}, Id, _Code) ->
    %% quic_h3 has no call that stops reading alone. The client answers
    %% with a reset, which quic_h3 reports as for any other.
    ?SAFE(quic:stop_sending(Quic, Id, ?H3_REQUEST_CANCELLED));
stop_sending({h2, _Conn}, _Id, _Code) ->
    ok;
stop_sending({h1, _Conn}, _Id, _Code) ->
    ok;
stop_sending({wt, Session, _Quic, _Prefix}, Id, Code) ->
    trevrpc_webtransport:stop_sending(Session, Id, Code).

%% @doc Puts a stream in manual flow control, which QMux streams always
%% are, and HTTP request streams from their acceptance, without waiting.
%% The client channel calls it for each stream it opens; a server's
%% streams are held from the start instead (see {@link hold_streams/1}).
%% Over QUIC it is erlang_quic's own asynchronous form of
%% `quic:set_stream_flow_control/3', which its HTTP/3 layer uses, and does
%% nothing on a connection that is gone: what was delivered before still
%% counts as not consumed.
-spec manual_flow_control(t(), non_neg_integer()) -> ok.
manual_flow_control({quic, Conn}, Id) ->
    quic_connection:set_stream_flow_control(Conn, Id, {manual, 0});
manual_flow_control(_Transport, _Id) ->
    ok.

%% @doc Makes every stream the peer opens from now on start in manual
%% flow control, as QMux streams do, without a message to the connection
%% process for each as it opens: over native QUIC, erlang_quic holds them,
%% as for its HTTP/3 layer. A held stream earns credit only as its reader
%% returns it, which bounds what it buffers by its window, but its bytes
%% earn connection credit as they arrive, until
%% {@link track_flow_control/3} makes them count against the connection's
%% window too, as a server call does once it read its REQUEST. Called from
%% the connection's ALPN handler, in the connection's own process, so it
%% applies before any stream opens.
%%
%% erlang_quic drains its packets with selective receives, which pass over
%% the other messages in its mailbox. Measured with 64 concurrent calls, a
%% message from the owner for each stream as it opened cost a fifth of the
%% throughput of client streams; one from each call once it read its
%% REQUEST costs nothing measurable.
-spec hold_streams(t()) -> ok.
hold_streams({quic, Conn}) ->
    quic_connection:hold_peer_streams(Conn);
hold_streams(_Transport) ->
    ok.

%% @doc Makes a held stream's unread bytes count against the connection's
%% window, as well as its own, once its reader returned `Consumed' bytes.
-spec track_flow_control(t(), non_neg_integer(), non_neg_integer()) -> ok.
track_flow_control({quic, Conn}, Id, Consumed) ->
    quic_connection:set_stream_flow_control(Conn, Id, {manual, Consumed});
track_flow_control(_Transport, _Id, _Consumed) ->
    ok.

%% @doc Tells the connection that nothing reads stream `Id' any more, as
%% when its call ended: over WebTransport, the bytes its call left unread
%% no longer hold the session back, and over HTTP/1.1 the connection reads
%% and drops the rest of the request body. The other transports return
%% credit for each byte as it is read or dropped.
-spec forget(t(), non_neg_integer()) -> ok.
forget({wt, Session, _Quic, _Prefix}, Id) ->
    trevrpc_webtransport:forget(Session, Id);
forget({h1, Conn}, Id) ->
    %% The rest of the request body is read and dropped, so that the next
    %% request can follow it.
    _ = ?SAFE(h1_connection:release_window(Conn, Id)),
    ok;
forget(_Transport, _Id) ->
    ok.

%% @doc Asks for a `send_ready' event once the send queue drained. Over
%% HTTP/3 the writer that was refused is told itself, and HTTP/2 writers
%% wait in {@link send_data/5}.
-spec notify_send_ready(t()) -> ok.
notify_send_ready({quic, Conn}) ->
    _ = ?SAFE(quic_connection:notify_send_ready(Conn)),
    ok;
notify_send_ready({qmux, Session, _}) ->
    qmux:notify_send_ready(Session);
notify_send_ready(_Transport) ->
    ok.

-spec open_stream(t()) -> {ok, non_neg_integer()} | {error, term()}.
open_stream({quic, Conn}) -> ?SAFE(quic:open_stream(Conn));
open_stream({qmux, Session, _}) -> qmux:open_stream(Session);
open_stream(_Transport) -> {error, unsupported}.

%% @doc Sends a datagram. Only native QUIC and WebTransport carry them here;
%% erlang_quic refuses one that does not fit, or that finds the congestion
%% window full, at once, and a WebTransport session drops it.
-spec send_datagram(t(), iodata()) -> ok | {error, term()}.
send_datagram({quic, Conn}, Payload) ->
    ?SAFE(quic:send_datagram(Conn, Payload));
send_datagram({wt, Session, _Quic, _Prefix}, Payload) ->
    trevrpc_webtransport:send_datagram(Session, Payload);
send_datagram(_Transport, _Payload) ->
    {error, unsupported}.

%% @doc The largest datagram payload the connection takes now: 0 when it
%% carries none, as on every transport but native QUIC and WebTransport, or
%% when the peer does not take them. A WebTransport session's datagrams
%% start with its quarter stream ID.
-spec datagram_max_size(t()) -> non_neg_integer().
datagram_max_size({quic, Conn}) ->
    quic_datagram_max_size(Conn);
datagram_max_size({wt, _Session, Quic, Prefix}) when is_integer(Prefix) ->
    max(0, quic_datagram_max_size(Quic) - Prefix);
datagram_max_size(_Transport) ->
    0.

quic_datagram_max_size(Conn) ->
    case ?SAFE(quic:datagram_max_size(Conn)) of
        Size when is_integer(Size), Size >= 0 -> Size;
        _ -> 0
    end.

%% @doc Tells an HTTP client that the connection takes no more requests;
%% those it sent already are served. The connection's owner decides when
%% the calls are cut off and the connection closes, so erlang_h2 waits for
%% it. QUIC and QMux connections refuse new streams one by one instead.
-spec goaway(t()) -> ok.
goaway({h3, H3, _Quic}) ->
    _ = ?SAFE(quic_h3:goaway(H3)),
    ok;
goaway({h2, Conn}) ->
    _ = ?SAFE(h2:goaway(Conn, #{timeout => infinity})),
    ok;
goaway({h1, Conn}) ->
    %% The next response says `Connection: close', after which erlang_h1
    %% closes the connection; the owner closes an idle one.
    _ = ?SAFE(h1_connection:send_goaway(Conn, no_error)),
    ok;
goaway({wt, Session, _Quic, _Prefix}) ->
    trevrpc_webtransport:drain(Session);
goaway(_Transport) ->
    ok.

-spec close(t(), non_neg_integer()) -> ok.
close({quic, Conn}, Code) ->
    _ = ?SAFE(quic:close(Conn, Code)),
    ok;
close({qmux, Session, _}, Code) ->
    qmux:close(Session, Code);
close({h3, H3, _Quic}, _Code) ->
    _ = ?SAFE(quic_h3:close(H3, ?H3_NO_ERROR)),
    ok;
close({h2, Conn}, _Code) ->
    %% h2:goaway/1 writes the GOAWAY before it returns, unless goaway/1
    %% sent it already at shutdown, when it does nothing.
    _ = ?SAFE(h2:goaway(Conn)),
    _ = ?SAFE(h2:close(Conn)),
    ok;
close({h1, Conn}, _Code) ->
    _ = ?SAFE(h1_connection:close(Conn)),
    ok;
close({wt, Session, _Quic, _Prefix}, _Code) ->
    trevrpc_webtransport:close(Session).
