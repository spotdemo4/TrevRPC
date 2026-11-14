%% @private A connection of any transport: native QUIC (an erlang_quic
%% connection), QMux (a `trevrpc_qmux' session), or the HTTP mapping over
%% HTTP/3 (a `quic_h3' connection) or HTTP/2 (an erlang_h2 connection).
%%
%% Each carries bidirectional streams with flow control, and sends its owner
%% events tagged `quic', `trevrpc_qmux', `quic_h3' or `h2'. QUIC and QMux
%% streams are the RPCs' own streams; HTTP request streams carry one RPC in
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
    send_data/4,
    send_data/5,
    send_data_async/4,
    consume/3,
    reset_stream/3,
    stop_sending/3,
    manual_flow_control/2,
    notify_send_ready/1,
    open_stream/1,
    goaway/1,
    close/2
]).

-export_type([t/0, kind/0]).

-type kind() :: quic | qmux | h3 | h2.

%% A call on a connection that may be gone.
-define(SAFE(Call),
    try
        Call
    catch
        _:_ -> {error, closed}
    end
).

%% An HTTP/3 connection is the `quic_h3' process and its QUIC connection.
-type t() :: {quic, pid()} | {qmux, pid()} | {h3, pid(), pid()} | {h2, pid()}.

-define(H3_NO_ERROR, 16#100).
-define(H3_REQUEST_CANCELLED, 16#10c).

-spec pid(t()) -> pid().
pid(Transport) -> element(2, Transport).

-spec kind(t()) -> kind().
kind(Transport) -> element(1, Transport).

%% @doc The tag of the events the connection sends its owner.
-spec tag(t()) -> quic | trevrpc_qmux | quic_h3 | h2.
tag({quic, _}) -> quic;
tag({qmux, _}) -> trevrpc_qmux;
tag({h3, _, _}) -> quic_h3;
tag({h2, _}) -> h2.

%% @doc Writes `Data', finishing the stream with `Fin', waiting as long as
%% it takes over HTTP/2.
-spec send_data(t(), non_neg_integer(), iodata(), boolean()) -> ok | {error, term()}.
send_data(Transport, Id, Data, Fin) ->
    send_data(Transport, Id, Data, Fin, infinity).

%% @doc Writes `Data', finishing the stream with `Fin'. A full send queue
%% returns `{error, send_queue_full}' at once, except over HTTP/2, which
%% waits up to `Timeout' milliseconds for the peer's window and returns
%% `{error, timeout}' after. quic_h3 and erlang_h2 take a binary only (they
%% size and frame the payload whole), so the HTTP writes copy `Data' into
%% one.
-spec send_data(t(), non_neg_integer(), iodata(), boolean(), timeout()) ->
    ok | {error, term()}.
send_data({quic, Conn}, Id, Data, Fin, _Timeout) ->
    ?SAFE(quic:send_data(Conn, Id, Data, Fin));
send_data({qmux, Session}, Id, Data, Fin, _Timeout) ->
    trevrpc_qmux:send_data(Session, Id, Data, Fin);
send_data({h3, H3, _Quic}, Id, Data, Fin, _Timeout) ->
    ?SAFE(quic_h3:send_data(H3, Id, iolist_to_binary(Data), Fin));
send_data({h2, Conn}, Id, Data, Fin, Timeout) ->
    ?SAFE(h2:send_data(Conn, Id, iolist_to_binary(Data), Fin, #{block => Timeout})).

%% @doc Queues data without waiting and without the send queue's limit.
%% quic_h3 has no such write: when it refuses one, the caller is sent
%% `{quic_h3, H3, {send_ready, Id}}' once the queue drained, to write it
%% again then.
-spec send_data_async(t(), non_neg_integer(), iodata(), boolean()) ->
    ok | {error, send_queue_full}.
send_data_async({quic, Conn}, Id, Data, Fin) ->
    _ = ?SAFE(quic:send_data_async(Conn, Id, Data, Fin)),
    ok;
send_data_async({qmux, Session}, Id, Data, Fin) ->
    trevrpc_qmux:send_data_async(Session, Id, Data, Fin);
send_data_async({h3, H3, _Quic}, Id, Data, Fin) ->
    case ?SAFE(quic_h3:send_data(H3, Id, iolist_to_binary(Data), Fin)) of
        {error, send_queue_full} -> {error, send_queue_full};
        _ -> ok
    end;
send_data_async({h2, Conn}, Id, Data, Fin) ->
    %% HTTP/2 buffers what the peer's window does not take yet.
    _ = ?SAFE(h2:send_data(Conn, Id, iolist_to_binary(Data), Fin)),
    ok.

-spec consume(t(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
consume({quic, Conn}, Id, Bytes) -> ?SAFE(quic:consume(Conn, Id, Bytes));
consume({qmux, Session}, Id, Bytes) -> trevrpc_qmux:consume(Session, Id, Bytes);
consume({h3, H3, _Quic}, Id, Bytes) -> ?SAFE(quic_h3:consume(H3, Id, Bytes));
consume({h2, Conn}, Id, Bytes) -> ?SAFE(h2:consume(Conn, Id, Bytes)).

%% @doc Resets the sending side. An HTTP request stream is cancelled
%% whole, its receiving side as well.
-spec reset_stream(t(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
reset_stream({quic, Conn}, Id, Code) ->
    ?SAFE(quic:reset_stream(Conn, Id, Code));
reset_stream({qmux, Session}, Id, Code) ->
    trevrpc_qmux:reset_stream(Session, Id, Code);
reset_stream({h3, H3, _Quic}, Id, _Code) ->
    ?SAFE(quic_h3:cancel(H3, Id, ?H3_REQUEST_CANCELLED));
reset_stream({h2, Conn}, Id, _Code) ->
    ?SAFE(h2:cancel(Conn, Id, cancel)).

%% @doc Stops the receiving side. HTTP/2 cannot: the connection consumes
%% what still arrives.
-spec stop_sending(t(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
stop_sending({quic, Conn}, Id, Code) ->
    ?SAFE(quic:stop_sending(Conn, Id, Code));
stop_sending({qmux, Session}, Id, Code) ->
    trevrpc_qmux:stop_sending(Session, Id, Code);
stop_sending({h3, _H3, Quic}, Id, _Code) ->
    %% quic_h3 has no call that stops reading alone. The client answers
    %% with a reset, which quic_h3 reports as for any other.
    ?SAFE(quic:stop_sending(Quic, Id, ?H3_REQUEST_CANCELLED));
stop_sending({h2, _Conn}, _Id, _Code) ->
    ok.

%% @doc Puts a stream in manual flow control, which QMux streams always
%% are, and HTTP request streams from their acceptance.
-spec manual_flow_control(t(), non_neg_integer()) -> ok.
manual_flow_control({quic, Conn}, Id) ->
    _ = ?SAFE(quic:set_stream_flow_control(Conn, Id, manual)),
    ok;
manual_flow_control(_Transport, _Id) ->
    ok.

%% @doc Asks for a `send_ready' event once the send queue drained. Over
%% HTTP/3 the writer that was refused is told itself, and HTTP/2 writers
%% wait in {@link send_data/5}.
-spec notify_send_ready(t()) -> ok.
notify_send_ready({quic, Conn}) ->
    _ = ?SAFE(quic_connection:notify_send_ready(Conn)),
    ok;
notify_send_ready({qmux, Session}) ->
    trevrpc_qmux:notify_send_ready(Session);
notify_send_ready(_Transport) ->
    ok.

-spec open_stream(t()) -> {ok, non_neg_integer()} | {error, term()}.
open_stream({quic, Conn}) -> ?SAFE(quic:open_stream(Conn));
open_stream({qmux, Session}) -> trevrpc_qmux:open_stream(Session);
open_stream(_Transport) -> {error, unsupported}.

%% @doc Tells an HTTP/3 client that the connection takes no more requests;
%% those it sent already are served. erlang_h2 closes a connection 100 ms
%% after its GOAWAY, calls in flight or not (benoitc/erlang_h2#41), so
%% HTTP/2 connections, like QUIC and QMux ones, refuse new requests one by
%% one instead, and send their GOAWAY as they close.
-spec goaway(t()) -> ok.
goaway({h3, H3, _Quic}) ->
    _ = ?SAFE(quic_h3:goaway(H3)),
    ok;
goaway(_Transport) ->
    ok.

-spec close(t(), non_neg_integer()) -> ok.
close({quic, Conn}, Code) ->
    _ = ?SAFE(quic:close(Conn, Code)),
    ok;
close({qmux, Session}, Code) ->
    trevrpc_qmux:close(Session, Code);
close({h3, H3, _Quic}, _Code) ->
    _ = ?SAFE(quic_h3:close(H3, ?H3_NO_ERROR)),
    ok;
close({h2, Conn}, _Code) ->
    %% h2:goaway/1 writes the GOAWAY before it returns.
    _ = ?SAFE(h2:goaway(Conn)),
    _ = ?SAFE(h2:close(Conn)),
    ok.
