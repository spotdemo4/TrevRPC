%% @private A connection of either transport: native QUIC (an erlang_quic
%% connection) or QMux (a `trevrpc_qmux' session).
%%
%% Both carry the same bidirectional streams with the same flow control, and
%% send their owner the same events, tagged `quic' or `trevrpc_qmux'. These
%% functions never raise: a connection that is gone reports
%% `{error, closed}'.
-module(trevrpc_transport).

-export([
    pid/1,
    kind/1,
    tag/1,
    send_data/4,
    send_data_async/4,
    consume/3,
    reset_stream/3,
    stop_sending/3,
    manual_flow_control/2,
    notify_send_ready/1,
    open_stream/1,
    close/2
]).

-export_type([t/0, kind/0]).

-type kind() :: quic | qmux.

%% An erlang_quic call whose connection may be gone.
-define(SAFE(Call),
    try
        Call
    catch
        _:_ -> {error, closed}
    end
).
-type t() :: {kind(), pid()}.

-spec pid(t()) -> pid().
pid({_, Pid}) -> Pid.

-spec kind(t()) -> kind().
kind({Kind, _}) -> Kind.

%% @doc The tag of the events the connection sends its owner.
-spec tag(t()) -> quic | trevrpc_qmux.
tag({quic, _}) -> quic;
tag({qmux, _}) -> trevrpc_qmux.

-spec send_data(t(), non_neg_integer(), iodata(), boolean()) -> ok | {error, term()}.
send_data({quic, Conn}, Id, Data, Fin) -> ?SAFE(quic:send_data(Conn, Id, Data, Fin));
send_data({qmux, Session}, Id, Data, Fin) -> trevrpc_qmux:send_data(Session, Id, Data, Fin).

%% @doc Queues data without waiting and without the send queue's limit.
-spec send_data_async(t(), non_neg_integer(), iodata(), boolean()) -> ok.
send_data_async({quic, Conn}, Id, Data, Fin) ->
    _ = ?SAFE(quic:send_data_async(Conn, Id, Data, Fin)),
    ok;
send_data_async({qmux, Session}, Id, Data, Fin) ->
    trevrpc_qmux:send_data_async(Session, Id, Data, Fin).

-spec consume(t(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
consume({quic, Conn}, Id, Bytes) -> ?SAFE(quic:consume(Conn, Id, Bytes));
consume({qmux, Session}, Id, Bytes) -> trevrpc_qmux:consume(Session, Id, Bytes).

-spec reset_stream(t(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
reset_stream({quic, Conn}, Id, Code) -> ?SAFE(quic:reset_stream(Conn, Id, Code));
reset_stream({qmux, Session}, Id, Code) -> trevrpc_qmux:reset_stream(Session, Id, Code).

-spec stop_sending(t(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
stop_sending({quic, Conn}, Id, Code) -> ?SAFE(quic:stop_sending(Conn, Id, Code));
stop_sending({qmux, Session}, Id, Code) -> trevrpc_qmux:stop_sending(Session, Id, Code).

%% @doc Puts a stream in manual flow control, which QMux streams always are.
-spec manual_flow_control(t(), non_neg_integer()) -> ok.
manual_flow_control({quic, Conn}, Id) ->
    _ = ?SAFE(quic:set_stream_flow_control(Conn, Id, manual)),
    ok;
manual_flow_control({qmux, _Session}, _Id) ->
    ok.

%% @doc Asks for a `send_ready' event once the send queue drained.
-spec notify_send_ready(t()) -> ok.
notify_send_ready({quic, Conn}) ->
    _ = ?SAFE(quic_connection:notify_send_ready(Conn)),
    ok;
notify_send_ready({qmux, Session}) ->
    trevrpc_qmux:notify_send_ready(Session).

-spec open_stream(t()) -> {ok, non_neg_integer()} | {error, term()}.
open_stream({quic, Conn}) -> ?SAFE(quic:open_stream(Conn));
open_stream({qmux, Session}) -> trevrpc_qmux:open_stream(Session).

-spec close(t(), non_neg_integer()) -> ok.
close({quic, Conn}, Code) ->
    _ = ?SAFE(quic:close(Conn, Code)),
    ok;
close({qmux, Session}, Code) ->
    trevrpc_qmux:close(Session, Code).
