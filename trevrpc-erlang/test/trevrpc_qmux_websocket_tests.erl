%% A QMux session over WebSocket (trevrpc_qmux_websocket) against a peer
%% written by hand: the test process owns the session and plays the peer over
%% a raw TLS connection, framing WebSocket messages itself.
-module(trevrpc_qmux_websocket_tests).

-include_lib("eunit/include/eunit.hrl").

-define(PARAMETERS, #{
    initial_max_data => 1000,
    initial_max_stream_data_bidi_local => 100,
    initial_max_stream_data_bidi_remote => 100,
    initial_max_streams_bidi => 1,
    max_record_size => 16382
}).

websocket_test_() ->
    {setup,
        fun() ->
            {ok, _} = application:ensure_all_started(ssl),
            logger:set_primary_config(level, none),
            trevrpc_test_tls:credentials()
        end,
        fun(_) -> logger:set_primary_config(level, notice) end, fun(Credentials) ->
            [
                {Name, {timeout, 30, fun() -> Test(Credentials) end}}
             || {Name, Test} <- [
                    {"a Ping flood from a peer that reads nothing is bounded", fun ping_flood/1},
                    {"a WebSocket session drains its Close", fun ws_close/1}
                ]
            ]
        end}.

%% A server session over WebSocket advertising Parameters, owned by the test
%% process, and the client end of its TLS connection, in passive mode.
pair(#{cert := Cert, key := Key}, Parameters, Opts) ->
    KeyType = element(1, Key),
    {ok, Listen} = ssl:listen(0, [
        binary,
        {active, false},
        {ip, {127, 0, 0, 1}},
        {certs_keys, [#{cert => [Cert], key => {KeyType, public_key:der_encode(KeyType, Key)}}]}
    ]),
    {ok, {_, Port}} = ssl:sockname(Listen),
    Self = self(),
    Acceptor = spawn_link(fun() ->
        {ok, Accepted} = ssl:transport_accept(Listen),
        {ok, Tls} = ssl:handshake(Accepted, 5000),
        ok = ssl:controlling_process(Tls, Self),
        Self ! {accepted, self(), Tls}
    end),
    {ok, Peer} = ssl:connect(
        {127, 0, 0, 1}, Port, [binary, {active, false}, {verify, verify_none}], 5000
    ),
    Server =
        receive
            {accepted, Acceptor, Tls} -> Tls
        after 5000 -> error(not_accepted)
        end,
    ok = ssl:close(Listen),
    {ok, Session, Queue} = qmux:start({qmux_ssl, Server}, server, self(), Opts#{
        parameters => Parameters,
        framing => trevrpc_qmux_websocket,
        stream_flow_control => manual
    }),
    put({send_queue, Session}, Queue),
    {Session, Peer}.

event(Session) ->
    receive
        {qmux, Session, Event} -> Event
    after 5000 -> error(no_event)
    end.

%% A WebSocket peer that sends Pings and reads nothing. The writer, held
%% still, takes its window of a stream's data and no more; then 1000 Pings
%% add nothing to it, and once it runs again exactly one Pong, to the
%% latest Ping, reaches the peer with the data.
ping_flood(Credentials) ->
    {Session, Socket} = pair(Credentials, maps:put(initial_max_streams_bidi, 3, ?PARAMETERS), #{}),
    Large = 1 bsl 30,
    Parameters = qmux_frame:transport_parameters(
        maps:merge(?PARAMETERS, #{
            initial_max_data => Large,
            initial_max_stream_data_bidi_local => Large,
            initial_max_stream_data_bidi_remote => Large
        })
    ),
    ok = ssl:send(Socket, ws_record([Parameters])),
    ?assertMatch({connected, _}, event(Session)),
    ok = ssl:send(Socket, ws_record([qmux_frame:stream(0, 0, [<<"go">>], false)])),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    Writer = writer(Session),
    erlang:suspend_process(Writer),
    Size = 1 bsl 20,
    ok = qmux:send_data_async(
        Session, get({send_queue, Session}), 0, binary:copy(<<"d">>, Size), false
    ),
    %% A stream the peer opens is a marker: its event follows every Ping
    %% sent before it.
    Marker = fun(Id) ->
        ok = ssl:send(Socket, ws_record([qmux_frame:stream(Id, 0, [<<"m">>], false)])),
        ?assertEqual({stream_opened, Id}, event(Session)),
        ?assertMatch({stream_data, Id, <<"m">>, false}, event(Session))
    end,
    Marker(4),
    {message_queue_len, Filled} = process_info(Writer, message_queue_len),
    Ping = ws_frame:encode({ping, binary:copy(<<"p">>, 125)}, client),
    _ = [ok = ssl:send(Socket, Ping) || _ <- lists:seq(1, 1000)],
    Marker(8),
    ?assertEqual({message_queue_len, Filled}, process_info(Writer, message_queue_len)),
    erlang:resume_process(Writer),
    ?assertEqual(1, ws_pongs(Socket, Size)),
    qmux:close(Session, 0).

%% The session's writer: the process it monitors besides its owner. The
%% session starts it as it takes the socket over, which qmux:start/4 asks
%% for before it returns, so any call after that finds it running.
writer(Session) ->
    {ok, _} = qmux:peername(Session),
    {monitors, Monitors} = process_info(Session, monitors),
    [Writer] = [Pid || {process, Pid} <- Monitors, Pid =/= self()],
    Writer.

ws_record(Frames) ->
    ws_frame:encode({binary, iolist_to_binary(Frames)}, client).

%% The Pongs the peer reads with Size bytes of stream 0's data.
ws_pongs(Socket, Size) ->
    ws_pongs(Socket, Size, ws_frame:init_parser(#{role => client}), 0).

ws_pongs(_Socket, Size, _Parser, Pongs) when Size =< 0 ->
    Pongs;
ws_pongs(Socket, Size, Parser, Pongs) ->
    {ok, Data} = ssl:recv(Socket, 0, 5000),
    {ok, Messages, Next} = ws_frame:parse(Parser, Data),
    Read = lists:sum([
        byte_size(Bytes)
     || {binary, Record} <- Messages,
        {stream, 0, _, Bytes, _} <- element(2, qmux_frame:decode_frames(Record))
    ]),
    ws_pongs(Socket, Size - Read, Next, Pongs + length([pong || {pong, _} <- Messages])).

%% A WebSocket session that ends on its own, here as the peer sent no
%% parameters, still sends its Close: the session waits for its writer,
%% held still until then, to drain it before the socket closes.
ws_close(Credentials) ->
    {Session, Socket} = pair(Credentials, ?PARAMETERS, #{handshake_timeout => 200}),
    Writer = writer(Session),
    %% The session calls stop_writer/1 as it ends, to wait for its writer.
    _ = erlang:trace_pattern({qmux, stop_writer, 1}, true, [local]),
    1 = erlang:trace(Session, true, [call]),
    erlang:suspend_process(Writer),
    ?assertEqual({closed, handshake_timeout}, event(Session)),
    receive
        {trace, Session, call, {qmux, stop_writer, _}} -> ok
    after 5000 -> error(not_ending)
    end,
    _ = erlang:trace_pattern({qmux, stop_writer, 1}, false, [local]),
    erlang:resume_process(Writer),
    Messages = ws_messages(Socket, ws_frame:init_parser(#{role => client})),
    ?assertMatch([_], [Close || Close <- Messages, is_close(Close)]).

ws_messages(Socket, Parser) ->
    ws_messages(Socket, Parser, []).

%% The messages the peer reads until a Close, or until the socket closes.
ws_messages(Socket, Parser, Acc) ->
    case ssl:recv(Socket, 0, 5000) of
        {ok, Data} ->
            {ok, Messages, Next} = ws_frame:parse(Parser, Data),
            All = Acc ++ Messages,
            case [Close || Close <- Messages, is_close(Close)] of
                [] -> ws_messages(Socket, Next, All);
                _ -> All
            end;
        {error, _} ->
            Acc
    end.

is_close(close) -> true;
is_close(Message) -> element(1, Message) =:= close.
