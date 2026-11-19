%% A QMux session against a peer written by hand: the test process owns the
%% session and plays the peer over a raw TLS connection.
-module(trevrpc_qmux_tests).

-include_lib("eunit/include/eunit.hrl").

-define(PARAMETERS, #{
    initial_max_data => 1000,
    initial_max_stream_data_bidi_local => 100,
    initial_max_stream_data_bidi_remote => 100,
    initial_max_streams_bidi => 1,
    max_record_size => 16382
}).

%%% Fixture

qmux_test_() ->
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
                    {"stream data across records", fun data_across_records/1},
                    {"sending waits for the peer's credit", fun send_credit/1},
                    {"writes queued together share a record", fun coalesced_writes/1},
                    {"writers see the send queue fill and the session end", fun send_queue/1},
                    {"concurrent writers overshoot the limit by one write",
                        fun concurrent_writers/1},
                    {"a refused write never shows in the send queue", fun refused_write/1},
                    {"dropped writes leave the send queue", fun dropped_writes/1},
                    {"data written before a close goes first", fun close_after_write/1},
                    {"a completed peer stream returns its credit", fun stream_credit/1},
                    {"a reset carries the bytes sent as final size", fun reset_final_size/1},
                    {"QX_PING is answered", fun ping/1},
                    {"the first frame must be QX_TRANSPORT_PARAMETERS", fun first_frame/1},
                    {"the stream window is enforced", fun stream_window/1},
                    {"the connection window is enforced", fun connection_window/1},
                    {"the stream limit is enforced", fun stream_limit/1},
                    {"data out of order is refused", fun out_of_order/1},
                    {"the peer's close ends the session", fun peer_close/1},
                    {"a peer that stops reading is lost", fun stalled_peer/1},
                    {"a peer that sends no parameters is closed", fun silent_peer/1},
                    {"a Ping flood from a peer that reads nothing is bounded", fun ping_flood/1},
                    {"a WebSocket session drains its Close", fun ws_close/1},
                    {"a writer blocked at the close holds it until the send timeout",
                        fun blocked_close/1}
                ]
            ]
        end}.

%% A server session advertising Parameters, owned by the test process, and
%% the client end of its TLS connection, in passive mode.
pair(Credentials, Parameters) ->
    pair(Credentials, Parameters, #{}).

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
    {ok, Session, Queue} = trevrpc_qmux:start(Server, server, self(), Opts#{
        parameters => Parameters
    }),
    erase(Peer),
    put({send_queue, Session}, Queue),
    {Session, Peer}.

%% Writes to the session's stream as its owner would.
send_data(Session, Id, Data, Fin) ->
    trevrpc_qmux:send_data(Session, get({send_queue, Session}), Id, Data, Fin).

%% A pair whose peer sent its parameters, with Peer's overriding the
%% defaults, and read the session's.
connected(Credentials, Peer) ->
    connected(Credentials, ?PARAMETERS, Peer).

connected(Credentials, Local, Peer) ->
    connected(Credentials, Local, Peer, #{}).

connected(Credentials, Local, Peer, Opts) ->
    {Session, Socket} = pair(Credentials, Local, Opts),
    send(Socket, [trevrpc_qmux_frame:transport_parameters(maps:merge(?PARAMETERS, Peer))]),
    ?assertMatch([{transport_parameters, _}], next(Socket)),
    ?assertMatch({connected, _}, event(Session)),
    {Session, Socket}.

send(Socket, Frames) ->
    ok = ssl:send(Socket, trevrpc_qmux_frame:record(Frames)).

%% The frames of the next record from the session.
next(Socket) ->
    Buffer =
        case get(Socket) of
            undefined -> <<>>;
            Saved -> Saved
        end,
    case trevrpc_qmux_frame:decode_record(Buffer, 1 bsl 20) of
        {ok, Record, Rest} ->
            put(Socket, Rest),
            {ok, Frames} = trevrpc_qmux_frame:decode_frames(Record),
            Frames;
        more ->
            case ssl:recv(Socket, 0, 5000) of
                {ok, Data} ->
                    put(Socket, <<Buffer/binary, Data/binary>>),
                    next(Socket);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% The next frame the predicate accepts, skipping the others.
await(Socket, Accept) ->
    await(Socket, Accept, []).

await(Socket, Accept, []) ->
    case next(Socket) of
        {error, Reason} -> error({no_frame, Reason});
        Frames -> await(Socket, Accept, Frames)
    end;
await(Socket, Accept, [Frame | Rest]) ->
    case Accept(Frame) of
        true -> Frame;
        false -> await(Socket, Accept, Rest)
    end.

%% The CONNECTION_CLOSE the session ends with.
close_frame(Socket) ->
    await(Socket, fun(Frame) -> element(1, Frame) =:= connection_close end).

%% The stream data the session sends on Id until its FIN.
read_stream(Socket, Id) ->
    read_stream(Socket, Id, 0, []).

read_stream(Socket, Id, Offset, Acc) ->
    case
        await(Socket, fun(Frame) ->
            element(1, Frame) =:= stream andalso element(2, Frame) =:= Id
        end)
    of
        {stream, Id, Offset, Data, true} ->
            iolist_to_binary(lists:reverse(Acc, [Data]));
        {stream, Id, Offset, Data, false} ->
            read_stream(Socket, Id, Offset + byte_size(Data), [Data | Acc])
    end.

event(Session) ->
    receive
        {trevrpc_qmux, Session, Event} -> Event
    after 5000 -> error(no_event)
    end.

%% Stream data events for Id until the FIN, consumed as they arrive.
received(Session, Id) ->
    received(Session, Id, <<>>).

received(Session, Id, Acc) ->
    case event(Session) of
        {stream_data, Id, Data, Fin} ->
            ok = trevrpc_qmux:consume(Session, Id, byte_size(Data)),
            case Fin of
                true -> <<Acc/binary, Data/binary>>;
                false -> received(Session, Id, <<Acc/binary, Data/binary>>)
            end;
        {stream_opened, Id} ->
            received(Session, Id, Acc)
    end.

transport_error(Name) ->
    trevrpc_qmux_frame:error_code(Name).

%%% Tests

%% A peer stream's data in several records reaches the owner in order, and
%% the reply goes back on it.
data_across_records(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [trevrpc_qmux_frame:stream(0, 0, [<<"hello ">>], false)]),
    send(Socket, [trevrpc_qmux_frame:stream(0, 6, [<<"over ">>], false)]),
    send(Socket, [trevrpc_qmux_frame:stream(0, 11, [<<"records">>], true)]),
    ?assertEqual(<<"hello over records">>, received(Session, 0)),
    ok = send_data(Session, 0, <<"reply">>, true),
    ?assertEqual(<<"reply">>, read_stream(Socket, 0)),
    trevrpc_qmux:close(Session, 0).

%% With 10 bytes of stream credit, 25 bytes go out as 10, then the rest
%% once MAX_STREAM_DATA allows it.
send_credit(Credentials) ->
    {Session, Socket} = connected(Credentials, #{initial_max_stream_data_bidi_local => 10}),
    send(Socket, [trevrpc_qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    Data = binary:copy(<<"d">>, 25),
    ok = send_data(Session, 0, Data, true),
    ?assertEqual({stream, 0, 0, binary:copy(<<"d">>, 10), false}, await(Socket, fun is_stream/1)),
    send(Socket, [trevrpc_qmux_frame:max_stream_data(0, 25)]),
    ?assertEqual({stream, 0, 10, binary:copy(<<"d">>, 15), true}, await(Socket, fun is_stream/1)),
    trevrpc_qmux:close(Session, 0).

is_stream(Frame) -> element(1, Frame) =:= stream.

%% Writes queued while the session is busy go out together, in one record
%% and one STREAM frame.
coalesced_writes(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [trevrpc_qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    ok = sys:suspend(Session),
    _ = [ok = send_data(Session, 0, <<N>>, N =:= 10) || N <- lists:seq(1, 10)],
    ok = sys:resume(Session),
    ?assertEqual([{stream, 0, 0, list_to_binary(lists:seq(1, 10)), true}], next(Socket)),
    trevrpc_qmux:close(Session, 0).

%% Writers count what they write in the send queue themselves, so a full
%% queue refuses them before the session handles it, and a session that
%% ended refuses them too.
send_queue(Credentials) ->
    {Session, Socket} = connected(Credentials, ?PARAMETERS, #{}, #{send_queue_limit => 10}),
    send(Socket, [trevrpc_qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    ok = sys:suspend(Session),
    ok = send_data(Session, 0, binary:copy(<<"q">>, 10), false),
    ?assertEqual({error, send_queue_full}, send_data(Session, 0, <<"r">>, false)),
    ok = sys:resume(Session),
    ?assertEqual({stream, 0, 0, binary:copy(<<"q">>, 10), false}, await(Socket, fun is_stream/1)),
    %% What was sent left the queue.
    ok = send_data(Session, 0, <<"r">>, true),
    ?assertEqual({stream, 0, 10, <<"r">>, true}, await(Socket, fun is_stream/1)),
    trevrpc_qmux:close(Session, 0),
    ?assertMatch({closed, _}, await_closed(Session)),
    ?assertEqual({error, closed}, send_data(Session, 0, <<"late">>, false)).

%% 200 writers of 10 bytes, released together, against a limit of 100,
%% while the session handles nothing: exactly ten writes are taken, as each
%% is taken only while the writes taken before it are under the limit, and
%% the refused writes never show in the queue, as an observer reading it
%% throughout sees.
concurrent_writers(Credentials) ->
    {Session, Socket} = connected(Credentials, ?PARAMETERS, #{}, #{send_queue_limit => 100}),
    send(Socket, [trevrpc_qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    Queue = get({send_queue, Session}),
    Self = self(),
    ok = sys:suspend(Session),
    Writers = [
        spawn(fun() ->
            receive
                go -> ok
            end,
            Self !
                {wrote, self(), trevrpc_qmux:send_data(Session, Queue, 0, <<"cccccccccc">>, false)}
        end)
     || _ <- lists:seq(1, 200)
    ],
    Observer = spawn(fun() -> observe(Queue, 0) end),
    _ = [Writer ! go || Writer <- Writers],
    Results = [
        receive
            {wrote, Writer, Result} -> Result
        after 5000 -> error(no_result)
        end
     || Writer <- Writers
    ],
    ?assertEqual(10, length([ok || ok <- Results])),
    ?assertEqual(190, length([full || {error, send_queue_full} <- Results])),
    ?assertEqual(100, trevrpc_qmux:queued(Queue)),
    Observer ! {stop, self()},
    receive
        {highest, Highest} -> ?assertEqual(100, Highest)
    after 5000 -> error(no_observer)
    end,
    ok = sys:resume(Session),
    ?assertEqual(binary:copy(<<"c">>, 100), read_bytes(Socket, 0, 100)),
    trevrpc_qmux:close(Session, 0).

%% The most bytes the queue showed until told to stop.
observe(Queue, Highest) ->
    receive
        {stop, From} -> From ! {highest, max(Highest, trevrpc_qmux:queued(Queue))}
    after 0 -> observe(Queue, max(Highest, trevrpc_qmux:queued(Queue)))
    end.

%% A write refused for a full queue never touches it: the refused writer,
%% traced, only reads the queue's atomics, so the session can never see
%% bytes that are not coming, and, draining the queue, wakes the writer it
%% refused: the owner that asked for send_ready gets it.
refused_write(Credentials) ->
    {Session, Socket} = connected(Credentials, ?PARAMETERS, #{}, #{send_queue_limit => 10}),
    send(Socket, [trevrpc_qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    Queue = get({send_queue, Session}),
    ok = sys:suspend(Session),
    ok = send_data(Session, 0, binary:copy(<<"q">>, 10), false),
    {Refused, Calls} = traced_atomics(fun() ->
        trevrpc_qmux:send_data(Session, Queue, 0, binary:copy(<<"r">>, 1000), false)
    end),
    ?assertEqual({error, send_queue_full}, Refused),
    ?assertEqual([get, get], Calls),
    ?assertEqual(10, trevrpc_qmux:queued(Queue)),
    ok = trevrpc_qmux:notify_send_ready(Session),
    ok = sys:resume(Session),
    ?assertEqual(send_ready, event(Session)),
    ?assertEqual(0, trevrpc_qmux:queued(Queue)),
    ?assertEqual({stream, 0, 0, binary:copy(<<"q">>, 10), false}, await(Socket, fun is_stream/1)),
    trevrpc_qmux:close(Session, 0).

%% What Fun returns in a process of its own, and the atomics functions it
%% called there, in order.
traced_atomics(Fun) ->
    Self = self(),
    Writer = spawn(fun() ->
        receive
            go -> Self ! {done, self(), Fun()}
        end
    end),
    _ = erlang:trace_pattern({atomics, '_', '_'}, true, [global]),
    1 = erlang:trace(Writer, true, [call]),
    Writer ! go,
    Result =
        receive
            {done, Writer, R} -> R
        after 5000 -> error(no_result)
        end,
    Ref = erlang:trace_delivered(Writer),
    receive
        {trace_delivered, Writer, Ref} -> ok
    after 5000 -> error(trace_not_delivered)
    end,
    _ = erlang:trace_pattern({atomics, '_', '_'}, false, [global]),
    {Result, traced_calls(Writer, [])}.

traced_calls(Writer, Acc) ->
    receive
        {trace, Writer, call, {atomics, Function, _Args}} -> traced_calls(Writer, [Function | Acc])
    after 0 -> lists:reverse(Acc)
    end.

%% Size bytes of stream Id's data from the session, from offset 0.
read_bytes(Socket, Id, Size) ->
    read_bytes(Socket, Id, Size, <<>>).

read_bytes(_Socket, _Id, Size, Acc) when byte_size(Acc) >= Size ->
    Acc;
read_bytes(Socket, Id, Size, Acc) ->
    Offset = byte_size(Acc),
    {stream, Id, Offset, Data, _Fin} = await(Socket, fun(Frame) ->
        element(1, Frame) =:= stream andalso element(2, Frame) =:= Id
    end),
    read_bytes(Socket, Id, Size, <<Acc/binary, Data/binary>>).

%% Writes the session drops, to a stream that never opened or that was
%% reset, and the data a reset drops, are taken off the send queue: with a
%% limit of 10, each write of 10 bytes is taken only if the queue emptied.
dropped_writes(Credentials) ->
    {Session, Socket} = connected(
        Credentials, ?PARAMETERS, #{initial_max_stream_data_bidi_local => 0}, #{
            send_queue_limit => 10
        }
    ),
    send(Socket, [trevrpc_qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    Ten = binary:copy(<<"t">>, 10),
    %% A stream that never opened.
    ok = send_data(Session, 8, Ten, false),
    sync(Session),
    %% Data without credit stays queued, then a reset drops it.
    ok = send_data(Session, 0, Ten, false),
    sync(Session),
    ?assertEqual({error, send_queue_full}, send_data(Session, 0, <<"x">>, false)),
    ok = trevrpc_qmux:reset_stream(Session, 0, 1),
    %% A stream that was reset, twice.
    ok = send_data(Session, 0, Ten, false),
    sync(Session),
    ok = send_data(Session, 0, Ten, false),
    sync(Session),
    ok = send_data(Session, 0, Ten, false),
    trevrpc_qmux:close(Session, 0).

%% Returns once the session handled every write sent to it before.
sync(Session) ->
    {ok, _} = trevrpc_qmux:peername(Session),
    ok.

%% A write and then a close, queued while the session is busy: the data
%% goes out before the CONNECTION_CLOSE.
close_after_write(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [trevrpc_qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    ok = sys:suspend(Session),
    ok = send_data(Session, 0, <<"last">>, true),
    trevrpc_qmux:close(Session, 0),
    ok = sys:resume(Session),
    ?assertEqual(
        {stream, 0, 0, <<"last">>, true},
        await(Socket, fun(Frame) ->
            element(1, Frame) =:= stream orelse element(1, Frame) =:= connection_close
        end)
    ),
    ?assertMatch({connection_close, application, 0, _}, close_frame(Socket)).

%% Once both directions of the peer's only stream end, the session allows
%% another.
stream_credit(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [trevrpc_qmux_frame:stream(0, 0, [<<"one">>], true)]),
    ?assertEqual(<<"one">>, received(Session, 0)),
    ok = send_data(Session, 0, <<"done">>, true),
    ?assertEqual(
        {max_streams, bidi, 2}, await(Socket, fun(Frame) -> element(1, Frame) =:= max_streams end)
    ),
    send(Socket, [trevrpc_qmux_frame:stream(4, 0, [<<"two">>], true)]),
    ?assertEqual(<<"two">>, received(Session, 4)),
    trevrpc_qmux:close(Session, 0).

%% A reset after data that went out, and data still queued for lack of
%% credit, has the bytes sent as its final size.
reset_final_size(Credentials) ->
    {Session, Socket} = connected(Credentials, #{initial_max_stream_data_bidi_local => 4}),
    send(Socket, [trevrpc_qmux_frame:stream(0, 0, [<<"x">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ok = send_data(Session, 0, <<"abcdefgh">>, false),
    ?assertEqual({stream, 0, 0, <<"abcd">>, false}, await(Socket, fun is_stream/1)),
    ok = trevrpc_qmux:reset_stream(Session, 0, 7),
    ?assertEqual(
        {reset_stream, 0, 7, 4}, await(Socket, fun(Frame) -> element(1, Frame) =:= reset_stream end)
    ),
    trevrpc_qmux:close(Session, 0).

ping(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [trevrpc_qmux_frame:ping(1)]),
    ?assertEqual(
        {ping_response, 1}, await(Socket, fun(Frame) -> element(1, Frame) =:= ping_response end)
    ),
    %% A sequence number that does not increase is a protocol violation.
    send(Socket, [trevrpc_qmux_frame:ping(1)]),
    ?assertMatch({connection_close, transport, 16#0a, _, _}, close_frame(Socket)),
    ?assertMatch({closed, _}, event(Session)).

first_frame(Credentials) ->
    {Session, Socket} = pair(Credentials, ?PARAMETERS),
    send(Socket, [trevrpc_qmux_frame:max_data(10)]),
    ?assertMatch({connection_close, transport, 16#0a, _, _}, close_frame(Socket)),
    ?assertMatch({closed, _}, event(Session)).

stream_window(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [trevrpc_qmux_frame:stream(0, 0, [binary:copy(<<"w">>, 101)], false)]),
    ?assertMatch({connection_close, transport, 3, _, _}, close_frame(Socket)),
    ?assertMatch({closed, _}, await_closed(Session)).

%% Two streams within their windows, together over the connection's.
connection_window(Credentials) ->
    {Session, Socket} = connected(
        Credentials,
        maps:merge(?PARAMETERS, #{initial_max_data => 150, initial_max_streams_bidi => 2}),
        #{}
    ),
    send(Socket, [
        trevrpc_qmux_frame:stream(0, 0, [binary:copy(<<"a">>, 100)], false),
        trevrpc_qmux_frame:stream(4, 0, [binary:copy(<<"b">>, 51)], false)
    ]),
    ?assertMatch({connection_close, transport, 3, _, _}, close_frame(Socket)),
    ?assertMatch({closed, _}, await_closed(Session)).

stream_limit(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [trevrpc_qmux_frame:stream(4, 0, [<<"second">>], false)]),
    ?assertEqual(transport_error(stream_limit), element(3, close_frame(Socket))),
    ?assertMatch({closed, _}, await_closed(Session)).

out_of_order(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [trevrpc_qmux_frame:stream(0, 0, [<<"ab">>], false)]),
    send(Socket, [trevrpc_qmux_frame:stream(0, 3, [<<"d">>], false)]),
    ?assertEqual(transport_error(protocol_violation), element(3, close_frame(Socket))),
    ?assertMatch({closed, _}, await_closed(Session)).

peer_close(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    Monitor = monitor(process, Session),
    send(Socket, [trevrpc_qmux_frame:application_close(9, <<"bye">>)]),
    ?assertMatch({closed, _}, await_closed(Session)),
    receive
        {'DOWN', Monitor, process, Session, _} -> ok
    after 5000 -> error(session_alive)
    end.

%% The closed event, past the stream events before it.
await_closed(Session) ->
    case event(Session) of
        {closed, _} = Closed -> Closed;
        _ -> await_closed(Session)
    end.

%% With credit for all of it, a peer that reads none of 64 MiB fills the
%% socket's buffers: the blocked write times out and the session ends.
stalled_peer(Credentials) ->
    Large = 1 bsl 30,
    Peer = #{
        initial_max_data => Large,
        initial_max_stream_data_bidi_local => Large,
        initial_max_stream_data_bidi_remote => Large
    },
    {Session, Socket} = connected(Credentials, ?PARAMETERS, Peer, #{send_timeout => 300}),
    send(Socket, [trevrpc_qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    Chunk = binary:copy(<<"z">>, 1 bsl 20),
    _ = [
        trevrpc_qmux:send_data_async(Session, get({send_queue, Session}), 0, Chunk, false)
     || _ <- lists:seq(1, 64)
    ],
    ?assertMatch({closed, _}, await_closed(Session)).

%% A WebSocket peer that sends Pings and reads nothing. The writer, held
%% still, takes its window of a stream's data and no more; then 1000 Pings
%% add nothing to it, and once it runs again exactly one Pong, to the
%% latest Ping, reaches the peer with the data.
ping_flood(Credentials) ->
    {Session, Socket} = pair(Credentials, maps:put(initial_max_streams_bidi, 3, ?PARAMETERS), #{
        framing => websocket
    }),
    Large = 1 bsl 30,
    Parameters = trevrpc_qmux_frame:transport_parameters(
        maps:merge(?PARAMETERS, #{
            initial_max_data => Large,
            initial_max_stream_data_bidi_local => Large,
            initial_max_stream_data_bidi_remote => Large
        })
    ),
    ok = ssl:send(Socket, ws_record([Parameters])),
    ?assertMatch({connected, _}, event(Session)),
    ok = ssl:send(Socket, ws_record([trevrpc_qmux_frame:stream(0, 0, [<<"go">>], false)])),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    Writer = writer(Session),
    erlang:suspend_process(Writer),
    Size = 1 bsl 20,
    ok = trevrpc_qmux:send_data_async(
        Session, get({send_queue, Session}), 0, binary:copy(<<"d">>, Size), false
    ),
    %% A stream the peer opens is a marker: its event follows every Ping
    %% sent before it.
    Marker = fun(Id) ->
        ok = ssl:send(Socket, ws_record([trevrpc_qmux_frame:stream(Id, 0, [<<"m">>], false)])),
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
    trevrpc_qmux:close(Session, 0).

%% The session's writer, once it started: the process the session
%% monitors that runs the writer's loop.
writer(Session) ->
    {monitors, Monitors} = process_info(Session, monitors),
    Writers = [
        Pid
     || {process, Pid} <- Monitors,
        {current_function, {trevrpc_qmux, Function, _}} <- [process_info(Pid, current_function)],
        lists:member(Function, [writer_loop, write_batch, write_out])
    ],
    case Writers of
        [Writer] ->
            Writer;
        [] ->
            timer:sleep(1),
            writer(Session)
    end.

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
        {stream, 0, _, Bytes, _} <- element(2, trevrpc_qmux_frame:decode_frames(Record))
    ]),
    ws_pongs(Socket, Size - Read, Next, Pongs + length([pong || {pong, _} <- Messages])).

%% A WebSocket session that ends on its own, here as the peer sent no
%% parameters, still sends its Close: the session waits for its writer,
%% held still until then, to drain it before the socket closes.
ws_close(Credentials) ->
    {Session, Socket} = pair(Credentials, ?PARAMETERS, #{
        framing => websocket, handshake_timeout => 200
    }),
    Writer = writer(Session),
    erlang:suspend_process(Writer),
    ?assertEqual({closed, handshake_timeout}, event(Session)),
    ok = until_ended(Session),
    erlang:resume_process(Writer),
    Messages = ws_messages(Socket, ws_frame:init_parser(#{role => client})),
    ?assertMatch([_], [Close || Close <- Messages, is_close(Close)]).

%% Returns once the session waits for its writer as it ends, or ended.
until_ended(Session) ->
    case process_info(Session, current_function) of
        {current_function, {trevrpc_qmux, stop_writer, 1}} ->
            ok;
        undefined ->
            ok;
        _ ->
            timer:sleep(1),
            until_ended(Session)
    end.

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

%% A peer that reads nothing, with credit for all it is sent: the writer
%% blocks. A close then ends the session once the blocked write times out,
%% after the close timeout and the moment the writer gets to drain, instead
%% of waiting on the writer, or on a flush, for good.
blocked_close(Credentials) ->
    Large = 1 bsl 30,
    Peer = #{
        initial_max_data => Large,
        initial_max_stream_data_bidi_local => Large,
        initial_max_stream_data_bidi_remote => Large
    },
    {Session, Socket} = connected(Credentials, ?PARAMETERS, Peer, #{send_timeout => 1500}),
    send(Socket, [trevrpc_qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    Chunk = binary:copy(<<"z">>, 1 bsl 20),
    _ = [
        trevrpc_qmux:send_data_async(Session, get({send_queue, Session}), 0, Chunk, false)
     || _ <- lists:seq(1, 64)
    ],
    Monitor = monitor(process, Session),
    trevrpc_qmux:close(Session, 0),
    receive
        {'DOWN', Monitor, process, Session, _} -> ok
    after 8000 -> error({session_held, process_info(Session, current_stacktrace)})
    end.

silent_peer(Credentials) ->
    {Session, _Socket} = pair(Credentials, ?PARAMETERS, #{handshake_timeout => 200}),
    ?assertEqual({closed, handshake_timeout}, event(Session)).
