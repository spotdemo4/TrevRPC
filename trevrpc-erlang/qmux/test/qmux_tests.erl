%% A QMux session against a peer written by hand: the test process owns the
%% session and plays the peer over a raw TLS or TCP connection.
-module(qmux_tests).

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
            credentials()
        end,
        fun(_) -> logger:set_primary_config(level, notice) end, fun(Credentials) ->
            [
                {
                    lists:flatten([Name, " over ", atom_to_list(Transport)]),
                    {timeout, 30, fun() -> Test({Transport, Credentials}) end}
                }
             || Transport <- [ssl, tcp],
                {Name, Test} <- tests()
            ]
        end}.

tests() ->
    [
        {"stream data across records", fun data_across_records/1},
        {"sending waits for the peer's credit", fun send_credit/1},
        {"writes queued together share a record", fun coalesced_writes/1},
        {"writers see the send queue fill and the session end", fun send_queue/1},
        {"concurrent writers overshoot the limit by one write", fun concurrent_writers/1},
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
        {"a writer blocked at the close holds it until the send timeout", fun blocked_close/1},
        {"auto streams return credit as data is delivered", fun auto_credit/1},
        {"auto streams hold credit back while the owner is behind", fun owner_behind/1},
        {"a stream switches between auto and manual", fun switch_flow_control/1},
        {"the peer's unidirectional streams are receive-only", fun peer_uni_streams/1},
        {"our unidirectional streams are send-only", fun local_uni_streams/1},
        {"unidirectional streams wait for the peer's limit", fun uni_stream_limit/1},
        {"datagrams go both ways", fun datagrams/1},
        {"datagrams are refused unless negotiated", fun datagrams_refused/1},
        {"datagrams go to a peer that accepts them only", fun datagrams_one_way/1},
        {"datagrams take turns with stream data", fun datagrams_and_streams/1}
    ].

%% A server certificate for localhost.
credentials() ->
    San = {'Extension', {2, 5, 29, 17}, false, [{dNSName, "localhost"}]},
    #{cert := Cert, key := Key} = public_key:pkix_test_root_cert("localhost", [
        {key, {namedCurve, secp256r1}}, {digest, sha256}, {extensions, [San]}
    ]),
    #{cert => Cert, key => Key}.

%% A server session advertising Parameters, owned by the test process, over
%% TLS or TCP, and the peer's end of its connection, in passive mode, as
%% `{Module, Socket}' with Module `ssl' or `gen_tcp'.
pair(Setup, Parameters) ->
    pair(Setup, Parameters, #{}).

pair({ssl, #{cert := Cert, key := Key}}, Parameters, Opts) ->
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
    start(qmux_ssl, Server, {ssl, Peer}, Parameters, Opts);
pair({tcp, _Credentials}, Parameters, Opts) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {ip, {127, 0, 0, 1}}]),
    {ok, Port} = inet:port(Listen),
    {ok, Peer} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}], 5000),
    {ok, Server} = gen_tcp:accept(Listen, 5000),
    ok = gen_tcp:close(Listen),
    start(qmux_tcp, Server, {gen_tcp, Peer}, Parameters, Opts).

start(Transport, Server, Peer, Parameters, Opts) ->
    {ok, Session, Queue} = qmux:start({Transport, Server}, server, self(), Opts#{
        parameters => Parameters,
        stream_flow_control => maps:get(stream_flow_control, Opts, manual)
    }),
    erase(Peer),
    put({send_queue, Session}, Queue),
    {Session, Peer}.

%% Writes to the session's stream as its owner would.
send_data(Session, Id, Data, Fin) ->
    qmux:send_data(Session, get({send_queue, Session}), Id, Data, Fin).

%% A pair whose peer sent its parameters, with Peer's overriding the
%% defaults, and read the session's.
connected(Credentials, Peer) ->
    connected(Credentials, ?PARAMETERS, Peer).

connected(Credentials, Local, Peer) ->
    connected(Credentials, Local, Peer, #{}).

connected(Credentials, Local, Peer, Opts) ->
    {Session, Socket} = pair(Credentials, Local, Opts),
    send(Socket, [qmux_frame:transport_parameters(maps:merge(?PARAMETERS, Peer))]),
    ?assertMatch([{transport_parameters, _}], next(Socket)),
    ?assertMatch({connected, _}, event(Session)),
    {Session, Socket}.

send({Module, Socket}, Frames) ->
    ok = Module:send(Socket, qmux_frame:record(Frames)).

%% The frames of the next record from the session.
next(Socket) ->
    Buffer =
        case get(Socket) of
            undefined -> <<>>;
            Saved -> Saved
        end,
    case qmux_frame:decode_record(Buffer, 1 bsl 20) of
        {ok, Record, Rest} ->
            put(Socket, Rest),
            {ok, Frames} = qmux_frame:decode_frames(Record, 1 bsl 20),
            Frames;
        more ->
            {Module, Raw} = Socket,
            case Module:recv(Raw, 0, 5000) of
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
        {qmux, Session, Event} -> Event
    after 5000 -> error(no_event)
    end.

%% Stream data events for Id until the FIN, consumed as they arrive.
received(Session, Id) ->
    received(Session, Id, <<>>).

received(Session, Id, Acc) ->
    case event(Session) of
        {stream_data, Id, Data, Fin} ->
            ok = qmux:consume(Session, Id, byte_size(Data)),
            case Fin of
                true -> <<Acc/binary, Data/binary>>;
                false -> received(Session, Id, <<Acc/binary, Data/binary>>)
            end;
        {stream_opened, Id} ->
            received(Session, Id, Acc)
    end.

transport_error(Name) ->
    qmux_frame:error_code(Name).

%%% Tests

%% A peer stream's data in several records reaches the owner in order, and
%% the reply goes back on it.
data_across_records(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [qmux_frame:stream(0, 0, [<<"hello ">>], false)]),
    send(Socket, [qmux_frame:stream(0, 6, [<<"over ">>], false)]),
    send(Socket, [qmux_frame:stream(0, 11, [<<"records">>], true)]),
    ?assertEqual(<<"hello over records">>, received(Session, 0)),
    ok = send_data(Session, 0, <<"reply">>, true),
    ?assertEqual(<<"reply">>, read_stream(Socket, 0)),
    qmux:close(Session, 0).

%% With 10 bytes of stream credit, 25 bytes go out as 10, then the rest
%% once MAX_STREAM_DATA allows it.
send_credit(Credentials) ->
    {Session, Socket} = connected(Credentials, #{initial_max_stream_data_bidi_local => 10}),
    send(Socket, [qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    Data = binary:copy(<<"d">>, 25),
    ok = send_data(Session, 0, Data, true),
    ?assertEqual({stream, 0, 0, binary:copy(<<"d">>, 10), false}, await(Socket, fun is_stream/1)),
    send(Socket, [qmux_frame:max_stream_data(0, 25)]),
    ?assertEqual({stream, 0, 10, binary:copy(<<"d">>, 15), true}, await(Socket, fun is_stream/1)),
    qmux:close(Session, 0).

is_stream(Frame) -> element(1, Frame) =:= stream.

%% Writes queued while the session is busy go out together, in one record
%% and one STREAM frame.
coalesced_writes(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    ok = sys:suspend(Session),
    _ = [ok = send_data(Session, 0, <<N>>, N =:= 10) || N <- lists:seq(1, 10)],
    ok = sys:resume(Session),
    ?assertEqual([{stream, 0, 0, list_to_binary(lists:seq(1, 10)), true}], next(Socket)),
    qmux:close(Session, 0).

%% Writers count what they write in the send queue themselves, so a full
%% queue refuses them before the session handles it, and a session that
%% ended refuses them too.
send_queue(Credentials) ->
    {Session, Socket} = connected(Credentials, ?PARAMETERS, #{}, #{send_queue_limit => 10}),
    send(Socket, [qmux_frame:stream(0, 0, [<<"go">>], false)]),
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
    qmux:close(Session, 0),
    ?assertMatch({closed, _}, await_closed(Session)),
    ?assertEqual({error, closed}, send_data(Session, 0, <<"late">>, false)).

%% 200 writers of 10 bytes, released together, against a limit of 100,
%% while the session handles nothing: exactly ten writes are taken, as each
%% is taken only while the writes taken before it are under the limit, and
%% the refused writes never show in the queue, as an observer reading it
%% throughout sees.
concurrent_writers(Credentials) ->
    {Session, Socket} = connected(Credentials, ?PARAMETERS, #{}, #{send_queue_limit => 100}),
    send(Socket, [qmux_frame:stream(0, 0, [<<"go">>], false)]),
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
                {wrote, self(), qmux:send_data(Session, Queue, 0, <<"cccccccccc">>, false)}
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
    ?assertEqual(100, qmux:queued(Queue)),
    Observer ! {stop, self()},
    receive
        {highest, Highest} -> ?assertEqual(100, Highest)
    after 5000 -> error(no_observer)
    end,
    ok = sys:resume(Session),
    ?assertEqual(binary:copy(<<"c">>, 100), read_bytes(Socket, 0, 100)),
    qmux:close(Session, 0).

%% The most bytes the queue showed until told to stop.
observe(Queue, Highest) ->
    receive
        {stop, From} -> From ! {highest, max(Highest, qmux:queued(Queue))}
    after 0 -> observe(Queue, max(Highest, qmux:queued(Queue)))
    end.

%% A write refused for a full queue never touches it: the refused writer,
%% traced, only reads the queue's atomics, so the session can never see
%% bytes that are not coming, and, draining the queue, wakes the writer it
%% refused: the owner that asked for send_ready gets it.
refused_write(Credentials) ->
    {Session, Socket} = connected(Credentials, ?PARAMETERS, #{}, #{send_queue_limit => 10}),
    send(Socket, [qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    Queue = get({send_queue, Session}),
    ok = sys:suspend(Session),
    ok = send_data(Session, 0, binary:copy(<<"q">>, 10), false),
    {Refused, Calls} = traced_atomics(fun() ->
        qmux:send_data(Session, Queue, 0, binary:copy(<<"r">>, 1000), false)
    end),
    ?assertEqual({error, send_queue_full}, Refused),
    ?assertEqual([get, get], Calls),
    ?assertEqual(10, qmux:queued(Queue)),
    ok = qmux:notify_send_ready(Session),
    ok = sys:resume(Session),
    ?assertEqual(send_ready, event(Session)),
    ?assertEqual(0, qmux:queued(Queue)),
    ?assertEqual({stream, 0, 0, binary:copy(<<"q">>, 10), false}, await(Socket, fun is_stream/1)),
    qmux:close(Session, 0).

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
    send(Socket, [qmux_frame:stream(0, 0, [<<"go">>], false)]),
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
    ok = qmux:reset_stream(Session, 0, 1),
    %% A stream that was reset, twice.
    ok = send_data(Session, 0, Ten, false),
    sync(Session),
    ok = send_data(Session, 0, Ten, false),
    sync(Session),
    ok = send_data(Session, 0, Ten, false),
    qmux:close(Session, 0).

%% Returns once the session handled every write sent to it before.
sync(Session) ->
    {ok, _} = qmux:peername(Session),
    ok.

%% A write and then a close, queued while the session is busy: the data
%% goes out before the CONNECTION_CLOSE.
close_after_write(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    ok = sys:suspend(Session),
    ok = send_data(Session, 0, <<"last">>, true),
    qmux:close(Session, 0),
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
    send(Socket, [qmux_frame:stream(0, 0, [<<"one">>], true)]),
    ?assertEqual(<<"one">>, received(Session, 0)),
    ok = send_data(Session, 0, <<"done">>, true),
    ?assertEqual(
        {max_streams, bidi, 2}, await(Socket, fun(Frame) -> element(1, Frame) =:= max_streams end)
    ),
    send(Socket, [qmux_frame:stream(4, 0, [<<"two">>], true)]),
    ?assertEqual(<<"two">>, received(Session, 4)),
    qmux:close(Session, 0).

%% A reset after data that went out, and data still queued for lack of
%% credit, has the bytes sent as its final size.
reset_final_size(Credentials) ->
    {Session, Socket} = connected(Credentials, #{initial_max_stream_data_bidi_local => 4}),
    send(Socket, [qmux_frame:stream(0, 0, [<<"x">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ok = send_data(Session, 0, <<"abcdefgh">>, false),
    ?assertEqual({stream, 0, 0, <<"abcd">>, false}, await(Socket, fun is_stream/1)),
    ok = qmux:reset_stream(Session, 0, 7),
    ?assertEqual(
        {reset_stream, 0, 7, 4}, await(Socket, fun(Frame) -> element(1, Frame) =:= reset_stream end)
    ),
    qmux:close(Session, 0).

ping(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [qmux_frame:ping(1)]),
    ?assertEqual(
        {ping_response, 1}, await(Socket, fun(Frame) -> element(1, Frame) =:= ping_response end)
    ),
    %% A sequence number that does not increase is a protocol violation.
    send(Socket, [qmux_frame:ping(1)]),
    ?assertMatch({connection_close, transport, 16#0a, _, _}, close_frame(Socket)),
    ?assertMatch({closed, _}, event(Session)).

first_frame(Credentials) ->
    {Session, Socket} = pair(Credentials, ?PARAMETERS),
    send(Socket, [qmux_frame:max_data(10)]),
    ?assertMatch({connection_close, transport, 16#0a, _, _}, close_frame(Socket)),
    ?assertMatch({closed, _}, event(Session)).

stream_window(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [qmux_frame:stream(0, 0, [binary:copy(<<"w">>, 101)], false)]),
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
        qmux_frame:stream(0, 0, [binary:copy(<<"a">>, 100)], false),
        qmux_frame:stream(4, 0, [binary:copy(<<"b">>, 51)], false)
    ]),
    ?assertMatch({connection_close, transport, 3, _, _}, close_frame(Socket)),
    ?assertMatch({closed, _}, await_closed(Session)).

stream_limit(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [qmux_frame:stream(4, 0, [<<"second">>], false)]),
    ?assertEqual(transport_error(stream_limit), element(3, close_frame(Socket))),
    ?assertMatch({closed, _}, await_closed(Session)).

out_of_order(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [qmux_frame:stream(0, 0, [<<"ab">>], false)]),
    send(Socket, [qmux_frame:stream(0, 3, [<<"d">>], false)]),
    ?assertEqual(transport_error(protocol_violation), element(3, close_frame(Socket))),
    ?assertMatch({closed, _}, await_closed(Session)).

peer_close(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    Monitor = monitor(process, Session),
    send(Socket, [qmux_frame:application_close(9, <<"bye">>)]),
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
    send(Socket, [qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    Chunk = binary:copy(<<"z">>, 1 bsl 20),
    _ = [
        qmux:send_data_async(Session, get({send_queue, Session}), 0, Chunk, false)
     || _ <- lists:seq(1, 64)
    ],
    ?assertMatch({closed, _}, await_closed(Session)).

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
    send(Socket, [qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    Chunk = binary:copy(<<"z">>, 1 bsl 20),
    _ = [
        qmux:send_data_async(Session, get({send_queue, Session}), 0, Chunk, false)
     || _ <- lists:seq(1, 64)
    ],
    Monitor = monitor(process, Session),
    qmux:close(Session, 0),
    receive
        {'DOWN', Monitor, process, Session, _} -> ok
    after 8000 -> error({session_held, process_info(Session, current_stacktrace)})
    end.

silent_peer(Credentials) ->
    {Session, _Socket} = pair(Credentials, ?PARAMETERS, #{handshake_timeout => 200}),
    ?assertEqual({closed, handshake_timeout}, event(Session)).

%% Waits Ms milliseconds for a frame from the session, and fails if one
%% arrives.
quiet({Module, Raw} = Socket, Ms) ->
    ?assertEqual(<<>>, get_buffer(Socket)),
    ?assertEqual({error, timeout}, Module:recv(Raw, 0, Ms)).

get_buffer(Socket) ->
    case get(Socket) of
        undefined -> <<>>;
        Saved -> Saved
    end.

is_frame(Type) -> fun(Frame) -> element(1, Frame) =:= Type end.

%% An auto stream returns credit for what is delivered: past half its
%% window, a MAX_STREAM_DATA goes out without consume/3, which is a no-op.
auto_credit(Setup) ->
    {Session, Socket} = connected(Setup, ?PARAMETERS, #{}, #{stream_flow_control => auto}),
    send(Socket, [qmux_frame:stream(0, 0, [binary:copy(<<"a">>, 60)], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, _, false}, event(Session)),
    ?assertEqual({max_stream_data, 0, 160}, await(Socket, is_frame(max_stream_data))),
    ?assertEqual(ok, qmux:consume(Session, 0, 60)),
    qmux:close(Session, 0).

%% While the owner's mailbox holds more than max_owner_queue messages, an
%% auto stream's credit waits and datagrams are dropped; once it catches up,
%% the credit goes out.
owner_behind(Setup) ->
    %% What earlier tests left in this process's mailbox would count too.
    flush_mailbox(),
    Local = maps:merge(?PARAMETERS, #{max_datagram_frame_size => 100}),
    {Session, Socket} = connected(Setup, Local, #{}, #{
        stream_flow_control => auto, max_owner_queue => 5
    }),
    _ = [self() ! {filler, N} || N <- lists:seq(1, 10)],
    send(Socket, [
        qmux_frame:stream(0, 0, [binary:copy(<<"a">>, 60)], false),
        qmux_frame:datagram(<<"dropped">>)
    ]),
    quiet(Socket, 100),
    _ = [
        receive
            {filler, N} -> ok
        end
     || N <- lists:seq(1, 10)
    ],
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, _, false}, event(Session)),
    ?assertEqual({max_stream_data, 0, 160}, await(Socket, is_frame(max_stream_data))),
    send(Socket, [qmux_frame:datagram(<<"kept">>)]),
    ?assertEqual({datagram, <<"kept">>}, event(Session)),
    qmux:close(Session, 0).

flush_mailbox() ->
    receive
        _ -> flush_mailbox()
    after 0 -> ok
    end.

%% A manual stream switched to auto returns the credit for what was
%% delivered at once; switched back, it waits for consume/3 again.
switch_flow_control(Setup) ->
    {Session, Socket} = connected(Setup, #{}),
    send(Socket, [qmux_frame:stream(0, 0, [binary:copy(<<"a">>, 60)], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, _, false}, event(Session)),
    quiet(Socket, 50),
    ok = qmux:set_stream_flow_control(Session, 0, auto),
    ?assertEqual({max_stream_data, 0, 160}, await(Socket, is_frame(max_stream_data))),
    ok = qmux:set_stream_flow_control(Session, 0, manual),
    send(Socket, [qmux_frame:stream(0, 60, [binary:copy(<<"b">>, 60)], false)]),
    ?assertMatch({stream_data, 0, _, false}, event(Session)),
    quiet(Socket, 50),
    ok = qmux:consume(Session, 0, 60),
    ?assertEqual({max_stream_data, 0, 220}, await(Socket, is_frame(max_stream_data))),
    ?assertEqual({error, unknown_stream}, qmux:set_stream_flow_control(Session, 4, auto)),
    qmux:close(Session, 0).

%% The peer opens a unidirectional stream within our limit and finishes it,
%% which allows another; writes to it are dropped, and STOP_SENDING for it,
%% as it has no sending side, is a STREAM_STATE_ERROR.
peer_uni_streams(Setup) ->
    Local = maps:merge(?PARAMETERS, #{
        initial_max_streams_uni => 1, initial_max_stream_data_uni => 100
    }),
    {Session, Socket} = connected(Setup, Local, #{}),
    send(Socket, [qmux_frame:stream(2, 0, [<<"uni">>], true)]),
    ?assertEqual({stream_opened, 2}, event(Session)),
    ?assertEqual({stream_data, 2, <<"uni">>, true}, event(Session)),
    %% Its receiving side ended, and it has no other: it is gone.
    ?assertEqual({error, unknown_stream}, qmux:consume(Session, 2, 3)),
    ok = send_data(Session, 2, <<"dropped">>, true),
    ?assertEqual({max_streams, uni, 2}, await(Socket, is_frame(max_streams))),
    send(Socket, [qmux_frame:stream(6, 0, [<<"more">>], false)]),
    ?assertEqual({stream_opened, 6}, event(Session)),
    ?assertMatch({stream_data, 6, <<"more">>, false}, event(Session)),
    send(Socket, [qmux_frame:stop_sending(6, 0)]),
    ?assertEqual(transport_error(stream_state), element(3, close_frame(Socket))),
    ?assertMatch({closed, _}, await_closed(Session)).

%% Our unidirectional stream sends as the peer's credit allows; STREAM
%% frames from the peer on it, as it has no receiving side, are a
%% STREAM_STATE_ERROR.
local_uni_streams(Setup) ->
    {Session, Socket} = connected(Setup, #{
        initial_max_streams_uni => 1, initial_max_stream_data_uni => 10
    }),
    {ok, 3} = qmux:open_unidirectional_stream(Session),
    ok = send_data(Session, 3, binary:copy(<<"u">>, 25), true),
    ?assertEqual({stream, 3, 0, binary:copy(<<"u">>, 10), false}, await(Socket, fun is_stream/1)),
    send(Socket, [qmux_frame:max_stream_data(3, 25)]),
    ?assertEqual({stream, 3, 10, binary:copy(<<"u">>, 15), true}, await(Socket, fun is_stream/1)),
    send(Socket, [qmux_frame:stream(3, 0, [<<"no">>], false)]),
    ?assertEqual(transport_error(stream_state), element(3, close_frame(Socket))),
    ?assertMatch({closed, _}, await_closed(Session)).

%% Without stream credit, opening a unidirectional stream is refused, and
%% `{streams_available, uni}' follows the peer's MAX_STREAMS.
uni_stream_limit(Setup) ->
    {Session, Socket} = connected(Setup, #{initial_max_streams_uni => 1}),
    {ok, 3} = qmux:open_unidirectional_stream(Session),
    ?assertEqual({error, stream_limit}, qmux:open_unidirectional_stream(Session)),
    %% The bidirectional limit is another.
    {ok, 1} = qmux:open_stream(Session),
    send(Socket, [qmux_frame:max_streams(uni, 2)]),
    ?assertEqual({streams_available, uni}, event(Session)),
    ?assertEqual({ok, 7}, qmux:open_unidirectional_stream(Session)),
    qmux:close(Session, 0).

%% With max_datagram_frame_size on both ends, datagrams go both ways, up to
%% the peer's limit; a larger one from the peer is a PROTOCOL_VIOLATION.
datagrams(Setup) ->
    Local = maps:merge(?PARAMETERS, #{max_datagram_frame_size => 100}),
    {Session, Socket} = connected(Setup, Local, #{max_datagram_frame_size => 50}),
    send(Socket, [qmux_frame:datagram(<<"ping">>)]),
    ?assertEqual({datagram, <<"ping">>}, event(Session)),
    ?assertEqual(48, qmux:datagram_max_size(Session)),
    Largest = binary:copy(<<"d">>, 48),
    ok = qmux:send_datagram(Session, Largest),
    ?assertEqual({datagram, Largest}, await(Socket, is_frame(datagram))),
    ?assertEqual({error, datagram_too_large}, qmux:send_datagram(Session, [Largest, <<"d">>])),
    send(Socket, [qmux_frame:datagram(binary:copy(<<"x">>, 99))]),
    ?assertEqual(transport_error(protocol_violation), element(3, close_frame(Socket))),
    ?assertMatch({closed, _}, await_closed(Session)).

%% Datagrams are refused before the peer's parameters arrive, and go each
%% way only to an end that advertised max_datagram_frame_size: with neither
%% end advertising it, the session sends none and refuses the peer's.
datagrams_refused(Setup) ->
    {Unconnected, _} = pair(Setup, ?PARAMETERS),
    ?assertEqual({error, not_connected}, qmux:send_datagram(Unconnected, <<"early">>)),
    ?assertEqual(0, qmux:datagram_max_size(Unconnected)),
    qmux:close(Unconnected, 0),
    {Session, Socket} = connected(Setup, #{}),
    ?assertEqual({error, datagrams_not_supported}, qmux:send_datagram(Session, <<"no">>)),
    ?assertEqual(0, qmux:datagram_max_size(Session)),
    send(Socket, [qmux_frame:datagram(<<"no">>)]),
    ?assertEqual(transport_error(protocol_violation), element(3, close_frame(Socket))),
    ?assertMatch({closed, _}, await_closed(Session)).

%% A session that advertised no max_datagram_frame_size still sends
%% datagrams to a peer that did (RFC 9221 Section 3), and refuses the
%% peer's.
datagrams_one_way(Setup) ->
    {Session, Socket} = connected(Setup, #{max_datagram_frame_size => 100}),
    ok = qmux:send_datagram(Session, <<"out">>),
    ?assertEqual({datagram, <<"out">>}, await(Socket, is_frame(datagram))),
    send(Socket, [qmux_frame:datagram(<<"in">>)]),
    ?assertEqual(transport_error(protocol_violation), element(3, close_frame(Socket))),
    ?assertMatch({closed, _}, await_closed(Session)).

%% Datagrams queued while a stream has 1 MiB to send take turns with its
%% data at the front of records: a record of datagrams, filled up with
%% stream data, then one of stream data, and so on, so the 40 datagrams all
%% go out before the stream's FIN.
datagrams_and_streams(Setup) ->
    Large = 1 bsl 30,
    Local = maps:merge(?PARAMETERS, #{max_datagram_frame_size => 2000}),
    {Session, Socket} = connected(Setup, Local, #{
        initial_max_data => Large,
        initial_max_stream_data_bidi_local => Large,
        max_datagram_frame_size => 1200
    }),
    send(Socket, [qmux_frame:stream(0, 0, [<<"go">>], false)]),
    ?assertEqual({stream_opened, 0}, event(Session)),
    ?assertMatch({stream_data, 0, <<"go">>, false}, event(Session)),
    Queue = get({send_queue, Session}),
    ok = sys:suspend(Session),
    Chunk = binary:copy(<<"s">>, 1 bsl 16),
    _ = [ok = qmux:send_data_async(Session, Queue, 0, Chunk, N =:= 16) || N <- lists:seq(1, 16)],
    Self = self(),
    Datagrams = [<<N, (binary:copy(<<"d">>, 999))/binary>> || N <- lists:seq(1, 40)],
    %% Each datagram's call waits behind the writes: the trace tells as each
    %% arrives in the session's mailbox.
    1 = erlang:trace(Session, true, ['receive']),
    _ = [
        spawn_link(fun() -> Self ! {sent, qmux:send_datagram(Session, Datagram)} end)
     || Datagram <- Datagrams
    ],
    _ = [
        receive
            {trace, Session, 'receive', {'$gen_call', _, {send_datagram, _}}} -> ok
        after 5000 -> error(datagram_not_queued)
        end
     || _ <- Datagrams
    ],
    1 = erlang:trace(Session, false, ['receive']),
    ok = sys:resume(Session),
    [{datagram, _} | _] = First = next(Socket),
    ?assertMatch([{stream, 0, _, _, false} | _], next(Socket)),
    Rest = datagrams_until_fin(Socket, 0, []),
    ?assertEqual(
        lists:sort(Datagrams), lists:sort([D || {datagram, D} <- First] ++ Rest)
    ),
    _ = [
        receive
            {sent, Sent} -> ?assertEqual(ok, Sent)
        after 5000 -> error(no_datagrams)
        end
     || _ <- Datagrams
    ],
    qmux:close(Session, 0).

%% The datagrams the session sends before stream Id's FIN, which ends the
%% record it is in.
datagrams_until_fin(Socket, Id, Acc) ->
    Frames = next(Socket),
    Datagrams = Acc ++ [Datagram || {datagram, Datagram} <- Frames],
    case [Fin || {stream, StreamId, _, _, true} = Fin <- Frames, StreamId =:= Id] of
        [] -> datagrams_until_fin(Socket, Id, Datagrams);
        [_] -> Datagrams
    end.
