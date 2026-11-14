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
                    {"a peer that sends no parameters is closed", fun silent_peer/1}
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
    {ok, Session} = trevrpc_qmux:start(Server, server, self(), Opts#{parameters => Parameters}),
    erase(Peer),
    {Session, Peer}.

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
    ok = trevrpc_qmux:send_data(Session, 0, <<"reply">>, true),
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
    ok = trevrpc_qmux:send_data(Session, 0, Data, true),
    ?assertEqual({stream, 0, 0, binary:copy(<<"d">>, 10), false}, await(Socket, fun is_stream/1)),
    send(Socket, [trevrpc_qmux_frame:max_stream_data(0, 25)]),
    ?assertEqual({stream, 0, 10, binary:copy(<<"d">>, 15), true}, await(Socket, fun is_stream/1)),
    trevrpc_qmux:close(Session, 0).

is_stream(Frame) -> element(1, Frame) =:= stream.

%% Once both directions of the peer's only stream end, the session allows
%% another.
stream_credit(Credentials) ->
    {Session, Socket} = connected(Credentials, #{}),
    send(Socket, [trevrpc_qmux_frame:stream(0, 0, [<<"one">>], true)]),
    ?assertEqual(<<"one">>, received(Session, 0)),
    ok = trevrpc_qmux:send_data(Session, 0, <<"done">>, true),
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
    ok = trevrpc_qmux:send_data(Session, 0, <<"abcdefgh">>, false),
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
    _ = [trevrpc_qmux:send_data_async(Session, 0, Chunk, false) || _ <- lists:seq(1, 64)],
    ?assertMatch({closed, _}, await_closed(Session)).

silent_peer(Credentials) ->
    {Session, _Socket} = pair(Credentials, ?PARAMETERS, #{handshake_timeout => 200}),
    ?assertEqual({closed, handshake_timeout}, event(Session)).
