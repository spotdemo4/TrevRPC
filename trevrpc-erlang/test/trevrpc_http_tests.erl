%% The HTTP mapping over HTTP/3 and HTTP/2, against the HTTP clients of
%% quic_h3 and erlang_h2 played by hand, as Cronet would: calls, the
%% refusals before a call is accepted, cancellation and the stream limit.
-module(trevrpc_http_tests).

-include_lib("eunit/include/eunit.hrl").

-define(UNARY, <<"/test.Echo/Unary">>).
-define(BIDI, <<"/test.Echo/Bidi">>).
-define(BLOCK, <<"/test.Echo/Block">>).
-define(SINK, <<"/test.Echo/Sink">>).
-define(TRAP, <<"/test.Echo/Trap">>).
-define(FLOOD, <<"/test.Echo/Flood">>).
-define(MAX_FRAME, 1 bsl 22).
-define(MEDIA_TYPE, <<"application/trevrpc">>).

%%% Fixture

http_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Env) ->
        [
            {
                lists:flatten(io_lib:format("~s over ~s", [Name, Version])),
                {timeout, 30, fun() -> run(Test, Env#{version => Version}) end}
            }
         || Version <- [h3, h2],
            {Name, Test} <- [
                {"a unary call", fun unary_test/1},
                {"a bidirectional call", fun bidi_test/1},
                {"other paths get 404", fun not_found_test/1},
                {"other methods get 405", fun method_test/1},
                {"other media types get 415", fun media_type_test/1},
                {"the admission decides", fun admission_test/1},
                {"a refused call ends with its STATUS", fun unimplemented_test/1},
                {"a reset cancels the call", fun reset_test/1},
                {"calls past the stream limit get 503", fun stream_limit_test/1},
                {"a call past its deadline ends with deadline_exceeded", fun deadline_test/1},
                {"a request without a body is invalid", fun empty_test/1},
                {"a call that does not read holds the client back", fun flow_control_test/1},
                {"a writer waits for a client that reads slowly", fun slow_reader_test/1},
                {"a reset frees a writer that waits", fun blocked_reset_test/1},
                {"shutdown sends GOAWAY, refuses and cancels calls", fun shutdown_test/1}
            ]
        ] ++
            [
                {"HTTP/2 responses advertise HTTP/3", fun() -> run(fun alt_svc_test/1, Env) end},
                {"native QUIC beside HTTP/3", fun() -> run(fun native_test/1, Env) end},
                {"an idle HTTP/2 connection is closed", fun() -> run(fun idle_test/1, Env) end},
                {"shutdown does not wait for a client that stops reading",
                    {timeout, 30, fun() ->
                        run(fun stalled_shutdown_test/1, Env#{version => h3})
                    end}},
                {"native connections stop unidirectional streams", fun() ->
                    run(fun native_uni_test/1, Env)
                end},
                {"HTTP is not offered unless enabled", fun() -> run(fun disabled_test/1, Env) end}
            ]
    end}.

%% Handlers and the admission report to the test that runs. The tests share
%% a process, so what earlier ones left behind goes first.
run(Test, Env) ->
    flush(),
    persistent_term:put(?MODULE, self()),
    Test(Env).

flush() ->
    receive
        _ -> flush()
    after 0 -> ok
    end.

setup() ->
    {ok, _} = application:ensure_all_started(trevrpc),
    logger:set_primary_config(level, none),
    Credentials = trevrpc_test_tls:credentials(),
    Admission = fun(#{headers := Headers} = Request) ->
        case lists:keyfind(<<"x-admission">>, 1, Headers) of
            {_, <<"deny">>} ->
                false;
            {_, <<"crash">>} ->
                error(crash);
            {_, <<"report">>} ->
                persistent_term:get(?MODULE) ! {admission, Request},
                true;
            false ->
                true
        end
    end,
    Server = start_server(Credentials, #{
        http_admission => Admission, max_streams_per_connection => 2
    }),
    {ok, Port} = trevrpc_server:port(Server),
    {ok, TcpPort} = trevrpc_server:tcp_port(Server),
    #{credentials => Credentials, server => Server, port => Port, tcp_port => TcpPort}.

cleanup(#{server := Server}) ->
    trevrpc_server:stop(Server),
    persistent_term:erase(?MODULE),
    logger:set_primary_config(level, notice).

start_server(#{cert := Cert, key := Key}, Opts) ->
    {ok, Server} = trevrpc_server:start(
        maps:merge(
            #{
                ip => {127, 0, 0, 1},
                cert => Cert,
                key => Key,
                http => true,
                routes => routes()
            },
            Opts
        )
    ),
    Server.

routes() ->
    #{
        ?UNARY => #{kind => unary, handler => fun(Request, _Call) -> {ok, Request} end},
        ?BIDI => #{kind => bidirectional_streaming, handler => fun echo/1},
        %% Reports its start, then reads nothing.
        ?SINK => #{
            kind => client_streaming,
            handler => fun(_Call) ->
                Test = persistent_term:get(?MODULE),
                Test ! {blocked, self()},
                receive
                after infinity -> ok
                end
            end
        },
        %% Traps exits, so shutdown gives it the graceful shutdown timeout.
        ?TRAP => #{
            kind => unary,
            handler => fun(_Request, _Call) ->
                process_flag(trap_exit, true),
                Test = persistent_term:get(?MODULE),
                Test ! {blocked, self()},
                receive
                after infinity -> ok
                end
            end
        },
        %% Sends `Count' responses of `Size' bytes once told to go, reporting
        %% its start and each response before it sends it.
        ?FLOOD => #{
            kind => server_streaming,
            handler => fun(<<Count:32, Size:32>>, Call) ->
                persistent_term:get(?MODULE) ! {flooding, self()},
                receive
                    go -> ok
                end,
                Message = binary:copy(<<"f">>, Size),
                Test = persistent_term:get(?MODULE),
                lists:foreach(
                    fun(N) ->
                        Test ! {sending, self(), N},
                        ok = trevrpc_call:send(Call, Message)
                    end,
                    lists:seq(1, Count)
                )
            end
        },
        %% Reports its start, then waits to be stopped.
        ?BLOCK => #{
            kind => unary,
            handler => fun(_Request, _Call) ->
                Test = persistent_term:get(?MODULE),
                Test ! {blocked, self()},
                receive
                after infinity -> ok
                end
            end
        }
    }.

echo(Call) ->
    case trevrpc_call:recv(Call) of
        {ok, Message} ->
            ok = trevrpc_call:send(Call, Message),
            echo(Call);
        eof ->
            ok;
        {error, Status} ->
            {error, Status}
    end.

%%% Tests

unary_test(Env) ->
    Client = connect(Env),
    {200, Headers, Body} = call(Client, ?UNARY, [<<"hello">>]),
    ?assertEqual(?MEDIA_TYPE, header(<<"content-type">>, Headers)),
    Frames = frames(Body),
    ?assertEqual([<<"hello">>], [M || {message, M} <- Frames]),
    ?assertMatch(#{code := ok}, status(Frames)),
    close(Client).

bidi_test(Env) ->
    Client = connect(Env),
    {ok, Id} = open(Client, post(?MEDIA_TYPE)),
    ok = send(Client, Id, request_frame(?BIDI, bidirectional_streaming), false),
    {200, Headers, <<>>, false} = response_head(Client, Id),
    ?assertEqual(?MEDIA_TYPE, header(<<"content-type">>, Headers)),
    %% Each message comes back before the next is sent.
    lists:foreach(
        fun(N) ->
            Message = integer_to_binary(N),
            ok = send(Client, Id, trevrpc_frame:encode(message, Message), false),
            ?assertEqual([{message, Message}], read_frames(Client, Id, 1))
        end,
        lists:seq(1, 5)
    ),
    ok = send(Client, Id, <<>>, true),
    {Body, true} = body(Client, Id, <<>>),
    ?assertMatch(#{code := ok}, status(frames(Body))),
    close(Client).

not_found_test(Env) ->
    Client = connect(Env),
    ?assertMatch({404, _, _}, exchange(Client, post(<<"/other">>, ?MEDIA_TYPE, []), <<>>)),
    %% The query is not part of the path.
    {200, _, Body} = exchange(
        Client, post(<<"/trevrpc?x=1">>, ?MEDIA_TYPE, []), unary_body(?UNARY, <<"q">>)
    ),
    ?assertMatch(#{code := ok}, status(frames(Body))),
    close(Client).

method_test(Env) ->
    Client = connect(Env),
    {405, Headers, _} = exchange(Client, request(<<"PUT">>, <<"/trevrpc">>, ?MEDIA_TYPE, []), <<>>),
    ?assertEqual(<<"POST">>, header(<<"allow">>, Headers)),
    close(Client).

media_type_test(Env) ->
    Client = connect(Env),
    Body = unary_body(?UNARY, <<"m">>),
    lists:foreach(
        fun(Type) ->
            ?assertMatch({415, _, _}, exchange(Client, post(Type), Body), Type)
        end,
        [<<"application/grpc">>, <<"application/trevrpc; charset=utf-8">>, <<>>]
    ),
    Twice = [{<<"content-type">>, ?MEDIA_TYPE}],
    ?assertMatch({415, _, _}, exchange(Client, post(<<"/trevrpc">>, ?MEDIA_TYPE, Twice), Body)),
    %% Any case.
    ?assertMatch({200, _, _}, exchange(Client, post(<<"Application/TrevRPC">>), Body)),
    close(Client).

admission_test(#{version := Version} = Env) ->
    Client = connect(Env),
    Body = unary_body(?UNARY, <<"a">>),
    Request = fun(Value) -> post(<<"/trevrpc">>, ?MEDIA_TYPE, [{<<"x-admission">>, Value}]) end,
    ?assertMatch({403, _, _}, exchange(Client, Request(<<"deny">>), Body)),
    ?assertMatch({500, _, _}, exchange(Client, Request(<<"crash">>), Body)),
    ?assertMatch({200, _, _}, exchange(Client, Request(<<"report">>), Body)),
    receive
        {admission, Admitted} ->
            Protocol =
                case Version of
                    h3 -> <<"HTTP/3.0">>;
                    h2 -> <<"HTTP/2.0">>
                end,
            ?assertMatch(
                #{
                    method := <<"POST">>,
                    protocol := Protocol,
                    path := <<"/trevrpc">>,
                    authority := <<"localhost">>,
                    secure := true,
                    peer := {{127, 0, 0, 1}, _}
                },
                Admitted
            ),
            ?assertEqual([], [Name || {<<$:, _/binary>> = Name, _} <- maps:get(headers, Admitted)])
    after 1000 -> error(no_admission)
    end,
    close(Client).

unimplemented_test(Env) ->
    Client = connect(Env),
    {200, _, Body} = call(Client, <<"/test.Echo/Missing">>, [<<"x">>]),
    ?assertMatch(#{code := unimplemented}, status(frames(Body))),
    close(Client).

reset_test(Env) ->
    Client = connect(Env),
    {ok, Id} = open(Client, post(?MEDIA_TYPE)),
    ok = send(Client, Id, unary_body(?BLOCK, <<"r">>), true),
    Handler =
        receive
            {blocked, Pid} -> Pid
        after 5000 -> error(not_started)
        end,
    Monitor = monitor(process, Handler),
    cancel(Client, Id),
    receive
        {'DOWN', Monitor, process, Handler, _} -> ok
    after 5000 -> error(not_cancelled)
    end,
    %% The connection carries on.
    ?assertMatch({200, _, _}, call(Client, ?UNARY, [<<"after">>])),
    close(Client).

stream_limit_test(Env) ->
    Client = connect(Env),
    Ids = [
        begin
            {ok, Id} = open(Client, post(?MEDIA_TYPE)),
            ok = send(Client, Id, unary_body(?BLOCK, <<"b">>), true),
            receive
                {blocked, _} -> ok
            after 5000 -> error(not_started)
            end,
            Id
        end
     || _ <- [1, 2]
    ],
    ?assertMatch({503, _, _}, exchange(Client, post(?MEDIA_TYPE), unary_body(?UNARY, <<"x">>))),
    [cancel(Client, Id) || Id <- Ids],
    close(Client).

alt_svc_test(#{port := Port} = Env) ->
    Client = connect(Env#{version => h2}),
    Expected = iolist_to_binary(io_lib:format("h3=\":~b\"; ma=86400", [Port])),
    {200, Headers, _} = call(Client, ?UNARY, [<<"a">>]),
    ?assertEqual(Expected, header(<<"alt-svc">>, Headers)),
    {404, Refused, _} = exchange(Client, post(<<"/other">>, ?MEDIA_TYPE, []), <<>>),
    ?assertEqual(Expected, header(<<"alt-svc">>, Refused)),
    close(Client),
    %% HTTP/3 has nothing to advertise.
    H3 = connect(Env#{version => h3}),
    {200, H3Headers, _} = call(H3, ?UNARY, [<<"a">>]),
    ?assertEqual(<<>>, header(<<"alt-svc">>, H3Headers)),
    close(H3).

native_test(#{port := Port, credentials := #{cacerts := CaCerts}}) ->
    {ok, Channel} = trevrpc_channel:connect(
        #{host => "localhost", port => Port, cacerts => CaCerts, transport => quic}, 5000
    ),
    ?assertMatch(
        {ok, <<"native">>, _},
        trevrpc_client:unary(Channel, ?UNARY, <<"native">>, #{timeout => 5000})
    ),
    trevrpc_channel:close(Channel).

idle_test(#{credentials := Credentials}) ->
    Server = start_server(Credentials, #{connection_idle_timeout => 300}),
    {ok, TcpPort} = trevrpc_server:tcp_port(Server),
    Client = connect(#{version => h2, tcp_port => TcpPort}),
    ?assertMatch({200, _, _}, call(Client, ?UNARY, [<<"i">>])),
    {h2, Conn} = Client,
    Monitor = monitor(process, Conn),
    receive
        {'DOWN', Monitor, process, Conn, _} -> ok
    after 5000 -> error(not_closed)
    end,
    trevrpc_server:stop(Server).

deadline_test(Env) ->
    Client = connect(Env),
    Request = trevrpc_wire:encode_request(#{method => ?BLOCK, timeout_nanos => 200000000}),
    Body = iolist_to_binary([
        trevrpc_frame:encode(request, Request), trevrpc_frame:encode(message, <<"d">>)
    ]),
    {200, _, Response} = exchange(Client, post(?MEDIA_TYPE), Body),
    ?assertMatch(#{code := deadline_exceeded}, status(frames(Response))),
    close(Client).

empty_test(Env) ->
    Client = connect(Env),
    %% END_STREAM on the HEADERS: the call fails at once, not at the initial
    %% request timeout.
    {ok, Id} = open_ended(Client, post(?MEDIA_TYPE)),
    {200, _, <<>>, false} = response_head(Client, Id),
    {Body, true} = body(Client, Id, <<>>),
    ?assertMatch(
        #{code := invalid_argument, message := <<"request stream ended before REQUEST">>},
        status(frames(Body))
    ),
    close(Client).

flow_control_test(#{credentials := Credentials} = Env) ->
    %% Streams receive one frame ahead: 1 KiB and its header.
    Server = start_server(Credentials, #{max_frame_size => 1024}),
    Client = connect(Env#{port => port(Server), tcp_port => tcp_port(Server)}),
    {ok, Id} = open(Client, post(?MEDIA_TYPE)),
    ok = send(Client, Id, request_frame(?SINK, client_streaming), false),
    Sink =
        receive
            {blocked, Pid} -> Pid
        after 5000 -> error(not_started)
        end,
    %% Nothing reads past the REQUEST, so the client is held back: over
    %% HTTP/3 its 16 MiB send queue fills, over HTTP/2 a send waits 500 ms
    %% for the window.
    Chunk = binary:copy(<<0>>, 65536),
    ?assertEqual(held_back, push(Client, Id, Chunk, 320)),
    ok = stuck(Client, Id, Chunk),
    %% What reached the call so far is within the credit.
    {messages, Messages} = process_info(Sink, messages),
    Delivered = lists:sum([byte_size(Bin) || {trevrpc_stream, _, {data, Bin, _}} <- Messages]),
    ?assert(Delivered =< max_delivered(Env), Delivered),
    close(Client),
    trevrpc_server:stop(Server).

%% Over HTTP/3, a full send queue alone could be the client outpacing a
%% server that reads everything. The client stays held back across a round
%% trip, in which such a server would have granted more credit: what it has
%% sent, all within the server's connection credit, does not grow, and the
%% stream still refuses more. That credit is about the connection window,
%% and nothing more can reach the call than the client sent.
stuck({quic_h3, Conn}, Id, Chunk) ->
    Quic = quic_h3:get_quic_conn(Conn),
    {ok, #{data_sent := Sent}} = quic:get_stats(Quic),
    ok = quic:send_ping(Quic),
    ok = acked(Quic, 250),
    ?assertMatch({ok, #{data_sent := Sent}}, quic:get_stats(Quic)),
    ?assertEqual({error, send_queue_full}, quic_h3:send_data(Conn, Id, Chunk, false)),
    %% The connection window, slid on by what the server did read: the
    %% request's HEADERS and its REQUEST frame, at most one frame.
    ?assert(Sent =< trevrpc_quic:connection_window(1024, 4 * 64) + 1024, Sent),
    ok;
stuck({h2, _Conn}, _Id, _Chunk) ->
    ok.

%% Waits until nothing the client sent is unacknowledged.
acked(_Quic, 0) ->
    error(never_acked);
acked(Quic, N) ->
    case quic:get_send_queue_info(Quic) of
        {ok, #{in_flight := 0}} ->
            ok;
        _ ->
            timer:sleep(20),
            acked(Quic, N - 1)
    end.

%% How far ahead of a call that does not read its stream may get. Over
%% HTTP/2, one frame. quic_h3 delivers a request's HEADERS before its
%% stream can be put in manual flow control, and erlang_quic's automatic
%% credit grants the stream a whole connection window on that first
%% delivery (benoitc/erlang_quic#554); the connection window, here 256
%% frames of the stream limit's four times 64 streams, bounds it still.
max_delivered(#{version := h2}) -> 2048;
max_delivered(#{version := h3}) -> trevrpc_quic:connection_window(1024, 4 * 64).

%% Sends `Chunk' up to `N' times, and says whether the client was held
%% back before it sent them all: its send queue full (HTTP/3), or a send
%% that waited 500 ms for the window (HTTP/2).
push(_Client, _Id, _Chunk, 0) ->
    sent_all;
push({quic_h3, Conn} = Client, Id, Chunk, N) ->
    case quic_h3:send_data(Conn, Id, Chunk, false) of
        ok -> push(Client, Id, Chunk, N - 1);
        {error, send_queue_full} -> held_back
    end;
push({h2, Conn} = Client, Id, Chunk, N) ->
    case h2:send_data(Conn, Id, Chunk, false, #{block => 500}) of
        ok -> push(Client, Id, Chunk, N - 1);
        {error, timeout} -> held_back
    end.

slow_reader_test(#{credentials := Credentials, version := Version} = Env) ->
    Server = start_server(Credentials, #{}),
    Client = connect(Env#{port => port(Server), tcp_port => tcp_port(Server)}),
    %% The client reads nothing until the writer waits: over HTTP/3 once the
    %% connection's 16 MiB send queue is full, over HTTP/2 once the window
    %% is.
    Count = flood_count(Version),
    {ok, Flood} = open_manual(Client, post(?MEDIA_TYPE)),
    ok = send(Client, Flood, flood_body(Count, 1000), true),
    _ = await_waiting(Client, Flood, Version),
    %% A refused call's STATUS, too long to fit what is left of the queue,
    %% waits for it as well.
    Missing = <<"/test.Echo/", (binary:copy(<<"m">>, 1500))/binary>>,
    {ok, Refused} = open(Client, post(?MEDIA_TYPE)),
    ok = send(Client, Refused, unary_body(Missing, <<"x">>), true),
    {Frames, RefusedStatus} = drain(
        Client, Flood, Refused, trevrpc_frame:reader(?MAX_FRAME), [], none
    ),
    ?assertEqual(Count, length([M || {message, M} <- Frames])),
    ?assertMatch(#{code := ok}, status(Frames)),
    ?assertMatch(#{code := unimplemented}, RefusedStatus),
    close(Client),
    trevrpc_server:stop(Server).

flood_body(Count, Size) ->
    unary_body(?FLOOD, <<Count:32, Size:32>>, server_streaming).

%% Over HTTP/3 the 16 MiB send queue must fill, over HTTP/2 a 64 KiB window.
flood_count(h3) -> 20000;
flood_count(h2) -> 200.

%% Starts the flood on `Id' and returns once its writer waits. Over HTTP/3
%% it waits for quic_h3's send queue, which a trace of the writer shows: the
%% client's receive window, by which the queue drains, is erlang_quic's
%% default. Over HTTP/2 it waits for the client's 64 KiB window once it
%% starts the 66th response, whose end lies past it. Returns the writer.
await_waiting(Client, Id, Version) ->
    Writer =
        receive
            {flooding, Pid} -> Pid
        after 5000 -> error(not_started)
        end,
    {200, _, <<>>, false} = response_head(Client, Id),
    case Version of
        h3 ->
            Pattern = {trevrpc_stream, wait_send_ready, 4},
            {module, trevrpc_stream} = code:ensure_loaded(trevrpc_stream),
            _ = erlang:trace_pattern(Pattern, true, [local]),
            _ = erlang:trace(Writer, true, [call]),
            Writer ! go,
            Waited =
                receive
                    {trace, Writer, call, {trevrpc_stream, wait_send_ready, _}} -> ok
                after 10000 -> never_waited
                end,
            _ = erlang:trace(Writer, false, [call]),
            _ = erlang:trace_pattern(Pattern, false, [local]),
            ok = Waited,
            Writer;
        h2 ->
            Writer ! go,
            %% 66 responses of 1000 bytes and a 3-byte header end at 66198.
            receive
                {sending, Writer, 66} -> ok
            after 10000 -> error(never_waited)
            end,
            Writer
    end.

%% Reads the whole response of `Flood', returning its credit as it goes, and
%% the STATUS of `Refused' when it comes.
drain(Client, Flood, Refused, Reader, Acc, RefusedStatus) ->
    drain(Client, Flood, Refused, Reader, Acc, RefusedStatus, <<>>).

drain(_Client, done, _Refused, _Reader, Acc, RefusedStatus, _Body) when RefusedStatus =/= none ->
    {Acc, RefusedStatus};
drain({Tag, Conn} = Client, Flood, Refused, Reader, Acc, RefusedStatus, Body) ->
    receive
        {Tag, Conn, {data, Flood, Data, Fin}} ->
            %% A stream that ended is gone, and needs no credit.
            _ = consume(Client, Flood, byte_size(Data)),
            {ok, Frames, Next} = trevrpc_frame:feed(Data, Reader),
            Done =
                case Fin of
                    true -> done;
                    false -> Flood
                end,
            drain(Client, Done, Refused, Next, Acc ++ Frames, RefusedStatus, Body);
        {Tag, Conn, {response, Refused, 200, _}} ->
            drain(Client, Flood, Refused, Reader, Acc, RefusedStatus, Body);
        {Tag, Conn, {data, Refused, Data, false}} ->
            drain(Client, Flood, Refused, Reader, Acc, RefusedStatus, <<Body/binary, Data/binary>>);
        {Tag, Conn, {data, Refused, Data, true}} ->
            Status = status(frames(<<Body/binary, Data/binary>>)),
            drain(Client, Flood, Refused, Reader, Acc, Status, <<>>)
    after 10000 -> error({stalled, length(Acc), RefusedStatus})
    end.

blocked_reset_test(#{credentials := Credentials, version := Version} = Env) ->
    Server = start_server(Credentials, #{}),
    Client = connect(Env#{port => port(Server), tcp_port => tcp_port(Server)}),
    {ok, Id} = open_manual(Client, post(?MEDIA_TYPE)),
    ok = send(Client, Id, flood_body(flood_count(Version), 1000), true),
    Writer = await_waiting(Client, Id, Version),
    Monitor = monitor(process, Writer),
    cancel(Client, Id),
    receive
        {'DOWN', Monitor, process, Writer, _} -> ok
    after 5000 -> error(still_waiting)
    end,
    ?assertMatch({200, _, _}, call(Client, ?UNARY, [<<"after">>])),
    close(Client),
    trevrpc_server:stop(Server).

shutdown_test(#{credentials := Credentials, version := Version} = Env) ->
    Server = start_server(Credentials, #{graceful_shutdown_timeout => 300}),
    {Tag, Conn} = Client = connect(Env#{port => port(Server), tcp_port => tcp_port(Server)}),
    {ok, Running} = open(Client, post(?MEDIA_TYPE)),
    ok = send(Client, Running, unary_body(?TRAP, <<"t">>), true),
    Handler =
        receive
            {blocked, Pid} -> Pid
        after 5000 -> error(not_started)
        end,
    Monitor = monitor(process, Handler),
    %% The connection starts shutting down with a request on its way, which
    %% it takes before its GOAWAY, and then refuses.
    [Owner] = connections(Server),
    ok = sys:suspend(Owner),
    ok = trevrpc_server_conn:shutdown(Owner),
    {ok, Late} = open(Client, post(?MEDIA_TYPE)),
    _ = send(Client, Late, unary_body(?UNARY, <<"late">>), true),
    ok = await_queued(Owner, 2, 100),
    ok = sys:resume(Owner),
    {503, _, _, _} = response_head(Client, Late),
    %% HTTP/3 clients hear at once that no more requests are taken. HTTP/2
    %% ones only as the connection closes, after the call that ran is cut
    %% off at the graceful shutdown timeout, as erlang_h2 would close it
    %% 100 ms after a GOAWAY.
    First =
        receive
            {Tag, Conn, {goaway, _}} -> goaway;
            {Tag, Conn, {goaway, _, _}} -> goaway;
            {Tag, Conn, {stream_reset, Running, _}} -> reset
        after 5000 -> error(no_goaway_or_reset)
        end,
    case {Version, First} of
        {h3, goaway} ->
            receive
                {Tag, Conn, {stream_reset, Running, _}} -> ok
            after 5000 -> error(not_reset)
            end;
        {h2, reset} ->
            receive
                {Tag, Conn, {goaway, _, _}} -> ok
            after 5000 -> error(no_goaway)
            end;
        Order ->
            error({unexpected_order, Order})
    end,
    receive
        {'DOWN', Monitor, process, Handler, _} -> ok
    after 5000 -> error(not_cancelled)
    end,
    trevrpc_server:stop(Server).

stalled_shutdown_test(#{credentials := Credentials} = Env) ->
    Server = start_server(Credentials, #{graceful_shutdown_timeout => 300}),
    Client = connect(Env#{port => port(Server), tcp_port => tcp_port(Server)}),
    %% The client stops reading: the flood fills quic_h3's send queue, and a
    %% refused call's STATUS waits for it to drain.
    {ok, Flood} = open_manual(Client, post(?MEDIA_TYPE)),
    ok = send(Client, Flood, flood_body(flood_count(h3), 1000), true),
    _ = await_waiting(Client, Flood, h3),
    Missing = <<"/test.Echo/", (binary:copy(<<"m">>, 1500))/binary>>,
    {ok, Refused} = open(Client, post(?MEDIA_TYPE)),
    ok = send(Client, Refused, unary_body(Missing, <<"x">>), true),
    {200, _, _, false} = response_head(Client, Refused),
    %% Shutdown ends the connection at the graceful shutdown timeout, not
    %% when the server gives up on it 5 s later.
    Started = erlang:monotonic_time(millisecond),
    ok = trevrpc_server:stop(Server),
    Took = erlang:monotonic_time(millisecond) - Started,
    ?assert(Took < 3000, Took),
    close(Client).

%% The processes that own the server's connections. The server's state is
%% `#state{name, config, counters, connections, tcp}'.
connections(Server) ->
    maps:values(element(5, sys:get_state(Server))).

await_queued(_Pid, _Length, 0) ->
    error(not_queued);
await_queued(Pid, Length, N) ->
    case process_info(Pid, message_queue_len) of
        {message_queue_len, Queued} when Queued >= Length ->
            ok;
        _ ->
            timer:sleep(20),
            await_queued(Pid, Length, N - 1)
    end.

native_uni_test(#{port := Port}) ->
    Opts = maps:merge(trevrpc_quic:transport_opts(?MAX_FRAME, 4, 30000), #{verify => false}),
    {ok, Conn} = quic:connect("127.0.0.1", Port, Opts, self()),
    receive
        {quic, Conn, {connected, _}} -> ok
    after 5000 -> error(not_connected)
    end,
    {ok, Uni} = quic:open_unidirectional_stream(Conn),
    ok = quic:send_data(Conn, Uni, binary:copy(<<"u">>, 1000), false),
    receive
        {quic, Conn, {stop_sending, Uni, 1}} -> ok
    after 5000 -> error(not_stopped)
    end,
    %% The connection carries on.
    {ok, Id} = quic:open_stream(Conn),
    ok = quic:send_data(Conn, Id, unary_body(?UNARY, <<"n">>), true),
    ?assertMatch(#{code := ok}, status(frames(quic_response(Conn, Id, <<>>)))),
    quic:close(Conn, 0).

quic_response(Conn, Id, Acc) ->
    receive
        {quic, Conn, {stream_data, Id, Data, true}} ->
            <<Acc/binary, Data/binary>>;
        {quic, Conn, {stream_data, Id, Data, false}} ->
            quic_response(Conn, Id, <<Acc/binary, Data/binary>>)
    after 5000 -> error(no_response)
    end.

port(Server) ->
    {ok, Port} = trevrpc_server:port(Server),
    Port.

tcp_port(Server) ->
    {ok, Port} = trevrpc_server:tcp_port(Server),
    Port.

disabled_test(#{credentials := #{cert := Cert, key := Key}}) ->
    {ok, Server} = trevrpc_server:start(#{
        ip => {127, 0, 0, 1}, cert => Cert, key => Key, routes => routes()
    }),
    {ok, Port} = trevrpc_server:port(Server),
    {ok, TcpPort} = trevrpc_server:tcp_port(Server),
    ?assertMatch({error, _}, quic_h3:connect("127.0.0.1", Port, h3_opts())),
    ?assertMatch({error, _}, h2:connect("127.0.0.1", TcpPort, #{verify => verify_none})),
    trevrpc_server:stop(Server).

%%% Requests

post(Type) ->
    post(<<"/trevrpc">>, Type, []).

post(Path, Type, Extra) ->
    request(<<"POST">>, Path, Type, Extra).

request(Method, Path, Type, Extra) ->
    Base = [
        {<<":method">>, Method},
        {<<":scheme">>, <<"https">>},
        {<<":authority">>, <<"localhost">>},
        {<<":path">>, Path}
    ],
    Base ++
        case Type of
            <<>> -> [];
            _ -> [{<<"content-type">>, Type}]
        end ++ Extra.

request_frame(Method) ->
    request_frame(Method, unary).

request_frame(Method, Kind) ->
    trevrpc_frame:encode(request, trevrpc_wire:encode_request(#{method => Method, kind => Kind})).

unary_body(Method, Message) ->
    unary_body(Method, Message, unary).

unary_body(Method, Message, Kind) ->
    iolist_to_binary([request_frame(Method, Kind), trevrpc_frame:encode(message, Message)]).

%% A call at the HTTP path that sends `Messages' and finishes.
call(Client, Method, Messages) ->
    Body = [request_frame(Method) | [trevrpc_frame:encode(message, M) || M <- Messages]],
    exchange(Client, post(?MEDIA_TYPE), iolist_to_binary(Body)).

%% Sends a request with `Body' and reads the whole response. A refusal can
%% come, and end the request stream, before the body is sent.
exchange(Client, Headers, Body) ->
    {ok, Id} = open(Client, Headers),
    _ = send(Client, Id, Body, true),
    case response_head(Client, Id) of
        {Status, ResponseHeaders, Data, true} ->
            {Status, ResponseHeaders, Data};
        {Status, ResponseHeaders, Data, false} ->
            {Rest, true} = body(Client, Id, Data),
            {Status, ResponseHeaders, Rest}
    end.

frames(Body) ->
    {ok, Frames, Reader} = trevrpc_frame:feed(Body, trevrpc_frame:reader(?MAX_FRAME)),
    true = trevrpc_frame:at_boundary(Reader),
    Frames.

status(Frames) ->
    [Status] = [Payload || {status, Payload} <- Frames],
    {ok, Decoded} = trevrpc_wire:decode_status(Status),
    Decoded.

header(Name, Headers) ->
    case lists:keyfind(Name, 1, Headers) of
        {_, Value} -> Value;
        false -> <<>>
    end.

%%% Clients: quic_h3 for HTTP/3 and erlang_h2 for HTTP/2, both sending
%%% their events to the test process.

h3_opts() ->
    #{verify => verify_none, sync => true}.

connect(#{version := h3, port := Port}) ->
    {ok, Conn} = quic_h3:connect("127.0.0.1", Port, h3_opts()),
    {quic_h3, Conn};
connect(#{version := h2, tcp_port := Port}) ->
    {ok, Conn} = h2:connect("127.0.0.1", Port, #{verify => verify_none}),
    %% A connection the server closes exits abnormally, and takes its linked
    %% caller with it.
    true = unlink(Conn),
    ok = h2:wait_connected(Conn),
    {h2, Conn}.

close({quic_h3, Conn}) -> quic_h3:close(Conn);
close({h2, Conn}) -> h2:close(Conn).

open({quic_h3, Conn}, Headers) -> quic_h3:request(Conn, Headers, #{end_stream => false});
open({h2, Conn}, Headers) -> h2:request(Conn, Headers, #{end_stream => false}).

%% A request whose HEADERS end the stream.
open_ended({quic_h3, Conn}, Headers) -> quic_h3:request(Conn, Headers, #{end_stream => true});
open_ended({h2, Conn}, Headers) -> h2:request(Conn, Headers, #{end_stream => true}).

%% A request whose response the client credits only with consume/3.
open_manual({quic_h3, Conn}, Headers) ->
    quic_h3:request(Conn, Headers, #{end_stream => false, flow_control => manual});
open_manual({h2, Conn}, Headers) ->
    h2:request(Conn, Headers, #{end_stream => false, flow_control => manual}).

consume({quic_h3, Conn}, Id, Bytes) -> quic_h3:consume(Conn, Id, Bytes);
consume({h2, Conn}, Id, Bytes) -> h2:consume(Conn, Id, Bytes).

send({quic_h3, Conn}, Id, Data, Fin) -> quic_h3:send_data(Conn, Id, iolist_to_binary(Data), Fin);
send({h2, Conn}, Id, Data, Fin) -> h2:send_data(Conn, Id, iolist_to_binary(Data), Fin).

cancel({quic_h3, Conn}, Id) -> quic_h3:cancel(Conn, Id);
cancel({h2, Conn}, Id) -> h2:cancel(Conn, Id).

%% The response's status and headers, and the body that came with them.
response_head({Tag, Conn}, Id) ->
    receive
        {Tag, Conn, {response, Id, Status, Headers}} ->
            {Status, Headers, <<>>, false};
        {Tag, Conn, {stream_reset, Id, Code}} ->
            error({reset, Code})
    after 5000 -> error(no_response)
    end.

%% The rest of the body, after `Acc', up to the end of the stream.
body({Tag, Conn} = Client, Id, Acc) ->
    receive
        {Tag, Conn, {data, Id, Data, true}} -> {<<Acc/binary, Data/binary>>, true};
        {Tag, Conn, {data, Id, Data, false}} -> body(Client, Id, <<Acc/binary, Data/binary>>);
        {Tag, Conn, {trailers, Id, _}} -> {Acc, true};
        {Tag, Conn, {stream_reset, Id, Code}} -> error({reset, Code})
    after 5000 -> error({no_body, Acc})
    end.

%% The next `N' frames of the response.
read_frames(Client, Id, N) ->
    read_frames(Client, Id, N, trevrpc_frame:reader(?MAX_FRAME), []).

read_frames(_Client, _Id, N, _Reader, Acc) when length(Acc) >= N ->
    Acc;
read_frames({Tag, Conn} = Client, Id, N, Reader, Acc) ->
    receive
        {Tag, Conn, {data, Id, Data, _Fin}} ->
            {ok, Frames, Next} = trevrpc_frame:feed(Data, Reader),
            read_frames(Client, Id, N, Next, Acc ++ Frames)
    after 5000 -> error({no_frames, Acc})
    end.
