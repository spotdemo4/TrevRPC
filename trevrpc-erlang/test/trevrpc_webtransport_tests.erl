%% WebTransport over HTTP/3, against erlang-webtransport's client standing
%% in for a browser: sessions, their refusals, and calls on them, among
%% them those that 0.5.13's credit model could stall or deadlock (see
%% trevrpc_webtransport).
-module(trevrpc_webtransport_tests).

-include_lib("eunit/include/eunit.hrl").

-define(UNARY, <<"/test.Echo/Unary">>).
-define(BIDI, <<"/test.Echo/Bidi">>).
-define(SLOW, <<"/test.Echo/Slow">>).
-define(FAIL, <<"/test.Echo/Fail">>).
-define(STUCK, <<"/test.Echo/Stuck">>).
-define(FLOOD, <<"/test.Echo/Flood">>).
-define(DATAGRAMS, <<"/test.Echo/Datagrams">>).
-define(SLEEP, <<"/test.Echo/Sleep">>).
-define(PEER, <<"/test.Echo/Peer">>).
%% The name a test registers under to hear from the server's interceptor.
-define(HOOKS, trevrpc_webtransport_tests_hooks).
%% Small frames, so that a stream's window is small too: 64 KiB.
-define(MAX_FRAME, 65536).
-define(CHUNK, 16384).

%%% Fixture

webtransport_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Env) ->
        [
            {Name, {timeout, 60, fun() -> Test(Env) end}}
         || {Name, Test} <- [
                {"a draft-02 session carries calls", fun legacy_test/1},
                {"a latest-draft session carries calls", fun latest_test/1},
                {"trevrpc-1 is selected when offered", fun protocol_test/1},
                {"other protocols get 400", fun other_protocol_test/1},
                {"other paths get 404", fun not_found_test/1},
                {"the admission decides", fun admission_test/1},
                {"concurrent calls share a session", fun concurrent_test/1},
                {"a session keeps connection metadata", fun connection_metadata_test/1},
                {"a full-duplex call that writes as it reads finishes", fun duplex_test/1},
                {"a slow reader holds its stream back, not the session forever",
                    fun slow_reader_test/1},
                {"a session holds about a window of a slow reader's stream", fun bounded_test/1},
                {"a blocked call's deadline frees the session", fun deadline_test/1},
                {"a call far behind on reading while it writes fails", fun overflow_test/1},
                {"a call knows the client's address", fun peer_test/1},
                {"a stop waits for the client to read the status", fun stop_test/1},
                {"a stop comes after a mebibyte the status left unread", fun stop_limit_test/1},
                {"responses travel as session datagrams", fun datagrams_test/1},
                {"a connection carries one session", fun second_session_test/1},
                {"a session ends with its connection process", fun conn_exit_test/1},
                {"shutdown cuts a session's calls off and closes it", fun shutdown_test/1}
            ]
        ]
    end}.

setup() ->
    {ok, _} = application:ensure_all_started(trevrpc),
    {ok, _} = application:ensure_all_started(webtransport),
    logger:set_primary_config(level, none),
    #{cert := Cert, key := Key} = trevrpc_test_tls:credentials(),
    Admission = fun
        (#{origin := <<"https://evil.example">>}) -> false;
        (#{}) -> true
    end,
    {ok, Server} = trevrpc_server:start(#{
        ip => {127, 0, 0, 1},
        cert => Cert,
        key => Key,
        tcp => false,
        max_frame_size => ?MAX_FRAME,
        webtransport => true,
        webtransport_admission => Admission,
        interceptors => [fun interceptor/1],
        routes => #{
            ?UNARY => #{kind => unary, handler => fun(Request, _Call) -> {ok, Request} end},
            ?BIDI => #{kind => kind(?BIDI), handler => fun echo/1},
            ?SLOW => #{kind => kind(?SLOW), handler => fun slow/1},
            ?FAIL => #{kind => kind(?FAIL), handler => fun fail/1},
            ?STUCK => #{kind => kind(?STUCK), handler => fun(_Call) -> timer:sleep(infinity) end},
            ?FLOOD => #{kind => kind(?FLOOD), handler => fun flood/1},
            ?PEER => #{
                kind => kind(?PEER),
                handler => fun(_Request, Call) -> {ok, term_to_binary(trevrpc_call:peer(Call))} end
            },
            ?DATAGRAMS => #{
                kind => kind(?DATAGRAMS),
                unreliable => #{responses => true},
                handler => fun(_Request, Call) ->
                    _ = [ok = trevrpc_call:send(Call, <<N>>) || N <- lists:seq(1, 5)],
                    ok
                end
            }
        }
    }),
    {ok, Port} = trevrpc_server:port(Server),
    #{server => Server, port => Port}.

kind(?UNARY) -> unary;
kind(?BIDI) -> bidirectional_streaming;
kind(?SLOW) -> client_streaming;
kind(?FAIL) -> bidirectional_streaming;
kind(?STUCK) -> client_streaming;
kind(?FLOOD) -> bidirectional_streaming;
kind(?DATAGRAMS) -> server_streaming;
kind(?SLEEP) -> unary;
kind(?PEER) -> unary.

%% Tells the test registered as ?HOOKS, if any, how each call ended.
interceptor(_Info) ->
    case whereis(?HOOKS) of
        undefined -> ok;
        Test -> {ok, fun(#{status := Status}) -> Test ! {finished, Status} end}
    end.

cleanup(#{server := Server}) ->
    trevrpc_server:stop(Server),
    logger:set_primary_config(level, notice).

%% Sends each request back as it reads it.
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

%% Takes 20 ms over each request, then answers with how many bytes it read.
slow(Call) ->
    slow(Call, 0).

slow(Call, Bytes) ->
    case trevrpc_call:recv(Call) of
        {ok, Message} ->
            timer:sleep(20),
            slow(Call, Bytes + byte_size(Message));
        eof ->
            {ok, integer_to_binary(Bytes)};
        {error, Status} ->
            {error, Status}
    end.

%% Fails at its first request.
fail(Call) ->
    {ok, _} = trevrpc_call:recv(Call),
    {error, trevrpc_status:new(aborted, <<"abort">>)}.

%% Writes until it cannot, and never reads.
flood(Call) ->
    case trevrpc_call:send(Call, binary:copy(<<0>>, 1024)) of
        ok ->
            timer:sleep(1),
            flood(Call);
        {error, Status} ->
            {error, Status}
    end.

%%% Tests

legacy_test(#{port := Port}) ->
    Session = connect(Port, #{compat_mode => legacy_browser_compat}),
    ?assertEqual({ok, [<<"hello">>]}, call(Session, ?UNARY, [<<"hello">>])),
    ?assertEqual({ok, [<<"a">>, <<"b">>]}, call(Session, ?BIDI, [<<"a">>, <<"b">>])),
    close(Session).

latest_test(#{port := Port}) ->
    Session = connect(Port, #{compat_mode => latest}),
    ?assertEqual({ok, [<<"hello">>]}, call(Session, ?UNARY, [<<"hello">>])),
    close(Session).

protocol_test(#{port := Port}) ->
    Session = connect(Port, #{protocols => [<<"other">>, <<"trevrpc-1">>]}),
    ?assertEqual({ok, [<<"hello">>]}, call(Session, ?UNARY, [<<"hello">>])),
    close(Session).

other_protocol_test(#{port := Port}) ->
    ?assertMatch({error, _}, try_connect(Port, <<"/trevrpc">>, #{protocols => [<<"trevrpc-2">>]})).

not_found_test(#{port := Port}) ->
    ?assertMatch({error, _}, try_connect(Port, <<"/other">>, #{})),
    %% The query is not part of the path.
    {ok, Session} = try_connect(Port, <<"/trevrpc?x=1">>, #{}),
    close(Session).

admission_test(#{port := Port}) ->
    ?assertMatch(
        {error, _},
        try_connect(Port, <<"/trevrpc">>, #{
            headers => [{<<"origin">>, <<"https://evil.example">>}]
        })
    ),
    {ok, Session} = try_connect(Port, <<"/trevrpc">>, #{
        headers => [{<<"origin">>, <<"https://app.example">>}]
    }),
    close(Session).

concurrent_test(#{port := Port}) ->
    Session = connect(Port, #{}),
    Self = self(),
    Messages = [integer_to_binary(N) || N <- lists:seq(1, 20)],
    Pids = [
        spawn_link(fun() ->
            %% Each call runs in its own process, which gets its stream's
            %% events.
            put(router, Self),
            Self ! {self(), call(Session, ?BIDI, [M, M])}
        end)
     || M <- Messages
    ],
    %% The client's handler sends every event to the session's owner, this
    %% process, which hands each to the call of its stream.
    Results = route(Session, Pids, #{}),
    ?assertEqual([{ok, [M, M]} || M <- Messages], [maps:get(Pid, Results) || Pid <- Pids]),
    close(Session).

connection_metadata_test(#{port := Port}) ->
    Session = connect(Port, #{}),
    Call = fun(Set) ->
        call_frames(Session, #{method => ?UNARY, connection_metadata => Set}, [<<"a">>])
    end,
    Offered = Call(#{id => 1, metadata => #{<<"x">> => <<"y">>}}),
    ?assertMatch(
        [{ok, #{connection_metadata := 1}}],
        [trevrpc_wire:decode_answer(Answer) || {answer, Answer} <- Offered]
    ),
    ?assertMatch(#{code := ok}, status(Call(#{id => 1}))),
    ?assertMatch(
        #{code := invalid_argument, message := <<"unknown connection metadata set 2">>},
        status(Call(#{id => 2}))
    ),
    close(Session).

%% erlang-webtransport's advice of a handler that waits for its worker
%% deadlocks a call like this one: its call process writes while the
%% session would wait for it to read.
duplex_test(#{port := Port}) ->
    Session = connect(Port, #{compat_mode => legacy_browser_compat}),
    Messages = lists:duplicate(64, binary:copy(<<7>>, ?CHUNK)),
    ?assertEqual({ok, Messages}, call(Session, ?BIDI, Messages)),
    close(Session).

%% A call that reads slowly holds the session's other calls back, but they
%% finish once it read enough.
slow_reader_test(#{port := Port}) ->
    #{client := Session, session := Server} = session(Port),
    Self = self(),
    Chunk = binary:copy(<<1>>, ?CHUNK),
    Slow = spawn_link(fun() ->
        put(router, Self),
        Self ! {self(), call(Session, ?SLOW, lists:duplicate(32, Chunk))}
    end),
    %% The events of the slow call's stream wait here meanwhile.
    ok = wait_until(fun() -> blocked(Server) end),
    Fast = spawn_link(fun() ->
        put(router, Self),
        Self ! {self(), call(Session, ?UNARY, [<<"hi">>])}
    end),
    Results = route(Session, [Slow, Fast], #{}),
    ?assertEqual({ok, [integer_to_binary(32 * ?CHUNK)]}, maps:get(Slow, Results)),
    ?assertEqual({ok, [<<"hi">>]}, maps:get(Fast, Results)),
    close(Session).

%% erlang-webtransport returns a chunk's credit when the session's handler
%% returns, so a session that took every chunk at once would hold the whole
%% upload of a client faster than its call; this one holds about a window
%% beyond what the call read.
bounded_test(#{port := Port}) ->
    #{client := Session, session := Server, conns := Conns} = session(Port),
    Self = self(),
    Count = 128,
    Caller = spawn_link(fun() ->
        put(router, Self),
        Self ! {self(), call(Session, ?SLOW, lists:duplicate(Count, binary:copy(<<1>>, ?CHUNK)))}
    end),
    Sampler = spawn_link(fun() -> sample(Server, Conns, 0) end),
    Results = route(Session, [Caller], #{}),
    ?assertEqual({ok, [integer_to_binary(Count * ?CHUNK)]}, maps:get(Caller, Results)),
    Sampler ! {stop, self()},
    Held =
        receive
            {held, Max} -> Max
        end,
    %% Well under the 2 MiB upload: what is in flight and a window, counted
    %% with the packet buffers its chunks keep alive.
    ?assertMatch(H when H < 1024 * 1024, Held),
    close(Session).

initial_call(Pid) ->
    case proc_lib:initial_call(Pid) of
        {Module, _, _} -> Module;
        false -> undefined
    end.

%% The most binary data the server's session, its connection processes and
%% their call processes ever held at once, sampled every 10 ms.
sample(Session, Conns, Max) ->
    receive
        {stop, Pid} -> Pid ! {held, Max}
    after 10 ->
        Linked = [
            Pid
         || Conn <- Conns,
            {links, Links} <- [process_info(Conn, links)],
            Pid <- Links,
            is_pid(Pid)
        ],
        Held = lists:sum([held(Pid) || Pid <- lists:usort([Session | Conns ++ Linked])]),
        sample(Session, Conns, max(Max, Held))
    end.

held(Pid) ->
    case process_info(Pid, binary) of
        {binary, Binaries} -> lists:sum([Size || {_, Size, _} <- Binaries]);
        undefined -> 0
    end.

%% A session that waits on a call whose handler reads nothing is freed by
%% the call's deadline.
deadline_test(#{port := Port}) ->
    #{client := Session, session := Server} = session(Port),
    {ok, Stream} = webtransport:open_stream(Session, bidi),
    Request = trevrpc_frame:encode(
        request,
        trevrpc_wire:encode_request(#{
            method => ?STUCK, kind => kind(?STUCK), timeout_nanos => 500000000
        })
    ),
    Chunk = trevrpc_frame:encode(message, binary:copy(<<1>>, ?CHUNK)),
    ok = send(Session, Stream, [Request | lists:duplicate(16, Chunk)], nofin),
    ok = wait_until(fun() -> blocked(Server) end),
    Self = self(),
    Unary = spawn_link(fun() ->
        put(router, Self),
        Self ! {self(), call(Session, ?UNARY, [<<"hi">>])}
    end),
    Stuck = spawn_link(fun() ->
        put(router, Self),
        Self !
            {
                self(),
                {status, status(response(Session, Stream, trevrpc_frame:reader(?MAX_FRAME), []))}
            }
    end),
    Self ! {stream, Stuck, Session, Stream},
    Results = route(Session, [Unary, Stuck], #{}),
    ?assertEqual({ok, [<<"hi">>]}, maps:get(Unary, Results)),
    ?assertMatch({status, #{code := deadline_exceeded}}, maps:get(Stuck, Results)),
    close(Session).

%% A call that writes without reading would let its stream run ahead
%% without end, each write freeing the session for another chunk; it fails
%% once its stream is four windows ahead.
overflow_test(#{port := Port}) ->
    Session = connect(Port, #{compat_mode => legacy_browser_compat}),
    {ok, Stream} = webtransport:open_stream(Session, bidi),
    Request = trevrpc_frame:encode(
        request, trevrpc_wire:encode_request(#{method => ?FLOOD, kind => kind(?FLOOD)})
    ),
    Chunk = trevrpc_frame:encode(message, binary:copy(<<1>>, ?CHUNK)),
    true = register(?HOOKS, self()),
    try
        ok = send(Session, Stream, [Request | lists:duplicate(128, Chunk)], nofin),
        Frames = response(Session, Stream, trevrpc_frame:reader(?MAX_FRAME), []),
        ?assertMatch(#{code := resource_exhausted}, status(Frames)),
        %% Its interceptor hears the same.
        receive
            {finished, Status} -> ?assertMatch(#{code := resource_exhausted}, Status)
        after 5000 -> error(no_finish)
        end
    after
        unregister(?HOOKS)
    end,
    close(Session).

peer_test(#{port := Port}) ->
    Session = connect(Port, #{compat_mode => legacy_browser_compat}),
    {ok, [Peer]} = call(Session, ?PEER, [<<>>]),
    ?assertMatch({{127, 0, 0, 1}, _}, binary_to_term(Peer)),
    close(Session).

%% Firefox tears down a stream it gets STOP_SENDING on, with the status it
%% has not read: a call that ends before the client finished its requests
%% stops them only after a while.
stop_test(#{port := Port}) ->
    Session = connect(Port, #{compat_mode => legacy_browser_compat}),
    {ok, Stream} = webtransport:open_stream(Session, bidi),
    Request = trevrpc_frame:encode(
        request, trevrpc_wire:encode_request(#{method => ?FAIL, kind => kind(?FAIL)})
    ),
    ok = send(Session, Stream, [Request, trevrpc_frame:encode(message, <<"x">>)], nofin),
    Sent = erlang:monotonic_time(millisecond),
    Frames = response(Session, Stream, trevrpc_frame:reader(?MAX_FRAME), []),
    [Status] = [Payload || {status, Payload} <- Frames],
    ?assertMatch({ok, #{code := aborted}}, trevrpc_wire:decode_status(Status)),
    receive
        {webtransport, Session, {stream_closed, Stream, {stop_sending, _}}} ->
            ?assert(erlang:monotonic_time(millisecond) - Sent >= 900)
    after 5000 -> error(no_stop_sending)
    end,
    close(Session).

%% Past a mebibyte of requests after the status, the client is stopped at
%% once: with the second the stop otherwise waits made a minute, any stop
%% the test sees is the mebibyte's.
stop_limit_test(#{port := Port}) ->
    ok = application:set_env(trevrpc, webtransport_stop_timeout, 60000),
    Session =
        try
            connect(Port, #{compat_mode => legacy_browser_compat})
        after
            application:unset_env(trevrpc, webtransport_stop_timeout)
        end,
    {ok, Stream} = webtransport:open_stream(Session, bidi),
    Request = trevrpc_frame:encode(
        request, trevrpc_wire:encode_request(#{method => ?FAIL, kind => kind(?FAIL)})
    ),
    ok = send(Session, Stream, [Request, trevrpc_frame:encode(message, <<"x">>)], nofin),
    Frames = response(Session, Stream, trevrpc_frame:reader(?MAX_FRAME), []),
    ?assertMatch(#{code := aborted}, status(Frames)),
    ok = send(Session, Stream, binary:copy(<<0>>, 2 * 1048576), nofin),
    receive
        {webtransport, Session, {stream_closed, Stream, {stop_sending, _}}} -> ok
    after 30000 -> error(no_stop_sending)
    end,
    close(Session).

datagrams_test(#{port := Port}) ->
    Session = connect(Port, #{compat_mode => legacy_browser_compat}),
    {ok, Stream} = webtransport:open_stream(Session, bidi),
    Request = trevrpc_frame:encode(
        request,
        trevrpc_wire:encode_request(#{
            method => ?DATAGRAMS, kind => kind(?DATAGRAMS), datagram_flow => 1
        })
    ),
    ok = send(Session, Stream, [Request, trevrpc_frame:encode(message, <<>>)], fin),
    Frames = response(Session, Stream, trevrpc_frame:reader(?MAX_FRAME), []),
    ?assertMatch(#{code := ok}, status(Frames)),
    %% Each datagram starts with the call's flow.
    Datagrams = datagrams(Session, []),
    ?assertNotEqual([], Datagrams),
    ?assert(
        lists:all(
            fun
                (<<1, _/binary>>) -> true;
                (_) -> false
            end,
            Datagrams
        )
    ),
    close(Session).

datagrams(Session, Acc) ->
    receive
        {webtransport, Session, {datagram, Payload}} -> datagrams(Session, [Payload | Acc])
    after 500 -> lists:reverse(Acc)
    end.

%% erlang-webtransport's client opens one session per connection, so the
%% CONNECTs are sent by hand.
second_session_test(#{port := Port}) ->
    {ok, H3} = quic_h3:connect("127.0.0.1", Port, #{
        settings => wt_h3:client_settings(legacy_browser_compat, #{}),
        sync => true,
        verify => verify_none,
        quic_opts => #{max_datagram_frame_size => 65535, reset_stream_at => true},
        h3_datagram_enabled => true,
        stream_reset_final_size => true
    }),
    Authority = iolist_to_binary(io_lib:format("127.0.0.1:~b", [Port])),
    Connect = fun() ->
        {ok, Id} = wt_h3:request_session(H3, Authority, <<"/trevrpc">>, [], legacy_browser_compat),
        receive
            {quic_h3, H3, {response, Id, Status, _Headers}} -> Status
        after 5000 -> error(no_response)
        end
    end,
    ?assertEqual(200, Connect()),
    ?assertEqual(503, Connect()),
    quic_h3:close(H3).

conn_exit_test(#{port := Port}) ->
    #{client := Session, conn := Conn} = session(Port),
    exit(Conn, kill),
    receive
        {webtransport, Session, closed} -> ok
    after 5000 -> error(not_closed)
    end.

%% A server shutting down cuts a session's calls off, as on every transport
%% (their handlers get the grace period only if they trap exits), and
%% closes the session.
shutdown_test(_Env) ->
    #{cert := Cert, key := Key} = trevrpc_test_tls:credentials(),
    Self = self(),
    {ok, Server} = trevrpc_server:start(#{
        ip => {127, 0, 0, 1},
        cert => Cert,
        key => Key,
        tcp => false,
        webtransport => true,
        webtransport_admission => fun(_) -> true end,
        graceful_shutdown_timeout => 5000,
        routes => #{
            ?SLEEP => #{
                kind => unary,
                handler => fun(_Request, _Call) ->
                    Self ! started,
                    timer:sleep(infinity)
                end
            }
        }
    }),
    {ok, Port} = trevrpc_server:port(Server),
    Session = connect(Port, #{compat_mode => legacy_browser_compat}),
    {ok, Stream} = webtransport:open_stream(Session, bidi),
    Request = [
        trevrpc_frame:encode(
            request, trevrpc_wire:encode_request(#{method => ?SLEEP, kind => kind(?SLEEP)})
        ),
        trevrpc_frame:encode(message, <<"bye">>)
    ],
    ok = send(Session, Stream, Request, fin),
    receive
        started -> ok
    after 5000 -> error(not_started)
    end,
    Stopper = spawn_link(fun() -> Self ! {stopped, trevrpc_server:stop(Server)} end),
    %% Whatever of the call arrives before the session closes has no status.
    Events = until_closed(Session, []),
    ?assertEqual([], [Event || {stream_fin, _, _, _} = Event <- Events]),
    receive
        {stopped, Result} -> ?assertEqual(ok, Result)
    after 10000 -> error({not_stopped, Stopper})
    end.

until_closed(Session, Acc) ->
    receive
        {webtransport, Session, closed} -> lists:reverse(Acc);
        {webtransport, Session, Event} -> until_closed(Session, [Event | Acc])
    after 10000 -> error({not_closed, Acc})
    end.

%%% Server processes

%% A new session, and the server's processes for it: the session, the
%% HTTP/3 connection's and the session's connection processes (`conns'),
%% and the latter (`conn').
session(Port) ->
    Before = processes(),
    Session = connect(Port, #{compat_mode => legacy_browser_compat}),
    Find = fun() ->
        New = processes() -- Before,
        Servers = [P || P <- New, P =/= Session, initial_call(P) =:= webtransport_session],
        Conns = [P || P <- New, initial_call(P) =:= trevrpc_server_conn],
        case {Servers, Conns} of
            {[Server], [_, _]} -> {ok, Server, Conns};
            _ -> false
        end
    end,
    ok = wait_until(fun() -> Find() =/= false end),
    {ok, Server, Conns} = Find(),
    [Conn] = [C || C <- Conns, element(1, element(2, sys:get_state(C))) =:= wt],
    #{client => Session, session => Server, conns => Conns, conn => Conn}.

%% Whether the server's session waits for one of its calls to read.
blocked(Session) ->
    process_info(Session, current_function) =:= {current_function, {trevrpc_webtransport, wait, 2}}.

wait_until(Condition) ->
    wait_until(Condition, 1000).

wait_until(_Condition, 0) ->
    timeout;
wait_until(Condition, Tries) ->
    case Condition() of
        true ->
            ok;
        false ->
            timer:sleep(5),
            wait_until(Condition, Tries - 1)
    end.

%%% Client

connect(Port, Opts) ->
    {ok, Session} = try_connect(Port, <<"/trevrpc">>, Opts),
    Session.

try_connect(Port, Path, Opts) ->
    webtransport:connect(
        "127.0.0.1",
        Port,
        Path,
        maps:merge(
            #{transport => h3, verify => verify_none, timeout => 5000}, Opts
        )
    ).

close(Session) ->
    _ = webtransport:close_session(Session),
    ok.

%% Calls `Method' with `Messages' on a stream of its own, and returns the
%% messages of the response, or the status it ended with. The stream's
%% events come to the calling process: the session's owner, or the process
%% `route/3' hands them to.
call(Session, Method, Messages) ->
    Frames = call_frames(Session, #{method => Method}, Messages),
    case status(Frames) of
        #{code := ok} -> {ok, [M || {message, M} <- Frames]};
        Status -> {error, Status}
    end.

%% The frames of the response to a call whose REQUEST has `Fields'.
call_frames(Session, #{method := Method} = Fields, Messages) ->
    {ok, Stream} = webtransport:open_stream(Session, bidi),
    register_stream(Session, Stream),
    Request = [
        trevrpc_frame:encode(request, trevrpc_wire:encode_request(Fields#{kind => kind(Method)}))
        | [trevrpc_frame:encode(message, M) || M <- Messages]
    ],
    ok = send(Session, Stream, Request, fin),
    response(Session, Stream, trevrpc_frame:reader(?MAX_FRAME), []).

%% The status a response ended with.
status(Frames) ->
    [Payload] = [Payload || {status, Payload} <- Frames],
    {ok, Status} = trevrpc_wire:decode_status(Payload),
    Status.

%% Sends what the session's send buffer takes, waiting for it to drain.
send(Session, Stream, Data, Fin) ->
    send(Session, Stream, iolist_to_binary(Data), Fin, 1000).

send(_Session, _Stream, _Data, _Fin, 0) ->
    {error, send_buffer_full};
send(Session, Stream, Data, Fin, Tries) ->
    case webtransport:send(Session, Stream, Data, Fin) of
        {error, send_buffer_full} ->
            timer:sleep(10),
            send(Session, Stream, Data, Fin, Tries - 1);
        Result ->
            Result
    end.

response(Session, Stream, Reader, Acc) ->
    receive
        {webtransport, Session, {stream, Stream, bidi, Data}} ->
            {ok, Frames, Next} = trevrpc_frame:feed(Data, Reader),
            response(Session, Stream, Next, Acc ++ Frames);
        {webtransport, Session, {stream_fin, Stream, bidi, Data}} ->
            {ok, Frames, _} = trevrpc_frame:feed(Data, Reader),
            Acc ++ Frames;
        {webtransport, Session, {stream_closed, Stream, Reason}} ->
            error({stream_closed, Reason})
    after 30000 -> error(no_response)
    end.

%% Calls from processes other than the session's owner tell it which
%% stream is theirs.
register_stream(Session, Stream) ->
    case get(router) of
        undefined -> ok;
        Router -> Router ! {stream, self(), Session, Stream}
    end.

%% Hands the stream events this process gets to the calls of `Pids', until
%% each returned.
route(_Session, Pids, Results) when map_size(Results) =:= length(Pids) ->
    Results;
route(Session, Pids, Results) ->
    receive
        {Pid, Result} when is_pid(Pid) ->
            route(Session, Pids, Results#{Pid => Result});
        {webtransport, Session, {_, Stream, _, _}} = Event ->
            case get({stream, Stream}) of
                undefined -> self() ! Event;
                Pid -> Pid ! Event
            end,
            route(Session, Pids, Results);
        {webtransport, Session, {stream_closed, Stream, _}} = Event ->
            case get({stream, Stream}) of
                undefined -> self() ! Event;
                Pid -> Pid ! Event
            end,
            route(Session, Pids, Results);
        {stream, Pid, Session, Stream} ->
            put({stream, Stream}, Pid),
            route(Session, Pids, Results)
    after 30000 -> error({timeout, Results})
    end.

%%% The session's handler, called as erlang-webtransport would

%% A handler whose connection process is this one, with stream windows of
%% `Window' bytes and a connection window of `ConnectionWindow'.
handler(Window, ConnectionWindow) ->
    {ok, State} = trevrpc_webtransport:init(self(), #{}, #{
        conn => self(),
        window => Window,
        connection_window => ConnectionWindow,
        quic => self(),
        session_id => 0
    }),
    _ = mailbox(),
    State.

%% Delivers `Bytes' bytes on stream `Id'. A flush the handler posts itself
%% lets it go on if it would wait.
deliver(Id, Bytes, State) ->
    self() ! {trevrpc_wt, flush},
    Result = trevrpc_webtransport:handle_stream(Id, bidi, binary:copy(<<0>>, Bytes), State),
    {element(2, Result), mailbox()}.

%% The messages waiting here, without the handler's own.
mailbox() ->
    receive
        {trevrpc_wt, flush} -> mailbox();
        {trevrpc_wt, {settle, _}} -> mailbox();
        Message -> [Message | mailbox()]
    after 0 -> []
    end.

overflow(Messages) ->
    [Id || {trevrpc_wt, _, {stream_overflow, Id}} <- Messages].

%% A stream four windows ahead of its call fails it.
stream_overflow_test() ->
    State = handler(10, 1000),
    {State1, Before} = deliver(0, 40, State),
    ?assertEqual([], overflow(Before)),
    {_, After} = deliver(0, 1, State1),
    ?assertEqual([0], overflow(After)).

%% So does a stream a window ahead while the session is four connection
%% windows ahead.
connection_overflow_test() ->
    {_, Overflowed} = lists:foldl(
        fun(Id, {State, Acc}) ->
            {Next, Messages} = deliver(Id, 15, State),
            {Next, Acc ++ overflow(Messages)}
        end,
        {handler(10, 25), []},
        [4 * N || N <- lists:seq(0, 7)]
    ),
    %% The seventh stream brings the session to 105 bytes, past 100; its
    %% bytes no longer count, so the eighth brings it back to 105.
    ?assertEqual([24, 28], Overflowed).

%% A write whose writer gave up waiting is dropped while it is queued.
cancel_test() ->
    {State, _} = deliver(0, 1, handler(1000, 100000)),
    {ok, Refused} = trevrpc_webtransport:handle_action_failed(
        {send, 0, <<"first">>}, send_queue_full, State
    ),
    Alias = alias(),
    {ok, Queued, []} = trevrpc_webtransport:handle_info(
        {trevrpc_wt, {send, Alias, 0, <<"later">>, false}}, Refused
    ),
    {ok, Cancelled, []} = trevrpc_webtransport:handle_info({trevrpc_wt, {cancel, Alias}}, Queued),
    Retry =
        receive
            {timeout, _, retry} = Timeout -> Timeout
        after 1000 -> error(no_retry)
        end,
    {ok, Retried, Writes} = trevrpc_webtransport:handle_info(Retry, Cancelled),
    ?assertEqual([{send, 0, <<"first">>}], Writes),
    receive
        {trevrpc_wt, flush} -> ok
    after 0 -> error(no_flush)
    end,
    ?assertMatch({ok, _, []}, trevrpc_webtransport:handle_info({trevrpc_wt, flush}, Retried)),
    receive
        {trevrpc_wt_sent, Alias, _} -> error(answered)
    after 0 -> ok
    end,
    unalias(Alias).

%% A session that waits on a call drops the datagrams written meanwhile,
%% and still takes a drain, a close, a reset or a stop.
waiting_test_() ->
    [
        ?_assertEqual({ok, [drain_session]}, wait_with([{send_datagram, <<"d">>}, drain])),
        ?_assertEqual({ok, [{close_session, 0, <<"server shutdown">>}]}, wait_with([close])),
        ?_assertEqual({ok, [{reset_stream, 0, 1}]}, wait_with([{reset_stream, 0, 1}])),
        %% A stop waits for the client to read; the session no longer does.
        ?_assertEqual({ok, []}, wait_with([{stop_sending, 0, 1}]))
    ].

wait_with(Ops) ->
    State = handler(10, 100000),
    _ = [self() ! {trevrpc_wt, Op} || Op <- Ops],
    Result = trevrpc_webtransport:handle_stream(0, bidi, binary:copy(<<0>>, 20), State),
    _ = mailbox(),
    case Result of
        {ok, _, Actions} -> {ok, Actions};
        {ok, _} -> {ok, []}
    end.
