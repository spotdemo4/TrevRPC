%% The HTTP fallback over HTTP/3, HTTP/2 and HTTP/1.1, against the HTTP
%% clients of quic_h3, erlang_h2 and erlang_h1 (trevrpc_test_http): the requests TrevRPC does
%% not claim, beside calls, the stream slot they take, request and response
%% bodies, failing handlers and shutdown. The cases mirror the Go and Rust
%% servers' fallback tests.
-module(trevrpc_fallback_tests).

-include_lib("eunit/include/eunit.hrl").

-import(trevrpc_test_http, [
    h3_opts/0,
    close/1,
    open/3,
    cancel/2,
    exchange/3,
    response/2,
    response_head/2,
    request/3,
    header/2
]).

-define(UNARY, <<"/test.Echo/Unary">>).
-define(MEDIA_TYPE, <<"application/trevrpc">>).
-define(MAX_FRAME, 1 bsl 22).
-define(CHUNK, 65536).

%%% Fixture

fallback_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Env) ->
        [
            {
                lists:flatten(io_lib:format("~s over ~s", [Name, Version])),
                {timeout, 60, fun() -> run(Test, Env#{version => Version}) end}
            }
         || {Name, Test, Versions} <- [
                {"unclaimed paths go to the fallback", fun serves_test/1, [h3, h2, h1]},
                {"the HTTP path stays TrevRPC's", fun claimed_test/1, [h3, h2, h1]},
                {"a fallback alone serves every path", fun alone_test/1, [h3, h2, h1]},
                {"a request takes a stream slot", fun slot_test/1, [h3, h2]},
                {"a CONNECT off the HTTP path goes to the fallback", fun connect_test/1, [h3]},
                {"a request for another name than the connection's gets 421",
                    fun misdirected_test/1, [h3, h2, h1]},
                {"a WebTransport CONNECT for another name gets 421", fun misdirected_connect_test/1,
                    [h3]},
                {"misdirected_request_check => false serves every name", fun unchecked_test/1, [
                    h3, h2, h1
                ]},
                {"a HEAD response keeps the headers of its body", fun head_test/1, [h3, h2, h1]},
                {"a large response goes out whole", fun large_reply_test/1, [h3, h2, h1]},
                {"a request in flight outlasts the HTTP/1.1 request timeout",
                    fun long_request_test/1, [h1]},
                {"request bodies are read as they come", fun upload_test/1, [h3, h2, h1]},
                {"response bodies are streamed", fun download_test/1, [h3, h2, h1]},
                {"a handler that fails before it answers gets 500", fun crash_test/1, [
                    h3, h2, h1
                ]},
                {"a handler that fails while it streams cuts the response off",
                    fun crash_stream_test/1, [h3, h2, h1]},
                {"a handler that does not answer gets 204", fun silent_test/1, [h3, h2, h1]},
                {"a handler's own Alt-Svc wins", fun own_alt_svc_test/1, [h2, h1]},
                {"a reset stops the handler", fun reset_test/1, [h3, h2]},
                {"shutdown lets requests finish, refuses new ones, then cuts off",
                    fun shutdown_test/1, [h3, h2]},
                {"shutdown closes an HTTP/1.1 connection after its request", fun shutdown_h1_test/1,
                    [h1]},
                {"a client that stops reading is dropped after the TCP user timeout",
                    fun stalled_reader_test/1, [h1]}
            ],
            Version <- Versions
        ] ++
            [
                {"misdirected_request_check must be a boolean", fun() ->
                    run(fun invalid_misdirected_test/1, Env)
                end},
                {"HTTP is not offered without a fallback, HTTP or WebTransport", fun() ->
                    run(fun disabled_test/1, Env)
                end},
                {"the fallback must be a fun of one argument", fun() ->
                    run(fun invalid_test/1, Env)
                end}
            ]
    end}.

%% Handlers report to the test that runs. The tests share a process, so
%% what earlier ones left behind goes first.
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
    {ok, _} = application:ensure_all_started(h1),
    logger:set_primary_config(level, none),
    Credentials = trevrpc_test_tls:credentials(),
    Server = start_server(Credentials, #{http => true, webtransport => true}),
    #{
        credentials => Credentials,
        server => Server,
        port => port(Server),
        tcp_port => tcp_port(Server)
    }.

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
                max_streams_per_connection => 2,
                webtransport_admission => fun(_) -> true end,
                routes => #{
                    ?UNARY => #{kind => unary, handler => fun(Request, _Call) -> {ok, Request} end}
                },
                http_fallback => fun fallback/1
            },
            Opts
        )
    ),
    Server.

fallback(Req) ->
    Test = persistent_term:get(?MODULE),
    case trevrpc_http_request:path(Req) of
        <<"/hold">> ->
            %% Answers once told to, reporting its start.
            Test ! {holding, self()},
            receive
                release -> trevrpc_http_request:reply(Req, 200, [], <<"released">>)
            end;
        <<"/trap">> ->
            %% Traps exits and never answers.
            process_flag(trap_exit, true),
            Test ! {holding, self()},
            receive
            after infinity -> ok
            end;
        <<"/upload">> ->
            {Size, Hash} = read_all(Req, 0, crypto:hash_init(sha256)),
            Body = [integer_to_binary(Size), $\s, binary:encode_hex(Hash)],
            trevrpc_http_request:reply(Req, 200, [], Body);
        <<"/download">> ->
            Count = binary_to_integer(trevrpc_http_request:qs(Req)),
            ok = trevrpc_http_request:stream_reply(Req, 200, [
                {<<"content-type">>, <<"application/octet-stream">>}
            ]),
            lists:foreach(
                fun(N) ->
                    ok = trevrpc_http_request:stream_body(Req, binary:copy(<<N>>, ?CHUNK), nofin)
                end,
                lists:seq(1, Count)
            ),
            trevrpc_http_request:stream_body(Req, <<>>, fin);
        <<"/crash">> ->
            error(crash);
        <<"/crash-stream">> ->
            ok = trevrpc_http_request:stream_reply(Req, 200, []),
            ok = trevrpc_http_request:stream_body(Req, <<"part">>, nofin),
            error(crash);
        <<"/silent">> ->
            ok;
        <<"/head">> ->
            trevrpc_http_request:reply(Req, 200, [{<<"content-length">>, <<"5">>}], <<"hello">>);
        <<"/large">> ->
            trevrpc_http_request:reply(Req, 200, [], binary:copy(<<"l">>, 1024 * 1024));
        <<"/slow">> ->
            %% Streams for longer than the request timeout.
            ok = trevrpc_http_request:stream_reply(Req, 200, []),
            lists:foreach(
                fun(_) ->
                    timer:sleep(100),
                    ok = trevrpc_http_request:stream_body(Req, <<"s">>, nofin)
                end,
                lists:seq(1, 5)
            ),
            trevrpc_http_request:stream_body(Req, <<>>, fin);
        <<"/flood">> ->
            %% Streams until a write fails, and reports the failure.
            Test ! {holding, self()},
            ok = trevrpc_http_request:stream_reply(Req, 200, []),
            flood(Req, Test);
        <<"/alt-svc">> ->
            trevrpc_http_request:reply(Req, 200, [{<<"Alt-Svc">>, <<"clear">>}], <<>>);
        Path ->
            {ok, Body, _} = trevrpc_http_request:read_body(Req),
            Query =
                case trevrpc_http_request:qs(Req) of
                    <<>> -> <<>>;
                    Qs -> <<"?", Qs/binary>>
                end,
            Reply = [
                <<"fallback ">>,
                trevrpc_http_request:protocol(Req),
                $\s,
                trevrpc_http_request:method(Req),
                $\s,
                Path,
                Query,
                $\s,
                trevrpc_http_request:header(<<"x-test">>, Req, <<"-">>),
                $\n,
                Body
            ],
            trevrpc_http_request:reply(
                Req, 200, [{<<"content-type">>, <<"text/plain">>}], Reply
            )
    end.

flood(Req, Test) ->
    case trevrpc_http_request:stream_body(Req, binary:copy(<<"f">>, ?CHUNK), nofin) of
        ok -> flood(Req, Test);
        {error, Reason} -> Test ! {flood_failed, self(), Reason}
    end.

read_all(Req, Size, Hash) ->
    case trevrpc_http_request:read_body(Req, #{length => ?CHUNK}) of
        {ok, Data, _} ->
            {Size + byte_size(Data), crypto:hash_final(crypto:hash_update(Hash, Data))};
        {more, Data, _} ->
            read_all(Req, Size + byte_size(Data), crypto:hash_update(Hash, Data))
    end.

%%% Tests

serves_test(#{version := Version} = Env) ->
    Client = connect(Env),
    %% Request after request, on one connection.
    lists:foreach(
        fun(N) ->
            Body = <<"body ", (integer_to_binary(N))/binary>>,
            {200, Headers, Reply} = exchange(
                Client, request(<<"POST">>, <<"/some/path?n=1">>, [{<<"x-test">>, <<"t">>}]), Body
            ),
            Expected = iolist_to_binary([
                <<"fallback ">>, protocol(Version), <<" POST /some/path?n=1 t\n">>, Body
            ]),
            ?assertEqual(Expected, Reply),
            ?assertEqual(<<"text/plain">>, header(<<"content-type">>, Headers)),
            ?assertEqual(alt_svc(Version, Env), header(<<"alt-svc">>, Headers))
        end,
        [1, 2]
    ),
    close(Client).

claimed_test(#{version := Version} = Env) ->
    Client = connect(Env),
    %% A GET at the HTTP path is not a call, nor the fallback's.
    {Status, _, _} = exchange(Client, request(<<"GET">>, <<"/trevrpc">>, []), <<>>),
    case Version of
        h1 -> ?assertEqual(505, Status);
        _ -> ?assertEqual(405, Status)
    end,
    close(Client),
    %% Calls carry on beside the fallback.
    case Version of
        h1 ->
            ok;
        _ ->
            Rpc = connect(Env),
            Body = iolist_to_binary([
                trevrpc_frame:encode(
                    request, trevrpc_wire:encode_request(#{method => ?UNARY, kind => unary})
                ),
                trevrpc_frame:encode(message, <<"hello">>)
            ]),
            {200, _, Frames} = exchange(
                Rpc, request(<<"POST">>, <<"/trevrpc">>, [{<<"content-type">>, ?MEDIA_TYPE}]), Body
            ),
            {ok, Decoded, _} = trevrpc_frame:feed(Frames, trevrpc_frame:reader(?MAX_FRAME)),
            ?assertEqual([<<"hello">>], [M || {message, M} <- Decoded]),
            close(Rpc)
    end.

alone_test(#{version := Version, credentials := Credentials} = Env) ->
    Server = start_server(Credentials, #{}),
    try
        Client = connect(Env#{port => port(Server), tcp_port => tcp_port(Server)}),
        %% With neither HTTP nor WebTransport, nothing is claimed.
        {200, _, Reply} = exchange(Client, request(<<"GET">>, <<"/trevrpc">>, []), <<>>),
        Expected = iolist_to_binary([<<"fallback ">>, protocol(Version), <<" GET /trevrpc -\n">>]),
        ?assertEqual(Expected, Reply),
        close(Client),
        %% Native calls go on.
        #{cacerts := CaCerts} = Credentials,
        {ok, Channel} = trevrpc_channel:connect(
            #{host => "localhost", port => port(Server), cacerts => CaCerts, transport => quic},
            5000
        ),
        ?assertMatch(
            {ok, <<"native">>, _},
            trevrpc_client:unary(Channel, ?UNARY, <<"native">>, #{timeout => 5000})
        ),
        trevrpc_channel:close(Channel)
    after
        trevrpc_server:stop(Server)
    end.

slot_test(#{credentials := Credentials} = Env) ->
    Server = start_server(Credentials, #{}),
    Client = connect(Env#{port => port(Server), tcp_port => tcp_port(Server)}),
    Held = [
        begin
            {ok, Id} = open(Client, request(<<"GET">>, <<"/hold">>, []), true),
            {Id, holding()}
        end
     || _ <- [1, 2]
    ],
    ?assertMatch({503, _, _}, exchange(Client, request(<<"GET">>, <<"/other">>, []), <<>>)),
    [{Id1, Pid1}, {Id2, Pid2}] = Held,
    [Owner] = connections(Server),
    trace_received(Owner),
    Pid1 ! release,
    ?assertMatch({200, _, <<"released">>}, response(Client, Id1)),
    %% The slot it held is free again once the connection handled the end
    %% of the request's process.
    await_received(Owner, fun
        ({'DOWN', _, process, Pid, _}) -> Pid =:= Pid1;
        (_) -> false
    end),
    _ = sys:get_state(Owner),
    ?assertMatch({200, _, _}, exchange(Client, request(<<"GET">>, <<"/other">>, []), <<>>)),
    Pid2 ! release,
    ?assertMatch({200, _, <<"released">>}, response(Client, Id2)),
    close(Client),
    trevrpc_server:stop(Server).

%% Over HTTP/3, a CONNECT is a WebTransport session only at the HTTP path.
%% quic_h3 sends and takes an extended CONNECT only on a connection whose
%% client advertised extended CONNECT itself.
connect_test(#{port := Port}) ->
    {ok, Conn} = quic_h3:connect(
        "127.0.0.1", Port, (h3_opts())#{settings => #{enable_connect_protocol => 1}}
    ),
    Client = {quic_h3, Conn},
    Connect = [
        {<<":method">>, <<"CONNECT">>},
        {<<":protocol">>, <<"webtransport">>},
        {<<":scheme">>, <<"https">>},
        {<<":authority">>, <<"localhost">>},
        {<<":path">>, <<"/tunnel">>}
    ],
    ?assertMatch(
        {200, _, <<"fallback HTTP/3.0 CONNECT /tunnel -\n">>}, exchange(Client, Connect, <<>>)
    ),
    close(Client).

%% The clients send the server name localhost. The port, case and a
%% trailing dot do not count, and a connection without a server name serves
%% every host.
misdirected_test(Env) ->
    Client = connect(Env, "localhost"),
    Misdirected = [
        for(<<"other.example">>, request(<<"GET">>, <<"/app">>, [])),
        for(<<"other.example:443">>, request(<<"GET">>, <<"/trevrpc">>, [])),
        for(
            <<"other.example">>,
            request(<<"POST">>, <<"/trevrpc">>, [{<<"content-type">>, ?MEDIA_TYPE}])
        )
    ],
    lists:foreach(
        fun(Request) -> ?assertMatch({421, _, _}, exchange(Client, Request, <<>>)) end, Misdirected
    ),
    lists:foreach(
        fun(Host) ->
            ?assertMatch(
                {200, _, <<"fallback ", _/binary>>},
                exchange(Client, for(Host, request(<<"GET">>, <<"/app">>, [])), <<>>)
            )
        end,
        [<<"localhost">>, <<"LocalHost.:443">>]
    ),
    close(Client),
    Unnamed = connect(Env),
    ?assertMatch(
        {200, _, <<"fallback ", _/binary>>},
        exchange(Unnamed, for(<<"other.example">>, request(<<"GET">>, <<"/app">>, [])), <<>>)
    ),
    close(Unnamed).

misdirected_connect_test(#{port := Port}) ->
    {ok, Conn} = quic_h3:connect("127.0.0.1", Port, (h3_opts())#{
        settings => #{enable_connect_protocol => 1}, quic_opts => #{server_name => "localhost"}
    }),
    Client = {quic_h3, Conn},
    Connect = [
        {<<":method">>, <<"CONNECT">>},
        {<<":protocol">>, <<"webtransport">>},
        {<<":scheme">>, <<"https">>},
        {<<":authority">>, <<"other.example">>},
        {<<":path">>, <<"/trevrpc">>}
    ],
    ?assertMatch({421, _, _}, exchange(Client, Connect, <<>>)),
    close(Client).

unchecked_test(#{credentials := Credentials} = Env) ->
    Server = start_server(Credentials, #{http => true, misdirected_request_check => false}),
    try
        Client = connect(Env#{port => port(Server), tcp_port => tcp_port(Server)}, "localhost"),
        ?assertMatch(
            {200, _, <<"fallback ", _/binary>>},
            exchange(Client, for(<<"other.example">>, request(<<"GET">>, <<"/app">>, [])), <<>>)
        ),
        close(Client)
    after
        trevrpc_server:stop(Server)
    end.

head_test(Env) ->
    Client = connect(Env),
    {200, Headers, Body} = exchange(Client, request(<<"HEAD">>, <<"/head">>, []), <<>>),
    ?assertEqual(<<"5">>, header(<<"content-length">>, Headers)),
    ?assertEqual(<<>>, Body),
    %% The connection carries on.
    ?assertMatch(
        {200, _, <<"hello">>}, exchange(Client, request(<<"GET">>, <<"/head">>, []), <<>>)
    ),
    close(Client).

large_reply_test(Env) ->
    Client = connect(Env),
    {200, _, Body} = exchange(Client, request(<<"GET">>, <<"/large">>, []), <<>>),
    ?assertEqual(binary:copy(<<"l">>, 1024 * 1024), Body),
    close(Client).

%% erlang_h1's own timers would cut off a response to a client that sends
%% nothing, and a body that pauses: the connection is idle only without a
%% request in flight.
long_request_test(#{credentials := Credentials} = Env) ->
    Server = start_server(Credentials, #{tls_handshake_timeout => 200}),
    try
        Client = connect(Env#{tcp_port => tcp_port(Server)}),
        ?assertMatch(
            {200, _, <<"sssss">>}, exchange(Client, request(<<"GET">>, <<"/slow">>, []), <<>>)
        ),
        close(Client),
        {ok, Socket} = ssl:connect(
            {127, 0, 0, 1},
            tcp_port(Server),
            [
                binary,
                {active, false},
                {verify, verify_none},
                {alpn_advertised_protocols, [<<"http/1.1">>]}
            ],
            5000
        ),
        ok = ssl:send(
            Socket, <<"POST /upload HTTP/1.1\r\nhost: x\r\ntransfer-encoding: chunked\r\n\r\n">>
        ),
        lists:foreach(
            fun(_) ->
                timer:sleep(100),
                ok = ssl:send(Socket, <<"1\r\nu\r\n">>)
            end,
            lists:seq(1, 5)
        ),
        ok = ssl:send(Socket, <<"0\r\n\r\n">>),
        Expected = iolist_to_binary([<<"5 ">>, binary:encode_hex(crypto:hash(sha256, <<"uuuuu">>))]),
        {ok, Response} = ssl:recv(Socket, 0, 5000),
        ?assertMatch([<<"HTTP/1.1 200 OK">> | _], binary:split(Response, <<"\r\n">>)),
        ?assertEqual(Expected, lists:last(binary:split(Response, <<"\r\n\r\n">>))),
        %% With no request in flight, the connection is closed after the
        %% request timeout.
        ?assertEqual({error, closed}, ssl:recv(Socket, 0, 5000))
    after
        trevrpc_server:stop(Server)
    end.

upload_test(Env) ->
    Client = connect(Env),
    Body = crypto:strong_rand_bytes(3 * 1024 * 1024 + 17),
    Expected = iolist_to_binary([
        integer_to_binary(byte_size(Body)), $\s, binary:encode_hex(crypto:hash(sha256, Body))
    ]),
    ?assertMatch(
        {200, _, Expected}, exchange(Client, request(<<"POST">>, <<"/upload">>, []), Body)
    ),
    close(Client).

download_test(Env) ->
    Client = connect(Env),
    Count = 64,
    {200, Headers, Body} = exchange(
        Client, request(<<"GET">>, <<"/download">>, [], <<"64">>), <<>>
    ),
    ?assertEqual(<<"application/octet-stream">>, header(<<"content-type">>, Headers)),
    Expected = iolist_to_binary([binary:copy(<<N>>, ?CHUNK) || N <- lists:seq(1, Count)]),
    ?assertEqual(byte_size(Expected), byte_size(Body)),
    ?assert(Expected =:= Body),
    close(Client).

crash_test(Env) ->
    Client = connect(Env),
    ?assertMatch({500, _, <<>>}, exchange(Client, request(<<"GET">>, <<"/crash">>, []), <<>>)),
    %% The connection carries on.
    ?assertMatch({200, _, _}, exchange(Client, request(<<"GET">>, <<"/after">>, []), <<>>)),
    close(Client).

crash_stream_test(#{version := Version} = Env) ->
    {Tag, Conn} = Client = connect(Env),
    {ok, Id} = open(Client, request(<<"GET">>, <<"/crash-stream">>, []), true),
    {200, _, _, false} = response_head(Client, Id),
    %% The client learns that the body is cut off: the stream is reset, or
    %% over HTTP/1.1, which cannot reset one, the connection closes.
    Cut = fun Cut() ->
        receive
            {Tag, Conn, {data, Id, _, false}} -> Cut();
            {Tag, Conn, {data, Id, _, true}} -> error(finished);
            {Tag, Conn, {stream_reset, Id, _}} -> ok;
            {Tag, Conn, {closed, _}} when Version =:= h1 -> ok
        after 5000 -> error(not_cut_off)
        end
    end,
    ok = Cut(),
    close(Client).

silent_test(Env) ->
    Client = connect(Env),
    ?assertMatch({204, _, <<>>}, exchange(Client, request(<<"GET">>, <<"/silent">>, []), <<>>)),
    close(Client).

own_alt_svc_test(Env) ->
    Client = connect(Env),
    {200, Headers, _} = exchange(Client, request(<<"GET">>, <<"/alt-svc">>, []), <<>>),
    ?assertEqual([<<"clear">>], [V || {<<"alt-svc">>, V} <- Headers]),
    close(Client).

reset_test(Env) ->
    Client = connect(Env),
    {ok, Id} = open(Client, request(<<"GET">>, <<"/hold">>, []), true),
    Handler = holding(),
    Monitor = monitor(process, Handler),
    cancel(Client, Id),
    receive
        {'DOWN', Monitor, process, Handler, _} -> ok
    after 5000 -> error(not_stopped)
    end,
    %% Its slot is free again.
    ?assertMatch({200, _, _}, exchange(Client, request(<<"GET">>, <<"/a">>, []), <<>>)),
    ?assertMatch({200, _, _}, exchange(Client, request(<<"GET">>, <<"/b">>, []), <<>>)),
    close(Client).

shutdown_test(#{credentials := Credentials} = Env) ->
    Server = start_server(Credentials, #{graceful_shutdown_timeout => 500}),
    Client = connect(Env#{port => port(Server), tcp_port => tcp_port(Server)}),
    {ok, Held} = open(Client, request(<<"GET">>, <<"/hold">>, []), true),
    Holder = holding(),
    {ok, Trapped} = open(Client, request(<<"GET">>, <<"/trap">>, []), true),
    Trapper = holding(),
    Monitor = monitor(process, Trapper),
    %% The connection starts shutting down with a request on its way, which
    %% it takes before its GOAWAY, and refuses.
    [Owner] = connections(Server),
    trace_received(Owner),
    ok = sys:suspend(Owner),
    ok = trevrpc_server_conn:shutdown(Owner),
    {ok, Late} = open(Client, request(<<"GET">>, <<"/late">>, []), true),
    await_received(Owner, fun
        ({_, _, {request, _, _, <<"/late">>, _}}) -> true;
        (_) -> false
    end),
    ok = sys:resume(Owner),
    ?assertMatch({503, _, _}, response(Client, Late)),
    %% A request in flight finishes within the graceful shutdown timeout,
    %% and one that does not is cut off after it.
    Holder ! release,
    ?assertMatch({200, _, <<"released">>}, response(Client, Held)),
    {Tag, Conn} = Client,
    receive
        {Tag, Conn, {stream_reset, Trapped, _}} -> ok
    after 5000 -> error(not_reset)
    end,
    receive
        {'DOWN', Monitor, process, Trapper, _} -> ok
    after 5000 -> error(not_killed)
    end,
    trevrpc_server:stop(Server).

shutdown_h1_test(#{credentials := Credentials} = Env) ->
    Server = start_server(Credentials, #{graceful_shutdown_timeout => 500}),
    {h1, Conn} = Client = connect(Env#{port => port(Server), tcp_port => tcp_port(Server)}),
    {ok, Held} = open(Client, request(<<"GET">>, <<"/hold">>, []), true),
    Holder = holding(),
    [Owner] = connections(Server),
    ok = trevrpc_server_conn:shutdown(Owner),
    %% The shutdown is a cast: the call waits for it to be handled, so that
    %% the response goes out after it.
    _ = sys:get_state(Owner),
    Holder ! release,
    {200, Headers, <<"released">>} = response(Client, Held),
    ?assertEqual(<<"close">>, header(<<"connection">>, Headers)),
    receive
        {h1, Conn, {closed, _}} -> ok
    after 5000 -> error(not_closed)
    end,
    trevrpc_server:stop(Server).

%% erlang_h1's writes wait as long as the socket does: a client that stops
%% reading holds the handler's write until the kernel drops the connection
%% after `tcp_user_timeout' (TCP_USER_TIMEOUT, which only Linux has). The
%% write then fails, unless the connection's process, seeing the socket
%% close between two writes, kills the handler first.
stalled_reader_test(#{credentials := Credentials}) ->
    case os:type() of
        {unix, linux} -> stalled_reader(Credentials);
        _ -> ok
    end.

stalled_reader(Credentials) ->
    Server = start_server(Credentials, #{tcp_user_timeout => 500}),
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1},
        tcp_port(Server),
        [
            binary,
            {active, false},
            {recbuf, 4096},
            {verify, verify_none},
            {alpn_advertised_protocols, [<<"http/1.1">>]}
        ],
        5000
    ),
    try
        ok = ssl:send(Socket, <<"GET /flood HTTP/1.1\r\nhost: x\r\n\r\n">>),
        Handler = holding(),
        Monitor = monitor(process, Handler),
        Start = erlang:monotonic_time(millisecond),
        %% Under 5 s, so that a call timeout of that length would fail the
        %% test: the drop takes about 0.9 s. A call that timed out would
        %% fail the write with `{error, closed}' too (trevrpc_transport's
        %% ?SAFE), so the reason cannot tell them apart.
        receive
            {flood_failed, Handler, _} -> ok;
            {'DOWN', Monitor, process, Handler, killed} -> ok
        after 4000 -> error(not_dropped)
        end,
        ?assert(erlang:monotonic_time(millisecond) - Start >= 500)
    after
        _ = ssl:close(Socket),
        trevrpc_server:stop(Server)
    end.

disabled_test(#{credentials := #{cert := Cert, key := Key}}) ->
    {ok, Server} = trevrpc_server:start(#{
        ip => {127, 0, 0, 1}, cert => Cert, key => Key, routes => #{}
    }),
    ?assertMatch({error, _}, quic_h3:connect("127.0.0.1", port(Server), h3_opts())),
    ?assertMatch({error, _}, h2:connect("127.0.0.1", tcp_port(Server), #{verify => verify_none})),
    trevrpc_server:stop(Server).

invalid_misdirected_test(#{credentials := #{cert := Cert, key := Key}}) ->
    ?assertEqual(
        {error, {invalid_option, misdirected_request_check}},
        trevrpc_server:start(#{
            cert => Cert, key => Key, routes => #{}, misdirected_request_check => yes
        })
    ).

invalid_test(#{credentials := #{cert := Cert, key := Key}}) ->
    ?assertEqual(
        {error, {invalid_option, http_fallback}},
        trevrpc_server:start(#{
            cert => Cert, key => Key, routes => #{}, http_fallback => fun(_, _) -> ok end
        })
    ).

%%% Helpers

port(Server) ->
    {ok, Port} = trevrpc_server:port(Server),
    Port.

tcp_port(Server) ->
    {ok, Port} = trevrpc_server:tcp_port(Server),
    Port.

connections(Server) ->
    maps:values(element(5, sys:get_state(Server))).

%% Traces the messages that arrive in Pid's mailbox, even while it is
%% suspended, until await_received/2 sees the one it waits for.
trace_received(Pid) ->
    1 = erlang:trace(Pid, true, ['receive']),
    ok.

await_received(Pid, Match) ->
    receive
        {trace, Pid, 'receive', Message} ->
            case Match(Message) of
                true ->
                    erlang:trace(Pid, false, ['receive']),
                    flush_trace(Pid);
                false ->
                    await_received(Pid, Match)
            end
    after 5000 -> error(not_received)
    end.

flush_trace(Pid) ->
    receive
        {trace, Pid, _, _} -> flush_trace(Pid)
    after 0 -> ok
    end.

holding() ->
    receive
        {holding, Pid} -> Pid
    after 5000 -> error(not_started)
    end.

protocol(h3) -> <<"HTTP/3.0">>;
protocol(h2) -> <<"HTTP/2.0">>;
protocol(h1) -> <<"HTTP/1.1">>.

alt_svc(h3, _Env) -> <<>>;
alt_svc(_, #{port := Port}) -> iolist_to_binary(io_lib:format("h3=\":~b\"; ma=86400", [Port])).

request(Method, Path, Extra, Query) ->
    request(Method, <<Path/binary, "?", Query/binary>>, Extra).

%% A client of the test's version, on the server's port for it.
connect(#{version := h3, port := Port}) -> trevrpc_test_http:connect(h3, Port);
connect(#{version := Version, tcp_port := Port}) -> trevrpc_test_http:connect(Version, Port).

%% A client like connect/1 that sends `ServerName' as the TLS server name.
connect(#{version := h3, port := Port}, ServerName) ->
    trevrpc_test_http:connect(h3, Port, ServerName);
connect(#{version := Version, tcp_port := Port}, ServerName) ->
    trevrpc_test_http:connect(Version, Port, ServerName).

%% `Request' for `Authority'.
for(Authority, Request) ->
    lists:keystore(<<":authority">>, 1, Request, {<<":authority">>, Authority}).
