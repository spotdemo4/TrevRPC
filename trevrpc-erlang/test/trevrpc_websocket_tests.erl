%% QMux over WebSocket on the server's TCP listener, against a browser
%% played by hand: the HTTP/1.1 upgrade, its refusals, and a QMux session
%% over the upgraded connection.
-module(trevrpc_websocket_tests).

-include_lib("eunit/include/eunit.hrl").

-define(UNARY, <<"/test.Echo/Unary">>).
-define(MAX_RECORD, 65536).

%%% Fixture

websocket_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Env) ->
        [
            {Name, {timeout, 30, fun() -> Test(Env) end}}
         || {Name, Test} <- [
                {"a WebSocket upgrade carries QMux", fun session_test/1},
                {"other paths get 404", fun not_found_test/1},
                {"other requests at the HTTP path get 505", fun not_upgrade_test/1},
                {"the subprotocol is required", fun subprotocol_test/1},
                {"invalid WebSocket headers get 400", fun invalid_test/1},
                {"the admission decides", fun admission_test/1},
                {"a connection without ALPN is HTTP/1.1", fun no_alpn_test/1},
                {"pings are answered", fun ping_test/1},
                {"text messages close the session", fun text_test/1},
                {"messages over max_record_size close the session", fun too_big_test/1},
                {"the peer's Close is answered", fun close_test/1},
                {"browsers are refused without an admission", fun default_admission_test/1},
                {"a connection closes after a refused request", fun one_request_test/1},
                {"an upgrade must be a GET", fun method_test/1},
                {"an idle connection is closed", fun idle_test/1},
                {"a full server refuses upgrades", fun full_test/1},
                {"HTTP/1.1 is not offered without webtransport", fun disabled_test/1}
            ]
        ]
    end}.

setup() ->
    {ok, _} = application:ensure_all_started(trevrpc),
    logger:set_primary_config(level, none),
    Credentials = trevrpc_test_tls:credentials(),
    Admission = fun
        (#{origin := <<"https://evil.example">>}) -> false;
        (#{origin := <<"https://crash.example">>}) -> error(crash);
        (#{}) -> true
    end,
    Server = start_server(Credentials, #{webtransport_admission => Admission}),
    #{credentials => Credentials, server => Server, port => tcp_port(Server)}.

cleanup(#{server := Server}) ->
    trevrpc_server:stop(Server),
    logger:set_primary_config(level, notice).

start_server(#{cert := Cert, key := Key}, Opts) ->
    {ok, Server} = trevrpc_server:start(
        maps:merge(
            #{
                ip => {127, 0, 0, 1},
                cert => Cert,
                key => Key,
                webtransport => true,
                routes => #{
                    ?UNARY => #{kind => unary, handler => fun(Request, _Call) -> {ok, Request} end}
                }
            },
            Opts
        )
    ),
    Server.

tcp_port(Server) ->
    {ok, Port} = trevrpc_server:tcp_port(Server),
    Port.

%%% Tests

session_test(#{port := Port}) ->
    {101, Headers, Received, Socket} = upgrade(Port, <<"/trevrpc">>, []),
    ?assertEqual(<<"qmux-02.trevrpc-1">>, header(<<"sec-websocket-protocol">>, Headers)),
    Parameters = trevrpc_quic:qmux_parameters(1 bsl 22, 4, 0, infinity),
    {ok, Session} = trevrpc_qmux:start(Socket, client, self(), #{
        parameters => Parameters, framing => websocket, received => Received
    }),
    receive
        {trevrpc_qmux, Session, {connected, _}} -> ok
    after 5000 -> error(not_connected)
    end,
    {ok, Id} = trevrpc_qmux:open_stream(Session),
    Request = [
        trevrpc_frame:encode(request, trevrpc_wire:encode_request(#{method => ?UNARY})),
        trevrpc_frame:encode(message, <<"hello">>)
    ],
    ok = trevrpc_qmux:send_data(Session, Id, Request, true),
    Frames = response(Session, Id, trevrpc_frame:reader(1 bsl 22), []),
    ?assert(lists:member({message, <<"hello">>}, Frames)),
    [Status] = [Payload || {status, Payload} <- Frames],
    ?assertMatch({ok, #{code := ok}}, trevrpc_wire:decode_status(Status)),
    trevrpc_qmux:close(Session, 0).

%% The frames of a stream's response, up to its FIN.
response(Session, Id, Reader, Acc) ->
    receive
        {trevrpc_qmux, Session, {stream_data, Id, Bin, Fin}} ->
            %% A stream that ended is gone, and needs no credit.
            _ = trevrpc_qmux:consume(Session, Id, byte_size(Bin)),
            {ok, Frames, Next} = trevrpc_frame:feed(Bin, Reader),
            case Fin of
                true -> Acc ++ Frames;
                false -> response(Session, Id, Next, Acc ++ Frames)
            end;
        {trevrpc_qmux, Session, {closed, Reason}} ->
            error({closed, Reason})
    after 5000 -> error(no_response)
    end.

not_found_test(#{port := Port}) ->
    ?assertMatch({404, _, _, _}, upgrade(Port, <<"/other">>, [])),
    %% The query is not part of the path.
    ?assertMatch({101, _, _, _}, upgrade(Port, <<"/trevrpc?x=1">>, [])).

not_upgrade_test(#{port := Port}) ->
    ?assertMatch(
        {505, _, _, _},
        request(Port, [<<"http/1.1">>], <<"GET /trevrpc HTTP/1.1\r\nhost: x\r\n\r\n">>)
    ).

subprotocol_test(#{port := Port}) ->
    ?assertMatch(
        {400, _, _, _},
        upgrade(Port, <<"/trevrpc">>, [{<<"sec-websocket-protocol">>, <<"chat, qmux-01">>}])
    ),
    %% Offered among others, it is selected.
    {101, Headers, _, _} = upgrade(Port, <<"/trevrpc">>, [
        {<<"sec-websocket-protocol">>, <<"chat, qmux-02.trevrpc-1">>}
    ]),
    ?assertEqual(<<"qmux-02.trevrpc-1">>, header(<<"sec-websocket-protocol">>, Headers)).

invalid_test(#{port := Port}) ->
    ?assertMatch(
        {400, _, _, _}, upgrade(Port, <<"/trevrpc">>, [{<<"sec-websocket-version">>, <<"8">>}])
    ),
    ?assertMatch(
        {400, _, _, _}, upgrade(Port, <<"/trevrpc">>, [{<<"sec-websocket-key">>, <<"short">>}])
    ).

admission_test(#{port := Port}) ->
    Origin = fun(O) -> [{<<"origin">>, O}] end,
    ?assertMatch({403, _, _, _}, upgrade(Port, <<"/trevrpc">>, Origin(<<"https://evil.example">>))),
    ?assertMatch(
        {500, _, _, _}, upgrade(Port, <<"/trevrpc">>, Origin(<<"https://crash.example">>))
    ),
    ?assertMatch({101, _, _, _}, upgrade(Port, <<"/trevrpc">>, Origin(<<"https://app.example">>))).

no_alpn_test(#{port := Port}) ->
    ?assertMatch({101, _, _, _}, upgrade(Port, <<"/trevrpc">>, [], [])).

ping_test(#{port := Port}) ->
    {101, _, Received, Socket} = upgrade(Port, <<"/trevrpc">>, []),
    ok = ssl:send(Socket, ws_frame:encode({ping, <<"p">>}, client)),
    ?assert(lists:member({pong, <<"p">>}, read_until(Socket, Received, {pong, <<"p">>}))).

text_test(#{port := Port}) ->
    {101, _, Received, Socket} = upgrade(Port, <<"/trevrpc">>, []),
    ok = ssl:send(Socket, ws_frame:encode({text, <<"hi">>}, client)),
    Messages = read_all(Socket, Received),
    ?assertMatch({close, 1003, _}, lists:last(Messages)),
    %% A CONNECTION_CLOSE comes before it.
    ?assertMatch(
        {ok, [{connection_close, transport, _, _, _}]},
        trevrpc_qmux_frame:decode_frames(last_record(Messages))
    ).

too_big_test(#{port := Port}) ->
    {101, _, Received, Socket} = upgrade(Port, <<"/trevrpc">>, []),
    ok = ssl:send(Socket, ws_frame:encode({binary, binary:copy(<<0>>, ?MAX_RECORD + 1)}, client)),
    ?assertMatch({close, 1009, _}, lists:last(read_all(Socket, Received))).

close_test(#{port := Port}) ->
    {101, _, Received, Socket} = upgrade(Port, <<"/trevrpc">>, []),
    ok = ssl:send(Socket, ws_frame:encode({close, 1000, <<>>}, client)),
    Messages = read_all(Socket, Received),
    ?assertEqual([{close, 1000, <<>>}], [M || M <- Messages, element(1, M) =:= close]).

default_admission_test(#{credentials := Credentials}) ->
    Server = start_server(Credentials, #{}),
    try
        Port = tcp_port(Server),
        ?assertMatch(
            {403, _, _, _}, upgrade(Port, <<"/trevrpc">>, [{<<"origin">>, <<"https://a.example">>}])
        ),
        ?assertMatch({101, _, _, _}, upgrade(Port, <<"/trevrpc">>, []))
    after
        trevrpc_server:stop(Server)
    end.

%% A client cannot hold its handshake slot by sending request after request.
one_request_test(#{port := Port}) ->
    Head = <<"GET / HTTP/1.1\r\nhost: x\r\n\r\n">>,
    {404, Headers, <<"not found\n">>, Socket} = request(Port, [<<"http/1.1">>], Head),
    ?assertEqual(<<"close">>, header(<<"connection">>, Headers)),
    _ = ssl:send(Socket, Head),
    ?assertEqual({error, closed}, ssl:recv(Socket, 0, 5000)).

method_test(#{port := Port}) ->
    {505, _, _, _} = upgrade(Port, <<"/trevrpc">>, [], [<<"http/1.1">>], <<"POST">>),
    ?assertMatch({101, _, _, _}, upgrade(Port, <<"/trevrpc">>, [], [<<"http/1.1">>], <<"GET">>)).

idle_test(#{credentials := Credentials}) ->
    Server = start_server(Credentials, #{tls_handshake_timeout => 200}),
    try
        {ok, Socket} = connect(tcp_port(Server), [<<"http/1.1">>]),
        ?assertEqual({error, closed}, ssl:recv(Socket, 0, 5000))
    after
        trevrpc_server:stop(Server)
    end.

full_test(#{credentials := Credentials}) ->
    Server = start_server(Credentials, #{max_connections => 1}),
    try
        Port = tcp_port(Server),
        {101, _, Received, First} = upgrade(Port, <<"/trevrpc">>, []),
        %% The server sends its QMux parameters once it counted the session.
        ?assertMatch([{binary, _} | _], first_messages(First, Received)),
        ?assertMatch({503, _, _, _}, upgrade(Port, <<"/trevrpc">>, []))
    after
        trevrpc_server:stop(Server)
    end.

disabled_test(#{credentials := Credentials}) ->
    Server = start_server(Credentials, #{webtransport => false}),
    try
        Port = tcp_port(Server),
        ?assertMatch({error, _}, connect(Port, [<<"http/1.1">>])),
        {ok, Socket} = connect(Port, []),
        %% The server closes a connection it does not serve right after the
        %% handshake, so the request may fail to go out. Either way no
        %% response comes.
        _ = ssl:send(Socket, <<"GET /trevrpc HTTP/1.1\r\nhost: x\r\n\r\n">>),
        ?assertEqual({error, closed}, ssl:recv(Socket, 0, 5000))
    after
        trevrpc_server:stop(Server)
    end.

%%% The browser

connect(Port, Alpn) ->
    AlpnOpts = [{alpn_advertised_protocols, Alpn} || Alpn =/= []],
    ssl:connect(
        {127, 0, 0, 1},
        Port,
        [binary, {active, false}, {verify, verify_none} | AlpnOpts],
        5000
    ).

%% Sends a WebSocket upgrade to Path with Extra replacing or adding headers,
%% and returns the response's status, headers and the bytes past them, with
%% the socket.
upgrade(Port, Path, Extra) ->
    upgrade(Port, Path, Extra, [<<"http/1.1">>]).

upgrade(Port, Path, Extra, Alpn) ->
    upgrade(Port, Path, Extra, Alpn, <<"GET">>).

upgrade(Port, Path, Extra, Alpn, Method) ->
    Base = [
        {<<"host">>, <<"localhost">>},
        {<<"upgrade">>, <<"websocket">>},
        {<<"connection">>, <<"Upgrade">>},
        {<<"sec-websocket-key">>, base64:encode(crypto:strong_rand_bytes(16))},
        {<<"sec-websocket-version">>, <<"13">>},
        {<<"sec-websocket-protocol">>, <<"qmux-02.trevrpc-1">>}
    ],
    Headers = lists:foldl(
        fun({Name, Value}, Acc) -> lists:keystore(Name, 1, Acc, {Name, Value}) end, Base, Extra
    ),
    Head = [
        Method,
        <<" ">>,
        Path,
        <<" HTTP/1.1\r\n">>,
        [[Name, <<": ">>, Value, <<"\r\n">>] || {Name, Value} <- Headers],
        <<"\r\n">>
    ],
    request(Port, Alpn, Head).

request(Port, Alpn, Head) ->
    {ok, Socket} = connect(Port, Alpn),
    ok = ssl:send(Socket, Head),
    {Status, Headers, Rest} = read_head(Socket, <<>>),
    {Status, Headers, Rest, Socket}.

read_head(Socket, Acc) ->
    case binary:split(Acc, <<"\r\n\r\n">>) of
        [Head, Rest] ->
            [StatusLine | Lines] = binary:split(Head, <<"\r\n">>, [global]),
            [_, Code | _] = binary:split(StatusLine, <<" ">>, [global]),
            Headers = [
                begin
                    [Name, Value] = binary:split(Line, <<":">>),
                    {string:lowercase(Name), string:trim(Value)}
                end
             || Line <- Lines
            ],
            {binary_to_integer(Code), Headers, Rest};
        [_] ->
            {ok, More} = ssl:recv(Socket, 0, 5000),
            read_head(Socket, <<Acc/binary, More/binary>>)
    end.

header(Name, Headers) ->
    proplists:get_value(Name, Headers).

%% The server's messages until Wanted arrives.
read_until(Socket, Received, Wanted) ->
    read_until(Socket, Received, Wanted, ws_frame:init_parser(#{role => client}), []).

read_until(Socket, Bin, Wanted, Parser, Acc) ->
    {ok, Messages, Next} = ws_frame:parse(Parser, Bin),
    All = Acc ++ Messages,
    case lists:member(Wanted, All) of
        true ->
            All;
        false ->
            {ok, More} = ssl:recv(Socket, 0, 5000),
            read_until(Socket, More, Wanted, Next, All)
    end.

%% The first messages the server sends.
first_messages(Socket, Received) ->
    first_messages(Socket, Received, ws_frame:init_parser(#{role => client})).

first_messages(Socket, Bin, Parser) ->
    case ws_frame:parse(Parser, Bin) of
        {ok, [], Next} ->
            {ok, More} = ssl:recv(Socket, 0, 5000),
            first_messages(Socket, More, Next);
        {ok, Messages, _} ->
            Messages
    end.

%% The server's messages until it closes the connection.
read_all(Socket, Received) ->
    read_all(
        Socket, Received, ws_frame:init_parser(#{role => client, max_message => 1 bsl 20}), []
    ).

read_all(Socket, Bin, Parser, Acc) ->
    {ok, Messages, Next} = ws_frame:parse(Parser, Bin),
    case ssl:recv(Socket, 0, 5000) of
        {ok, More} -> read_all(Socket, More, Next, Acc ++ Messages);
        {error, closed} -> Acc ++ Messages
    end.

last_record(Messages) ->
    lists:last([Record || {binary, Record} <- Messages]).

%%% Admission

same_origin_test() ->
    Admit = trevrpc_admission:same_origin([
        <<"https://App.Example:443">>, "http://other.example:8080"
    ]),
    Request = fun(Origin) ->
        Base = #{
            method => <<"GET">>,
            protocol => <<"HTTP/1.1">>,
            path => <<"/trevrpc">>,
            authority => <<"RPC.example:8443">>,
            secure => true,
            headers => []
        },
        case Origin of
            none -> Base;
            _ -> Base#{origin => Origin}
        end
    end,
    ?assert(Admit(Request(none))),
    ?assert(Admit(Request(<<"https://rpc.example:8443">>))),
    ?assert(Admit(Request(<<"https://app.example">>))),
    ?assert(Admit(Request(<<"HTTPS://APP.EXAMPLE">>))),
    ?assert(Admit(Request(<<"http://other.example:8080">>))),
    ?assertNot(Admit(Request(<<"http://rpc.example:8443">>))),
    ?assertNot(Admit(Request(<<"https://app.example:444">>))),
    ?assertNot(Admit(Request(<<"null">>))),
    ?assertNot(Admit(Request(<<"https://app.example/path">>))),
    ?assertError(badarg, trevrpc_admission:same_origin([<<"null">>])).

normalize_origin_test() ->
    ?assertEqual(
        {ok, <<"https://[::1]:8443">>}, trevrpc_admission:normalize_origin(<<"https://[::1]:8443">>)
    ),
    ?assertEqual({ok, <<"http://a">>}, trevrpc_admission:normalize_origin(<<"http://A:80">>)),
    ?assertEqual(error, trevrpc_admission:normalize_origin(<<"https://::1">>)),
    ?assertEqual(error, trevrpc_admission:normalize_origin(<<"https://a:+1">>)),
    ?assertEqual(error, trevrpc_admission:normalize_origin(<<"https://a:70000">>)),
    ?assertEqual(error, trevrpc_admission:normalize_origin(<<"https://u@a">>)).

admit_test() ->
    Request = #{
        method => <<"GET">>,
        protocol => <<"HTTP/1.1">>,
        path => <<"/">>,
        authority => <<"a">>,
        secure => true,
        headers => []
    },
    ?assert(trevrpc_admission:admit(fun trevrpc_admission:no_origin/1, Request)),
    ?assertNot(
        trevrpc_admission:admit(fun trevrpc_admission:no_origin/1, Request#{origin => <<"x">>})
    ),
    ?assertMatch(
        {error, {error, boom}}, trevrpc_admission:admit(fun(_) -> error(boom) end, Request)
    ),
    ?assertMatch({error, {bad_return, 1}}, trevrpc_admission:admit(fun(_) -> 1 end, Request)).
