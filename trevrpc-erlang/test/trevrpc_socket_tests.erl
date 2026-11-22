-module(trevrpc_socket_tests).

-include_lib("eunit/include/eunit.hrl").

%% TCP_USER_TIMEOUT reaches a connected socket, and the sockets a listener
%% accepts, which inherit it; there is none for `infinity' or off Linux.
user_timeout_test() ->
    ?assertEqual([], trevrpc_socket:user_timeout(infinity)),
    case os:type() of
        {unix, linux} ->
            Local = {ip, {127, 0, 0, 1}},
            {ok, Listen} = gen_tcp:listen(0, [
                binary, {active, false}, Local | trevrpc_socket:user_timeout(7000)
            ]),
            {ok, Port} = inet:port(Listen),
            {ok, Client} = gen_tcp:connect({127, 0, 0, 1}, Port, [
                binary, {active, false} | trevrpc_socket:user_timeout(9000)
            ]),
            {ok, Server} = gen_tcp:accept(Listen, 5000),
            ?assertEqual(7000, user_timeout_of(Server)),
            ?assertEqual(9000, user_timeout_of(Client)),
            [ok = gen_tcp:close(Socket) || Socket <- [Client, Server, Listen]];
        _ ->
            ?assertEqual([], trevrpc_socket:user_timeout(7000))
    end.

user_timeout_of(Socket) ->
    {ok, [{raw, 6, 18, <<Ms:32/native>>}]} = inet:getopts(Socket, [{raw, 6, 18, 4}]),
    Ms.

%% tcp_user_timeout is infinity or a positive number of milliseconds that
%% fits the socket option.
check_user_timeout_test() ->
    [
        ?assertEqual(ok, trevrpc_socket:check_user_timeout(Opts))
     || Opts <- [
            #{},
            #{tcp_user_timeout => infinity},
            #{tcp_user_timeout => 1},
            #{tcp_user_timeout => 16#FFFFFFFF}
        ]
    ],
    [
        ?assertEqual(
            {error, {invalid_option, tcp_user_timeout}},
            trevrpc_socket:check_user_timeout(#{tcp_user_timeout => Bad})
        )
     || Bad <- [0, -1, 1.5, 16#100000000, forever]
    ].

%% A channel refuses to start with a bad tcp_user_timeout, rather than
%% crashing when it connects.
channel_rejects_bad_user_timeout_test() ->
    ?assertEqual(
        {error, {invalid_option, tcp_user_timeout}},
        trevrpc_channel:start(#{host => "localhost", port => 1, tcp_user_timeout => 0})
    ).

%% Nor does a server.
server_rejects_bad_user_timeout_test() ->
    #{cert := Cert, key := Key} = trevrpc_test_tls:credentials(),
    ?assertEqual(
        {error, {invalid_option, tcp_user_timeout}},
        trevrpc_server:start(#{
            ip => {127, 0, 0, 1}, cert => Cert, key => Key, routes => #{}, tcp_user_timeout => -1
        })
    ).

%% The server's TCP listener gets the connection idle timeout as its
%% TCP_USER_TIMEOUT unless tcp_user_timeout says otherwise.
server_listener_options_test() ->
    UserTimeout = fun(Opts) ->
        maps:get(user_timeout, trevrpc_server:tcp_listener_options(Opts, 4433))
    end,
    ?assertEqual(30000, UserTimeout(#{})),
    ?assertEqual(5000, UserTimeout(#{connection_idle_timeout => 5000})),
    ?assertEqual(infinity, UserTimeout(#{connection_idle_timeout => infinity})),
    ?assertEqual(7000, UserTimeout(#{connection_idle_timeout => 5000, tcp_user_timeout => 7000})).

%% The TCP listener sets TCP_USER_TIMEOUT on the sockets it accepts, through
%% ssl:listen, and a channel on its QMux connection, through ssl:connect with
%% its TLS options; by default the channel takes its idle timeout.
listener_and_channel_sockets_test() ->
    case os:type() of
        {unix, linux} ->
            {ok, _} = application:ensure_all_started(ssl),
            Raw = fun(Ms) -> {raw, 6, 18, <<Ms:32/native>>} end,
            ?assert(
                lists:member(
                    Raw(12000),
                    trevrpc_channel:tls_opts(#{verify => false, idle_timeout => 12000}, "localhost")
                )
            ),
            process_flag(trap_exit, true),
            Self = self(),
            {ok, Listener} = trevrpc_tcp_listener:start_link(
                #{
                    ip => {127, 0, 0, 1},
                    port => 0,
                    handshake_timeout => 5000,
                    max_handshakes => 1,
                    user_timeout => 7000
                },
                trevrpc_test_tls:credentials(),
                #{
                    qmux => fun(Socket) ->
                        Self ! {accepted, ssl_user_timeout_of(Socket)},
                        ok
                    end
                }
            ),
            {ok, Port} = trevrpc_tcp_listener:port(Listener),
            TlsOpts = trevrpc_channel:tls_opts(
                #{verify => false, tcp_user_timeout => 9000}, "localhost"
            ),
            {ok, Client} = ssl:connect({127, 0, 0, 1}, Port, TlsOpts, 5000),
            ?assertEqual(9000, ssl_user_timeout_of(Client)),
            receive
                {accepted, Accepted} -> ?assertEqual(7000, Accepted)
            after 5000 -> error(not_accepted)
            end,
            ok = ssl:close(Client),
            ok = trevrpc_tcp_listener:stop(Listener);
        _ ->
            ok
    end.

ssl_user_timeout_of(Socket) ->
    {ok, [{raw, 6, 18, <<Ms:32/native>>}]} = ssl:getopts(Socket, [{raw, 6, 18, 4}]),
    Ms.
