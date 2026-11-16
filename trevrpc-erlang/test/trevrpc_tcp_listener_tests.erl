%% The TCP listener on its own.
-module(trevrpc_tcp_listener_tests).

-include_lib("eunit/include/eunit.hrl").

-define(IP, {127, 0, 0, 1}).

%% stop/1 returns only once the listening socket is closed, so a server
%% restarted on the same port can bind it at once, however late the
%% listener gets to run: here it is suspended until after stop/1 was
%% called.
stop_waits_for_the_socket_test() ->
    {ok, _} = application:ensure_all_started(ssl),
    %% The listener is linked to the test process and exits on stop/1.
    process_flag(trap_exit, true),
    {ok, Listener} = trevrpc_tcp_listener:start_link(
        #{ip => ?IP, port => 0, handshake_timeout => 1000, max_handshakes => 1},
        trevrpc_test_tls:credentials(),
        #{qmux => fun(_Socket) -> ok end}
    ),
    {ok, Port} = trevrpc_tcp_listener:port(Listener),
    true = erlang:suspend_process(Listener),
    Self = self(),
    Stopper = spawn_link(fun() -> Self ! {self(), trevrpc_tcp_listener:stop(Listener)} end),
    try
        %% Waiting in a receive, stop/1 can only be waiting for the
        %% listener; one that returned has exited.
        await_receive(Stopper),
        ?assertMatch(
            {current_function, {trevrpc_tcp_listener, stop, 1}},
            process_info(Stopper, current_function)
        ),
        ?assertMatch({error, eaddrinuse}, listen(Port))
    after
        true = erlang:resume_process(Listener)
    end,
    receive
        {Stopper, Result} -> ?assertEqual(ok, Result)
    end,
    ?assertEqual(ok, listen(Port)).

%% Returns once Pid waits in a receive or has exited.
await_receive(Pid) ->
    case process_info(Pid, status) of
        {status, waiting} ->
            ok;
        undefined ->
            ok;
        _ ->
            erlang:yield(),
            await_receive(Pid)
    end.

%% Binds the port as the server's listener does.
listen(Port) ->
    case gen_tcp:listen(Port, [{ip, ?IP}, {reuseaddr, true}]) of
        {ok, Socket} -> gen_tcp:close(Socket);
        Error -> Error
    end.
