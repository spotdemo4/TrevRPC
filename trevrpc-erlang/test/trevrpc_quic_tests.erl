%% erlang_quic takes the options trevrpc_quic gives it, on both ends of a
%% connection.
-module(trevrpc_quic_tests).

-include_lib("eunit/include/eunit.hrl").

-define(MAX_FRAME, 1 bsl 22).

%% Both ends' congestion controllers get a minimum recovery period of 1 ms,
%% not erlang_quic's default of 100 ms. This checks that the option reaches
%% them; that recovery then ends a round trip after a loss is erlang_quic's
%% behaviour (quic_cc_newreno and quic_cc_cubic), which the lossy benchmarks
%% measure.
recovery_period_test_() ->
    %% Longer than the handshake waits below, so a slow handshake fails with
    %% their errors rather than eunit's default 5 s timeout.
    {timeout, 30, fun recovery_period/0}.

recovery_period() ->
    {ok, _} = application:ensure_all_started(quic),
    Self = self(),
    Credentials = trevrpc_test_tls:credentials(),
    Opts = trevrpc_quic:transport_opts(?MAX_FRAME, 4, 30000),
    Name = ?MODULE,
    {ok, _} = quic:start_server(
        Name,
        0,
        maps:merge(maps:with([cert, key], Credentials), Opts#{
            connection_handler => fun(Conn) ->
                Self ! {server_conn, Conn},
                {ok, Self}
            end
        })
    ),
    try
        {ok, Port} = quic:get_server_port(Name),
        {ok, Client} = quic:connect("127.0.0.1", Port, Opts#{verify => false}, self()),
        try
            receive
                {quic, Client, {connected, _}} -> ok
            after 10000 -> error(not_connected)
            end,
            Server =
                receive
                    {server_conn, Conn} -> Conn
                after 10000 -> error(no_server_connection)
                end,
            ?assertEqual(1, recovery_period(Client)),
            ?assertEqual(1, recovery_period(Server))
        after
            quic:close(Client, 0)
        end
    after
        quic:stop_server(Name)
    end.

%% The minimum recovery period of a connection's congestion controller.
%% erlang_quic has no getter for it on the connection, so this finds the
%% controller (a `cc_wrapper' record, which `quic_cc:min_recovery_duration/1'
%% reads) among the fields of the connection's state. Should a release of
%% erlang_quic lay its state out differently, the test fails saying so
%% rather than about the option.
recovery_period(Conn) ->
    State =
        case sys:get_state(Conn) of
            {_StateName, Data} when is_tuple(Data) -> Data;
            Other -> error({unexpected_connection_state, Other})
        end,
    case [Field || Field <- tuple_to_list(State), is_record_tagged(Field, cc_wrapper)] of
        [CC] -> quic_cc:min_recovery_duration(CC);
        Found -> error({no_single_cc_wrapper_in_connection_state, length(Found)})
    end.

is_record_tagged(Field, Tag) ->
    is_tuple(Field) andalso tuple_size(Field) > 0 andalso element(1, Field) =:= Tag.
