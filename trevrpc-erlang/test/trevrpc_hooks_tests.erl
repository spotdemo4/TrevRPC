%% A server's authorizer and interceptors: what they see, the order they
%% run in, the context they pass on, and the finish functions that report
%% how each call ended, however it ended.
-module(trevrpc_hooks_tests).

-include_lib("eunit/include/eunit.hrl").

%% The logger handler of hook_logs/0.
-export([log/2]).

-define(UNARY, <<"/test.Hooks/Unary">>).
-define(FAIL, <<"/test.Hooks/Fail">>).
-define(SLEEP, <<"/test.Hooks/Sleep">>).
-define(STREAM, <<"/test.Hooks/Stream">>).
-define(BIDI, <<"/test.Hooks/Bidi">>).
-define(CONTEXT, <<"/test.Hooks/Context">>).
-define(UNKNOWN, <<"/test.Hooks/Unknown">>).
-define(CRASH, <<"/test.Hooks/Crash">>).
-define(BIG, <<"/test.Hooks/Big">>).
-define(TOKEN, <<"s3cret">>).

%%% The server

routes() ->
    #{
        ?UNARY => #{kind => unary, handler => fun(Request, _Call) -> {ok, Request} end},
        ?FAIL => #{
            kind => unary,
            handler => fun(_Request, _Call) ->
                {error, trevrpc_status:new(internal, <<"database is down">>)}
            end
        },
        ?SLEEP => #{
            kind => unary,
            handler => fun(_Request, _Call) ->
                timer:sleep(infinity),
                {ok, <<>>}
            end
        },
        ?STREAM => #{
            kind => server_streaming,
            handler => fun(Request, Call) ->
                ok = trevrpc_call:send(Call, Request),
                ok = trevrpc_call:send(Call, Request),
                ok
            end
        },
        ?BIDI => #{kind => bidirectional_streaming, handler => fun echo/1},
        %% Hands its process to the test, which stops it from outside, as
        %% nothing a handler raises can.
        ?CRASH => #{
            kind => unary,
            handler => fun(_Request, Call) ->
                #{<<"x-test">> := Pid} = trevrpc_call:metadata(Call),
                binary_to_term(Pid) ! {handler, self()},
                timer:sleep(infinity),
                {ok, <<>>}
            end
        },
        %% Answers with more than a client that does not read takes, so that
        %% writing its end blocks.
        ?BIG => #{
            kind => client_streaming,
            handler => fun(_Call) -> {ok, binary:copy(<<0>>, 1 bsl 21)} end
        },
        ?CONTEXT => #{
            kind => unary,
            handler => fun(_Request, Call) ->
                {ok, term_to_binary({trevrpc_call:context(Call), trevrpc_call:peer(Call)})}
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

%% Runs Test with a server that has the hooks of Opts and a channel to it
%% over native QUIC.
with_server(Opts, Test) ->
    with_server(quic, Opts, Test).

%% The same over `Transport', `quic' or `tcp' (QMux over TLS). The
%% channel's own options go under `channel'.
with_server(Transport, Opts0, Test) ->
    {ChannelOpts, Opts} =
        case maps:take(channel, Opts0) of
            {Taken, Rest} -> {Taken, Rest};
            error -> {#{}, Opts0}
        end,
    {ok, _} = application:ensure_all_started(quic),
    Credentials = trevrpc_test_tls:credentials(),
    {ok, Server} = trevrpc_server:start(
        maps:merge(
            #{
                ip => {127, 0, 0, 1},
                cert => maps:get(cert, Credentials),
                key => maps:get(key, Credentials),
                tcp => Transport =:= tcp,
                routes => routes()
            },
            Opts
        )
    ),
    {ok, Port} = trevrpc_server:port(Server),
    TcpPort =
        case Transport of
            tcp -> element(2, trevrpc_server:tcp_port(Server));
            quic -> Port
        end,
    {ok, Channel} = trevrpc_channel:start(ChannelOpts#{
        host => {127, 0, 0, 1},
        port => Port,
        tcp_port => TcpPort,
        transport => Transport,
        server_name => "localhost",
        cacerts => maps:get(cacerts, Credentials)
    }),
    ok = trevrpc_channel:wait_until_ready(Channel, 30000),
    try
        Test(#{server => Server, channel => Channel})
    after
        trevrpc_channel:close(Channel),
        %% A test may have stopped it.
        try
            trevrpc_server:stop(Server)
        catch
            exit:_ -> ok
        end
    end.

unary(Channel, Method) ->
    unary(Channel, Method, #{}).

unary(Channel, Method, Opts) ->
    case trevrpc_client:unary(Channel, Method, <<"hi">>, maps:merge(#{timeout => 5000}, Opts)) of
        {ok, Response, _} -> {ok, Response};
        {error, Status} -> {error, Status}
    end.

%% An interceptor that reports each call it sees, and how it ended, to the
%% test process, tagged with `Name'.
recorder(Name) ->
    Test = self(),
    fun(Info) ->
        Test ! {intercepted, Name, Info},
        {ok, fun(Finished) -> Test ! {finished, Name, Finished} end}
    end.

finished(Name) ->
    receive
        {finished, Name, Finished} -> Finished
    after 5000 -> error({no_finish, Name})
    end.

intercepted(Name) ->
    receive
        {intercepted, Name, Info} -> Info
    after 5000 -> error({not_intercepted, Name})
    end.

%% No hook ran beyond those the test took, once something the code
%% provides showed that the call is over: hooks report before then. The
%% call's own stream messages, such as a late reset, may still arrive here,
%% the client's process.
nothing_more() ->
    receive
        {intercepted, _, _} = Message -> error({unexpected, Message});
        {finished, _, _} = Message -> error({unexpected, Message});
        authorized = Message -> error({unexpected, Message})
    after 0 -> ok
    end.

hooks_test_() ->
    logger:set_primary_config(level, none),
    [
        {Name, {timeout, 30, Test}}
     || {Name, Test} <- [
            {"authorizer hides which methods exist", fun hides_methods/0},
            {"authorizer sees the call over QUIC", fun() -> authorizer_info(quic) end},
            {"authorizer sees the call over QMux", fun() -> authorizer_info(tcp) end},
            {"authorizer runs on an admitted call", fun admitted_first/0},
            {"hook failures do not log the call's metadata", fun hook_logs/0},
            {"authorizer and interceptors pass a context on", fun context/0},
            {"authorizer that crashes", fun crashing_authorizer/0},
            {"authorizer that answers something else", fun malformed_authorizer/0},
            {"interceptors run in order, finish in reverse", fun order/0},
            {"interceptor that rejects the call", fun rejecting_interceptor/0},
            {"interceptor that crashes", fun crashing_interceptor/0},
            {"failed call", fun failed_call/0},
            {"streamed call", fun streamed_call/0},
            {"call past its deadline", fun deadline/0},
            {"call the client cancels", fun cancelled/0},
            {"call cut off by shutdown", fun shutdown/0},
            {"finish that crashes", fun crashing_finish/0},
            {"call process stopped from outside", fun crashed_call/0},
            {"call whose connection closes", fun closed_connection/0},
            {"call stopped while it writes its end", fun stopped_writing/0},
            {"optimistic call refused and replayed", fun replayed/0}
        ]
    ].

hides_methods() ->
    with_server(#{authorizer => trevrpc_authorizer:bearer(?TOKEN)}, fun(#{channel := Channel}) ->
        %% Unauthenticated, a method the server has and one it does not
        %% fail alike.
        [
            ?assertMatch(
                {error, #{code := unauthenticated, message := <<"request is not authenticated">>}},
                unary(Channel, Method)
            )
         || Method <- [?UNARY, ?UNKNOWN]
        ],
        {ok, Bearer} = trevrpc_bearer:metadata(?TOKEN),
        ?assertEqual({ok, <<"hi">>}, unary(Channel, ?UNARY, #{metadata => Bearer})),
        ?assertMatch(
            {error, #{code := unimplemented}}, unary(Channel, ?UNKNOWN, #{metadata => Bearer})
        )
    end).

authorizer_info(Transport) ->
    Test = self(),
    Authorizer = fun(Info) ->
        Test ! {authorized, Info},
        ok
    end,
    with_server(Transport, #{authorizer => Authorizer}, fun(#{channel := Channel}) ->
        {ok, _} = unary(Channel, ?UNARY, #{metadata => #{<<"x-user">> => <<"ann">>}}),
        Info =
            receive
                {authorized, I} -> I
            end,
        ?assertMatch(
            #{
                method := ?UNARY,
                service := <<"test.Hooks">>,
                method_name := <<"Unary">>,
                kind := unary,
                metadata := #{<<"x-user">> := <<"ann">>},
                peer := {{127, 0, 0, 1}, Port},
                deadline := Deadline
            } when is_integer(Port) andalso is_integer(Deadline),
            Info
        ),
        ?assertNot(is_map_key(context, Info))
    end).

context() ->
    Test = self(),
    Authorizer = trevrpc_authorizer:chain([
        fun(_) -> {ok, #{user => <<"ann">>, role => guest}} end,
        fun(_) -> {ok, #{role => admin}} end
    ]),
    Interceptor = fun(#{context := Context}) ->
        Test ! {context, Context},
        {ok, undefined, #{span => 7}}
    end,
    Opts = #{authorizer => Authorizer, interceptors => [Interceptor, fun(_) -> ok end]},
    with_server(Opts, fun(#{channel := Channel}) ->
        {ok, Bin} = unary(Channel, ?CONTEXT),
        receive
            {context, Seen} -> ?assertEqual(#{user => <<"ann">>, role => admin}, Seen)
        end,
        {Context, Peer} = binary_to_term(Bin),
        ?assertEqual(#{user => <<"ann">>, role => admin, span => 7}, Context),
        ?assertMatch({{127, 0, 0, 1}, _}, Peer)
    end).

crashing_authorizer() ->
    Opts = #{authorizer => fun(_) -> error(boom) end, interceptors => [recorder(i)]},
    with_server(Opts, fun(#{channel := Channel}) ->
        ?assertMatch(
            {error, #{code := internal, message := <<"internal server error">>}},
            unary(Channel, ?UNARY)
        ),
        %% A call the authorizer rejected never reaches the interceptors.
        nothing_more()
    end).

malformed_authorizer() ->
    with_server(#{authorizer => fun(_) -> yes end}, fun(#{channel := Channel}) ->
        ?assertMatch({error, #{code := internal}}, unary(Channel, ?UNARY))
    end).

order() ->
    with_server(#{interceptors => [recorder(a), recorder(b)]}, fun(#{channel := Channel}) ->
        ?assertEqual({ok, <<"hi">>}, unary(Channel, ?UNARY)),
        #{method := ?UNARY, kind := unary, context := #{}} = intercepted(a),
        _ = intercepted(b),
        %% The last registered runs first.
        receive
            {finished, First, _} -> ?assertEqual(b, First)
        after 5000 -> error(no_finish)
        end,
        Finished = finished(a),
        ?assertMatch(
            #{
                method := ?UNARY,
                service := <<"test.Hooks">>,
                method_name := <<"Unary">>,
                kind := unary,
                status := #{code := ok},
                elapsed := Elapsed
            } when is_integer(Elapsed) andalso Elapsed >= 0,
            Finished
        ),
        nothing_more()
    end).

rejecting_interceptor() ->
    Reject = fun(_) -> {error, trevrpc_status:new(resource_exhausted, <<"slow down">>)} end,
    Opts = #{interceptors => [recorder(a), Reject, recorder(c)]},
    with_server(Opts, fun(#{channel := Channel}) ->
        ?assertMatch(
            {error, #{code := resource_exhausted, message := <<"slow down">>}},
            unary(Channel, ?UNARY)
        ),
        _ = intercepted(a),
        ?assertMatch(
            #{status := #{code := resource_exhausted, message := <<"slow down">>}}, finished(a)
        ),
        %% Later interceptors do not run.
        nothing_more()
    end).

crashing_interceptor() ->
    Opts = #{interceptors => [recorder(a), fun(_) -> exit(boom) end]},
    with_server(Opts, fun(#{channel := Channel}) ->
        ?assertMatch(
            {error, #{code := internal, message := <<"internal server error">>}},
            unary(Channel, ?UNARY)
        ),
        _ = intercepted(a),
        ?assertMatch(#{status := #{code := internal}}, finished(a))
    end).

failed_call() ->
    with_server(#{interceptors => [recorder(a)]}, fun(#{channel := Channel}) ->
        %% The client gets the masked message, finish functions the
        %% original.
        ?assertMatch(
            {error, #{code := internal, message := <<"internal server error">>}},
            unary(Channel, ?FAIL)
        ),
        _ = intercepted(a),
        ?assertMatch(
            #{status := #{code := internal, message := <<"database is down">>}}, finished(a)
        )
    end).

streamed_call() ->
    with_server(#{interceptors => [recorder(a)]}, fun(#{channel := Channel}) ->
        {ok, Call} = trevrpc_client:server_stream(Channel, ?STREAM, <<"x">>, #{timeout => 5000}),
        {ok, <<"x">>} = trevrpc_client:recv(Call),
        {ok, <<"x">>} = trevrpc_client:recv(Call),
        {eof, _} = trevrpc_client:recv(Call),
        _ = intercepted(a),
        ?assertMatch(#{kind := server_streaming, status := #{code := ok}}, finished(a))
    end).

deadline() ->
    with_server(#{interceptors => [recorder(a)]}, fun(#{channel := Channel}) ->
        %% The client resets the stream only once it reads past the
        %% deadline, so the server's deadline ends the call first.
        {ok, Call} = trevrpc_client:bidi(Channel, ?BIDI, #{timeout => 200}),
        _ = intercepted(a),
        #{status := Status, elapsed := Elapsed} = finished(a),
        ?assertEqual(deadline_exceeded, trevrpc_status:code(Status)),
        ?assert(Elapsed >= 150000),
        ?assertMatch({error, #{code := deadline_exceeded}}, trevrpc_client:recv(Call))
    end).

cancelled() ->
    with_server(#{interceptors => [recorder(a)]}, fun(#{channel := Channel}) ->
        {ok, Call} = trevrpc_client:bidi(Channel, ?BIDI, #{timeout => 5000}),
        ok = trevrpc_client:send(Call, <<"x">>),
        {ok, <<"x">>} = trevrpc_client:recv(Call),
        _ = intercepted(a),
        ok = trevrpc_client:cancel(Call),
        ?assertMatch(#{status := #{code := cancelled}}, finished(a))
    end).

shutdown() ->
    Opts = #{interceptors => [recorder(a)], graceful_shutdown_timeout => 100},
    with_server(Opts, fun(#{channel := Channel, server := Server}) ->
        Test = self(),
        spawn(fun() -> Test ! {result, unary(Channel, ?SLEEP)} end),
        _ = intercepted(a),
        ok = trevrpc_server:stop(Server),
        ?assertMatch(#{status := #{code := unavailable}}, finished(a)),
        receive
            {result, Result} -> ?assertMatch({error, _}, Result)
        after 5000 -> error(no_result)
        end
    end).

crashing_finish() ->
    Crash = fun(_) -> {ok, fun(_) -> error(boom) end} end,
    %% `a' registers first, so its finish runs after the crashing one.
    with_server(#{interceptors => [recorder(a), Crash]}, fun(#{channel := Channel}) ->
        ?assertEqual({ok, <<"hi">>}, unary(Channel, ?UNARY)),
        _ = intercepted(a),
        ?assertMatch(#{status := #{code := ok}}, finished(a))
    end).

%% A call the server refuses for optimistic stream compression, as its
%% route does not decode stream-compressed requests, is replayed without
%% it: refused at admission, before the authorizer, so that both hooks see
%% it once.
replayed() ->
    Test = self(),
    Authorizer = fun(_) ->
        Test ! authorized,
        ok
    end,
    Opts = #{authorizer => Authorizer, interceptors => [recorder(a)]},
    with_server(Opts, fun(#{channel := Channel}) ->
        {ok, Call} = trevrpc_client:bidi(Channel, ?BIDI, #{
            timeout => 5000, compression => stream
        }),
        [ok = trevrpc_client:send(Call, <<"x">>) || _ <- lists:seq(1, 4)],
        ok = trevrpc_client:close_send(Call),
        [{ok, <<"x">>} = trevrpc_client:recv(Call) || _ <- lists:seq(1, 4)],
        {eof, _} = trevrpc_client:recv(Call),
        receive
            authorized -> ok
        after 5000 -> error(not_authorized)
        end,
        _ = intercepted(a),
        _ = finished(a),
        nothing_more()
    end).

%% The authorizer runs once the server admitted the call: a call it holds
%% takes its slot in the request limit, and a call to a method the server
%% does not have past that limit is refused like any other, so that it
%% tells the caller nothing.
admitted_first() ->
    Test = self(),
    Authorizer = fun(#{metadata := Metadata}) ->
        case Metadata of
            #{<<"x-hold">> := _} ->
                Test ! {holding, self()},
                receive
                    release -> ok
                end;
            #{} ->
                ok
        end
    end,
    Opts = #{authorizer => Authorizer, max_concurrent_requests => 1},
    with_server(Opts, fun(#{channel := Channel}) ->
        spawn(fun() ->
            Test ! {held, unary(Channel, ?UNARY, #{metadata => #{<<"x-hold">> => <<"1">>}})}
        end),
        Holder =
            receive
                {holding, Pid} -> Pid
            after 5000 -> error(not_holding)
            end,
        ?assertMatch(
            {error, #{code := unavailable, message := <<"too many concurrent RPCs">>}},
            unary(Channel, ?UNKNOWN)
        ),
        Holder ! release,
        receive
            {held, Held} -> ?assertEqual({ok, <<"hi">>}, Held)
        after 5000 -> error(not_released)
        end,
        ?assertMatch({error, #{code := unimplemented}}, unary(Channel, ?UNKNOWN))
    end).

%% A hook gets the call's metadata, credentials included: what it raised or
%% returned is logged without them.
hook_logs() ->
    Primary = logger:get_primary_config(),
    {ok, #{level := Default}} = logger:get_handler_config(default),
    ok = logger:add_handler(?MODULE, ?MODULE, #{config => self(), level => all}),
    ok = logger:update_handler_config(default, level, none),
    ok = logger:set_primary_config(level, error),
    Authorizer = fun(#{metadata := Metadata}) ->
        case Metadata of
            #{<<"x-mode">> := <<"match">>} ->
                #{<<"absent">> := _} = Metadata,
                ok;
            #{<<"x-mode">> := <<"return">>} ->
                {weird, Metadata};
            #{} ->
                ok
        end
    end,
    Interceptor = fun(#{metadata := Metadata}) -> error({oops, Metadata}) end,
    try
        with_server(
            #{authorizer => Authorizer, interceptors => [Interceptor]},
            fun(#{channel := Channel}) ->
                [
                    ?assertMatch(
                        {error, #{code := internal}},
                        unary(Channel, ?UNARY, #{
                            metadata => #{
                                <<"authorization">> => <<"Bearer s3cret">>, <<"x-mode">> => Mode
                            }
                        })
                    )
                 || Mode <- [<<"match">>, <<"return">>, <<"intercept">>]
                ]
            end
        ),
        Logs = [
            receive
                {hook_log, Text} -> Text
            after 5000 -> timeout
            end
         || _ <- lists:seq(1, 3)
        ],
        [?assertEqual(nomatch, string:find(Text, "s3cret")) || Text <- Logs],
        [Match, Return, Intercept] = Logs,
        ?assertNotEqual(nomatch, string:find(Match, "badmatch")),
        ?assertNotEqual(nomatch, string:find(Match, "trevrpc_hooks_tests")),
        ?assertNotEqual(nomatch, string:find(Return, "{weird,2}")),
        ?assertNotEqual(nomatch, string:find(Intercept, "oops"))
    after
        ok = logger:remove_handler(?MODULE),
        ok = logger:update_handler_config(default, level, Default),
        ok = logger:set_primary_config(Primary)
    end.

log(#{msg := {Format, Args}}, #{config := Pid}) when is_list(Format) ->
    Text = lists:flatten(io_lib:format(Format, Args)),
    case
        string:find(Text, "trevrpc authorizer") =/= nomatch orelse
            string:find(Text, "trevrpc interceptor") =/= nomatch
    of
        true -> Pid ! {hook_log, Text};
        false -> ok
    end;
log(_Event, _Config) ->
    ok.

%% A call process that something stopped from outside ends with `internal'.
crashed_call() ->
    with_server(#{interceptors => [recorder(a)]}, fun(#{channel := Channel}) ->
        Test = self(),
        Metadata = #{<<"x-test">> => term_to_binary(Test)},
        spawn(fun() -> Test ! {result, unary(Channel, ?CRASH, #{metadata => Metadata})} end),
        _ = intercepted(a),
        Handler =
            receive
                {handler, Pid} -> Pid
            after 5000 -> error(no_handler)
            end,
        exit(Handler, boom),
        ?assertMatch(#{status := #{code := internal}}, finished(a)),
        receive
            {result, Result} -> ?assertMatch({error, #{code := internal}}, Result)
        after 5000 -> error(no_result)
        end
    end).

%% A call whose connection closes under it is cancelled.
closed_connection() ->
    with_server(#{interceptors => [recorder(a)]}, fun(#{channel := Channel}) ->
        {ok, _Call} = trevrpc_client:bidi(Channel, ?BIDI, #{timeout => 5000}),
        _ = intercepted(a),
        trevrpc_channel:close(Channel),
        ?assertMatch(#{status := #{code := cancelled}}, finished(a))
    end).

%% A call that the end of the graceful shutdown stops while it writes its
%% end, to a client that does not read it, reports the shutdown. The
%% channel's small frames make its receive window 64 KiB, which the 2 MiB
%% response overflows, so that the write waits for credit that never comes.
stopped_writing() ->
    Opts = #{
        interceptors => [recorder(a)],
        graceful_shutdown_timeout => 200,
        channel => #{max_frame_size => 65536}
    },
    with_server(Opts, fun(#{channel := Channel, server := Server}) ->
        {ok, Call} = trevrpc_client:client_stream(Channel, ?BIG, #{timeout => 10000}),
        ok = trevrpc_client:close_send(Call),
        _ = intercepted(a),
        ok = trevrpc_server:stop(Server),
        ?assertMatch(#{status := #{code := unavailable}}, finished(a))
    end).

%%% Ready-made authorizers

info(Metadata) ->
    #{metadata => Metadata}.

metadata_value_test() ->
    Authorizer = trevrpc_authorizer:metadata_value(<<"X-Key">>, <<"v">>),
    ?assertEqual(ok, Authorizer(info(#{<<"x-key">> => <<"v">>}))),
    [
        ?assertMatch({error, #{code := unauthenticated}}, Authorizer(info(Metadata)))
     || Metadata <- [#{}, #{<<"x-key">> => <<"w">>}, #{<<"x-key">> => <<"vv">>}]
    ].

bearer_test() ->
    Authorizer = trevrpc_authorizer:bearer(?TOKEN),
    {ok, Bearer} = trevrpc_bearer:metadata(?TOKEN),
    ?assertEqual(ok, Authorizer(info(Bearer))),
    ?assertMatch(
        {error, #{code := unauthenticated}},
        Authorizer(info(#{<<"authorization">> => <<"Bearer other">>}))
    ).

chain_test() ->
    Deny = fun(_) -> {error, trevrpc_status:new(permission_denied, <<"no">>)} end,
    Never = fun(_) -> error(not_reached) end,
    ?assertEqual({ok, #{}}, (trevrpc_authorizer:chain([]))(info(#{}))),
    ?assertEqual(
        {ok, #{a => 2, b => 1}},
        (trevrpc_authorizer:chain([
            fun(_) -> {ok, #{a => 1, b => 1}} end, fun(_) -> ok end, fun(_) -> {ok, #{a => 2}} end
        ]))(
            info(#{})
        )
    ),
    ?assertMatch(
        {error, #{code := permission_denied}},
        (trevrpc_authorizer:chain([fun(_) -> ok end, Deny, Never]))(info(#{}))
    ).

invalid_options_test_() ->
    Credentials = trevrpc_test_tls:credentials(),
    Base = #{
        ip => {127, 0, 0, 1},
        cert => maps:get(cert, Credentials),
        key => maps:get(key, Credentials),
        tcp => false,
        routes => #{}
    },
    [
        ?_assertEqual({error, Error}, trevrpc_server:start(maps:merge(Base, Opts)))
     || {Opts, Error} <- [
            {#{authorizer => fun() -> ok end}, {invalid_option, authorizer}},
            {#{interceptors => fun(_) -> ok end}, {invalid_option, interceptors}},
            {#{interceptors => [fun(_) -> ok end, nope]}, {invalid_option, interceptors}}
        ]
    ].
