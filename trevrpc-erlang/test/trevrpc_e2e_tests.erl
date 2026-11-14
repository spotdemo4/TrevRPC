%% Erlang client against Erlang server over native QUIC on loopback.
-module(trevrpc_e2e_tests).

-include_lib("eunit/include/eunit.hrl").

-define(UNARY, <<"/test.Echo/Unary">>).
-define(CLIENT_STREAM, <<"/test.Echo/ClientStream">>).
-define(SERVER_STREAM, <<"/test.Echo/ServerStream">>).
-define(BIDI, <<"/test.Echo/Bidi">>).
-define(SLEEP, <<"/test.Echo/Sleep">>).
-define(PARK, <<"/test.Echo/Park">>).
-define(HOLD, <<"/test.Echo/Hold">>).
-define(REJECT, <<"/test.Echo/Reject">>).
-define(STOP, <<"/test.Echo/Stop">>).
-define(CRASH, <<"/test.Echo/Crash">>).

%%% Fixture

credentials() ->
    %% #'Extension'{extnID = ?'id-ce-subjectAltName', critical = false, extnValue = ...}
    San = {'Extension', {2, 5, 29, 17}, false, [{dNSName, "localhost"}]},
    #{server_config := Server, client_config := Client} = public_key:pkix_test_data(#{
        %% TLS 1.3 takes no DSA keys, pkix_test_data's default.
        server_chain => #{
            root => [{key, {namedCurve, secp256r1}}],
            intermediates => [],
            peer => [{key, {namedCurve, secp256r1}}, {extensions, [San]}]
        },
        client_chain => #{
            root => [{key, {namedCurve, secp256r1}}],
            intermediates => [],
            peer => [{key, {namedCurve, secp256r1}}]
        }
    }),
    {KeyType, KeyDer} = proplists:get_value(key, Server),
    #{
        cert => proplists:get_value(cert, Server),
        key => public_key:der_decode(KeyType, KeyDer),
        cacerts => proplists:get_value(cacerts, Client)
    }.

%% A parked handler reports its pid to the test process registered as
%% trevrpc_e2e_parked.
routes() ->
    #{
        ?UNARY => #{kind => unary, handler => fun unary/2},
        ?CLIENT_STREAM => #{kind => client_streaming, handler => fun client_stream/1},
        ?SERVER_STREAM => #{kind => server_streaming, handler => fun server_stream/2},
        ?BIDI => #{kind => bidirectional_streaming, handler => fun bidi/1},
        ?SLEEP => #{kind => unary, handler => fun(_Request, _Call) -> timer:sleep(infinity) end},
        ?PARK => #{kind => bidirectional_streaming, handler => fun park/1},
        ?HOLD => #{kind => bidirectional_streaming, handler => fun hold/1},
        ?REJECT => #{kind => client_streaming, handler => fun reject/1},
        ?STOP => #{kind => client_streaming, handler => fun stop/1},
        ?CRASH => #{kind => unary, handler => fun(_Request, _Call) -> erlang:error(boom) end}
    }.

unary(<<"fail">>, _Call) ->
    {error, #{
        code => resource_exhausted,
        message => <<"slow down">>,
        metadata => #{<<"x-why">> => <<"limit">>},
        details => [#{type => <<"google.rpc.RetryInfo">>, value => <<10, 4, 8, 1, 16, 2>>}]
    }};
unary(<<"internal">>, _Call) ->
    {error, trevrpc_status:new(internal, <<"database password is hunter2">>)};
unary(Request, Call) ->
    Metadata = trevrpc_call:metadata(Call),
    case Metadata of
        #{<<"interop-header">> := Value} ->
            ok = trevrpc_call:send_headers(Call, #{<<"interop-header">> => Value}),
            trevrpc_call:set_trailers(Call, #{<<"interop-trailer">> => Value});
        #{} ->
            ok
    end,
    {ok, <<Request/binary, (maps:get(<<"interop-suffix">>, Metadata, <<>>))/binary>>}.

client_stream(Call) ->
    client_stream(Call, <<>>, 0).

client_stream(Call, Acc, Count) ->
    case trevrpc_call:recv(Call) of
        {ok, <<"fail">>} -> {error, trevrpc_status:new(aborted, <<"abort">>)};
        {ok, Message} -> client_stream(Call, <<Acc/binary, Message/binary>>, Count + 1);
        eof -> {ok, <<Acc/binary, ":", (integer_to_binary(Count))/binary>>};
        {error, Status} -> {error, Status}
    end.

%% "<count>:<message>[:fail]"
server_stream(Request, Call) ->
    [Count, Message | Rest] = binary:split(Request, <<":">>, [global]),
    case trevrpc_call:metadata(Call) of
        #{<<"interop-header">> := Value} ->
            ok = trevrpc_call:send_headers(Call, #{<<"interop-header">> => Value});
        #{} ->
            ok
    end,
    [ok = trevrpc_call:send(Call, Message) || _ <- lists:seq(1, binary_to_integer(Count))],
    case Rest of
        [<<"fail">>] -> {error, trevrpc_status:new(failed_precondition, <<"stop">>)};
        [] -> ok
    end.

bidi(Call) ->
    case trevrpc_call:recv(Call) of
        {ok, <<"fail">>} ->
            {error, trevrpc_status:new(aborted, <<"abort">>)};
        {ok, Message} ->
            case trevrpc_call:send(Call, Message) of
                ok -> bidi(Call);
                Error -> Error
            end;
        eof ->
            ok;
        {error, Status} ->
            {error, Status}
    end.

%% Reads the first request, then ends the call without reading the rest.
reject(Call) ->
    {ok, _First} = trevrpc_call:recv(Call),
    {error, trevrpc_status:new(failed_precondition, <<"rejected">>)}.

%% Reads the first request, reports its pid, and ends the call without
%% reading the rest once told to.
stop(Call) ->
    {ok, _First} = trevrpc_call:recv(Call),
    trevrpc_e2e_parked ! {stopping, self()},
    receive
        go -> {error, trevrpc_status:new(failed_precondition, <<"stopped">>)}
    end.

%% Reports its pid like park/1, then returns once told to.
hold(_Call) ->
    trevrpc_e2e_parked ! {held, self()},
    receive
        go -> ok
    end.

park(_Call) ->
    trevrpc_e2e_parked ! {parked, self()},
    timer:sleep(infinity).

start_server(Credentials, Opts) ->
    {ok, Server} = trevrpc_server:start(
        maps:merge(
            #{
                ip => {127, 0, 0, 1},
                cert => maps:get(cert, Credentials),
                key => maps:get(key, Credentials),
                routes => routes()
            },
            Opts
        )
    ),
    {ok, Port} = trevrpc_server:port(Server),
    {Server, Port}.

connect(Credentials, Port) ->
    {ok, Channel} = trevrpc_channel:start(#{
        host => {127, 0, 0, 1},
        port => Port,
        server_name => "localhost",
        cacerts => maps:get(cacerts, Credentials)
    }),
    ok = trevrpc_channel:wait_until_ready(Channel, 30000),
    Channel.

setup() ->
    {ok, _} = application:ensure_all_started(quic),
    logger:set_primary_config(level, none),
    Credentials = credentials(),
    {Server, Port} = start_server(Credentials, #{}),
    #{
        credentials => Credentials,
        server => Server,
        port => Port,
        channel => connect(Credentials, Port)
    }.

cleanup(#{server := Server, channel := Channel}) ->
    trevrpc_channel:close(Channel),
    trevrpc_server:stop(Server),
    logger:set_primary_config(level, notice).

e2e_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Env) ->
        [
            {Name, {timeout, 30, fun() -> Test(Env) end}}
         || {Name, Test} <- [
                {"unary", fun unary_test/1},
                {"unary metadata, headers and trailers", fun unary_metadata_test/1},
                {"unary error with details", fun unary_error_test/1},
                {"internal errors are masked", fun internal_test/1},
                {"handler crash", fun crash_test/1},
                {"unary 1 MiB", fun unary_large_test/1},
                {"client stream", fun client_stream_test/1},
                {"server stream", fun server_stream_test/1},
                {"server stream error", fun server_stream_error_test/1},
                {"server stream headers", fun server_stream_headers_test/1},
                {"bidi", fun bidi_test/1},
                {"bidi error", fun bidi_error_test/1},
                {"unknown method", fun unknown_method_test/1},
                {"deadline", fun deadline_test/1},
                {"cancel", fun cancel_test/1},
                {"caller exit cancels", fun caller_exit_test/1},
                {"invalid metadata", fun invalid_metadata_test/1},
                {"malformed requests", fun malformed_test/1},
                {"optimistic stream compression is refused", fun optimistic_test/1},
                {"connection metadata", fun connection_metadata_test/1},
                {"many concurrent calls", fun concurrent_test/1}
            ]
        ]
    end}.

%%% Calls

unary_test(#{channel := Channel}) ->
    ?assertMatch(
        {ok, <<"hello">>, #{headers := #{}, trailers := #{}}},
        trevrpc_client:unary(Channel, ?UNARY, <<"hello">>, #{timeout => 5000})
    ).

unary_metadata_test(#{channel := Channel}) ->
    ?assertMatch(
        {ok, <<"hello!">>, _},
        trevrpc_client:unary(Channel, ?UNARY, <<"hello">>, #{
            metadata => #{<<"interop-suffix">> => <<"!">>}
        })
    ),
    ?assertEqual(
        {ok, <<"hello">>, #{
            headers => #{<<"interop-header">> => <<"h">>},
            trailers => #{<<"interop-trailer">> => <<"h">>}
        }},
        trevrpc_client:unary(Channel, ?UNARY, <<"hello">>, #{
            metadata => #{<<"interop-header">> => <<"h">>}
        })
    ).

unary_error_test(#{channel := Channel}) ->
    ?assertEqual(
        {error, #{
            code => resource_exhausted,
            message => <<"slow down">>,
            metadata => #{<<"x-why">> => <<"limit">>},
            details => [#{type => <<"google.rpc.RetryInfo">>, value => <<10, 4, 8, 1, 16, 2>>}]
        }},
        trevrpc_client:unary(Channel, ?UNARY, <<"fail">>, #{})
    ).

internal_test(#{channel := Channel}) ->
    ?assertEqual(
        {error, #{code => internal, message => <<"internal server error">>}},
        trevrpc_client:unary(Channel, ?UNARY, <<"internal">>, #{})
    ).

crash_test(#{channel := Channel}) ->
    ?assertEqual(
        {error, #{code => internal, message => <<"internal server error">>}},
        trevrpc_client:unary(Channel, ?CRASH, <<>>, #{})
    ).

unary_large_test(#{channel := Channel}) ->
    Large = binary:copy(<<"x">>, 1048576),
    ?assertMatch({ok, Large, _}, trevrpc_client:unary(Channel, ?UNARY, Large, #{timeout => 10000})).

client_stream_test(#{channel := Channel}) ->
    {ok, Call} = trevrpc_client:client_stream(Channel, ?CLIENT_STREAM, #{timeout => 5000}),
    [ok = trevrpc_client:send(Call, M) || M <- [<<"a">>, <<"b">>, <<"c">>]],
    ?assertEqual({ok, <<"abc:3">>}, trevrpc_client:close_and_recv(Call)).

server_stream_test(#{channel := Channel}) ->
    {ok, Call} = trevrpc_client:server_stream(Channel, ?SERVER_STREAM, <<"3:s">>, #{timeout => 5000}),
    ?assertEqual([{ok, <<"s">>}, {ok, <<"s">>}, {ok, <<"s">>}, {eof, #{}}], recv_all(Call)).

server_stream_error_test(#{channel := Channel}) ->
    {ok, Call} = trevrpc_client:server_stream(Channel, ?SERVER_STREAM, <<"2:s:fail">>, #{}),
    ?assertEqual(
        [
            {ok, <<"s">>},
            {ok, <<"s">>},
            {error, #{code => failed_precondition, message => <<"stop">>}}
        ],
        recv_all(Call)
    ).

server_stream_headers_test(#{channel := Channel}) ->
    Opts = #{metadata => #{<<"interop-header">> => <<"h">>}},
    {ok, Call} = trevrpc_client:server_stream(Channel, ?SERVER_STREAM, <<"2:s">>, Opts),
    ?assertEqual({ok, #{<<"interop-header">> => <<"h">>}}, trevrpc_client:headers(Call)),
    ?assertEqual([{ok, <<"s">>}, {ok, <<"s">>}, {eof, #{}}], recv_all(Call)).

recv_all(Call) ->
    case trevrpc_client:recv(Call) of
        {ok, _} = Message -> [Message | recv_all(Call)];
        End -> [End]
    end.

bidi_test(#{channel := Channel}) ->
    {ok, Call} = trevrpc_client:bidi(Channel, ?BIDI, #{timeout => 5000}),
    [
        begin
            ok = trevrpc_client:send(Call, M),
            ?assertEqual({ok, M}, trevrpc_client:recv(Call))
        end
     || M <- [<<"1">>, <<"2">>, <<"3">>]
    ],
    ok = trevrpc_client:close_send(Call),
    ?assertEqual({eof, #{}}, trevrpc_client:recv(Call)).

bidi_error_test(#{channel := Channel}) ->
    {ok, Call} = trevrpc_client:bidi(Channel, ?BIDI, #{timeout => 5000}),
    ok = trevrpc_client:send(Call, <<"1">>),
    ?assertEqual({ok, <<"1">>}, trevrpc_client:recv(Call)),
    Abort = #{code => aborted, message => <<"abort">>},
    %% The second send may already find the call ended.
    case trevrpc_client:send(Call, <<"fail">>) of
        ok -> ok;
        {error, Status} -> ?assertEqual(Abort, Status)
    end,
    ?assertEqual({error, Abort}, trevrpc_client:recv(Call)).

unknown_method_test(#{channel := Channel}) ->
    ?assertEqual(
        {error, #{code => unimplemented, message => <<"unknown RPC method /test.Echo/Nope">>}},
        trevrpc_client:unary(Channel, <<"/test.Echo/Nope">>, <<>>, #{})
    ),
    ?assertMatch(
        {error, #{
            code := invalid_argument,
            message := <<"streaming RPC kind mismatch for /test.Echo/Bidi", _/binary>>
        }},
        trevrpc_client:unary(Channel, ?BIDI, <<>>, #{})
    ).

%% The client gives up at its deadline and resets the stream; the server
%% stops the handler.
deadline_test(#{channel := Channel}) ->
    register(trevrpc_e2e_parked, self()),
    try
        {ok, Call} = trevrpc_client:bidi(Channel, ?PARK, #{timeout => 300}),
        Handler =
            receive
                {parked, Pid} -> Pid
            after 5000 -> error(not_parked)
            end,
        Monitor = monitor(process, Handler),
        ?assertEqual(
            {error, #{code => deadline_exceeded, message => <<"deadline exceeded">>}},
            trevrpc_client:recv(Call)
        ),
        receive
            {'DOWN', Monitor, process, Handler, _} -> ok
        after 5000 -> error(handler_alive)
        end
    after
        unregister(trevrpc_e2e_parked)
    end,
    ?assertEqual(
        {error, #{code => deadline_exceeded, message => <<"deadline exceeded">>}},
        trevrpc_client:unary(Channel, ?SLEEP, <<>>, #{timeout => 200})
    ).

cancel_test(#{channel := Channel}) ->
    register(trevrpc_e2e_parked, self()),
    try
        {ok, Call} = trevrpc_client:bidi(Channel, ?PARK, #{}),
        Handler =
            receive
                {parked, Pid} -> Pid
            after 5000 -> error(not_parked)
            end,
        Monitor = monitor(process, Handler),
        ok = trevrpc_client:cancel(Call),
        receive
            {'DOWN', Monitor, process, Handler, Reason} -> ?assertEqual(killed, Reason)
        after 5000 -> error(handler_alive)
        end
    after
        unregister(trevrpc_e2e_parked)
    end.

caller_exit_test(#{channel := Channel}) ->
    register(trevrpc_e2e_parked, self()),
    try
        Caller = spawn(fun() ->
            {ok, _Call} = trevrpc_client:bidi(Channel, ?PARK, #{}),
            timer:sleep(infinity)
        end),
        Handler =
            receive
                {parked, Pid} -> Pid
            after 5000 -> error(not_parked)
            end,
        Monitor = monitor(process, Handler),
        exit(Caller, kill),
        receive
            {'DOWN', Monitor, process, Handler, _} -> ok
        after 5000 -> error(handler_alive)
        end
    after
        unregister(trevrpc_e2e_parked)
    end.

invalid_metadata_test(#{channel := Channel}) ->
    ?assertMatch(
        {error, #{code := invalid_argument}},
        trevrpc_client:unary(Channel, ?UNARY, <<>>, #{metadata => #{<<"Bad Key">> => <<>>}})
    ).

concurrent_test(#{channel := Channel}) ->
    Self = self(),
    Pids = [
        spawn_link(fun() ->
            Message = integer_to_binary(N),
            Self ! {self(), trevrpc_client:unary(Channel, ?UNARY, Message, #{timeout => 10000})}
        end)
     || N <- lists:seq(1, 50)
    ],
    Results = [
        receive
            {Pid, Result} -> Result
        after 15000 -> timeout
        end
     || Pid <- Pids
    ],
    ?assertEqual(
        [{ok, integer_to_binary(N)} || N <- lists:seq(1, 50)],
        [{ok, Message} || {ok, Message, _} <- Results]
    ).

%%% Raw requests, for what the client never sends

%% Sends REQUEST frames written by hand and returns the status the server
%% ends the stream with.
raw(Channel, Frames, Fin) ->
    {ok, #{conn := Conn, id := Id, ref := Ref}} = trevrpc_channel:open_stream(Channel, infinity),
    Stream = trevrpc_stream:new(Conn, Id, Ref, Channel, trevrpc_frame:reader(1 bsl 22)),
    {ok, Sent} = trevrpc_stream:send(Stream, Frames, Fin, infinity),
    Result = raw_status(Sent),
    trevrpc_channel:stream_done(Channel, Id, Ref),
    Result.

raw_status(Stream) ->
    Deadline = erlang:monotonic_time(millisecond) + 5000,
    case trevrpc_stream:next_frame(Stream, Deadline) of
        {ok, {status, Payload}, _} ->
            {ok, Status} = trevrpc_wire:decode_status(Payload),
            maps:with([code, message, refused_stream_compression], Status);
        {ok, {message, Message}, Next} ->
            {message, Message, raw_status(Next)};
        {ok, _, Next} ->
            raw_status(Next);
        Other ->
            Other
    end.

request(Fields) ->
    trevrpc_frame:encode(request, trevrpc_wire:encode_request(Fields)).

malformed_test(#{channel := Channel}) ->
    ?assertMatch(
        #{
            code := invalid_argument,
            message := <<"malformed RPC method path; expected /<service>/<method>">>
        },
        raw(Channel, request(#{method => <<"/nope">>}), true)
    ),
    ?assertMatch(
        #{code := invalid_argument, message := <<"unsupported TrevRPC RPC kind 7">>},
        raw(Channel, request(#{method => ?UNARY, kind => {unknown, 7}}), true)
    ),
    ?assertMatch(
        #{
            code := invalid_argument,
            message := <<"request stream must start with REQUEST, not MESSAGE">>
        },
        raw(Channel, trevrpc_frame:encode(message, <<"x">>), true)
    ),
    ?assertMatch(
        #{
            code := invalid_argument,
            message := <<"request stream ended before the request message">>
        },
        raw(Channel, request(#{method => ?UNARY}), true)
    ),
    ?assertMatch(
        #{
            code := invalid_argument,
            message := <<"request stream carried MESSAGE after the request message">>
        },
        raw(
            Channel,
            [
                request(#{method => ?UNARY}),
                trevrpc_frame:encode(message, <<"a">>),
                trevrpc_frame:encode(message, <<"b">>)
            ],
            true
        )
    ),
    %% Grease frames are skipped.
    ?assertMatch(
        {message, <<"a">>, #{code := ok}},
        raw(
            Channel,
            [
                trevrpc_frame:encode_raw(16#21, <<"g">>),
                request(#{method => ?UNARY}),
                trevrpc_frame:encode_raw(16#40, <<>>),
                trevrpc_frame:encode(message, <<"a">>)
            ],
            true
        )
    ).

optimistic_test(#{channel := Channel}) ->
    ?assertEqual(
        #{
            code => resource_exhausted,
            message => <<"stream-compressed requests refused">>,
            refused_stream_compression => true
        },
        raw(
            Channel,
            request(#{
                method => ?CLIENT_STREAM, kind => client_streaming, stream_compression => optimistic
            }),
            true
        )
    ).

connection_metadata_test(#{channel := Channel}) ->
    Offer = request(#{
        method => ?UNARY,
        metadata => #{<<"interop-suffix">> => <<"?">>},
        connection_metadata => #{
            id => 1, metadata => #{<<"interop-suffix">> => <<"!">>, <<"x">> => <<"y">>}
        }
    }),
    %% The call's own entry wins; the set is merged but not stored.
    ?assertMatch(
        {message, <<"d?">>, #{code := ok}},
        raw(Channel, [Offer, trevrpc_frame:encode(message, <<"d">>)], true)
    ),
    Reference = request(#{method => ?UNARY, connection_metadata => #{id => 1}}),
    ?assertMatch(
        #{code := invalid_argument, message := <<"unknown connection metadata set 1">>},
        raw(Channel, [Reference, trevrpc_frame:encode(message, <<"e">>)], true)
    ).

%%% Servers of their own

limits_test_() ->
    {setup,
        fun() ->
            {ok, _} = application:ensure_all_started(quic),
            logger:set_primary_config(level, none),
            credentials()
        end,
        fun(_) -> logger:set_primary_config(level, notice) end, fun(Credentials) ->
            [
                {timeout, 30, fun() -> stream_limit(Credentials) end},
                {timeout, 30, fun() -> frame_limit(Credentials) end},
                {timeout, 30, fun() -> initial_request_timeout(Credentials) end},
                {timeout, 60, fun() -> shutdown_and_reconnect(Credentials) end},
                {timeout, 60, fun() -> closed_connection_releases_slots(Credentials) end},
                {timeout, 60, fun() -> shutdown_waits_for_finishing_call(Credentials) end},
                {timeout, 120, fun() -> unread_uploads_keep_the_connection_window(Credentials) end},
                {timeout, 60, fun() -> subscriptions(Credentials) end},
                {timeout, 60, fun() -> stopped_handler_returns_credit(Credentials) end},
                {timeout, 30, fun() -> untrusted_server(Credentials) end}
            ]
        end}.

stream_limit(Credentials) ->
    {Server, Port} = start_server(Credentials, #{max_streams_per_connection => 2}),
    Channel = connect(Credentials, Port),
    register(trevrpc_e2e_parked, self()),
    try
        {ok, _A} = trevrpc_client:bidi(Channel, ?PARK, #{}),
        {ok, _B} = trevrpc_client:bidi(Channel, ?PARK, #{}),
        [
            receive
                {parked, _} -> ok
            after 5000 -> error(not_parked)
            end
         || _ <- [a, b]
        ],
        ?assertEqual(
            {error, #{code => unavailable, message => <<"too many concurrent RPCs">>}},
            trevrpc_client:unary(Channel, ?UNARY, <<"x">>, #{timeout => 5000})
        )
    after
        unregister(trevrpc_e2e_parked),
        trevrpc_channel:close(Channel),
        trevrpc_server:stop(Server)
    end.

frame_limit(Credentials) ->
    {Server, Port} = start_server(Credentials, #{max_frame_size => 1024}),
    Channel = connect(Credentials, Port),
    try
        ?assertMatch(
            {ok, _, _}, trevrpc_client:unary(Channel, ?UNARY, binary:copy(<<"x">>, 1024), #{})
        ),
        ?assertEqual(
            {error, #{
                code => resource_exhausted, message => <<"frame length 1025 exceeds maximum 1024">>
            }},
            trevrpc_client:unary(Channel, ?UNARY, binary:copy(<<"x">>, 1025), #{timeout => 5000})
        )
    after
        trevrpc_channel:close(Channel),
        trevrpc_server:stop(Server)
    end.

initial_request_timeout(Credentials) ->
    {Server, Port} = start_server(Credentials, #{initial_request_timeout => 200}),
    Channel = connect(Credentials, Port),
    try
        %% Half a REQUEST frame.
        <<Half:3/binary, _/binary>> = iolist_to_binary(request(#{method => ?UNARY})),
        ?assertMatch(
            #{code := deadline_exceeded, message := <<"initial request frame timeout">>},
            raw(Channel, Half, false)
        )
    after
        trevrpc_channel:close(Channel),
        trevrpc_server:stop(Server)
    end.

%% A call in flight when the server shuts down sees its stream reset; the
%% channel fails calls while reconnecting, then reconnects to the next
%% server on the same port.
shutdown_and_reconnect(Credentials) ->
    {Server, Port} = start_server(Credentials, #{graceful_shutdown_timeout => 500}),
    Channel = connect(Credentials, Port),
    register(trevrpc_e2e_parked, self()),
    try
        ok = trevrpc_channel:subscribe(Channel),
        await_state(Channel, {ready, 1}),
        {ok, Call} = trevrpc_client:bidi(Channel, ?PARK, #{}),
        receive
            {parked, _} -> ok
        after 5000 -> error(not_parked)
        end,
        ok = trevrpc_server:stop(Server),
        ?assertMatch({error, #{code := unavailable}}, trevrpc_client:recv(Call)),
        %% Once the channel noticed the server is gone, calls fail at once.
        await_state(Channel, connecting),
        ?assertEqual(
            {error, #{code => unavailable, message => <<"channel is reconnecting">>}},
            trevrpc_client:unary(Channel, ?UNARY, <<>>, #{timeout => 1000})
        ),
        {Server2, Port} = start_server(Credentials, #{port => Port}),
        try
            await_state(Channel, {ready, 2}),
            ?assertEqual(2, trevrpc_channel:generation(Channel)),
            ?assertMatch(
                {ok, <<"again">>, _}, trevrpc_client:unary(Channel, ?UNARY, <<"again">>, #{})
            )
        after
            trevrpc_server:stop(Server2)
        end
    after
        unregister(trevrpc_e2e_parked),
        trevrpc_channel:close(Channel)
    end.

await_state(Channel, State) ->
    receive
        {trevrpc_channel, Channel, State} -> ok
    after 30000 -> error({not_in_state, State})
    end.

%% A connection that closes with a call in flight gives the call's slot in
%% the server's request limit back.
closed_connection_releases_slots(Credentials) ->
    {Server, Port} = start_server(Credentials, #{max_concurrent_requests => 1}),
    register(trevrpc_e2e_parked, self()),
    try
        First = connect(Credentials, Port),
        {ok, _Call} = trevrpc_client:bidi(First, ?PARK, #{}),
        Handler =
            receive
                {parked, Pid} -> Pid
            after 5000 -> error(not_parked)
            end,
        Monitor = monitor(process, Handler),
        trevrpc_channel:close(First),
        %% The connection releases the slot before it stops the handler.
        receive
            {'DOWN', Monitor, process, Handler, _} -> ok
        after 10000 -> error(handler_alive)
        end,
        Second = connect(Credentials, Port),
        try
            ?assertMatch(
                {ok, <<"x">>, _}, trevrpc_client:unary(Second, ?UNARY, <<"x">>, #{timeout => 5000})
            )
        after
            trevrpc_channel:close(Second)
        end
    after
        unregister(trevrpc_e2e_parked),
        trevrpc_server:stop(Server)
    end.

%% Shutdown closes a connection as soon as its last call ends, including one
%% that was already writing its end when shutdown started.
shutdown_waits_for_finishing_call(Credentials) ->
    {Server, Port} = start_server(Credentials, #{graceful_shutdown_timeout => 10000}),
    Channel = connect(Credentials, Port),
    register(trevrpc_e2e_parked, self()),
    try
        {ok, _Call} = trevrpc_client:bidi(Channel, ?HOLD, #{}),
        Handler =
            receive
                {held, Pid} -> Pid
            after 5000 -> error(not_held)
            end,
        {links, [Conn]} = process_info(Handler, links),
        %% The connection takes the handler's finish request before the
        %% shutdown: suspended, it queues both in that order.
        ok = sys:suspend(Conn),
        1 = erlang:trace(Handler, true, [send]),
        Handler ! go,
        receive
            {trace, Handler, send, {'$gen_call', _, {finish, _, _}}, Conn} -> ok
        after 5000 -> error(no_finish)
        end,
        1 = erlang:trace(Server, true, [send]),
        Self = self(),
        spawn_link(fun() ->
            Started = erlang:monotonic_time(millisecond),
            ok = trevrpc_server:stop(Server),
            Self ! {stopped, erlang:monotonic_time(millisecond) - Started}
        end),
        receive
            {trace, Server, send, {'$gen_cast', shutdown}, Conn} -> ok
        after 5000 -> error(no_shutdown)
        end,
        ok = sys:resume(Conn),
        receive
            {stopped, Elapsed} -> ?assert(Elapsed < 5000)
        after 20000 -> error(shutdown_stalled)
        end
    after
        unregister(trevrpc_e2e_parked),
        trevrpc_channel:close(Channel)
    end.

%% Request data the server never reads gives its connection credit back:
%% after more of it than the 16 MiB connection window, the connection still
%% carries a 1 MiB call. Each upload's server answers after the first
%% message, while the rest is still arriving, and the client reads the
%% status without finishing its upload. (Finishing it first, with
%% close_send, can make erlang_quic answer the server's STOP_SENDING with a
%% RESET_STREAM whose final size contradicts the FIN it already sent, which
%% an erlang_quic server answers by closing the connection:
%% benoitc/erlang_quic#551.)
unread_uploads_keep_the_connection_window(Credentials) ->
    {Server, Port} = start_server(Credentials, #{}),
    Channel = connect(Credentials, Port),
    Chunk = binary:copy(<<"u">>, 1048576),
    Rejected = {error, #{code => failed_precondition, message => <<"rejected">>}},
    try
        [
            begin
                {ok, Call} = trevrpc_client:client_stream(Channel, ?REJECT, #{timeout => 10000}),
                _ = trevrpc_client:send(Call, <<"first">>),
                _ = trevrpc_client:send(Call, Chunk),
                ?assertEqual(Rejected, trevrpc_client:recv(Call))
            end
         || _ <- lists:seq(1, 20)
        ],
        Large = binary:copy(<<"x">>, 1048576),
        ?assertMatch(
            {ok, Large, _}, trevrpc_client:unary(Channel, ?UNARY, Large, #{timeout => 10000})
        )
    after
        trevrpc_channel:close(Channel),
        trevrpc_server:stop(Server)
    end.

%% A handler that ends its call with requests routed to it unread gives
%% their credit back: its process consumes every byte of a 1 MiB request it
%% never read. (erlang_quic would also release it once the stream is gone,
%% but a peer may keep the stream open.)
stopped_handler_returns_credit(Credentials) ->
    {Server, Port} = start_server(Credentials, #{}),
    Channel = connect(Credentials, Port),
    register(trevrpc_e2e_parked, self()),
    Chunk = binary:copy(<<"u">>, 1048576),
    try
        {ok, Call} = trevrpc_client:client_stream(Channel, ?STOP, #{timeout => 10000}),
        ok = trevrpc_client:send(Call, <<"first">>),
        Handler =
            receive
                {stopping, Pid} -> Pid
            after 5000 -> error(not_stopping)
            end,
        Monitor = monitor(process, Handler),
        1 = erlang:trace(Handler, true, ['receive', call]),
        _ = erlang:trace_pattern({quic, consume, 3}, true, []),
        ok = trevrpc_client:send(Call, Chunk),
        %% All of the request reached the handler's mailbox, unread.
        await_routed(Handler, byte_size(Chunk) + 5, 0),
        Handler ! go,
        ?assertMatch({error, #{code := failed_precondition}}, trevrpc_client:recv(Call)),
        receive
            {'DOWN', Monitor, process, Handler, _} -> ok
        after 5000 -> error(handler_alive)
        end,
        Delivered = erlang:trace_delivered(Handler),
        receive
            {trace_delivered, Handler, Delivered} -> ok
        end,
        ?assert(consumed(Handler, 0) >= byte_size(Chunk))
    after
        _ = erlang:trace_pattern({quic, consume, 3}, false, []),
        unregister(trevrpc_e2e_parked),
        trevrpc_channel:close(Channel),
        trevrpc_server:stop(Server)
    end.

%% Waits until data events of at least Bytes reached the process.
await_routed(_Pid, Bytes, Seen) when Seen >= Bytes ->
    ok;
await_routed(Pid, Bytes, Seen) ->
    receive
        {trace, Pid, 'receive', {trevrpc_stream, _, {data, Bin, _}}} ->
            await_routed(Pid, Bytes, Seen + byte_size(Bin))
    after 10000 -> error({routed, Seen})
    end.

%% The bytes the process returned with quic:consume/3.
consumed(Pid, Total) ->
    receive
        {trace, Pid, call, {quic, consume, [_, _, Bytes]}} -> consumed(Pid, Total + Bytes);
        {trace, Pid, 'receive', _} -> consumed(Pid, Total)
    after 0 -> Total
    end.

%% Subscribers get every change once, however often they subscribed, and a
%% last `closed'; an exited subscriber is forgotten and an unsubscribed one
%% gets nothing. A later call to the channel returns only after every state
%% message the channel sent before it, so an empty mailbox then means none
%% was sent.
subscriptions(Credentials) ->
    {Server, Port} = start_server(Credentials, #{}),
    Channel = connect(Credentials, Port),
    ok = trevrpc_channel:subscribe(Channel),
    ok = trevrpc_channel:subscribe(Channel),
    await_state(Channel, {ready, 1}),
    await_state(Channel, {ready, 1}),
    %% The channel's own DOWN for the subscriber is in its queue once its
    %% 'receive' trace arrives, so a later sys:get_state is handled after it.
    1 = erlang:trace(Channel, true, ['receive']),
    {Subscriber, Monitor} = spawn_monitor(fun() -> ok = trevrpc_channel:subscribe(Channel) end),
    receive
        {'DOWN', Monitor, process, Subscriber, normal} -> ok
    after 5000 -> error(subscriber_alive)
    end,
    receive
        {trace, Channel, 'receive', {'DOWN', _, process, Subscriber, _}} -> ok
    after 5000 -> error(channel_not_told)
    end,
    1 = erlang:trace(Channel, false, ['receive']),
    Delivered = erlang:trace_delivered(Channel),
    receive
        {trace_delivered, Channel, Delivered} -> ok
    end,
    flush_traces(Channel),
    %% The channel forgets it: its pid is nowhere in the channel's state.
    ?assertNot(contains(sys:get_state(Channel), Subscriber)),
    ok = trevrpc_server:stop(Server),
    await_state(Channel, connecting),
    _ = trevrpc_channel:generation(Channel),
    ?assertEqual(none, next_state(Channel)),
    ok = trevrpc_channel:unsubscribe(Channel),
    {Server2, Port} = start_server(Credentials, #{port => Port}),
    try
        ok = trevrpc_channel:wait_until_ready(Channel, 30000),
        _ = trevrpc_channel:generation(Channel),
        ?assertEqual(none, next_state(Channel)),
        ok = trevrpc_channel:subscribe(Channel),
        await_state(Channel, {ready, 2}),
        trevrpc_channel:close(Channel),
        await_state(Channel, closed)
    after
        trevrpc_server:stop(Server2)
    end.

flush_traces(Pid) ->
    receive
        {trace, Pid, _, _} -> flush_traces(Pid)
    after 0 -> ok
    end.

contains(Pid, Pid) -> true;
contains(Tuple, Pid) when is_tuple(Tuple) -> contains(tuple_to_list(Tuple), Pid);
contains(Map, Pid) when is_map(Map) -> contains(maps:to_list(Map), Pid);
contains([Head | Tail], Pid) -> contains(Head, Pid) orelse contains(Tail, Pid);
contains(_Term, _Pid) -> false.

next_state(Channel) ->
    receive
        {trevrpc_channel, Channel, State} -> State
    after 0 -> none
    end.

untrusted_server(Credentials) ->
    {Server, Port} = start_server(Credentials, #{}),
    Other = credentials(),
    try
        {ok, Channel} = trevrpc_channel:start(#{
            host => {127, 0, 0, 1},
            port => Port,
            server_name => "localhost",
            cacerts => maps:get(cacerts, Other)
        }),
        ?assertMatch(
            {error, #{code := unavailable}}, trevrpc_channel:wait_until_ready(Channel, 1000)
        ),
        trevrpc_channel:close(Channel)
    after
        trevrpc_server:stop(Server)
    end.
