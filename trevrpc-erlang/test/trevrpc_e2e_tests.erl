%% Erlang client against Erlang server on loopback, over native QUIC and
%% over QMux on TLS.
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
-define(PLAIN, <<"/test.Compressed/Plain">>).
-define(TEXT_UNARY, <<"/test.Compressed/Unary">>).
-define(TEXT_CLIENT_STREAM, <<"/test.Compressed/ClientStream">>).
-define(TEXT_BIDI, <<"/test.Compressed/Bidi">>).
%% The message type the golden dictionary compresses.
-define(TEXT, <<"test.Text">>).

%%% Fixture

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
        ?CRASH => #{kind => unary, handler => fun(_Request, _Call) -> erlang:error(boom) end},
        ?PLAIN => #{kind => unary, handler => fun counted_unary/2, compression => message},
        ?TEXT_UNARY => typed(#{kind => unary, handler => fun counted_unary/2}),
        ?TEXT_CLIENT_STREAM => typed(#{
            kind => client_streaming, handler => fun counted_client_stream/1
        }),
        ?TEXT_BIDI => typed(#{kind => bidirectional_streaming, handler => fun counted_bidi/1})
    }.

typed(Route) ->
    Route#{request_type => ?TEXT, response_type => ?TEXT}.

%% Echoes, ending with how many requests arrived compressed in its
%% trailers.
counted_unary(Request, Call) ->
    count_trailers(Call),
    {ok, Request}.

counted_client_stream(Call) ->
    counted_client_stream(Call, []).

counted_client_stream(Call, Acc) ->
    case trevrpc_call:recv(Call) of
        {ok, Message} ->
            counted_client_stream(Call, [Message | Acc]);
        eof ->
            count_trailers(Call),
            {ok, iolist_to_binary(lists:reverse(Acc))};
        {error, Status} ->
            {error, Status}
    end.

counted_bidi(Call) ->
    case trevrpc_call:recv(Call) of
        {ok, Message} ->
            ok = trevrpc_call:send(Call, Message),
            counted_bidi(Call);
        eof ->
            count_trailers(Call),
            ok;
        {error, Status} ->
            {error, Status}
    end.

count_trailers(Call) ->
    #{dictionary := Dictionary, plain := Plain} = trevrpc_call:compressed_received(Call),
    trevrpc_call:set_trailers(Call, #{
        <<"x-dictionary">> => integer_to_binary(Dictionary),
        <<"x-plain">> => integer_to_binary(Plain)
    }).

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
    Server.

%% The server's ports, for a next server to listen on.
ports(Server) ->
    {ok, Port} = trevrpc_server:port(Server),
    {ok, TcpPort} = trevrpc_server:tcp_port(Server),
    #{port => Port, tcp_port => TcpPort}.

%% A channel to the server over the transport the fixture names: `quic'
%% (the default) or `tcp' for QMux over TLS.
connect(Credentials, Server) ->
    Channel = start_channel(Server, Credentials),
    ok = trevrpc_channel:wait_until_ready(Channel, 30000),
    Channel.

start_channel(Server, Credentials) ->
    {ok, Channel} = trevrpc_channel:start(
        (ports(Server))#{
            host => {127, 0, 0, 1},
            transport => maps:get(transport, Credentials, quic),
            server_name => "localhost",
            cacerts => maps:get(cacerts, Credentials)
        }
    ),
    Channel.

setup(Transport) ->
    {ok, _} = application:ensure_all_started(quic),
    logger:set_primary_config(level, none),
    Credentials = (trevrpc_test_tls:credentials())#{transport => Transport},
    Server = start_server(Credentials, #{}),
    #{
        credentials => Credentials,
        server => Server,
        channel => connect(Credentials, Server)
    }.

cleanup(#{server := Server, channel := Channel}) ->
    trevrpc_channel:close(Channel),
    trevrpc_server:stop(Server),
    logger:set_primary_config(level, notice).

e2e_test_() ->
    [{atom_to_list(Transport), e2e(Transport)} || Transport <- [quic, tcp]].

e2e(Transport) ->
    {setup, fun() -> setup(Transport) end, fun cleanup/1, fun(Env) ->
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
                {"compression without a dictionary", fun plain_test/1},
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
    {ok, #{transport := Transport, id := Id, ref := Ref}} = trevrpc_channel:open_stream(
        Channel, infinity
    ),
    Stream = trevrpc_stream:new(Transport, Id, Ref, Channel, trevrpc_frame:reader(1 bsl 22)),
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

plain_test(#{credentials := Credentials, server := Server}) ->
    %% A channel of its own, whose first call queries.
    Channel = connect(Credentials, Server),
    Text = binary:copy(<<"{\"service\":\"search\",\"status\":404}">>, 12),
    Opts = #{compression => message, timeout => 5000},
    Plain = fun(Method, Message, CallOpts) ->
        {ok, Message, #{trailers := #{<<"x-plain">> := Count}}} =
            trevrpc_client:unary(Channel, Method, Message, CallOpts),
        Count
    end,
    %% The first request goes before the answer, the second compressed.
    ?assertEqual(<<"0">>, Plain(?PLAIN, Text, Opts)),
    ?assertEqual(<<"1">>, Plain(?PLAIN, Text, Opts)),
    %% Not below 256 bytes, nor where the call's options do not allow it.
    ?assertEqual(<<"0">>, Plain(?PLAIN, binary:part(Text, 0, 255), Opts)),
    ?assertEqual(<<"0">>, Plain(?PLAIN, Text, #{})),
    %% A type without a dictionary here still compresses without one.
    ?assertEqual(<<"1">>, Plain(?TEXT_UNARY, Text, Opts#{request_type => ?TEXT})),
    %% Responses are compressed where the client accepts ID 0 and the route
    %% allows it.
    Request = trevrpc_frame:encode(message, Text),
    ?assertMatch(
        [{zstd_message, _}, {status, #{code := ok}}],
        raw_frames(Channel, [request(#{method => ?PLAIN, accept_dictionaries => [0]}), Request])
    ),
    ?assertMatch(
        [{message, Text}, {status, #{code := ok}}],
        raw_frames(Channel, [request(#{method => ?PLAIN}), Request])
    ),
    ?assertMatch(
        [{message, Text}, {status, #{code := ok}}],
        raw_frames(Channel, [request(#{method => ?TEXT_UNARY, accept_dictionaries => [0]}), Request])
    ),
    trevrpc_channel:close(Channel).

%% Sends frames written by hand and returns every frame of the response,
%% ANSWER and STATUS decoded.
raw_frames(Channel, Frames) ->
    {ok, #{transport := Transport, id := Id, ref := Ref}} = trevrpc_channel:open_stream(
        Channel, infinity
    ),
    Stream = trevrpc_stream:new(Transport, Id, Ref, Channel, trevrpc_frame:reader(1 bsl 22)),
    {ok, Sent} = trevrpc_stream:send(Stream, Frames, true, infinity),
    Result = collect_frames(Sent, erlang:monotonic_time(millisecond) + 5000, []),
    trevrpc_channel:stream_done(Channel, Id, Ref),
    Result.

collect_frames(Stream, Deadline, Acc) ->
    case trevrpc_stream:next_frame(Stream, Deadline) of
        {ok, {status, Payload}, _} ->
            {ok, Status} = trevrpc_wire:decode_status(Payload),
            lists:reverse(Acc, [{status, maps:with([code, message, metadata], Status)}]);
        {ok, {answer, Payload}, Next} ->
            {ok, Answer} = trevrpc_wire:decode_answer(Payload),
            collect_frames(Next, Deadline, [{answer, Answer} | Acc]);
        {ok, Frame, Next} ->
            collect_frames(Next, Deadline, [Frame | Acc]);
        Other ->
            lists:reverse(Acc, [Other])
    end.

%%% Dictionaries

compression_test_() ->
    [{atom_to_list(Transport), compression(Transport)} || Transport <- [quic, tcp]].

compression(Transport) ->
    {setup,
        fun() ->
            {ok, _} = application:ensure_all_started(quic),
            logger:set_primary_config(level, none),
            Credentials = (trevrpc_test_tls:credentials())#{
                transport => Transport, compression => dictionary_settings()
            },
            #{
                credentials => Credentials,
                server => start_server(Credentials, #{compression => dictionary_settings()})
            }
        end,
        fun(#{server := Server}) ->
            trevrpc_server:stop(Server),
            logger:set_primary_config(level, notice)
        end,
        fun(Env) ->
            [
                {Name, {timeout, 30, fun() -> Test(Env) end}}
             || {Name, Test} <- [
                    {"compressed both ways once answered", fun dictionary_test/1},
                    {"a request stream compresses once its answer arrives", fun mid_call_test/1},
                    {"a waiting upload takes its answer", fun upload_answer_test/1},
                    {"a malformed answer ends an upload", fun malformed_answer_test/1},
                    {"the server compresses only what the client accepts",
                        fun server_compression_test/1},
                    {"compressed requests are checked", fun compressed_requests_test/1},
                    {"decompressed sizes count towards the body limit", fun body_limit_test/1},
                    {"a new connection negotiates again", fun reconnect_test/1},
                    {"invalid compression settings", fun invalid_settings_test/1}
                ]
            ]
        end}.

dictionary_settings() ->
    #{dictionaries => [#{data => golden(<<"dict.d1.dictionary">>), messages => [?TEXT]}]}.

golden(Name) ->
    maps:get(Name, trevrpc_zstd_tests:vectors()).

%% A 71-byte message the golden dictionary compresses.
small() ->
    golden(<<"ok.small.message">>).

compressed_channel(#{credentials := Credentials, server := Server}) ->
    {ok, Channel} = trevrpc_channel:start(
        (ports(Server))#{
            host => {127, 0, 0, 1},
            transport => maps:get(transport, Credentials),
            server_name => "localhost",
            cacerts => maps:get(cacerts, Credentials),
            compression => maps:get(compression, Credentials)
        }
    ),
    ok = trevrpc_channel:wait_until_ready(Channel, 30000),
    Channel.

typed_opts() ->
    #{request_type => ?TEXT, response_type => ?TEXT, timeout => 5000}.

dictionary_count({eof, #{<<"x-dictionary">> := Count}}) -> Count;
dictionary_count({ok, _, #{trailers := #{<<"x-dictionary">> := Count}}}) -> Count.

dictionary_test(Env) ->
    Channel = compressed_channel(Env),
    Message = small(),
    Unary = fun() ->
        {ok, Message, _} =
            Result = trevrpc_client:unary(Channel, ?TEXT_UNARY, Message, typed_opts()),
        dictionary_count(Result)
    end,
    %% The first call's request goes before the server's answer; the
    %% response is compressed with the dictionary the answer confirms.
    ?assertEqual(<<"0">>, Unary()),
    ?assertEqual(<<"1">>, Unary()),
    %% With the shared set known, a stream compresses from its first
    %% request.
    {ok, Call} = trevrpc_client:bidi(Channel, ?TEXT_BIDI, typed_opts()),
    [ok = trevrpc_client:send(Call, Message) || _ <- lists:seq(1, 3)],
    ok = trevrpc_client:close_send(Call),
    [?assertEqual({ok, Message}, trevrpc_client:recv(Call)) || _ <- lists:seq(1, 3)],
    ?assertEqual(<<"3">>, dictionary_count(trevrpc_client:recv(Call))),
    %% Types without a dictionary go uncompressed.
    ?assertMatch(
        {ok, Message, #{trailers := #{<<"x-dictionary">> := <<"0">>}}},
        trevrpc_client:unary(Channel, ?TEXT_UNARY, Message, #{timeout => 5000})
    ),
    trevrpc_channel:close(Channel).

mid_call_test(Env) ->
    Channel = compressed_channel(Env),
    Message = small(),
    {ok, Call} = trevrpc_client:bidi(Channel, ?TEXT_BIDI, typed_opts()),
    ok = trevrpc_client:send(Call, Message),
    %% Reading the echo takes the ANSWER that came before it.
    ?assertEqual({ok, Message}, trevrpc_client:recv(Call)),
    ok = trevrpc_client:send(Call, Message),
    ok = trevrpc_client:close_send(Call),
    ?assertEqual({ok, Message}, trevrpc_client:recv(Call)),
    ?assertEqual(<<"1">>, dictionary_count(trevrpc_client:recv(Call))),
    trevrpc_channel:close(Channel).

upload_answer_test(Env) ->
    Channel = compressed_channel(Env),
    Message = small(),
    {ok, Call} = trevrpc_client:client_stream(Channel, ?TEXT_CLIENT_STREAM, typed_opts()),
    %% The server answers at dispatch. The ANSWER is held back while the
    %% first request goes, then a send that finds it waiting takes it
    %% without reading the response.
    Answer = take_stream_data(Call),
    ok = trevrpc_client:send(Call, Message),
    self() ! {trevrpc_stream, call_ref(Call), Answer},
    ok = trevrpc_client:send(Call, Message),
    ok = trevrpc_client:send(Call, Message),
    ok = trevrpc_client:close_send(Call),
    Expected = binary:copy(Message, 3),
    ?assertEqual({ok, Expected}, trevrpc_client:recv(Call)),
    ?assertEqual(<<"2">>, dictionary_count(trevrpc_client:recv(Call))),
    trevrpc_channel:close(Channel).

malformed_answer_test(Env) ->
    Channel = compressed_channel(Env),
    Message = small(),
    {ok, Call} = trevrpc_client:client_stream(Channel, ?TEXT_CLIENT_STREAM, typed_opts()),
    %% The ANSWER the server sends at dispatch is the stream's first data;
    %% it is replaced with a malformed one before a send takes it.
    {data, _Answer, false} = take_stream_data(Call),
    self() !
        {trevrpc_stream, call_ref(Call),
            {data, iolist_to_binary(trevrpc_frame:encode(answer, <<16#FF>>)), false}},
    Malformed = #{code => internal, message => <<"malformed ANSWER">>},
    ?assertMatch({error, Malformed}, trevrpc_client:send(Call, Message)),
    ?assertMatch({error, Malformed}, trevrpc_client:recv(Call)),
    trevrpc_channel:close(Channel).

%% The routing tag of a client call's stream events: the first field of
%% its record.
call_ref(Call) -> element(2, Call).

%% Takes the next data event of the call's stream out of the mailbox,
%% before the call reads it.
take_stream_data(Call) ->
    Ref = call_ref(Call),
    receive
        {trevrpc_stream, Ref, {data, _, _} = Data} -> Data
    after 5000 -> error(no_stream_data)
    end.

server_compression_test(Env) ->
    Channel = compressed_channel(Env),
    Message = small(),
    Request = fun(Fields) -> request(Fields#{method => ?TEXT_UNARY}) end,
    Body = trevrpc_frame:encode(message, Message),
    [D1, Plain] = trevrpc_zstd:query(trevrpc_zstd_tests:config()),
    %% Compressed with the dictionary the client accepts.
    [{zstd_message, Payload}, {status, #{code := ok}}] =
        raw_frames(Channel, [Request(#{accept_dictionaries => [1]}), Body]),
    ?assertEqual(golden(<<"ok.small.payload">>), Payload),
    ?assertMatch(
        [{message, Message}, {status, _}], raw_frames(Channel, [Request(#{}), Body])
    ),
    %% A query is answered; a dictionary the answer does not confirm is not
    %% used, even where the client accepts it.
    ?assertMatch(
        [{answer, #{dictionaries := [D1, Plain]}}, {zstd_message, _}, {status, _}],
        raw_frames(Channel, [
            Request(#{accept_dictionaries => [1], dictionary_query => [D1, Plain]}), Body
        ])
    ),
    Wrong = D1#{fingerprint => <<"12345678">>},
    ?assertMatch(
        [{answer, #{dictionaries := [Plain]}}, {message, Message}, {status, _}],
        raw_frames(Channel, [
            Request(#{accept_dictionaries => [1], dictionary_query => [Wrong, Plain]}), Body
        ])
    ),
    %% A streaming call gets its ANSWER at dispatch, before any response.
    ?assertMatch(
        [{answer, _}, {message, <<>>}, {status, #{code := ok}}],
        raw_frames(Channel, [
            request(#{
                method => ?TEXT_CLIENT_STREAM,
                kind => client_streaming,
                dictionary_query => [D1]
            })
        ])
    ),
    trevrpc_channel:close(Channel).

compressed_requests_test(Env) ->
    Channel = compressed_channel(Env),
    Unary = request(#{method => ?TEXT_UNARY}),
    Compressed = fun(Name) -> trevrpc_frame:encode(zstd_message, golden(Name)) end,
    ?assertMatch(
        [{message, _}, {status, #{code := ok, metadata := #{<<"x-dictionary">> := <<"1">>}}}],
        raw_frames(Channel, [Unary, Compressed(<<"ok.small.payload">>)])
    ),
    ?assertMatch(
        [{message, _}, {status, #{code := ok, metadata := #{<<"x-plain">> := <<"1">>}}}],
        raw_frames(Channel, [Unary, Compressed(<<"plain.records.payload">>)])
    ),
    ?assertMatch(
        [{status, #{code := invalid_argument, message := <<"invalid ZSTD_MESSAGE payload">>}}],
        raw_frames(Channel, [Unary, Compressed(<<"bad.trailing_bytes.payload">>)])
    ),
    ?assertMatch(
        [
            {status, #{
                code := invalid_argument,
                message := <<"compressed message uses dictionary 300, which is not accepted">>
            }}
        ],
        raw_frames(Channel, [Unary, Compressed(<<"bad.unknown_dictionary.payload">>)])
    ),
    ?assertMatch(
        [{status, #{code := resource_exhausted}}],
        raw_frames(Channel, [Unary, Compressed(<<"too_large.zeros.payload">>)])
    ),
    ?assertMatch(
        [{status, #{code := invalid_argument}}],
        raw_frames(Channel, [Unary, trevrpc_frame:encode(zstd_stream_message, <<0>>)])
    ),
    trevrpc_channel:close(Channel).

body_limit_test(#{credentials := Credentials}) ->
    Server = start_server(Credentials, #{max_stream_body_size => 4096}),
    Channel = connect(Credentials, Server),
    {ok, Zeros} = trevrpc_zstd:compress(trevrpc_zstd:plain(), 0, <<0:(10000 * 8)>>),
    ?assertMatch(
        [
            {status, #{
                code := resource_exhausted,
                message := <<"request stream exceeded maximum body size of 4096 bytes">>
            }}
        ],
        raw_frames(Channel, [
            request(#{method => ?TEXT_CLIENT_STREAM, kind => client_streaming}),
            trevrpc_frame:encode(zstd_message, Zeros)
        ])
    ),
    trevrpc_channel:close(Channel),
    trevrpc_server:stop(Server).

reconnect_test(#{credentials := Credentials}) ->
    Server = start_server(Credentials, #{
        compression => dictionary_settings(), graceful_shutdown_timeout => 0
    }),
    Channel = compressed_channel(#{credentials => Credentials, server => Server}),
    Message = small(),
    Unary = fun() ->
        dictionary_count(trevrpc_client:unary(Channel, ?TEXT_UNARY, Message, typed_opts()))
    end,
    ?assertEqual(<<"0">>, Unary()),
    ?assertEqual(<<"1">>, Unary()),
    ok = trevrpc_channel:subscribe(Channel),
    Ports = ports(Server),
    ok = trevrpc_server:stop(Server),
    await_state(Channel, connecting),
    %% The next server lacks the dictionary, so the next connection must
    %% not take the last one's shared set for its own.
    Server2 = start_server(Credentials, Ports),
    try
        await_state(Channel, {ready, 2}),
        ?assertEqual(<<"0">>, Unary()),
        ?assertEqual(<<"0">>, Unary())
    after
        trevrpc_server:stop(Server2),
        trevrpc_channel:close(Channel)
    end.

invalid_settings_test(#{credentials := Credentials, server := Server}) ->
    Invalid = #{dictionaries => [#{data => <<"not a dictionary">>}]},
    ?assertMatch(
        {error, #{code := invalid_argument}},
        trevrpc_channel:connect(
            (ports(Server))#{host => {127, 0, 0, 1}, compression => Invalid}, 1000
        )
    ),
    ?assertMatch(
        {error, {compression, _}},
        trevrpc_server:start(#{
            ip => {127, 0, 0, 1},
            cert => maps:get(cert, Credentials),
            key => maps:get(key, Credentials),
            routes => routes(),
            compression => Invalid
        })
    ),
    ?assertMatch(
        {error, {route, ?PLAIN, {invalid_compression, everything}}},
        trevrpc_server:start(#{
            ip => {127, 0, 0, 1},
            cert => maps:get(cert, Credentials),
            key => maps:get(key, Credentials),
            routes => #{
                ?PLAIN => #{
                    kind => unary, handler => fun counted_unary/2, compression => everything
                }
            }
        })
    ).

%%% Servers of their own

limits_test_() ->
    [{atom_to_list(Transport), limits(Transport)} || Transport <- [quic, tcp]].

limits(Transport) ->
    {setup,
        fun() ->
            {ok, _} = application:ensure_all_started(quic),
            logger:set_primary_config(level, none),
            (trevrpc_test_tls:credentials())#{transport => Transport}
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
    Server = start_server(Credentials, #{max_streams_per_connection => 2}),
    Channel = connect(Credentials, Server),
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
    Server = start_server(Credentials, #{max_frame_size => 1024}),
    Channel = connect(Credentials, Server),
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
    Server = start_server(Credentials, #{initial_request_timeout => 200}),
    Channel = connect(Credentials, Server),
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
    Server = start_server(Credentials, #{graceful_shutdown_timeout => 500}),
    Channel = connect(Credentials, Server),
    register(trevrpc_e2e_parked, self()),
    try
        ok = trevrpc_channel:subscribe(Channel),
        await_state(Channel, {ready, 1}),
        {ok, Call} = trevrpc_client:bidi(Channel, ?PARK, #{}),
        receive
            {parked, _} -> ok
        after 5000 -> error(not_parked)
        end,
        Ports = ports(Server),
        ok = trevrpc_server:stop(Server),
        ?assertMatch({error, #{code := unavailable}}, trevrpc_client:recv(Call)),
        %% Once the channel noticed the server is gone, calls fail at once.
        await_state(Channel, connecting),
        ?assertEqual(
            {error, #{code => unavailable, message => <<"channel is reconnecting">>}},
            trevrpc_client:unary(Channel, ?UNARY, <<>>, #{timeout => 1000})
        ),
        Server2 = start_server(Credentials, Ports),
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
    Server = start_server(Credentials, #{max_concurrent_requests => 1}),
    register(trevrpc_e2e_parked, self()),
    try
        First = connect(Credentials, Server),
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
        Second = connect(Credentials, Server),
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
    Server = start_server(Credentials, #{graceful_shutdown_timeout => 10000}),
    Channel = connect(Credentials, Server),
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
    Server = start_server(Credentials, #{}),
    Channel = connect(Credentials, Server),
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
    Server = start_server(Credentials, #{}),
    Channel = connect(Credentials, Server),
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
        _ = erlang:trace_pattern({trevrpc_transport, consume, 3}, true, []),
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
        _ = erlang:trace_pattern({trevrpc_transport, consume, 3}, false, []),
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

%% The bytes the process returned with trevrpc_transport:consume/3.
consumed(Pid, Total) ->
    receive
        {trace, Pid, call, {trevrpc_transport, consume, [_, _, Bytes]}} ->
            consumed(Pid, Total + Bytes);
        {trace, Pid, 'receive', _} ->
            consumed(Pid, Total)
    after 0 -> Total
    end.

%% Subscribers get every change once, however often they subscribed, and a
%% last `closed'; an exited subscriber is forgotten and an unsubscribed one
%% gets nothing. A later call to the channel returns only after every state
%% message the channel sent before it, so an empty mailbox then means none
%% was sent.
subscriptions(Credentials) ->
    Server = start_server(Credentials, #{}),
    Channel = connect(Credentials, Server),
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
    Ports = ports(Server),
    ok = trevrpc_server:stop(Server),
    await_state(Channel, connecting),
    _ = trevrpc_channel:generation(Channel),
    ?assertEqual(none, next_state(Channel)),
    ok = trevrpc_channel:unsubscribe(Channel),
    Server2 = start_server(Credentials, Ports),
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
    Server = start_server(Credentials, #{}),
    Other = trevrpc_test_tls:credentials(),
    try
        Channel = start_channel(Server, Credentials#{cacerts => maps:get(cacerts, Other)}),
        ?assertMatch(
            {error, #{code := unavailable}}, trevrpc_channel:wait_until_ready(Channel, 1000)
        ),
        trevrpc_channel:close(Channel)
    after
        trevrpc_server:stop(Server)
    end.

%%% Transport selection

transport_selection_test_() ->
    {setup,
        fun() ->
            {ok, _} = application:ensure_all_started(quic),
            logger:set_primary_config(level, none),
            trevrpc_test_tls:credentials()
        end,
        fun(_) -> logger:set_primary_config(level, notice) end, fun(Credentials) ->
            [
                {timeout, 30, fun() -> auto_prefers_quic(Credentials) end},
                {timeout, 30, fun() -> auto_falls_back_to_qmux(Credentials) end},
                {timeout, 30, fun() -> tcp_off(Credentials) end},
                {timeout, 30, fun() -> self_signed(quic) end},
                {timeout, 30, fun() -> self_signed(tcp) end},
                {timeout, 30, fun() -> expired_self_signed(Credentials) end}
            ]
        end}.

%% With both transports up, QUIC wins the race: QMux only starts after the
%% fallback delay.
auto_prefers_quic(Credentials) ->
    Server = start_server(Credentials, #{}),
    Channel = connect(Credentials#{transport => auto}, Server),
    try
        ?assertEqual(quic, trevrpc_channel:transport(Channel)),
        ?assertMatch({ok, <<"q">>, _}, trevrpc_client:unary(Channel, ?UNARY, <<"q">>, #{}))
    after
        trevrpc_channel:close(Channel),
        trevrpc_server:stop(Server)
    end.

%% A UDP port that swallows every datagram stands in for a network that
%% drops QUIC: the channel connects over QMux once the fallback delay
%% passed, without waiting for QUIC to time out.
auto_falls_back_to_qmux(Credentials) ->
    Server = start_server(Credentials, #{}),
    {ok, Sink} = gen_udp:open(0, [{ip, {127, 0, 0, 1}}, {active, false}]),
    {ok, SinkPort} = inet:port(Sink),
    #{tcp_port := TcpPort} = ports(Server),
    {ok, Channel} = trevrpc_channel:start(#{
        host => {127, 0, 0, 1},
        port => SinkPort,
        tcp_port => TcpPort,
        fallback_delay => 50,
        connect_timeout => 20000,
        server_name => "localhost",
        cacerts => maps:get(cacerts, Credentials)
    }),
    try
        ok = trevrpc_channel:wait_until_ready(Channel, 5000),
        ?assertEqual(qmux, trevrpc_channel:transport(Channel)),
        ?assertMatch({ok, <<"t">>, _}, trevrpc_client:unary(Channel, ?UNARY, <<"t">>, #{}))
    after
        trevrpc_channel:close(Channel),
        gen_udp:close(Sink),
        trevrpc_server:stop(Server)
    end.

%% A server without TCP serves QUIC alone: a QMux-only channel never
%% connects.
tcp_off(Credentials) ->
    Server = start_server(Credentials, #{tcp => false}),
    {ok, Port} = trevrpc_server:port(Server),
    Opts = #{
        host => {127, 0, 0, 1},
        port => Port,
        server_name => "localhost",
        cacerts => maps:get(cacerts, Credentials)
    },
    {ok, Auto} = trevrpc_channel:start(Opts),
    {ok, Tcp} = trevrpc_channel:start(Opts#{transport => tcp}),
    try
        ?assertEqual({error, disabled}, trevrpc_server:tcp_port(Server)),
        ok = trevrpc_channel:wait_until_ready(Auto, 30000),
        ?assertEqual(quic, trevrpc_channel:transport(Auto)),
        ?assertMatch({error, #{code := unavailable}}, trevrpc_channel:wait_until_ready(Tcp, 500))
    after
        trevrpc_channel:close(Auto),
        trevrpc_channel:close(Tcp),
        trevrpc_server:stop(Server)
    end.

%% A self-signed server certificate the client trusts by itself is accepted,
%% but still only for its own host name.
self_signed(Transport) ->
    Credentials = (trevrpc_test_tls:self_signed())#{transport => Transport},
    Server = start_server(Credentials, #{}),
    Channel = connect(Credentials, Server),
    Other = start_channel(Server, Credentials),
    ok = trevrpc_channel:close(Other),
    {ok, Wrong} = trevrpc_channel:start(
        (ports(Server))#{
            host => {127, 0, 0, 1},
            transport => Transport,
            server_name => "elsewhere",
            cacerts => maps:get(cacerts, Credentials)
        }
    ),
    try
        ?assertMatch({ok, <<"s">>, _}, trevrpc_client:unary(Channel, ?UNARY, <<"s">>, #{})),
        ?assertMatch({error, #{code := unavailable}}, trevrpc_channel:wait_until_ready(Wrong, 1000))
    after
        trevrpc_channel:close(Wrong),
        trevrpc_channel:close(Channel),
        trevrpc_server:stop(Server)
    end.

%% Trusting a self-signed certificate by itself does not make it valid after
%% it expired.
expired_self_signed(_Credentials) ->
    Credentials = (trevrpc_test_tls:self_signed([{validity, {{2020, 1, 1}, {2020, 1, 2}}}]))#{
        transport => tcp
    },
    Server = start_server(Credentials, #{}),
    Channel = start_channel(Server, Credentials),
    try
        ?assertMatch(
            {error, #{code := unavailable}}, trevrpc_channel:wait_until_ready(Channel, 1000)
        )
    after
        trevrpc_channel:close(Channel),
        trevrpc_server:stop(Server)
    end.

%%% QMux failures

qmux_failures_test_() ->
    {setup,
        fun() ->
            {ok, _} = application:ensure_all_started(quic),
            logger:set_primary_config(level, none),
            (trevrpc_test_tls:credentials())#{transport => tcp}
        end,
        fun(_) -> logger:set_primary_config(level, notice) end, fun(Credentials) ->
            [
                {timeout, 30, fun() -> silent_client(Credentials) end},
                {timeout, 30, fun() -> handshake_limit(Credentials) end},
                {timeout, 30, fun() -> client_session_dies(Credentials) end},
                {timeout, 30, fun() -> server_session_dies(Credentials) end},
                {timeout, 30, fun() -> listener_exit_stops_server(Credentials) end},
                {timeout, 60, fun() -> reprobe_window(Credentials) end},
                {timeout, 30, fun() -> refused_connection_fails_at_once(Credentials) end},
                {timeout, 30, fun() -> early_close_fails_the_attempt(Credentials) end},
                {timeout, 30, fun() -> one_fallback_per_round(Credentials) end}
            ]
        end}.

%% A client that completes TLS and sends nothing more is closed once the
%% handshake timeout passes, and gives its connection slot back.
silent_client(Credentials) ->
    Server = start_server(Credentials, #{max_connections => 1, tls_handshake_timeout => 300}),
    #{tcp_port := TcpPort} = ports(Server),
    {ok, Silent} = ssl:connect({127, 0, 0, 1}, TcpPort, [
        binary,
        {active, false},
        {verify, verify_none},
        {alpn_advertised_protocols, [trevrpc:qmux_alpn()]}
    ]),
    try
        ?assertEqual({error, closed}, drain(Silent)),
        Channel = connect(Credentials, Server),
        ?assertMatch({ok, <<"q">>, _}, trevrpc_client:unary(Channel, ?UNARY, <<"q">>, #{})),
        trevrpc_channel:close(Channel)
    after
        _ = ssl:close(Silent),
        trevrpc_server:stop(Server)
    end.

%% Reads until the socket closes, past the server's parameters.
drain(Socket) ->
    case ssl:recv(Socket, 0, 5000) of
        {ok, _} -> drain(Socket);
        Error -> Error
    end.

%% With one TLS handshake allowed at a time, a connection that never starts
%% its handshake holds up the next until the handshake timeout drops it.
handshake_limit(Credentials) ->
    Server = start_server(Credentials, #{max_tls_handshakes => 1, tls_handshake_timeout => 500}),
    #{tcp_port := TcpPort} = ports(Server),
    {ok, Idle} = gen_tcp:connect({127, 0, 0, 1}, TcpPort, [binary, {active, false}]),
    Channel = start_channel(Server, Credentials),
    try
        ?assertMatch(
            {error, #{code := unavailable}}, trevrpc_channel:wait_until_ready(Channel, 200)
        ),
        ok = trevrpc_channel:wait_until_ready(Channel, 10000),
        ?assertEqual({error, closed}, gen_tcp:recv(Idle, 0, 5000))
    after
        gen_tcp:close(Idle),
        trevrpc_channel:close(Channel),
        trevrpc_server:stop(Server)
    end.

%% The channel's session dying without a closed event is a lost connection.
client_session_dies(Credentials) ->
    Server = start_server(Credentials, #{}),
    Channel = connect(Credentials, Server),
    try
        ok = trevrpc_channel:subscribe(Channel),
        await_state(Channel, {ready, 1}),
        exit(session(Channel), kill),
        await_state(Channel, connecting),
        await_state(Channel, {ready, 2}),
        ?assertMatch({ok, <<"k">>, _}, trevrpc_client:unary(Channel, ?UNARY, <<"k">>, #{}))
    after
        trevrpc_channel:close(Channel),
        trevrpc_server:stop(Server)
    end.

%% The channel's current QMux session.
session(Channel) ->
    {ok, #{transport := {qmux, Session}, id := Id, ref := Ref}} =
        trevrpc_channel:open_stream(Channel, infinity),
    trevrpc_channel:stream_done(Channel, Id, Ref),
    Session.

%% A server session dying without a closed event ends its connection, which
%% gives its slot back: with one slot, the client reconnects.
server_session_dies(Credentials) ->
    Server = start_server(Credentials, #{max_connections => 1}),
    Channel = connect(Credentials, Server),
    try
        ok = trevrpc_channel:subscribe(Channel),
        await_state(Channel, {ready, 1}),
        %% #state{connections = #{Monitor => Pid}} of the server, and
        %% #state{role, owner} of a session.
        Connections = maps:values(element(5, sys:get_state(Server))),
        [Session] = [
            Pid
         || Pid <- processes(),
            proc_lib:translate_initial_call(Pid) =:= {trevrpc_qmux, init, 1},
            lists:member(element(4, sys:get_state(Pid)), Connections)
        ],
        exit(Session, kill),
        await_state(Channel, connecting),
        await_state(Channel, {ready, 2}),
        ?assertMatch({ok, <<"k">>, _}, trevrpc_client:unary(Channel, ?UNARY, <<"k">>, #{}))
    after
        trevrpc_channel:close(Channel),
        trevrpc_server:stop(Server)
    end.

%% A server whose TCP listener died stops rather than serve QUIC alone.
listener_exit_stops_server(Credentials) ->
    Server = start_server(Credentials, #{}),
    Monitor = monitor(process, Server),
    Listener = element(tuple_size(sys:get_state(Server)), sys:get_state(Server)),
    ?assert(is_pid(Listener)),
    exit(Listener, kill),
    receive
        {'DOWN', Monitor, process, Server, Reason} ->
            ?assertMatch({tcp_listener, killed}, Reason)
    after 10000 -> error(server_alive)
    end.

%% QMux winning a race skips QUIC on reconnects for the reprobe interval,
%% counted from that win: a reconnect within it does not extend it. The QUIC
%% address is a UDP socket that swallows datagrams, so QUIC never connects;
%% the channel's quic_attempt/1 calls show which rounds tried it. The
%% checkpoints count from when the test saw the first win, which is no
%% earlier than the win itself.
reprobe_window(Credentials) ->
    Server = start_server(Credentials, #{}),
    {ok, Sink} = gen_udp:open(0, [{ip, {127, 0, 0, 1}}, {active, false}]),
    {ok, SinkPort} = inet:port(Sink),
    #{tcp_port := TcpPort} = ports(Server),
    Interval = 6000,
    {ok, Channel} = trevrpc_channel:start(#{
        host => {127, 0, 0, 1},
        port => SinkPort,
        tcp_port => TcpPort,
        fallback_delay => 50,
        reprobe_interval => Interval,
        server_name => "localhost",
        cacerts => maps:get(cacerts, Credentials)
    }),
    try
        ok = trevrpc_channel:subscribe(Channel),
        1 = erlang:trace(Channel, true, [call]),
        _ = [
            erlang:trace_pattern({trevrpc_channel, Name, Arity}, true, [local])
         || {Name, Arity} <- [{race, 1}, {quic_attempt, 1}, {won, 4}]
        ],
        await_state(Channel, {ready, 1}),
        Won = erlang:monotonic_time(millisecond),
        %% The first round started before the tracing.
        _ = calls(Channel, won, []),
        %% A sixth into the window: straight to QMux, which must not extend
        %% the window past Won + Interval.
        sleep_until(Won + Interval div 6),
        exit(session(Channel), kill),
        ?assertEqual([race, won], calls(Channel, won, [])),
        await_state(Channel, connecting),
        await_state(Channel, {ready, 2}),
        %% Past the first window, and well before one opened by the second
        %% win would close: QUIC again.
        sleep_until(Won + Interval + 500),
        exit(session(Channel), kill),
        ?assertEqual([race, quic_attempt, won], calls(Channel, won, [])),
        await_state(Channel, connecting),
        await_state(Channel, {ready, 3}),
        ?assertEqual(qmux, trevrpc_channel:transport(Channel))
    after
        _ = erlang:trace_pattern({trevrpc_channel, '_', '_'}, false, [local]),
        trevrpc_channel:close(Channel),
        gen_udp:close(Sink),
        trevrpc_server:stop(Server)
    end.

sleep_until(Time) ->
    timer:sleep(max(0, Time - erlang:monotonic_time(millisecond))).

%% A server that refuses a QMux connection (here, for being full) closes it
%% right after TLS: the attempt fails then, not at its timeout, whether the
%% session's closed event comes before or after the attempt's result.
refused_connection_fails_at_once(Credentials) ->
    Server = start_server(Credentials, #{max_connections => 1}),
    Occupant = connect(Credentials, Server),
    {ok, Channel} = trevrpc_channel:start(
        (ports(Server))#{
            host => {127, 0, 0, 1},
            transport => tcp,
            connect_timeout => 10000,
            server_name => "localhost",
            cacerts => maps:get(cacerts, Credentials)
        }
    ),
    try
        1 = erlang:trace(Channel, true, [call]),
        _ = erlang:trace_pattern({trevrpc_channel, retry, 1}, true, [local]),
        receive
            {trace, Channel, call, {trevrpc_channel, retry, _}} -> ok
        after 5000 -> error(attempt_not_failed)
        end
    after
        _ = erlang:trace_pattern({trevrpc_channel, '_', '_'}, false, [local]),
        trevrpc_channel:close(Channel),
        trevrpc_channel:close(Occupant),
        trevrpc_server:stop(Server)
    end.

%% The same with the events in the order the race above cannot force: the
%% session reports its close before the attempt reports the session. The
%% attempt is held pending by a TCP listener that never answers TLS, and the
%% test sends the closed event of a session the channel has not heard of.
early_close_fails_the_attempt(Credentials) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {ip, {127, 0, 0, 1}}, {active, false}]),
    {ok, TcpPort} = inet:port(Listen),
    {ok, Channel} = trevrpc_channel:start(#{
        host => {127, 0, 0, 1},
        port => TcpPort,
        tcp_port => TcpPort,
        transport => tcp,
        connect_timeout => 20000,
        server_name => "localhost",
        cacerts => maps:get(cacerts, Credentials)
    }),
    try
        {ok, Held} = gen_tcp:accept(Listen, 5000),
        1 = erlang:trace(Channel, true, [call]),
        _ = erlang:trace_pattern({trevrpc_channel, retry, 1}, true, [local]),
        Session = spawn(fun() -> ok end),
        Channel ! {trevrpc_qmux, Session, {closed, connection_lost}},
        receive
            {trace, Channel, call, {trevrpc_channel, retry, _}} -> ok
        after 5000 -> error(attempt_not_failed)
        end,
        gen_tcp:close(Held)
    after
        _ = erlang:trace_pattern({trevrpc_channel, '_', '_'}, false, [local]),
        trevrpc_channel:close(Channel),
        gen_tcp:close(Listen)
    end.

%% QMux joins a round once: when it already failed (TCP refused) and QUIC
%% fails later, the round ends instead of trying QMux again.
one_fallback_per_round(Credentials) ->
    {ok, Sink} = gen_udp:open(0, [{ip, {127, 0, 0, 1}}, {active, false}]),
    {ok, SinkPort} = inet:port(Sink),
    {ok, Closed} = gen_tcp:listen(0, [{ip, {127, 0, 0, 1}}]),
    {ok, ClosedPort} = inet:port(Closed),
    ok = gen_tcp:close(Closed),
    {ok, Channel} = trevrpc_channel:start(#{
        host => {127, 0, 0, 1},
        port => SinkPort,
        tcp_port => ClosedPort,
        fallback_delay => 50,
        connect_timeout => 500,
        server_name => "localhost",
        cacerts => maps:get(cacerts, Credentials)
    }),
    try
        1 = erlang:trace(Channel, true, [call]),
        _ = erlang:trace_pattern({trevrpc_channel, tcp_attempt, 1}, true, [local]),
        _ = erlang:trace_pattern({trevrpc_channel, retry, 1}, true, [local]),
        %% The first round: one QMux attempt before the retry.
        ?assertEqual([tcp_attempt, retry], calls(Channel, retry, []))
    after
        _ = erlang:trace_pattern({trevrpc_channel, '_', '_'}, false, [local]),
        trevrpc_channel:close(Channel),
        gen_udp:close(Sink)
    end.

%% The traced calls of the channel until Last.
calls(Channel, Last, Acc) ->
    receive
        {trace, Channel, call, {trevrpc_channel, Last, _}} ->
            lists:reverse([Last | Acc]);
        {trace, Channel, call, {trevrpc_channel, Name, _}} ->
            calls(Channel, Last, [Name | Acc])
    after 5000 -> error({calls, lists:reverse(Acc)})
    end.
