%% Codecs on routes and calls: handlers and callers move terms, and
%% messages a codec cannot decode or encode end their calls.
-module(trevrpc_codec_tests).

-include_lib("eunit/include/eunit.hrl").

%% The logger handler of log_test/0.
-export([log/2]).

-define(UNARY, <<"/test.Typed/Unary">>).
-define(CLIENT_STREAM, <<"/test.Typed/ClientStream">>).
-define(SERVER_STREAM, <<"/test.Typed/ServerStream">>).
-define(BIDI, <<"/test.Typed/Bidi">>).
%% Routes without a codec, answering with bytes no codec here decodes.
-define(RAW_UNARY, <<"/test.Raw/Unary">>).
-define(RAW_SERVER_STREAM, <<"/test.Raw/ServerStream">>).

%%% The codec

%% Erlang terms; the server's codec raises on `unencodable'.
client_codec() ->
    #{encode => fun term_to_binary/1, decode => fun(Bin) -> binary_to_term(Bin, [safe]) end}.

codec() ->
    #{
        encode => fun
            (unencodable) -> erlang:error(unencodable);
            (Term) -> term_to_binary(Term)
        end,
        decode => fun(Bin) -> binary_to_term(Bin, [safe]) end
    }.

check_test() ->
    ?assertEqual(ok, trevrpc_codec:check(undefined)),
    ?assertEqual(ok, trevrpc_codec:check(codec())),
    ?assertEqual(error, trevrpc_codec:check(#{encode => fun term_to_binary/1})),
    ?assertEqual(error, trevrpc_codec:check(#{encode => fun term_to_binary/1, decode => decode})),
    ?assertEqual(error, trevrpc_codec:check(fun term_to_binary/1)).

without_codec_test() ->
    ?assertEqual({ok, <<"bytes">>}, trevrpc_codec:encode(undefined, <<"bytes">>)),
    ?assertEqual({ok, <<"bytes">>}, trevrpc_codec:decode(undefined, <<"bytes">>)).

with_codec_test() ->
    Codec = codec(),
    {ok, Bytes} = trevrpc_codec:encode(Codec, {hello, 1}),
    ?assertEqual({ok, {hello, 1}}, trevrpc_codec:decode(Codec, iolist_to_binary(Bytes))),
    ?assertEqual(error, trevrpc_codec:decode(Codec, <<"not a term">>)),
    ?assertEqual(error, trevrpc_codec:encode(Codec, unencodable)).

%% A failed encode is an error and a failed decode is not, and neither
%% logs what it failed on.
log_test() ->
    Self = self(),
    Primary = logger:get_primary_config(),
    {ok, #{level := Default}} = logger:get_handler_config(default),
    ok = logger:add_handler(trevrpc_codec_tests, ?MODULE, #{config => Self, level => all}),
    ok = logger:update_handler_config(default, level, none),
    ok = logger:set_primary_config(level, all),
    try
        Leaky = #{
            encode => fun(Message) -> erlang:error({badmatch, Message}) end,
            decode => fun(Bin) -> erlang:error({badmatch, Bin}) end
        },
        ?assertEqual(error, trevrpc_codec:encode(Leaky, <<"secret">>)),
        ?assertEqual(error, trevrpc_codec:decode(Leaky, <<"secret">>)),
        Gpb = #{
            encode => fun(_) ->
                erlang:error({gpb_type_error, {bad_integer, [{value, secret}, {path, "M.f"}]}})
            end,
            decode => fun(Bin) ->
                erlang:error(
                    {gpb_error, {decoding_failure, {Bin, 'M', {error, {badmatch, Bin}, []}}}}
                )
            end
        },
        ?assertEqual(error, trevrpc_codec:encode(Gpb, #{f => secret})),
        ?assertEqual(error, trevrpc_codec:decode(Gpb, <<"secret">>)),
        Logs = [
            receive
                {codec_log, Level, Text} -> {Level, Text}
            after 1000 -> timeout
            end
         || _ <- lists:seq(1, 4)
        ],
        ?assertMatch([{error, _}, {debug, _}, {error, _}, {debug, _}], Logs),
        [?assertEqual(nomatch, string:find(Text, "secret")) || {_, Text} <- Logs],
        ?assertNotEqual(nomatch, string:find(element(2, hd(Logs)), "badmatch")),
        ?assertNotEqual(nomatch, string:find(element(2, lists:nth(3, Logs)), "bad_integer"))
    after
        ok = logger:remove_handler(trevrpc_codec_tests),
        ok = logger:update_handler_config(default, level, Default),
        ok = logger:set_primary_config(Primary)
    end.

log(#{level := Level, msg := {Format, Args}}, #{config := Pid}) when is_list(Format) ->
    Pid ! {codec_log, Level, lists:flatten(io_lib:format(Format, Args))};
log(_Event, _Config) ->
    ok.

%%% Routes

routes() ->
    Typed = #{codec => codec()},
    #{
        ?UNARY => Typed#{kind => unary, handler => fun unary/2},
        ?CLIENT_STREAM => Typed#{kind => client_streaming, handler => fun client_stream/1},
        ?SERVER_STREAM => Typed#{kind => server_streaming, handler => fun server_stream/2},
        ?BIDI => Typed#{kind => bidirectional_streaming, handler => fun bidi/1},
        ?RAW_UNARY => #{kind => unary, handler => fun(_, _) -> {ok, <<"not a term">>} end},
        ?RAW_SERVER_STREAM => #{
            kind => server_streaming,
            handler => fun(_, Call) ->
                ok = trevrpc_call:send(Call, term_to_binary(first)),
                trevrpc_call:send(Call, <<"not a term">>)
            end
        }
    }.

unary({fail, Reason}, _Call) ->
    {error, trevrpc_status:new(failed_precondition, Reason)};
unary(unencodable, _Call) ->
    {ok, unencodable};
unary(Request, _Call) ->
    {ok, {echo, Request}}.

client_stream(Call) ->
    client_stream(Call, []).

client_stream(Call, Requests) ->
    case trevrpc_call:recv(Call) of
        {ok, Request} -> client_stream(Call, [Request | Requests]);
        eof -> {ok, lists:reverse(Requests)};
        {error, Status} -> {error, Status}
    end.

server_stream({count, N}, Call) ->
    lists:foreach(fun(I) -> ok = trevrpc_call:send(Call, {item, I}) end, lists:seq(1, N));
server_stream(unencodable, Call) ->
    trevrpc_call:send(Call, unencodable).

bidi(Call) ->
    case trevrpc_call:recv(Call) of
        {ok, Request} ->
            ok = trevrpc_call:send(Call, {echo, Request}),
            bidi(Call);
        eof ->
            ok;
        {error, Status} ->
            {error, Status}
    end.

%%% Fixture

setup() ->
    {ok, _} = application:ensure_all_started(quic),
    logger:set_primary_config(level, none),
    #{cert := Cert, key := Key, cacerts := CaCerts} = trevrpc_test_tls:credentials(),
    {ok, Server} = trevrpc_server:start(#{
        ip => {127, 0, 0, 1}, cert => Cert, key => Key, routes => routes(), tcp => false
    }),
    {ok, Port} = trevrpc_server:port(Server),
    {ok, Channel} = trevrpc_channel:start(#{
        host => {127, 0, 0, 1},
        port => Port,
        transport => quic,
        server_name => "localhost",
        cacerts => CaCerts
    }),
    ok = trevrpc_channel:wait_until_ready(Channel, 30000),
    #{server => Server, channel => Channel}.

cleanup(#{server := Server, channel := Channel}) ->
    trevrpc_channel:close(Channel),
    trevrpc_server:stop(Server),
    logger:set_primary_config(level, notice).

calls_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Env) ->
        [
            {Name, {timeout, 30, fun() -> Test(Env) end}}
         || {Name, Test} <- [
                {"unary", fun unary_test/1},
                {"client stream", fun client_stream_test/1},
                {"server stream", fun server_stream_test/1},
                {"bidi", fun bidi_test/1},
                {"undecodable request", fun undecodable_request_test/1},
                {"undecodable stream request", fun undecodable_stream_request_test/1},
                {"unencodable response", fun unencodable_response_test/1},
                {"unencodable request", fun unencodable_request_test/1},
                {"unencodable streamed request", fun unencodable_streamed_request_test/1},
                {"undecodable response", fun undecodable_response_test/1},
                {"undecodable streamed response", fun undecodable_streamed_response_test/1},
                {"invalid call codec", fun invalid_call_codec_test/1}
            ]
        ]
    end}.

opts() ->
    #{codec => client_codec(), timeout => 10000}.

unary_test(#{channel := Channel}) ->
    ?assertMatch(
        {ok, {echo, {hello, [1, 2]}}, #{headers := #{}, trailers := #{}}},
        trevrpc_client:unary(Channel, ?UNARY, {hello, [1, 2]}, opts())
    ),
    ?assertMatch(
        {error, #{code := failed_precondition, message := <<"no">>}},
        trevrpc_client:unary(Channel, ?UNARY, {fail, <<"no">>}, opts())
    ).

client_stream_test(#{channel := Channel}) ->
    {ok, Call} = trevrpc_client:client_stream(Channel, ?CLIENT_STREAM, opts()),
    ok = trevrpc_client:send(Call, one),
    ok = trevrpc_client:send(Call, {two, 2}),
    ?assertEqual({ok, [one, {two, 2}]}, trevrpc_client:close_and_recv(Call)).

server_stream_test(#{channel := Channel}) ->
    {ok, Call} = trevrpc_client:server_stream(Channel, ?SERVER_STREAM, {count, 3}, opts()),
    ?assertEqual({ok, {item, 1}}, trevrpc_client:recv(Call)),
    ?assertEqual({ok, {item, 2}}, trevrpc_client:recv(Call)),
    ?assertEqual({ok, {item, 3}}, trevrpc_client:recv(Call)),
    ?assertEqual({eof, #{}}, trevrpc_client:recv(Call)).

bidi_test(#{channel := Channel}) ->
    {ok, Call} = trevrpc_client:bidi(Channel, ?BIDI, opts()),
    ok = trevrpc_client:send(Call, ping),
    ?assertEqual({ok, {echo, ping}}, trevrpc_client:recv(Call)),
    ok = trevrpc_client:send(Call, #{map => 1}),
    ?assertEqual({ok, {echo, #{map => 1}}}, trevrpc_client:recv(Call)),
    ok = trevrpc_client:close_send(Call),
    ?assertEqual({eof, #{}}, trevrpc_client:recv(Call)).

%% Bytes that are no term reach a typed unary route from a caller without
%% a codec.
undecodable_request_test(#{channel := Channel}) ->
    ?assertMatch(
        {error, #{code := invalid_argument, message := <<"failed to decode request">>}},
        trevrpc_client:unary(Channel, ?UNARY, <<"not a term">>, #{timeout => 10000})
    ).

%% The handler gets the status from recv, and ends its call with it.
undecodable_stream_request_test(#{channel := Channel}) ->
    {ok, Call} = trevrpc_client:bidi(Channel, ?BIDI, #{timeout => 10000}),
    ok = trevrpc_client:send(Call, term_to_binary(ping)),
    ?assertEqual({ok, term_to_binary({echo, ping})}, trevrpc_client:recv(Call)),
    _ = trevrpc_client:send(Call, <<"not a term">>),
    ?assertMatch(
        {error, #{code := invalid_argument, message := <<"failed to decode message">>}},
        trevrpc_client:recv(Call)
    ).

%% A response the codec raises on fails the call as a crashing handler
%% does, whether returned or sent (the handler returns the status send
%% gave it).
unencodable_response_test(#{channel := Channel}) ->
    ?assertMatch(
        {error, #{code := internal, message := <<"internal server error">>}},
        trevrpc_client:unary(Channel, ?UNARY, unencodable, opts())
    ),
    {ok, Call} = trevrpc_client:server_stream(Channel, ?SERVER_STREAM, unencodable, opts()),
    ?assertMatch(
        {error, #{code := internal, message := <<"internal server error">>}},
        trevrpc_client:recv(Call)
    ).

%% A request the caller's codec cannot encode never opens a stream.
unencodable_request_test(#{channel := Channel}) ->
    ?assertEqual(
        {error, trevrpc_status:new(internal, <<"failed to encode request">>)},
        trevrpc_client:unary(Channel, ?UNARY, unencodable, #{codec => codec()})
    ),
    ?assertEqual(
        {error, trevrpc_status:new(internal, <<"failed to encode request">>)},
        trevrpc_client:server_stream(Channel, ?SERVER_STREAM, unencodable, #{codec => codec()})
    ).

%% One sent on a stream abandons the call.
unencodable_streamed_request_test(#{channel := Channel}) ->
    {ok, Call} = trevrpc_client:bidi(Channel, ?BIDI, #{codec => codec(), timeout => 10000}),
    ok = trevrpc_client:send(Call, ping),
    ?assertEqual({ok, {echo, ping}}, trevrpc_client:recv(Call)),
    Status = trevrpc_status:new(internal, <<"failed to encode request">>),
    ?assertEqual({error, Status}, trevrpc_client:send(Call, unencodable)),
    ?assertEqual({error, Status}, trevrpc_client:recv(Call)).

undecodable_response_test(#{channel := Channel}) ->
    ?assertMatch(
        {error, #{code := internal, message := <<"failed to decode response">>}},
        trevrpc_client:unary(Channel, ?RAW_UNARY, request, opts())
    ).

%% The call ends with the response it cannot decode, so it is finished.
undecodable_streamed_response_test(#{channel := Channel}) ->
    {ok, Call} = trevrpc_client:server_stream(Channel, ?RAW_SERVER_STREAM, request, opts()),
    ?assertEqual({ok, first}, trevrpc_client:recv(Call)),
    ?assertMatch(
        {error, #{code := internal, message := <<"failed to decode response">>}},
        trevrpc_client:recv(Call)
    ),
    ?assertError(call_finished, trevrpc_client:recv(Call)).

invalid_call_codec_test(#{channel := Channel}) ->
    ?assertMatch(
        {error, #{code := invalid_argument, message := <<"invalid codec">>}},
        trevrpc_client:unary(Channel, ?UNARY, hello, #{codec => #{encode => fun term_to_binary/1}})
    ).

invalid_route_codec_test() ->
    #{cert := Cert, key := Key} = trevrpc_test_tls:credentials(),
    Codec = #{encode => fun term_to_binary/1},
    ?assertEqual(
        {error, {route, ?UNARY, {invalid_codec, Codec}}},
        trevrpc_server:start(#{
            ip => {127, 0, 0, 1},
            cert => Cert,
            key => Key,
            tcp => false,
            routes => #{?UNARY => #{kind => unary, handler => fun unary/2, codec => Codec}}
        })
    ).
