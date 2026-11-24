%% Request validation: `buf.validate.Violations' details, and a server's
%% validator checking requests before its handlers see them.
-module(trevrpc_validation_tests).

-include_lib("eunit/include/eunit.hrl").

-define(UNARY, <<"/test.Validated/Unary">>).
-define(CLIENT_STREAM, <<"/test.Validated/ClientStream">>).
-define(UNCHECKED, <<"/test.Validated/Unchecked">>).
-define(RAW, <<"/test.Validated/Raw">>).
-define(CRASH, <<"/test.Validated/Crash">>).
-define(OK_STATUS, <<"/test.Validated/OkStatus">>).
-define(MALFORMED, <<"/test.Validated/Malformed">>).
-define(VALIDATED, <<"test.ValidatedRequest">>).
-define(PLAIN, <<"test.PlainRequest">>).
-define(CRASHING, <<"test.CrashingRequest">>).
-define(OK_STATUS_TYPE, <<"test.OkStatusRequest">>).
-define(MALFORMED_TYPE, <<"test.MalformedRequest">>).

%%% Violations

%% Every field of a violation, as gpb's maps hold them.
full_violation() ->
    #{
        field => #{
            elements => [
                #{
                    field_number => 3,
                    field_name => <<"items">>,
                    field_type => 'TYPE_MESSAGE',
                    subscript => {index, 2}
                },
                #{
                    field_number => -1,
                    field_name => <<"labels">>,
                    field_type => 'TYPE_MESSAGE',
                    key_type => 'TYPE_STRING',
                    value_type => 99,
                    subscript => {string_key, <<"a \"b\"\n">>}
                },
                #{field_name => <<"by_int">>, subscript => {int_key, -5}},
                #{field_name => <<"by_uint">>, subscript => {uint_key, 16#FFFFFFFFFFFFFFFF}},
                #{field_name => <<"by_bool">>, subscript => {bool_key, false}}
            ]
        },
        rule => #{
            elements => [
                #{field_number => 14, field_name => <<"string">>, field_type => 'TYPE_MESSAGE'},
                #{field_number => 2, field_name => <<"min_len">>, field_type => 'TYPE_UINT64'}
            ]
        },
        rule_id => <<"string.min_len">>,
        message => <<"value length must be at least 1 characters">>,
        for_key => false
    }.

roundtrip_test() ->
    Violations = [full_violation(), #{}, #{rule_id => <<"message.cel">>, for_key => true}],
    Encoded = iolist_to_binary(trevrpc_violations:encode(Violations)),
    ?assertEqual({ok, Violations}, trevrpc_violations:decode(Encoded)).

%% The bytes protobuf-go encodes for
%% `{violations: [{field: {elements: [{field_number: 1, field_name: "message",
%% field_type: TYPE_STRING}]}, rule: {elements: [{field_number: 14,
%% field_name: "string", field_type: TYPE_MESSAGE}, {field_number: 2,
%% field_name: "min_len", field_type: TYPE_UINT64}]}, rule_id:
%% "string.min_len", message: "value length must be at least 1 characters"}]}',
%% fields in number order.
known_bytes() ->
    Field = <<10, 13, 8, 1, 18, 7, "message", 24, 9>>,
    Rule =
        <<10, 12, 8, 14, 18, 6, "string", 24, 11, 10, 13, 8, 2, 18, 7, "min_len", 24, 4>>,
    Message = <<"value length must be at least 1 characters">>,
    Violation = <<
        18,
        14,
        "string.min_len",
        26,
        (byte_size(Message)),
        Message/binary,
        42,
        (byte_size(Field)),
        Field/binary,
        50,
        (byte_size(Rule)),
        Rule/binary
    >>,
    <<10, (byte_size(Violation)), Violation/binary>>.

known_violation() ->
    #{
        field => #{
            elements => [
                #{field_number => 1, field_name => <<"message">>, field_type => 'TYPE_STRING'}
            ]
        },
        rule => #{
            elements => [
                #{field_number => 14, field_name => <<"string">>, field_type => 'TYPE_MESSAGE'},
                #{field_number => 2, field_name => <<"min_len">>, field_type => 'TYPE_UINT64'}
            ]
        },
        rule_id => <<"string.min_len">>,
        message => <<"value length must be at least 1 characters">>
    }.

known_bytes_test() ->
    ?assertEqual(known_bytes(), iolist_to_binary(trevrpc_violations:encode([known_violation()]))),
    ?assertEqual({ok, [known_violation()]}, trevrpc_violations:decode(known_bytes())).

%% The occurrences of an embedded message field merge; the last scalar wins.
merge_test() ->
    First = <<10, 7, 18, 5, "items">>,
    Second = <<10, 6, 18, 4, "name">>,
    Violation = <<
        18,
        1,
        "a",
        42,
        (byte_size(First)),
        First/binary,
        18,
        1,
        "b",
        42,
        (byte_size(Second)),
        Second/binary
    >>,
    ?assertEqual(
        {ok, [
            #{
                rule_id => <<"b">>,
                field => #{elements => [#{field_name => <<"items">>}, #{field_name => <<"name">>}]}
            }
        ]},
        trevrpc_violations:decode(<<10, (byte_size(Violation)), Violation/binary>>)
    ).

unknown_fields_test() ->
    Violation = <<18, 1, "r", 120, 7, 162, 6, 2, "xy">>,
    ?assertEqual(
        {ok, [#{rule_id => <<"r">>}]},
        trevrpc_violations:decode(<<16, 1, 10, (byte_size(Violation)), Violation/binary>>)
    ).

malformed_test() ->
    ?assertEqual(error, trevrpc_violations:decode(<<10, 5, 18>>)),
    ?assertEqual(error, trevrpc_violations:decode(<<10, 3, 42, 1, 10>>)),
    ?assertEqual(error, trevrpc_violations:decode(<<11>>)).

status_test() ->
    Status = trevrpc_violations:status([known_violation()]),
    ?assertEqual(invalid_argument, trevrpc_status:code(Status)),
    ?assertEqual(
        <<"validation error: message: value length must be at least 1 characters">>,
        trevrpc_status:message(Status)
    ),
    ?assertEqual(
        [known_bytes()], trevrpc_status:find_details(Status, <<"buf.validate.Violations">>)
    ),
    ?assertEqual({ok, [known_violation()]}, trevrpc_violations:from_status(Status)).

status_counts_test() ->
    Cel = #{rule_id => <<"message.not_forbidden">>, message => <<"message must not be forbidden">>},
    Status = trevrpc_violations:status([Cel, known_violation(), #{}]),
    ?assertEqual(
        <<"validation error: message must not be forbidden (and 2 more)">>,
        trevrpc_status:message(Status)
    ).

%% At most 100 violations are listed; the message counts them all.
status_limit_test() ->
    Violations = [#{rule_id => integer_to_binary(N)} || N <- lists:seq(1, 150)],
    Status = trevrpc_violations:status(Violations),
    ?assertEqual(<<"validation error: [1] (and 149 more)">>, trevrpc_status:message(Status)),
    ?assertEqual({ok, lists:sublist(Violations, 100)}, trevrpc_violations:from_status(Status)).

status_without_violations_test() ->
    ?assertEqual(
        #{code => invalid_argument, message => <<"validation error">>},
        trevrpc_violations:status([])
    ).

%% A message that is not UTF-8 cannot be a status message.
status_utf8_test() ->
    Status = trevrpc_violations:status([#{message => <<255>>}]),
    ?assertEqual(<<"validation error">>, trevrpc_status:message(Status)),
    ?assertEqual({ok, [#{message => <<255>>}]}, trevrpc_violations:from_status(Status)).

format_test() ->
    Format = fun(Violation) -> iolist_to_binary(trevrpc_violations:format(Violation)) end,
    ?assertEqual(
        <<
            "items[2].labels[\"a \\\"b\\\"\\n\"].by_int[-5].by_uint[18446744073709551615].by_bool[false]:"
            " value length must be at least 1 characters"
        >>,
        Format(full_violation())
    ),
    ?assertEqual(<<"[string.min_len]">>, Format(#{rule_id => <<"string.min_len">>})),
    ?assertEqual(<<"[unknown]">>, Format(#{})),
    ?assertEqual(
        <<"m[\"\\x01\\x7f\\xff\"]: bad">>,
        Format(#{
            message => <<"bad">>,
            field => #{
                elements => [#{field_name => <<"m">>, subscript => {string_key, <<1, 127, 255>>}}]
            }
        })
    ).

from_status_test() ->
    ?assertEqual(error, trevrpc_violations:from_status(trevrpc_status:new(invalid_argument, <<>>))),
    ?assertEqual(
        error,
        trevrpc_violations:from_status(#{
            code => invalid_argument,
            details => [#{type => <<"buf.validate.Violations">>, value => <<11>>}]
        })
    ).

find_details_test() ->
    Status = #{
        code => invalid_argument,
        details => [
            #{type => <<"a">>, value => <<1>>},
            #{type => <<"b">>, value => <<2>>},
            #{type => <<"a">>, value => <<3>>},
            #{type => <<"a">>}
        ]
    },
    ?assertEqual([<<1>>, <<3>>, <<>>], trevrpc_status:find_details(Status, <<"a">>)),
    ?assertEqual([], trevrpc_status:find_details(trevrpc_status:ok(), <<"a">>)).

%%% A server's validator

%% Requests are messages decoded by a codec that tags them, so that the
%% check sees what the codec decoded alongside the bytes; an empty one breaks
%% `string.min_len' on its field `message', and "forbidden" a rule on the
%% message itself.
codec() ->
    #{decode => fun(Bin) -> {request, Bin} end, encode => fun(Bin) -> Bin end}.

validator(?VALIDATED) ->
    {ok, fun validate/2};
validator(?CRASHING) ->
    {ok, fun(_Request, _Encoded) -> erlang:error(boom) end};
validator(?OK_STATUS_TYPE) ->
    {ok, fun(_Request, _Encoded) -> {error, #{code => ok}} end};
validator(?MALFORMED_TYPE) ->
    {ok, fun(_Request, _Encoded) -> foo end};
validator(?PLAIN) ->
    none.

validate({request, Bin}, Bin) when Bin =/= <<>>, Bin =/= <<"forbidden">> ->
    ok;
validate({request, <<>>}, <<>>) ->
    {error, trevrpc_violations:status([known_violation()])};
validate({request, <<"forbidden">>}, <<"forbidden">>) ->
    {error,
        trevrpc_violations:status([
            #{
                rule_id => <<"message.not_forbidden">>,
                message => <<"message must not be \"forbidden\"">>
            }
        ])}.

routes() ->
    Typed = fun(Type, Route) -> Route#{request_type => Type, codec => codec()} end,
    #{
        ?UNARY => Typed(?VALIDATED, #{kind => unary, handler => fun echo/2}),
        ?CLIENT_STREAM => Typed(?VALIDATED, #{kind => client_streaming, handler => fun concat/1}),
        ?UNCHECKED => Typed(?PLAIN, #{kind => unary, handler => fun echo/2}),
        ?CRASH => Typed(?CRASHING, #{kind => unary, handler => fun echo/2}),
        ?OK_STATUS => Typed(?OK_STATUS_TYPE, #{kind => unary, handler => fun echo/2}),
        ?MALFORMED => Typed(?MALFORMED_TYPE, #{kind => unary, handler => fun echo/2}),
        %% No request type: never checked.
        ?RAW => #{kind => unary, handler => fun(Request, _Call) -> {ok, Request} end}
    }.

echo({request, Bin}, _Call) -> {ok, Bin}.

concat(Call) -> concat(Call, []).

concat(Call, Acc) ->
    case trevrpc_call:recv(Call) of
        {ok, {request, Bin}} -> concat(Call, [Bin | Acc]);
        eof -> {ok, iolist_to_binary(lists:reverse(Acc))};
        {error, Status} -> {error, Status}
    end.

server_opts(Credentials) ->
    #{
        ip => {127, 0, 0, 1},
        cert => maps:get(cert, Credentials),
        key => maps:get(key, Credentials),
        tcp => false,
        routes => routes(),
        validator => fun validator/1
    }.

setup() ->
    {ok, _} = application:ensure_all_started(quic),
    logger:set_primary_config(level, none),
    Credentials = trevrpc_test_tls:credentials(),
    {ok, Server} = trevrpc_server:start(server_opts(Credentials)),
    {ok, Port} = trevrpc_server:port(Server),
    {ok, Channel} = trevrpc_channel:start(#{
        host => {127, 0, 0, 1},
        port => Port,
        transport => quic,
        server_name => "localhost",
        cacerts => maps:get(cacerts, Credentials)
    }),
    ok = trevrpc_channel:wait_until_ready(Channel, 30000),
    #{credentials => Credentials, server => Server, channel => Channel}.

cleanup(#{server := Server, channel := Channel}) ->
    trevrpc_channel:close(Channel),
    trevrpc_server:stop(Server),
    logger:set_primary_config(level, notice).

server_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Env) ->
        [
            {Name, {timeout, 30, fun() -> Test(Env) end}}
         || {Name, Test} <- [
                {"valid request", fun valid_test/1},
                {"field rule", fun field_rule_test/1},
                {"message rule", fun message_rule_test/1},
                {"streamed request", fun streamed_test/1},
                {"type without rules", fun unchecked_test/1},
                {"route without a request type", fun raw_test/1},
                {"check that crashes", fun crash_test/1},
                {"check that answers an OK status", fun ok_status_test/1},
                {"check that answers something else", fun malformed_test/1}
            ]
        ]
    end}.

unary(Channel, Method, Request) ->
    case trevrpc_client:unary(Channel, Method, Request, #{timeout => 5000}) of
        {ok, Response, _} -> {ok, Response};
        {error, Status} -> {error, Status}
    end.

valid_test(#{channel := Channel}) ->
    ?assertEqual({ok, <<"hello">>}, unary(Channel, ?UNARY, <<"hello">>)).

field_rule_test(#{channel := Channel}) ->
    {error, Status} = unary(Channel, ?UNARY, <<>>),
    ?assertEqual(invalid_argument, trevrpc_status:code(Status)),
    ?assertEqual(
        <<"validation error: message: value length must be at least 1 characters">>,
        trevrpc_status:message(Status)
    ),
    ?assertEqual({ok, [known_violation()]}, trevrpc_violations:from_status(Status)).

message_rule_test(#{channel := Channel}) ->
    {error, Status} = unary(Channel, ?UNARY, <<"forbidden">>),
    ?assertEqual(invalid_argument, trevrpc_status:code(Status)),
    ?assertMatch(
        {ok, [#{rule_id := <<"message.not_forbidden">>} = Violation]} when
            not is_map_key(field, Violation),
        trevrpc_violations:from_status(Status)
    ).

%% The recv that reads the invalid message returns its status, which the
%% handler fails the call with.
streamed_test(#{channel := Channel}) ->
    {ok, Call} = trevrpc_client:client_stream(Channel, ?CLIENT_STREAM, #{timeout => 5000}),
    ok = trevrpc_client:send(Call, <<"a">>),
    _ = trevrpc_client:send(Call, <<>>),
    {error, Status} = trevrpc_client:close_and_recv(Call),
    ?assertEqual(invalid_argument, trevrpc_status:code(Status)),
    ?assertEqual({ok, [known_violation()]}, trevrpc_violations:from_status(Status)),
    {ok, Valid} = trevrpc_client:client_stream(Channel, ?CLIENT_STREAM, #{timeout => 5000}),
    [ok = trevrpc_client:send(Valid, M) || M <- [<<"a">>, <<"b">>]],
    ?assertEqual({ok, <<"ab">>}, trevrpc_client:close_and_recv(Valid)).

unchecked_test(#{channel := Channel}) ->
    ?assertEqual({ok, <<>>}, unary(Channel, ?UNCHECKED, <<>>)).

raw_test(#{channel := Channel}) ->
    ?assertEqual({ok, <<>>}, unary(Channel, ?RAW, <<>>)).

%% A check that crashes fails the call as a crashing handler does.
crash_test(#{channel := Channel}) ->
    ?assertEqual(
        {error, #{code => internal, message => <<"internal server error">>}},
        unary(Channel, ?CRASH, <<"hello">>)
    ).

%% A check that answers neither `ok' nor an error status fails the call as
%% one that crashes does.
ok_status_test(#{channel := Channel}) ->
    ?assertEqual(
        {error, #{code => internal, message => <<"internal server error">>}},
        unary(Channel, ?OK_STATUS, <<"hello">>)
    ).

malformed_test(#{channel := Channel}) ->
    ?assertEqual(
        {error, #{code => internal, message => <<"internal server error">>}},
        unary(Channel, ?MALFORMED, <<"hello">>)
    ).

%% The validator is asked once per request type, however many routes share
%% it, and its answer serves each of them.
bind_once_test() ->
    Self = self(),
    Validator = fun(Type) ->
        Self ! {bound, Type},
        validator(Type)
    end,
    {ok, Validates} = trevrpc_validator:bind(Validator, routes()),
    Bound = collect_bound([]),
    ?assertEqual(
        lists:sort([?VALIDATED, ?PLAIN, ?CRASHING, ?OK_STATUS_TYPE, ?MALFORMED_TYPE]), Bound
    ),
    ?assertEqual(
        lists:sort([?UNARY, ?CLIENT_STREAM, ?CRASH, ?OK_STATUS, ?MALFORMED]),
        lists:sort(maps:keys(Validates))
    ),
    ?assertEqual(maps:get(?UNARY, Validates), maps:get(?CLIENT_STREAM, Validates)).

collect_bound(Acc) ->
    receive
        {bound, Type} -> collect_bound([Type | Acc])
    after 0 -> lists:sort(Acc)
    end.

%% Rules that cannot be enforced fail the start.
start_test_() ->
    {setup,
        fun() ->
            {ok, _} = application:ensure_all_started(quic),
            trevrpc_test_tls:credentials()
        end,
        fun(Credentials) ->
            Opts = server_opts(Credentials),
            Start = fun(Validator) -> trevrpc_server:start(Opts#{validator => Validator}) end,
            [
                ?_assertEqual({error, {invalid_option, validator}}, Start(fun(_, _) -> none end)),
                ?_assertMatch(
                    {error, {validator, _, {error, nope}}},
                    Start(fun(_) -> erlang:error(nope) end)
                ),
                ?_assertMatch(
                    {error, {validator, _, {bad_return, {ok, nope}}}},
                    Start(fun(_) -> {ok, nope} end)
                )
            ]
        end}.
