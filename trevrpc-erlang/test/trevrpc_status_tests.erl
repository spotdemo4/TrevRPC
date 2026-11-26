%% Statuses: the `google.rpc.RetryInfo' detail, against the encodings the
%% Go, Rust, JavaScript and Python runtimes' tests use.
-module(trevrpc_status_tests).

-include_lib("eunit/include/eunit.hrl").

-define(RETRY_INFO, <<"google.rpc.RetryInfo">>).

retry_info(Hex) ->
    #{
        code => resource_exhausted,
        details => [#{type => ?RETRY_INFO, value => binary:decode_hex(Hex)}]
    }.

golden_encodings_match_every_runtime_test_() ->
    %% RetryInfo{retry_delay: Duration{seconds: 1, nanos: 2}} is the same
    %% bytes in every runtime's tests.
    [
        ?_test(begin
            #{details := [#{type := ?RETRY_INFO, value := Value}]} =
                trevrpc_status:with_retry_delay(#{code => unavailable}, Delay, nanosecond),
            ?assertEqual(Hex, binary:encode_hex(Value, lowercase)),
            ?assertEqual({ok, Delay}, trevrpc_status:retry_delay(retry_info(Hex), nanosecond))
        end)
     || {Delay, Hex} <- [
            {1_000_000_002, <<"0a0408011002">>},
            {30_000_000_000, <<"0a02081e">>},
            {500_000_000, <<"0a061080cab5ee01">>},
            {1_500_000_000, <<"0a0808011080cab5ee01">>},
            {0, <<"0a00">>}
        ]
    ].

milliseconds_are_the_default_unit_test() ->
    Status = trevrpc_status:with_retry_delay(trevrpc_status:new(unavailable, <<"busy">>), 1500),
    ?assertEqual({ok, 1500}, trevrpc_status:retry_delay(Status)),
    ?assertEqual({ok, 1}, trevrpc_status:retry_delay(Status, second)),
    %% Rounded down to a whole unit.
    ?assertEqual({ok, 1}, trevrpc_status:retry_delay(retry_info(<<"0a0408011002">>), second)),
    ?assertEqual({ok, 1000}, trevrpc_status:retry_delay(retry_info(<<"0a0408011002">>))).

a_negative_delay_is_zero_test() ->
    Status = trevrpc_status:with_retry_delay(#{code => unavailable}, -5),
    ?assertMatch(#{details := [#{value := <<16#0a, 0>>}]}, Status),
    ?assertEqual({ok, 0}, trevrpc_status:retry_delay(Status)).

a_delay_too_long_for_a_duration_is_the_longest_one_test() ->
    Longest = 16#7FFFFFFFFFFFFFFF,
    Status = trevrpc_status:with_retry_delay(#{code => unavailable}, Longest + 1, second),
    ?assertEqual(
        {ok, Longest * 1_000_000_000 + 999_999_999},
        trevrpc_status:retry_delay(Status, nanosecond)
    ).

the_delay_replaces_retry_info_and_keeps_other_details_test() ->
    Other = #{type => <<"buf.validate.Violations">>, value => <<1, 2>>},
    Status0 = #{
        code => resource_exhausted,
        message => <<"slow down">>,
        details => [#{type => ?RETRY_INFO, value => <<"0a02081e">>}, Other]
    },
    Status = trevrpc_status:with_retry_delay(Status0, 2000),
    ?assertMatch(
        #{
            code := resource_exhausted,
            message := <<"slow down">>,
            details := [Other, #{type := ?RETRY_INFO}]
        },
        Status
    ),
    ?assertEqual({ok, 2000}, trevrpc_status:retry_delay(Status)).

the_first_well_formed_detail_wins_test() ->
    Status = #{
        code => resource_exhausted,
        details => [
            #{type => <<"other">>, value => binary:decode_hex(<<"0a02081e">>)},
            #{type => ?RETRY_INFO, value => <<16#0a>>},
            #{type => ?RETRY_INFO, value => binary:decode_hex(<<"0a020805">>)},
            #{type => ?RETRY_INFO, value => binary:decode_hex(<<"0a02081e">>)}
        ]
    },
    ?assertEqual({ok, 5000}, trevrpc_status:retry_delay(Status)),
    ?assertEqual(error, trevrpc_status:retry_delay(trevrpc_status:new(not_found, <<"missing">>))),
    ?assertEqual(error, trevrpc_status:retry_delay(trevrpc_status:ok())).

decoding_follows_protobuf_rules_test_() ->
    [
        ?_assertEqual(Expected, trevrpc_status:retry_delay(retry_info(Hex), nanosecond))
     || {Hex, Expected} <- [
            %% Unknown fields are skipped, at either level.
            {<<"1a036162630a04080110022000">>, {ok, 1_000_000_002}},
            {<<"0a061a0008011002">>, {ok, 1_000_000_002}},
            %% The last value wins.
            {<<"0a02080a0a02080b">>, {ok, 11_000_000_000}},
            %% No delay, a malformed one, or an invalid one.
            {<<"">>, error},
            {<<"1a03616263">>, error},
            {<<"0a">>, error},
            {<<"0a040801">>, error},
            {<<"0a0108">>, error},
            {<<"0801">>, error},
            {<<"0a050d00000000">>, error},
            {<<"0a0b08ffffffffffffffffff01">>, error},
            {<<"0a0d080110ffffffffffffffffff01">>, error},
            {<<"0a080801108094ebdc03">>, error}
        ]
    ].
