-module(trevrpc_quic_pool_tests).

-include_lib("eunit/include/eunit.hrl").

%% Calls per second per connection the pool spreads and grows at in these
%% tests, and bytes per second.
-define(CALL_RATE, 1000).
-define(BYTE_RATE, 1000000).

%% Rates under both limits, and the call rate of N connections.
-define(IDLE, {0, 0, false}).
-define(CALLS(N), {N * ?CALL_RATE, 0, false}).

pool(Open) -> [{Slot, {conn, Slot}} || Slot <- lists:seq(1, Open)].

counters(Max) ->
    Counters = trevrpc_quic_pool:new(Max),
    ok = trevrpc_quic_pool:reset(Counters, Max),
    Counters.

limits(Min, Max) -> trevrpc_quic_pool:limits(Min, Max, ?CALL_RATE, ?BYTE_RATE).

loads(Counters, Loads) ->
    lists:foreach(
        fun({Slot, Load}) -> trevrpc_quic_pool:add_load(Counters, Slot, Load) end,
        lists:enumerate(Loads)
    ).

decide(Open, Counters, Limits, Datagrams, Streaming, Rates) ->
    trevrpc_quic_pool:decide(pool(Open), Counters, Limits, Datagrams, Streaming, Rates).

%% Below both rates every call goes to the generation's own connection,
%% however many calls it carries, so the others fall idle.
below_the_rates_calls_stay_on_the_own_connection_test() ->
    Counters = counters(4),
    loads(Counters, [5, 0, 0]),
    ?assertEqual(
        {{conn, 1}, false, false, false}, decide(3, Counters, limits(1, 4), undefined, false, ?IDLE)
    ).

%% At the call rate, or the byte rate, calls go to the connection with the
%% fewest in flight, the lowest numbered of those.
at_a_rate_calls_spread_test() ->
    Counters = counters(4),
    loads(Counters, [2, 1, 1]),
    ?assertMatch(
        {{conn, 2}, _, _, _}, decide(3, Counters, limits(1, 4), undefined, false, ?CALLS(1))
    ),
    ?assertMatch(
        {{conn, 2}, _, _, _},
        decide(3, Counters, limits(1, 4), undefined, false, {0, ?BYTE_RATE, false})
    ).

%% A minimum above one spreads every call, at any rate; a call with a
%% datagram flow stays on the generation's own connection.
a_minimum_spreads_and_datagrams_stay_test() ->
    Counters = counters(3),
    loads(Counters, [1, 0, 0]),
    ?assertMatch(
        {{conn, 2}, false, false, _}, decide(3, Counters, limits(3, 3), undefined, false, ?IDLE)
    ),
    ?assertMatch(
        {{conn, 1}, false, false, _}, decide(3, Counters, limits(3, 3), true, false, ?IDLE)
    ).

%% The pool grows when every connection carries the rate of all of them
%% and two calls, and has room.
busy_connections_grow_the_pool_test() ->
    Counters = counters(3),
    loads(Counters, [2, 2]),
    Limits = limits(1, 3),
    ?assertMatch({_, true, false, _}, decide(2, Counters, Limits, undefined, false, ?CALLS(2))),
    %% The rate of one connection is not that of two.
    ?assertMatch({_, false, false, _}, decide(2, Counters, Limits, undefined, false, ?CALLS(1))),
    %% No room.
    ?assertMatch(
        {_, false, false, _}, decide(2, Counters, limits(1, 2), undefined, false, ?CALLS(2))
    ),
    %% One connection with a call fewer.
    trevrpc_quic_pool:add_load(Counters, 2, -1),
    ?assertMatch(
        {{conn, 2}, false, false, _}, decide(2, Counters, Limits, undefined, false, ?CALLS(2))
    ).

%% Streaming calls crowd a connection whose round trip they could each make
%% faster than the call rate allows: a streaming call that would take it
%% asks for another connection and waits for it. A call that is not
%% streaming spreads, without waiting.
crowded_streams_grow_the_pool_test() ->
    Counters = counters(2),
    loads(Counters, [2]),
    ok = trevrpc_quic_pool:add_streams(Counters, 1, 2),
    ok = trevrpc_quic_pool:set_rtt(Counters, 1, 0),
    Limits = limits(1, 2),
    ?assertEqual(
        {{conn, 1}, true, true, false}, decide(1, Counters, Limits, undefined, true, ?IDLE)
    ),
    ?assertEqual(
        {{conn, 1}, false, false, false}, decide(1, Counters, Limits, undefined, false, ?IDLE)
    ),
    %% Spread to the other connection, which is not crowded.
    ?assertMatch({{conn, 2}, false, false, _}, decide(2, Counters, Limits, undefined, true, ?IDLE)),
    %% No room.
    ?assertMatch(
        {{conn, 1}, false, false, _}, decide(1, Counters, limits(1, 1), undefined, true, ?IDLE)
    ).

%% A connection whose round trip is not measured yet is not crowded.
unmeasured_connections_are_not_crowded_test() ->
    Counters = counters(2),
    ok = trevrpc_quic_pool:add_streams(Counters, 1, 8),
    ?assertMatch(
        {{conn, 1}, false, false, _}, decide(1, Counters, limits(1, 2), undefined, true, ?IDLE)
    ).

crowded_rule_test() ->
    %% One stream never crowds.
    ?assertNot(trevrpc_quic_pool:crowded(1, 0, 5000)),
    %% A round trip under a millisecond, which erlang_quic reads as 0.
    ?assert(trevrpc_quic_pool:crowded(2, 0, 5000)),
    ?assertNot(trevrpc_quic_pool:crowded(2, -1, 5000)),
    %% 64 streams over 20 ms make 3,200 round trips a second.
    ?assertNot(trevrpc_quic_pool:crowded(64, 20000, 5000)),
    ?assert(trevrpc_quic_pool:crowded(100, 20000, 5000)).

%% The call rate is measured over windows of at least 100 ms: the call
%% that ends one turns its calls into the rate, and asks for the byte rate,
%% keeping the last one until it is measured again.
call_rate_windows_test() ->
    Counters = counters(1),
    Start = erlang:monotonic_time(millisecond) + 1000,
    ?assertMatch({_, 0, true}, trevrpc_quic_pool:record(Counters, Start)),
    _ = [
        ?assertMatch({_, 0, false}, trevrpc_quic_pool:record(Counters, Start + 50))
     || _ <- lists:seq(1, 9)
    ],
    ?assertEqual({100, 0, true}, trevrpc_quic_pool:record(Counters, Start + 100)),
    ok = trevrpc_quic_pool:set_byte_rate(Counters, 7),
    ?assertEqual({100, 7, false}, trevrpc_quic_pool:record(Counters, Start + 150)),
    ?assertEqual({20, 7, true}, trevrpc_quic_pool:record(Counters, Start + 200)).

%% A window that a pause in calls stretched to twice its length clears the
%% byte rate, which was measured before the pause.
pauses_clear_the_byte_rate_test() ->
    Counters = counters(1),
    Start = erlang:monotonic_time(millisecond) + 1000,
    ?assertMatch({_, 0, true}, trevrpc_quic_pool:record(Counters, Start)),
    ok = trevrpc_quic_pool:set_byte_rate(Counters, 7),
    ?assertEqual({5, 7, true}, trevrpc_quic_pool:record(Counters, Start + 199)),
    ?assertEqual({5, 0, true}, trevrpc_quic_pool:record(Counters, Start + 399)).

%% A call picked for again, without counting, does not count toward the
%% call rate: of the calls in this window only the one that ends it does.
picking_again_does_not_count_test() ->
    Counters = counters(2),
    Start = erlang:monotonic_time(millisecond) + 1000,
    ?assertMatch({_, _, true}, trevrpc_quic_pool:record(Counters, Start)),
    _ = [
        trevrpc_quic_pool:choose(pool(2), Counters, limits(1, 2), undefined, true, false)
     || _ <- lists:seq(1, 9)
    ],
    ?assertEqual({10, 0, true}, trevrpc_quic_pool:record(Counters, Start + 100)).

%% Only one process at a time marks the pool growing, until its dial ends.
growth_is_claimed_once_test() ->
    Counters = counters(2),
    ?assert(trevrpc_quic_pool:claim_growth(Counters)),
    ?assertNot(trevrpc_quic_pool:claim_growth(Counters)),
    ok = trevrpc_quic_pool:end_growth(Counters),
    ?assert(trevrpc_quic_pool:claim_growth(Counters)).

%% A maximum below the minimum is raised to it.
limits_test() ->
    Limits = trevrpc_quic_pool:limits(3, 2, 1, 1),
    ?assertEqual({3, 3}, {trevrpc_quic_pool:min(Limits), trevrpc_quic_pool:max(Limits)}).
