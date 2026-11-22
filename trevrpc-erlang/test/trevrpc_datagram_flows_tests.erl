%% The datagram flows of a connection, with the test process as every
%% call's reader and the clock passed in.
-module(trevrpc_datagram_flows_tests).

-include_lib("eunit/include/eunit.hrl").

-define(F, trevrpc_datagram_flows).

datagram(Flow, Messages) ->
    iolist_to_binary(trevrpc_datagram:encode_messages(Flow, Messages)).

%% A datagram of `Flow' whose frame runs past its end.
malformed(Flow) ->
    iolist_to_binary([qmux_varint:encode(Flow), 0, 5, "ab"]).

reg(Flow, Flows) ->
    reg(Flow, true, Flows).

reg(Flow, Receives, Flows) ->
    Ref = make_ref(),
    Credit = ?F:credit(),
    {ok, Registered} = ?F:register(Flow, self(), Ref, Credit, Receives, Flows),
    {Ref, Credit, Registered}.

%% The datagrams the test process got for `Ref', as lists of messages.
received(Ref) ->
    receive
        {trevrpc_stream, Ref, {datagram, Messages, _Bytes}} ->
            [[Payload || {message, Payload} <- Messages] | received(Ref)]
    after 0 -> []
    end.

%% Whether the reader of `Ref' heard that the datagrams it waits for came.
arrived(Ref) ->
    receive
        {trevrpc_stream, Ref, datagrams_arrived} -> true
    after 0 -> false
    end.

flush() ->
    receive
        _ -> flush()
    after 0 -> ok
    end.

client_allocates_from_one_test() ->
    Flows0 = ?F:new(client, 0),
    {1, _, Flows1} = ?F:allocate(self(), make_ref(), true, Flows0),
    {2, _, Flows2} = ?F:allocate(self(), make_ref(), true, Flows1),
    ?assert(?F:in_use(1, Flows2)),
    ?assertNot(?F:in_use(3, Flows2)).

registered_flow_gets_its_datagrams_test() ->
    {Ref, _Credit, Flows} = reg(5, ?F:new(server, 8)),
    Next = ?F:dispatch(datagram(5, [<<"a">>, <<"b">>]), 0, Flows),
    ?assertEqual([[<<"a">>, <<"b">>]], received(Ref)),
    %% Datagrams of no flow, or of flow 0, are dropped.
    _ = ?F:dispatch(<<>>, 0, ?F:dispatch(datagram(0, [<<"c">>]), 0, Next)),
    ?assertEqual([], received(Ref)).

flow_in_use_test() ->
    {_Ref, _Credit, Flows} = reg(5, ?F:new(server, 8)),
    ?assertEqual(in_use, ?F:register(5, self(), make_ref(), ?F:credit(), true, Flows)).

held_until_registered_test() ->
    Flows = ?F:dispatch(datagram(7, [<<"early">>]), 0, ?F:new(server, 8)),
    {Ref, _Credit, _} = reg(7, Flows),
    ?assertEqual([[<<"early">>]], received(Ref)),
    flush().

held_datagrams_expire_test() ->
    Held = ?F:dispatch(datagram(7, [<<"early">>]), 0, ?F:new(server, 8)),
    {Ref, _Credit, _} = reg(7, ?F:expire(500, Held)),
    ?assertEqual([], received(Ref)),
    %% One that has not waited long enough stays.
    {Kept, _Credit2, _} = reg(7, ?F:expire(499, Held)),
    ?assertEqual([[<<"early">>]], received(Kept)),
    flush().

%% At most 64 datagrams are held; one past it is dropped, but counted on a
%% datagram of its flow that is held.
hold_limit_test() ->
    Full = lists:foldl(
        fun(N, Acc) -> ?F:dispatch(datagram(N, [<<"x">>]), 0, Acc) end,
        ?F:new(server, 8),
        lists:seq(1, 64)
    ),
    Over = ?F:dispatch(datagram(1, [<<"y">>]), 0, ?F:dispatch(datagram(65, [<<"z">>]), 0, Full)),
    {Ref1, _, Flows1} = reg(1, Over),
    ?assertEqual([[<<"x">>]], received(Ref1)),
    Await1 = ?F:await(1, self(), Ref1, 2, Flows1),
    ?assert(arrived(Ref1)),
    {Ref65, _, _} = reg(65, Await1),
    ?assertEqual([], received(Ref65)),
    flush().

hold_bytes_test() ->
    Big = binary:copy(<<"b">>, 40000),
    Flows = ?F:dispatch(
        datagram(2, [Big]), 0, ?F:dispatch(datagram(1, [Big]), 0, ?F:new(server, 8))
    ),
    {Ref1, _, Registered} = reg(1, Flows),
    ?assertEqual([[Big]], received(Ref1)),
    {Ref2, _, _} = reg(2, Registered),
    ?assertEqual([], received(Ref2)),
    flush().

%% A server drops the late datagrams of a flow that ended, instead of
%% holding them.
ended_flows_test() ->
    {_Ref, _Credit, Flows} = reg(3, ?F:new(server, 1)),
    Ended = ?F:release(3, Flows),
    Late = ?F:dispatch(datagram(3, [<<"late">>]), 0, Ended),
    {Again, _, Registered} = reg(3, Late),
    ?assertEqual([], received(Again)),
    %% It remembers one ended flow here: a second pushes the first out.
    Evicted = ?F:release(4, ?F:release(3, Registered)),
    {Held, _, _} = reg(3, ?F:dispatch(datagram(3, [<<"held">>]), 0, Evicted)),
    ?assertEqual([[<<"held">>]], received(Held)),
    flush().

%% A malformed datagram delivers nothing, but counts.
malformed_counts_test() ->
    {Ref, _Credit, Flows} = reg(5, ?F:new(server, 8)),
    Next = ?F:dispatch(malformed(5), 0, Flows),
    ?assertEqual([], received(Ref)),
    _ = ?F:await(5, self(), Ref, 1, Next),
    ?assert(arrived(Ref)),
    %% Held too, as a count.
    {Held, _, Claimed} = reg(6, ?F:dispatch(malformed(6), 0, ?F:new(server, 8))),
    _ = ?F:await(6, self(), Held, 1, Claimed),
    ?assert(arrived(Held)).

await_test() ->
    {Ref, _Credit, Flows} = reg(5, ?F:new(server, 8)),
    One = ?F:dispatch(datagram(5, [<<"a">>]), 0, Flows),
    Waiting = ?F:await(5, self(), Ref, 2, One),
    ?assertNot(arrived(Ref)),
    _ = ?F:dispatch(datagram(5, [<<"b">>]), 0, Waiting),
    ?assert(arrived(Ref)),
    %% A flow that is gone has nothing more coming.
    Gone = make_ref(),
    _ = ?F:await(9, self(), Gone, 1, Flows),
    ?assert(arrived(Gone)),
    flush().

%% The credit bounds what a reader that falls behind gets: 128 messages.
credit_test() ->
    {Ref, Credit, Flows} = reg(5, ?F:new(server, 8)),
    Sent = lists:foldl(
        fun(_, Acc) -> ?F:dispatch(datagram(5, [<<"m">>]), 0, Acc) end, Flows, lists:seq(1, 130)
    ),
    ?assertEqual(128, length(received(Ref))),
    %% Taking messages makes room.
    ok = ?F:take(Credit, 1, 1),
    Room = ?F:dispatch(datagram(5, [<<"m">>]), 0, Sent),
    ?assertEqual(1, length(received(Ref))),
    %% A closed credit gets nothing more.
    ok = ?F:take(Credit, 128, 128),
    ok = ?F:close(Credit),
    _ = ?F:dispatch(datagram(5, [<<"m">>]), 0, Room),
    ?assertEqual([], received(Ref)).

%% A call that reads no stream in the direction only counts its datagrams.
not_receiving_test() ->
    {Ref, _Credit, Flows} = reg(5, false, ?F:new(server, 8)),
    Next = ?F:dispatch(datagram(5, [<<"a">>]), 0, Flows),
    ?assertEqual([], received(Ref)),
    _ = ?F:await(5, self(), Ref, 1, Next),
    ?assert(arrived(Ref)).

%% A client holds nothing: its calls register before the server sends.
client_holds_nothing_test() ->
    Flows = ?F:dispatch(datagram(1, [<<"a">>]), 0, ?F:new(client, 0)),
    {Ref, _Credit, _} = reg(1, Flows),
    ?assertEqual([], received(Ref)).
