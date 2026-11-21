%% @private How a channel spreads its calls over its pool of QUIC
%% connections, and when the pool grows.
%%
%% An erlang_quic connection is one process, which every stream operation
%% and the connection's packet processing go through, so calls that share a
%% connection wait on each other: 16 concurrent unary calls ran 1.3 times as
%% fast over four connections as over one, and 64 bidirectional streams of
%% 4 KiB messages 3.5 times as fast. A pool starts with its generation's own
%% connection and the channel's minimum. While the channel starts calls
%% faster than its call rate, or moves bytes faster than its byte rate, each
%% call goes to the connection with the fewest calls in flight, and another
%% connection is dialled, up to the channel's maximum, when every one
%% carries one of those rates and two calls. Below both, every call goes to
%% the generation's own connection, so the ones the pool added fall idle and
%% close. A pool with a minimum above one spreads every call over its
%% connections regardless of the rates.
%%
%% Streaming calls stay open, so the rates miss them once they have started,
%% and a call never moves to another connection. A connection whose
%% streaming calls in flight could each make a round trip faster than the
%% call rate allows, at its round trip time, is therefore crowded too: the
%% pool spreads and grows as it does at the rates, and a streaming call that
%% finds the connection it would take crowded waits for the connection the
%% pool dials, up to the channel's crowded dial wait. On a slow link many
%% streams fit in a round trip without crowding it (64 over 20 ms make at
%% most 3,200 round trips a second), so such a channel keeps one connection,
%% and one congestion controller. The Rust channel's pool works the same
%% way.
%%
%% The counters are shared by the channel and the processes that open their
%% own streams (see trevrpc_channel:open_stream/3): the opens the channel
%% holds for stream credit, the version of its pool, the rates and the
%% window they are measured over, whether the pool is growing, and for each
%% connection of the pool, by number, its calls and streaming calls in
%% flight and its round trip time.
-module(trevrpc_quic_pool).

-export([new/1, reset/2, reset_slot/2]).
-export([queued/1, set_queued/2, version/1, set_version/2]).
-export([load/2, add_load/3, add_streams/3, rtt/2, set_rtt/3, set_byte_rate/2]).
-export([claim_growth/1, end_growth/1]).
-export([limits/4, single/0, max/1, min/1, choose/5, choose/6]).
%% For tests: the decision with given rates, a call counted at a given time,
%% and the crowding rule.
-export([decide/6, record/2, crowded/3]).

-export_type([counters/0, limits/0, choice/0]).

-type counters() :: atomics:atomics_ref().

%% The pool's minimum and maximum, and the call and byte rates, per second
%% and per connection, it spreads and grows at.
-record(limits, {
    min = 1 :: pos_integer(),
    max = 1 :: pos_integer(),
    call_rate = 1 :: pos_integer(),
    byte_rate = 1 :: pos_integer()
}).
-opaque limits() :: #limits{}.

%% Where a call goes, and whether the pool should dial another connection
%% for it, whether the call should wait for that connection because the one
%% it would take is crowded, and whether the call ended the window the rates
%% are measured over, so the byte rate wants measuring.
-type choice() :: {
    trevrpc_transport:t(), Grow :: boolean(), Crowded :: boolean(), Sample :: boolean()
}.

%% The call and byte rates of the last window, and whether the call that
%% asked ended it.
-type rates() :: {non_neg_integer(), non_neg_integer(), boolean()}.

-define(QUEUED, 1).
-define(VERSION, 2).
-define(WINDOW_START, 3).
-define(WINDOW_CALLS, 4).
-define(CALL_RATE, 5).
-define(BYTE_RATE, 6).
-define(GROWING, 7).
-define(SLOTS, 7).
-define(FIELDS, 3).
-define(LOAD, 1).
-define(STREAMS, 2).
-define(RTT, 3).
-define(SLOT(Slot, Field), (?SLOTS + (Slot - 1) * ?FIELDS + Field)).

%% A new call dials another connection only if it finds at least this many
%% calls on every one, so that the calls can use it.
-define(BUSY_CALLS, 2).
%% The shortest window the rates are measured over, in milliseconds.
-define(RATE_WINDOW, 100).
%% A window that lasted this long had a pause in calls, which clears the
%% byte rate.
-define(PAUSE, (2 * ?RATE_WINDOW)).

%% Counters for a pool of up to Max connections.
-spec new(pos_integer()) -> counters().
new(Max) ->
    Counters = atomics:new(?SLOTS + Max * ?FIELDS, [{signed, true}]),
    atomics:put(Counters, ?WINDOW_START, now_ms()),
    Counters.

%% A new generation's pool, of up to Max connections, starts empty and not
%% growing.
-spec reset(counters(), pos_integer()) -> ok.
reset(Counters, Max) ->
    atomics:put(Counters, ?GROWING, 0),
    lists:foreach(fun(Slot) -> reset_slot(Counters, Slot) end, lists:seq(1, Max)).

%% A connection that takes number Slot starts with no calls and no round
%% trip measured yet (-1).
-spec reset_slot(counters(), pos_integer()) -> ok.
reset_slot(Counters, Slot) ->
    atomics:put(Counters, ?SLOT(Slot, ?LOAD), 0),
    atomics:put(Counters, ?SLOT(Slot, ?STREAMS), 0),
    atomics:put(Counters, ?SLOT(Slot, ?RTT), -1).

-spec queued(counters()) -> integer().
queued(Counters) -> atomics:get(Counters, ?QUEUED).

-spec set_queued(counters(), non_neg_integer()) -> ok.
set_queued(Counters, Queued) -> atomics:put(Counters, ?QUEUED, Queued).

-spec version(counters()) -> integer().
version(Counters) -> atomics:get(Counters, ?VERSION).

-spec set_version(counters(), non_neg_integer()) -> ok.
set_version(Counters, Version) -> atomics:put(Counters, ?VERSION, Version).

%% The calls in flight on connection Slot.
-spec load(counters(), pos_integer()) -> integer().
load(Counters, Slot) -> atomics:get(Counters, ?SLOT(Slot, ?LOAD)).

-spec add_load(counters(), pos_integer(), integer()) -> ok.
add_load(Counters, Slot, N) -> atomics:add(Counters, ?SLOT(Slot, ?LOAD), N).

%% Counts N more streaming calls in flight on connection Slot.
-spec add_streams(counters(), pos_integer(), integer()) -> ok.
add_streams(Counters, Slot, N) -> atomics:add(Counters, ?SLOT(Slot, ?STREAMS), N).

%% Connection Slot's smoothed round trip, in microseconds, or -1 before it
%% is measured.
-spec rtt(counters(), pos_integer()) -> integer().
rtt(Counters, Slot) -> atomics:get(Counters, ?SLOT(Slot, ?RTT)).

-spec set_rtt(counters(), pos_integer(), integer()) -> ok.
set_rtt(Counters, Slot, Micros) -> atomics:put(Counters, ?SLOT(Slot, ?RTT), Micros).

%% The bytes per second the pool's connections moved, as last measured.
-spec set_byte_rate(counters(), non_neg_integer()) -> ok.
set_byte_rate(Counters, Rate) -> atomics:put(Counters, ?BYTE_RATE, Rate).

%% Marks the pool growing, and returns whether it was not yet: only the
%% process that marks it asks the channel to dial.
-spec claim_growth(counters()) -> boolean().
claim_growth(Counters) ->
    atomics:compare_exchange(Counters, ?GROWING, 0, 1) =:= ok.

%% The pool may grow again: its dial ended, or the pause after a failed one.
-spec end_growth(counters()) -> ok.
end_growth(Counters) -> atomics:put(Counters, ?GROWING, 0).

-spec limits(pos_integer(), pos_integer(), pos_integer(), pos_integer()) -> limits().
limits(Min, Max, CallRate, ByteRate) ->
    #limits{
        min = max(1, Min),
        max = max(max(1, Min), Max),
        call_rate = max(1, CallRate),
        byte_rate = max(1, ByteRate)
    }.

%% The limits of a pool that keeps one connection, as over QMux.
-spec single() -> limits().
single() -> #limits{}.

-spec max(limits()) -> pos_integer().
max(#limits{max = Max}) -> Max.

-spec min(limits()) -> pos_integer().
min(#limits{min = Min}) -> Min.

%% Picks the connection for a new call, of those numbered in Pool, the
%% generation's own first: the generation's own for a call with a datagram
%% flow (Datagrams is not undefined); else the one with the fewest calls in
%% flight, the lowest numbered of those, while the pool keeps more than one
%% connection, reaches its call or byte rate, or finds its own connection
%% crowded; and the generation's own otherwise. Counts the call toward the
%% call rate.
-spec choose(
    [{pos_integer(), trevrpc_transport:t()}, ...],
    counters(),
    limits(),
    boolean() | undefined,
    boolean()
) -> choice().
choose(Pool, Counters, Limits, Datagrams, Streaming) ->
    choose(Pool, Counters, Limits, Datagrams, Streaming, true).

%% Picks as choose/5 does, counting the call toward the call rate only if
%% Count: a call picked for again, after it was counted, is not counted
%% twice.
-spec choose(
    [{pos_integer(), trevrpc_transport:t()}, ...],
    counters(),
    limits(),
    boolean() | undefined,
    boolean(),
    boolean()
) -> choice().
choose([{1, Own}], _Counters, #limits{max = 1}, _Datagrams, _Streaming, _Count) ->
    {Own, false, false, false};
choose(Pool, Counters, Limits, Datagrams, Streaming, true) ->
    Rates = record(Counters, now_ms()),
    decide(Pool, Counters, Limits, Datagrams, Streaming, Rates);
choose(Pool, Counters, Limits, Datagrams, Streaming, false) ->
    Rates = {atomics:get(Counters, ?CALL_RATE), atomics:get(Counters, ?BYTE_RATE), false},
    decide(Pool, Counters, Limits, Datagrams, Streaming, Rates).

-spec decide(
    [{pos_integer(), trevrpc_transport:t()}, ...],
    counters(),
    limits(),
    boolean() | undefined,
    boolean(),
    rates()
) -> choice().
decide([{1, Own} | _] = Pool, Counters, #limits{} = Limits, Datagrams, Streaming, Rates) ->
    {Calls, Bytes, Sample} = Rates,
    Open = length(Pool),
    Spread =
        Limits#limits.min > 1 orelse
            reaches(Calls, Bytes, 1, Limits) orelse
            crowded_slot(Counters, 1, Limits),
    {Least, Slot, Transport} =
        case Spread andalso Datagrams =:= undefined of
            true -> lists:min([{load(Counters, S), S, T} || {S, T} <- Pool]);
            false -> {load(Counters, 1), 1, Own}
        end,
    Room = Open < Limits#limits.max,
    Crowded = Streaming andalso Spread andalso Room andalso crowded_slot(Counters, Slot, Limits),
    Busy = Room andalso Least >= ?BUSY_CALLS andalso reaches(Calls, Bytes, Open, Limits),
    {Transport, Busy orelse Crowded, Crowded, Sample}.

%% Counts a call started at Now, and returns the rates of the last window
%% and whether this call ended the window. The call rate is measured here;
%% the byte rate by the channel, from its connections, once the window
%% ends. The last byte rate holds until that measure replaces it, which
%% takes the connections' processes a while under load: cleared, calls
%% would go to the generation's own connection meanwhile and crowd it. A
%% window that a pause in calls stretched out does clear it, since the rate
%% before the pause would grow the pool for traffic that may be gone, and
%% yields a low call rate, which the next window corrects.
-spec record(counters(), integer()) -> rates().
record(Counters, Now) ->
    atomics:add(Counters, ?WINDOW_CALLS, 1),
    Start = atomics:get(Counters, ?WINDOW_START),
    Elapsed = Now - Start,
    Ended =
        Elapsed >= ?RATE_WINDOW andalso
            atomics:compare_exchange(Counters, ?WINDOW_START, Start, Now) =:= ok,
    _ =
        case Ended of
            true ->
                Calls = atomics:exchange(Counters, ?WINDOW_CALLS, 0),
                atomics:put(Counters, ?CALL_RATE, Calls * 1000 div Elapsed),
                case Elapsed >= ?PAUSE of
                    true -> atomics:put(Counters, ?BYTE_RATE, 0);
                    false -> ok
                end;
            false ->
                ok
        end,
    {atomics:get(Counters, ?CALL_RATE), atomics:get(Counters, ?BYTE_RATE), Ended}.

%% Whether the rates reach the call or byte rate for each of Connections.
reaches(Calls, Bytes, Connections, #limits{call_rate = CallRate, byte_rate = ByteRate}) ->
    Calls >= CallRate * Connections orelse Bytes >= ByteRate * Connections.

crowded_slot(Counters, Slot, #limits{call_rate = CallRate}) ->
    crowded(
        atomics:get(Counters, ?SLOT(Slot, ?STREAMS)),
        atomics:get(Counters, ?SLOT(Slot, ?RTT)),
        CallRate
    ).

%% Whether Streams streaming calls in flight, each making one round trip of
%% RttMicros at a time, could make round trips faster than CallRate a
%% second, with at least two of them. A connection whose round trip is not
%% measured yet (-1) is not crowded. erlang_quic measures round trips in
%% whole milliseconds, so one under a millisecond reads 0, taken as 1 µs.
-spec crowded(integer(), integer(), pos_integer()) -> boolean().
crowded(Streams, RttMicros, CallRate) ->
    Streams >= ?BUSY_CALLS andalso RttMicros >= 0 andalso
        Streams * 1000000 >= CallRate * max(1, RttMicros).

now_ms() -> erlang:monotonic_time(millisecond).
