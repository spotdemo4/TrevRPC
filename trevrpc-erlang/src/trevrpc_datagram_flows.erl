%% @private The datagram flows of one connection, kept by the process that
%% receives its events: `trevrpc_server_conn' on a server, `trevrpc_channel'
%% on a client.
%%
%% Each call with a flow registers it here. A datagram of a registered flow
%% goes to the process reading the call as a
%% `{trevrpc_stream, Ref, {datagram, Messages, Bytes}}' message, which
%% {@link trevrpc_stream} reads. Datagrams bypass flow control, and a
%% process's mailbox has no bound, so each flow has a credit, a `counters'
%% array the owner and the reader share: the owner adds the messages and
%% bytes it sends, the reader takes them off as it reads them, and a
%% datagram that would take the flow past 128 messages or 64 KiB is dropped.
%% The reader also closes the credit once its direction ended, after which
%% nothing more is sent. Receiving never blocks the owner.
%%
%% The owner alone counts the datagrams each flow received, those it
%% dropped too: the reader of a direction whose DATAGRAM_COUNT announced
%% more than arrived asks with {@link await/5} to hear once they have.
%%
%% A server holds the datagrams of flows it has not seen, as they usually
%% overtake their REQUEST: at most 64 datagrams and 64 KiB on the
%% connection, each for at most 500 ms. It remembers the flows that ended
%% recently, twice the per-connection stream limit of them, and drops their
%% late datagrams at once. A client allocates its flows, from 1 up, and
%% holds nothing: its calls register before the server can send.
-module(trevrpc_datagram_flows).

-export([new/2, credit/0, allocate/4, in_use/2, register/6, release/2]).
-export([dispatch/3, await/5, expire/2]).
-export([take/3, close/1]).

-export_type([flows/0, credit/0]).

-define(SINK_MESSAGES, 128).
-define(SINK_BYTES, 65536).
-define(HOLD_DATAGRAMS, 64).
-define(HOLD_BYTES, 65536).
-define(HOLD_TIME, 500).

%% The credit slots.
-define(QUEUED, 1).
-define(QUEUED_BYTES, 2).
-define(CLOSED, 3).

-opaque credit() :: counters:counters_ref().

-record(flow, {
    pid :: pid(),
    ref :: reference(),
    credit :: credit(),
    %% Whether the call reads a stream of messages in the direction it
    %% receives; without one, its datagrams are only counted.
    receives :: boolean(),
    %% The datagrams of the flow that arrived, delivered or not.
    arrived = 0 :: non_neg_integer(),
    %% How many datagrams its reader waits for, if it waits.
    awaiting :: pos_integer() | undefined
}).

-record(held, {
    flow :: pos_integer(),
    messages :: [trevrpc_datagram:message()] | malformed,
    size :: non_neg_integer(),
    expiry :: integer(),
    %% The flow's datagrams that found no room while this one was held:
    %% dropped, but counted when the flow claims this one.
    unheld = 0 :: non_neg_integer()
}).

-record(flows, {
    role :: server | client,
    flows = #{} :: #{pos_integer() => #flow{}},
    %% Recently ended flows (a server's), oldest first, and as a set.
    ended_capacity :: non_neg_integer(),
    ended = queue:new() :: queue:queue(pos_integer()),
    ended_set = #{} :: #{pos_integer() => true},
    held = queue:new() :: queue:queue(#held{}),
    held_count = 0 :: non_neg_integer(),
    held_bytes = 0 :: non_neg_integer(),
    %% The timer that expires held datagrams, while any are held.
    hold_timer :: reference() | undefined,
    %% The last flow a client allocated.
    next = 0 :: non_neg_integer()
}).

-opaque flows() :: #flows{}.

%% @doc The flows of a connection of `Role'; a server remembers
%% `EndedCapacity' ended flows.
-spec new(server | client, non_neg_integer()) -> flows().
new(Role, EndedCapacity) ->
    #flows{role = Role, ended_capacity = EndedCapacity}.

%% @doc A new flow's credit.
-spec credit() -> credit().
credit() ->
    counters:new(3, []).

%% @doc Allocates a client's next flow for the call read by `Pid' with
%% `Ref'; `Receives' says whether it reads a stream of responses.
-spec allocate(pid(), reference(), boolean(), flows()) -> {pos_integer(), credit(), flows()}.
allocate(Pid, Ref, Receives, #flows{role = client, next = Next} = Flows) ->
    Flow = Next + 1,
    Credit = credit(),
    {ok, Registered} = register(Flow, Pid, Ref, Credit, Receives, Flows#flows{next = Flow}),
    {Flow, Credit, Registered}.

%% @doc Whether `Flow' belongs to a call of the connection.
-spec in_use(pos_integer(), flows()) -> boolean().
in_use(Flow, #flows{flows = Live}) ->
    is_map_key(Flow, Live).

%% @doc Makes `Flow' the flow of the call read by `Pid' with `Ref', with
%% credit `Credit'. The datagrams held for it go to the call.
-spec register(pos_integer(), pid(), reference(), credit(), boolean(), flows()) ->
    {ok, flows()} | in_use.
register(Flow, Pid, Ref, Credit, Receives, #flows{flows = Live} = Flows) ->
    case is_map_key(Flow, Live) of
        true ->
            in_use;
        false ->
            Record = #flow{pid = Pid, ref = Ref, credit = Credit, receives = Receives},
            {ok, claim(Record, Flow, Flows)}
    end.

claim(Record, Flow, #flows{held_count = 0, flows = Live} = Flows) ->
    Flows#flows{flows = Live#{Flow => Record}};
claim(Record, Flow, #flows{held = Held} = Flows) ->
    {Mine, Others} = lists:partition(fun(#held{flow = F}) -> F =:= Flow end, queue:to_list(Held)),
    Claimed = lists:foldl(
        fun(#held{messages = Messages, unheld = Unheld}, Acc) ->
            Counted = arrive(Unheld, Acc),
            case Messages of
                malformed -> arrive(1, Counted);
                _ -> arrive(1, deliver(Messages, Counted))
            end
        end,
        Record,
        Mine
    ),
    Flows#flows{
        flows = (Flows#flows.flows)#{Flow => Claimed},
        held = queue:from_list(Others),
        held_count = length(Others),
        held_bytes = lists:sum([Size || #held{size = Size} <- Others])
    }.

%% @doc Forgets `Flow', whose call ended. A server drops the flow's late
%% datagrams from now on.
-spec release(pos_integer(), flows()) -> flows().
release(Flow, #flows{flows = Live} = Flows) ->
    Released = Flows#flows{flows = maps:remove(Flow, Live)},
    case Flows#flows.role of
        server -> remember(Flow, Released);
        client -> Released
    end.

remember(_Flow, #flows{ended_capacity = 0} = Flows) ->
    Flows;
remember(Flow, #flows{ended_set = Set} = Flows) when is_map_key(Flow, Set) ->
    Flows;
remember(Flow, #flows{ended = Ended, ended_set = Set, ended_capacity = Capacity} = Flows) ->
    case map_size(Set) < Capacity of
        true ->
            Flows#flows{ended = queue:in(Flow, Ended), ended_set = Set#{Flow => true}};
        false ->
            {{value, Oldest}, Rest} = queue:out(Ended),
            Flows#flows{
                ended = queue:in(Flow, Rest),
                ended_set = maps:remove(Oldest, Set#{Flow => true})
            }
    end.

%% @doc Takes a datagram the connection received at `Now', in
%% `erlang:monotonic_time(millisecond)' units: hands it to its flow's call,
%% holds it for a flow a server has not seen yet, or drops it. Never
%% blocks.
-spec dispatch(binary(), integer(), flows()) -> flows().
dispatch(Datagram, Now, Flows) ->
    case trevrpc_datagram:parse(Datagram) of
        {ok, Flow, Messages} -> dispatch(Flow, Messages, byte_size(Datagram), Now, Flows);
        {malformed, Flow} -> dispatch(Flow, malformed, 0, Now, Flows);
        error -> Flows
    end.

dispatch(Flow, Messages, Size, Now, #flows{flows = Live} = Flows) ->
    case Live of
        #{Flow := Record} ->
            Delivered =
                case Messages of
                    malformed -> Record;
                    _ -> deliver(Messages, Record)
                end,
            Flows#flows{flows = Live#{Flow := arrive(1, Delivered)}};
        #{} when Flows#flows.role =:= client ->
            Flows;
        #{} ->
            case is_map_key(Flow, Flows#flows.ended_set) of
                true -> Flows;
                false -> hold(Flow, Messages, Size, Now, Flows)
            end
    end.

%% Sends a datagram's messages to the flow's call, if it reads them and its
%% credit has room.
deliver(_Messages, #flow{receives = false} = Record) ->
    Record;
deliver([], Record) ->
    Record;
deliver(Messages, #flow{pid = Pid, ref = Ref, credit = Credit} = Record) ->
    Count = length(Messages),
    Bytes = lists:sum([byte_size(Payload) || {_, Payload} <- Messages]),
    Queued = counters:get(Credit, ?QUEUED),
    case
        counters:get(Credit, ?CLOSED) =:= 0 andalso Queued + Count =< ?SINK_MESSAGES andalso
            (Queued =:= 0 orelse counters:get(Credit, ?QUEUED_BYTES) + Bytes =< ?SINK_BYTES)
    of
        true ->
            counters:add(Credit, ?QUEUED, Count),
            counters:add(Credit, ?QUEUED_BYTES, Bytes),
            Pid ! {trevrpc_stream, Ref, {datagram, Messages, Bytes}},
            Record;
        false ->
            Record
    end.

%% Counts `N' datagrams of the flow, and tells a reader waiting for them
%% once enough arrived.
arrive(0, Record) ->
    Record;
arrive(N, #flow{arrived = Arrived, awaiting = Awaiting} = Record) ->
    Counted = Record#flow{arrived = Arrived + N},
    case Awaiting of
        Count when is_integer(Count), Arrived + N >= Count -> arrived(Counted);
        _ -> Counted
    end.

arrived(#flow{pid = Pid, ref = Ref} = Record) ->
    Pid ! {trevrpc_stream, Ref, datagrams_arrived},
    Record#flow{awaiting = undefined}.

hold(Flow, _Messages, Size, _Now, #flows{held_count = Count, held_bytes = Bytes} = Flows) when
    Count >= ?HOLD_DATAGRAMS; Bytes + Size > ?HOLD_BYTES
->
    %% No room: the datagram is dropped, and counted on one of its flow that
    %% is held, if there is one.
    Flows#flows{held = count_unheld(Flow, Flows#flows.held)};
hold(
    Flow, Messages, Size, Now, #flows{held = Held, held_count = Count, held_bytes = Bytes} = Flows
) ->
    Entry = #held{flow = Flow, messages = Messages, size = Size, expiry = Now + ?HOLD_TIME},
    arm(Flows#flows{held = queue:in(Entry, Held), held_count = Count + 1, held_bytes = Bytes + Size}).

count_unheld(Flow, Held) ->
    List = queue:to_list(Held),
    case lists:keyfind(Flow, #held.flow, List) of
        false ->
            Held;
        #held{unheld = Unheld} = Entry ->
            queue:from_list(
                lists:keyreplace(Flow, #held.flow, List, Entry#held{unheld = Unheld + 1})
            )
    end.

%% Arms the timer that expires the oldest held datagram, unless it runs.
arm(#flows{hold_timer = undefined, held = Held} = Flows) ->
    case queue:peek(Held) of
        {value, #held{expiry = Expiry}} ->
            Delay = max(0, Expiry - erlang:monotonic_time(millisecond)),
            Flows#flows{hold_timer = erlang:start_timer(Delay, self(), trevrpc_datagram_hold)};
        empty ->
            Flows
    end;
arm(Flows) ->
    Flows.

%% @doc Drops the held datagrams that waited long enough by `Now', when the
%% owner gets `{timeout, Timer, trevrpc_datagram_hold}'.
-spec expire(integer(), flows()) -> flows().
expire(Now, #flows{held = Held} = Flows) ->
    arm(expire(Now, Held, Flows#flows{hold_timer = undefined})).

expire(Now, Held, Flows) ->
    case queue:peek(Held) of
        {value, #held{expiry = Expiry, size = Size}} when Expiry =< Now ->
            expire(Now, queue:drop(Held), Flows#flows{
                held_count = Flows#flows.held_count - 1,
                held_bytes = Flows#flows.held_bytes - Size
            });
        _ ->
            Flows#flows{held = Held}
    end.

%% @doc The reader of `Flow' (`Pid', reading with `Ref') waits for `Count'
%% of its datagrams: it hears `{trevrpc_stream, Ref, datagrams_arrived}'
%% once they have arrived, at once if they have.
-spec await(pos_integer(), pid(), reference(), pos_integer(), flows()) -> flows().
await(Flow, Pid, Ref, Count, #flows{flows = Live} = Flows) ->
    case Live of
        #{Flow := #flow{ref = Ref, arrived = Arrived} = Record} when Arrived >= Count ->
            Flows#flows{flows = Live#{Flow := arrived(Record)}};
        #{Flow := #flow{ref = Ref} = Record} ->
            Flows#flows{flows = Live#{Flow := Record#flow{awaiting = Count}}};
        #{} ->
            %% The flow is gone: nothing more will arrive.
            Pid ! {trevrpc_stream, Ref, datagrams_arrived},
            Flows
    end.

%%% The reader's side of the credit

%% @doc The reader took `Messages' messages of `Bytes' bytes.
-spec take(credit(), non_neg_integer(), non_neg_integer()) -> ok.
take(Credit, Messages, Bytes) ->
    counters:sub(Credit, ?QUEUED, Messages),
    counters:sub(Credit, ?QUEUED_BYTES, Bytes).

%% @doc The reader's direction ended: nothing more is sent to it.
-spec close(credit()) -> ok.
close(Credit) ->
    counters:put(Credit, ?CLOSED, 1).
