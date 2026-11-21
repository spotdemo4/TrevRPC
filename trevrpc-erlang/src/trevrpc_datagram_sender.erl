%% @private The sender of one direction of a call's datagram messages.
%%
%% The calling process hands each MESSAGE or ZSTD_MESSAGE frame to a
%% process of its own and goes on: that process packs the frames that are
%% queued when it gets to them into as few datagrams as fit, in order,
%% without waiting for more, as the protocol asks. erlang_quic puts each
%% datagram in a packet of its own, through the one process of the
%% connection, so a datagram per message would cost a packet per message;
%% while one datagram is being sent the next messages queue, and a sender
%% that keeps up still sends each message at once.
%%
%% The two processes share an `atomics' array: the frames handed over and
%% not sent yet, and their bytes, which bound the queue (a caller past 128
%% frames or 64 KiB waits for it to drain, within its deadline, and drops
%% the frame if the wait runs out); the datagrams the transport accepted,
%% which DATAGRAM_COUNT announces; and the largest datagram the connection
%% takes.
%%
%% A frame belongs on the stream instead when it does not fit a datagram on
%% its own, which the caller sees from the shared size and writes itself
%% (asking the connection again, at most every 100 ms, before it gives up
%% on a frame for its size), and when its datagram's send fails. erlang_quic
%% refuses at once a datagram that finds the congestion window full or
%% pacing ahead, and tells nobody when there is room again, so the process
%% writes the frames of a datagram that failed on the stream itself, at
%% once, with {@link trevrpc_stream:send/4}, which waits for a full send
%% queue as the call's writes do: they arrive late under congestion, but
%% they arrive. The process and the call each write whole frames, so their
%% writes interleave frame by frame, in whichever order the connection
%% takes them, even while both wait for the send queue; the messages of an
%% unreliable direction may be reordered anyway, and the end of the
%% direction goes after both, as below.
%%
%% The process sends nothing after {@link finish/2}, which waits for the
%% queue to drain, and stops after one second without a frame, swapping its
%% queue count from 0 to a closed mark, so that a frame is never handed to a
%% process that already stopped: the caller sees the mark and starts
%% another. It monitors the caller and stops, dropping its queue, once the
%% caller is gone, even in the middle of a wait for the send queue; such a
%% wait also ends when the connection closes, as the process that routes
%% the stream's events tells every writer waiting for its send queue, and
%% at the call's deadline, past which it writes nothing more. The router's
%% wake-ups that come after a wait ended are dropped.
%%
%% For tests, the `trevrpc' application's environment values
%% `datagram_sender_idle' and `datagram_size_age' replace the second and
%% the 100 ms.
-module(trevrpc_datagram_sender).

-export([new/2, send/3, finish/2, sent/1]).

-export_type([sender/0]).

%% How long the largest datagram the connection takes is trusted before it
%% is asked again: path MTU discovery can raise it.
-define(MAX_SIZE_AGE, 100).
%% How long the process waits for a frame before it stops.
-define(IDLE, 1000).
%% The queue's bounds, those of a call's received datagram messages.
-define(MAX_QUEUED, 128).
-define(MAX_QUEUED_BYTES, 65536).

%% The atomics slots.
-define(QUEUED, 1).
-define(QUEUED_BYTES, 2).
-define(SENT, 3).
-define(MAX_SIZE, 4).
%% The queue count of a process that stopped, or never started: far below
%% any count a caller adds to it.
-define(CLOSED, -(1 bsl 40)).

-record(sender, {
    stream :: trevrpc_stream:stream(),
    %% How long the largest datagram is trusted.
    size_age = ?MAX_SIZE_AGE :: non_neg_integer(),
    transport :: trevrpc_transport:t(),
    prefix :: binary(),
    atomics :: atomics:atomics_ref(),
    pid :: pid() | undefined,
    %% When the caller last asked for the largest datagram, if it has.
    checked :: integer() | undefined
}).

-opaque sender() :: #sender{}.

%% The process's state.
-record(proc, {
    stream :: trevrpc_stream:stream(),
    transport :: trevrpc_transport:t(),
    prefix :: binary(),
    atomics :: atomics:atomics_ref(),
    deadline :: trevrpc_stream:deadline(),
    checked :: integer(),
    size_age :: non_neg_integer(),
    idle :: timeout(),
    %% Monitors the caller.
    caller :: reference() | undefined
}).

%% @doc The sender of a call's messages on flow `Flow', whose stream is
%% `Stream'. Nothing is asked of the connection, and no process starts,
%% before the first frame.
-spec new(trevrpc_stream:stream(), pos_integer()) -> sender().
new(Stream, Flow) ->
    Atomics = atomics:new(4, [{signed, true}]),
    atomics:put(Atomics, ?QUEUED, ?CLOSED),
    #sender{
        stream = Stream,
        size_age = application:get_env(trevrpc, datagram_size_age, ?MAX_SIZE_AGE),
        transport = trevrpc_stream:transport(Stream),
        prefix = iolist_to_binary(trevrpc_varint:encode(Flow)),
        atomics = Atomics
    }.

%% @doc Sends `Frame', a MESSAGE or ZSTD_MESSAGE frame, in a datagram.
%% `stream' means it does not fit one on its own, and belongs on the
%% stream. With the queue full, waits for it to drain until `Deadline', the
%% call's, and drops `Frame' if it does not.
-spec send(iodata(), trevrpc_stream:deadline(), sender()) -> {sent | stream, sender()}.
send(Frame, Deadline, Sender) ->
    Size = iolist_size(Frame),
    case fits(Size, Sender) of
        {false, Sized} ->
            {stream, Sized};
        {true, Sized} ->
            case room(Size, Deadline, Sized) of
                ok -> {sent, hand_over(Frame, Size, Deadline, Sized)};
                timeout -> {sent, Sized}
            end
    end.

%% @doc Ends the direction: waits for the queued frames to be sent, until
%% `Deadline', and returns the DATAGRAM_COUNT of a direction that sent
%% datagrams, which goes right before its end. Nothing is sent after it.
-spec finish(trevrpc_stream:deadline(), sender() | undefined) -> iodata().
finish(_Deadline, undefined) ->
    [];
finish(Deadline, #sender{atomics = Atomics, pid = Pid} = Sender) ->
    _ =
        case drain(Deadline, Sender) of
            ok ->
                case atomics:compare_exchange(Atomics, ?QUEUED, 0, ?CLOSED) of
                    ok -> Pid ! {?MODULE, stop};
                    _Closed -> ok
                end;
            timeout ->
                %% A frame it is still sending may yet go, uncounted.
                exit(Pid, kill),
                atomics:put(Atomics, ?QUEUED, ?CLOSED)
        end,
    case sent(Sender) of
        0 -> [];
        Sent -> trevrpc_frame:encode_datagram_count(Sent)
    end.

%% @doc How many datagrams the transport accepted so far.
-spec sent(sender() | undefined) -> non_neg_integer().
sent(undefined) -> 0;
sent(#sender{atomics = Atomics}) -> atomics:get(Atomics, ?SENT).

%%% The caller's side

%% Whether a frame of `Size' bytes fits a datagram. The caller asks the
%% connection for the largest datagram before its first frame, and again
%% before it gives up on a frame for its size once the size it knows is
%% 100 ms old, as path MTU discovery may have raised it; the process, while
%% it runs, keeps the size fresh too.
fits(Size, #sender{checked = undefined} = Sender) ->
    fits(Size, ask_size(Sender));
fits(Size, #sender{prefix = Prefix, atomics = Atomics, checked = Checked, size_age = Age} = Sender) ->
    case byte_size(Prefix) + Size =< atomics:get(Atomics, ?MAX_SIZE) of
        true ->
            {true, Sender};
        false ->
            case erlang:monotonic_time(millisecond) - Checked < Age of
                true ->
                    {false, Sender};
                false ->
                    Resized = ask_size(Sender),
                    {byte_size(Prefix) + Size =< atomics:get(Atomics, ?MAX_SIZE), Resized}
            end
    end.

ask_size(#sender{transport = Transport, atomics = Atomics} = Sender) ->
    atomics:put(Atomics, ?MAX_SIZE, trevrpc_transport:datagram_max_size(Transport)),
    Sender#sender{checked = erlang:monotonic_time(millisecond)}.

%% Waits for a full queue to drain.
room(Size, Deadline, #sender{atomics = Atomics} = Sender) ->
    case atomics:get(Atomics, ?QUEUED) of
        Queued when Queued >= ?MAX_QUEUED ->
            drain(Deadline, Sender);
        Queued when Queued > 0 ->
            case atomics:get(Atomics, ?QUEUED_BYTES) + Size > ?MAX_QUEUED_BYTES of
                true -> drain(Deadline, Sender);
                false -> ok
            end;
        _Empty ->
            ok
    end.

%% Waits until every frame handed over was sent, or `Deadline'.
drain(Deadline, #sender{atomics = Atomics, pid = Pid}) ->
    case atomics:get(Atomics, ?QUEUED) of
        Queued when Queued =< 0 ->
            ok;
        _ ->
            Alias = monitor(process, Pid, [{alias, reply_demonitor}]),
            Pid ! {?MODULE, flush, Alias},
            receive
                {Alias, flushed} ->
                    ok;
                {'DOWN', Alias, process, _, _} ->
                    ok
            after trevrpc_stream:timeout(Deadline) ->
                demonitor(Alias, [flush]),
                timeout
            end
    end.

%% Hands `Frame' to the process, starting one if none runs.
hand_over(Frame, Size, Deadline, #sender{atomics = Atomics, pid = Pid} = Sender) ->
    case atomics:add_get(Atomics, ?QUEUED, 1) of
        Queued when Queued > 0 ->
            atomics:add(Atomics, ?QUEUED_BYTES, Size),
            Pid ! {?MODULE, frame, Frame},
            Sender;
        _Closed ->
            atomics:put(Atomics, ?QUEUED, 1),
            atomics:put(Atomics, ?QUEUED_BYTES, Size),
            Started = start(Deadline, Sender),
            Started ! {?MODULE, frame, Frame},
            Sender#sender{pid = Started}
    end.

start(Deadline, #sender{stream = Stream, transport = Transport, prefix = Prefix} = Sender) ->
    Proc = #proc{
        stream = Stream,
        transport = Transport,
        prefix = Prefix,
        atomics = Sender#sender.atomics,
        deadline = Deadline,
        checked = erlang:monotonic_time(millisecond),
        size_age = Sender#sender.size_age,
        idle = application:get_env(trevrpc, datagram_sender_idle, ?IDLE)
    },
    Caller = self(),
    spawn(fun() -> loop(Proc#proc{caller = monitor(process, Caller)}) end).

%%% The process

loop(#proc{atomics = Atomics, idle = Idle, caller = Caller} = Proc) ->
    receive
        {?MODULE, frame, Frame} ->
            loop(batch(collect([Frame], 1), Proc));
        {?MODULE, flush, Alias} ->
            Alias ! {Alias, flushed},
            loop(Proc);
        {?MODULE, stop} ->
            ok;
        {'DOWN', Caller, process, _, _} ->
            ok;
        {trevrpc_stream, _Ref, _Late} ->
            %% A wake-up for a wait for the send queue that already ended.
            loop(Proc)
    after Idle ->
        case atomics:compare_exchange(Atomics, ?QUEUED, 0, ?CLOSED) of
            ok -> ok;
            _Queued -> loop(Proc)
        end
    end.

%% The frames already queued, in order.
collect(Acc, ?MAX_QUEUED) ->
    lists:reverse(Acc);
collect(Acc, Count) ->
    receive
        {?MODULE, frame, Frame} -> collect([Frame | Acc], Count + 1)
    after 0 ->
        lists:reverse(Acc)
    end.

%% Sends a batch of frames and takes it off the queue.
batch(Frames, #proc{atomics = Atomics} = Proc) ->
    Next = pack(Frames, false, refresh(Proc)),
    atomics:sub(Atomics, ?QUEUED_BYTES, lists:sum([iolist_size(Frame) || Frame <- Frames])),
    atomics:sub(Atomics, ?QUEUED, length(Frames)),
    Next.

%% Asks the connection for the largest datagram once it is too old.
refresh(#proc{checked = Checked, size_age = Age} = Proc) ->
    case erlang:monotonic_time(millisecond) - Checked < Age of
        true -> Proc;
        false -> resize(Proc)
    end.

resize(#proc{transport = Transport, atomics = Atomics} = Proc) ->
    atomics:put(Atomics, ?MAX_SIZE, trevrpc_transport:datagram_max_size(Transport)),
    Proc#proc{checked = erlang:monotonic_time(millisecond)}.

%% Sends `Frames' in as few datagrams as fit, writing on the stream those
%% of a datagram whose send failed. A send refused for its size, as when
%% path MTU discovery fell back, asks the connection for the largest
%% datagram, and packs that datagram's frames again, once.
pack([], _Retried, Proc) ->
    Proc;
pack(Frames, Retried, #proc{prefix = Prefix, atomics = Atomics} = Proc) ->
    case fill(Frames, atomics:get(Atomics, ?MAX_SIZE) - byte_size(Prefix), []) of
        {[], [Frame | Rest]} ->
            %% Not even this one fits on its own any more.
            stream([Frame], Proc),
            pack(Rest, false, Proc);
        {Datagram, Rest} ->
            case trevrpc_transport:send_datagram(Proc#proc.transport, [Prefix | Datagram]) of
                ok ->
                    atomics:add(Atomics, ?SENT, 1),
                    pack(Rest, false, Proc);
                {error, Reason} when
                    not Retried,
                    Reason =:= datagram_too_large orelse Reason =:= datagram_too_large_for_path
                ->
                    pack(Frames, true, resize(Proc));
                {error, _} ->
                    stream(Datagram, Proc),
                    pack(Rest, false, Proc)
            end
    end.

%% The frames from the front of `Frames' that fit `Room' bytes together.
fill([Frame | Rest] = Frames, Room, Acc) ->
    case iolist_size(Frame) of
        Size when Size =< Room -> fill(Rest, Room - Size, [Frame | Acc]);
        _ -> {lists:reverse(Acc), Frames}
    end;
fill([], _Room, Acc) ->
    {lists:reverse(Acc), []}.

%% Writes `Frames' on the stream, waiting for a full send queue within the
%% call's deadline as the call's own writes do, unless the deadline has
%% passed. The write returns once the connection took it, so it is on the
%% stream before the end the caller writes after `finish/2'. A stream that
%% failed is the call's to notice; a caller that went away while the write
%% waited ends the process.
stream(Frames, #proc{stream = Stream, deadline = Deadline, caller = Caller}) ->
    case trevrpc_stream:timeout(Deadline) of
        0 ->
            ok;
        _ ->
            case trevrpc_stream:send(Stream, Frames, false, Deadline, Caller) of
                {error, abandoned, _} -> exit(normal);
                _ -> ok
            end
    end.
