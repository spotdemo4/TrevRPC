%% trevrpc_datagram_sender against a stand-in for an erlang_quic connection,
%% which answers its calls as each test scripts: packing, the frames that
%% take the stream, the queue's bound, deadlines and the process's life.
%% The test process is also the stream's router, which a write that finds
%% the send queue full asks to say when it drained.
-module(trevrpc_datagram_sender_tests).

-include_lib("eunit/include/eunit.hrl").

-define(FLOW, 5).
-define(STREAM, 4).

%%% The stand-in connection

%% Answers datagram_max_size with its current size, each send_datagram with
%% the next result of its datagram script, and each stream write with the
%% next of its stream script (ok once a script runs out), reporting every
%% datagram and write, and the process that made it, to the test. A held
%% connection keeps the first datagram it gets, telling the test, and
%% answers it once released.
conn(Max) ->
    flush_mailbox(),
    Test = self(),
    spawn_link(fun() ->
        conn_loop(#{
            test => Test,
            max => Max,
            script => [],
            stream_script => [],
            held => false,
            pending => none
        })
    end).

conn_loop(#{test := Test, max := Max, held := Held, pending := Pending} = State) ->
    receive
        {'$gen_call', From, datagram_max_size} ->
            gen:reply(From, Max),
            conn_loop(State);
        {'$gen_call', {Pid, _} = From, {send_datagram, Data}} when Held, Pending =:= none ->
            Test ! {holding, Pid},
            conn_loop(State#{pending := {From, Data}});
        {'$gen_call', From, {send_datagram, Data}} when not Held ->
            conn_loop(answer(From, Data, State));
        {'$gen_call', {Pid, _} = From, {send_data, ?STREAM, Data, false}} ->
            {Result, Rest} = next(maps:get(stream_script, State)),
            Test ! {stream, iolist_to_binary(Data), Result, Pid},
            gen:reply(From, Result),
            conn_loop(State#{stream_script := Rest});
        {set, Changes} ->
            Next = maps:merge(State, Changes),
            case Next of
                #{held := false, pending := {From, Data}} ->
                    conn_loop(answer(From, Data, Next#{pending := none}));
                _ ->
                    conn_loop(Next)
            end
    end.

answer({Pid, _} = From, Data, #{test := Test, script := Script} = State) ->
    {Result, Rest} = next(Script),
    Test ! {datagram, iolist_to_binary(Data), Result, Pid},
    gen:reply(From, Result),
    State#{script := Rest}.

next([]) -> {ok, []};
next([Result | Rest]) -> {Result, Rest}.

set(Conn, Changes) ->
    Conn ! {set, Changes},
    ok.

flush_mailbox() ->
    receive
        _ -> flush_mailbox()
    after 0 -> ok
    end.

%% The next datagram the connection got: its messages and its result.
next_datagram() ->
    {Messages, Result, _Pid} = next_datagram_from(),
    {Messages, Result}.

%% ... and the process that sent it.
next_datagram_from() ->
    receive
        {datagram, Payload, Result, Pid} ->
            {ok, ?FLOW, Messages} = trevrpc_datagram:decode(Payload),
            {[Message || {message, Message} <- Messages], Result, Pid}
    after 5000 ->
        erlang:error(no_datagram)
    end.

%% The next write on the stream: its messages and its result.
next_write() ->
    receive
        {stream, Data, Result, _Pid} ->
            {ok, Frames, _} = trevrpc_frame:feed(Data, trevrpc_frame:reader(1 bsl 20)),
            {[Message || {message, Message} <- Frames], Result}
    after 5000 ->
        erlang:error(no_write)
    end.

nothing_else() ->
    receive
        {datagram, Payload, _, _} -> erlang:error({unexpected_datagram, Payload});
        {stream, Data, _, _} -> erlang:error({unexpected_write, Data})
    after 50 ->
        ok
    end.

%% Waits for `Pid' to exit, within `Timeout'.
await_exit(Pid, Timeout) ->
    Ref = monitor(process, Pid),
    receive
        {'DOWN', Ref, process, Pid, _} -> ok
    after Timeout ->
        erlang:error({still_alive, Pid})
    end.

frame(Message) ->
    trevrpc_frame:encode(message, Message).

sender(Conn) ->
    Stream = trevrpc_stream:new(
        {quic, Conn}, ?STREAM, make_ref(), self(), trevrpc_frame:reader(65536)
    ),
    trevrpc_datagram_sender:new(Stream, ?FLOW).

send_all(Messages, Sender) ->
    send_all(Messages, infinity, Sender).

send_all(Messages, Deadline, Sender) ->
    lists:foldl(
        fun(Message, Acc) ->
            {sent, Next} = trevrpc_datagram_sender:send(frame(Message), Deadline, Acc),
            Next
        end,
        Sender,
        Messages
    ).

%% Sends the first frame to a held connection, and returns once the
%% connection holds its datagram, so that what follows queues behind it,
%% with the process sending it.
send_held(Conn, Message) ->
    set(Conn, #{held => true}),
    Sender = send_all([Message], sender(Conn)),
    receive
        {holding, Pid} -> {Sender, Pid}
    after 5000 -> erlang:error(not_holding)
    end.

finish(Sender) ->
    iolist_to_binary(trevrpc_datagram_sender:finish(infinity, Sender)).

count(N) ->
    iolist_to_binary(trevrpc_frame:encode_datagram_count(N)).

%% Runs `Test' with `Key' of the `trevrpc' environment set to `Value'.
with_env(Key, Value, Test) ->
    ok = application:set_env(trevrpc, Key, Value),
    try
        Test()
    after
        ok = application:unset_env(trevrpc, Key)
    end.

%%% Packing

%% Frames queued while a datagram is on its way leave together in the next.
packs_queued_frames_test() ->
    Conn = conn(1200),
    {Held, _} = send_held(Conn, <<"m1">>),
    Sender = send_all([<<"m2">>, <<"m3">>, <<"m4">>], Held),
    set(Conn, #{held => false}),
    ?assertEqual({[<<"m1">>], ok}, next_datagram()),
    ?assertEqual({[<<"m2">>, <<"m3">>, <<"m4">>], ok}, next_datagram()),
    ?assertEqual(count(2), finish(Sender)),
    nothing_else().

%% No more frames than fit go in one datagram.
packs_within_the_size_test() ->
    Large = binary:copy(<<"x">>, 500),
    Conn = conn(1200),
    {Held, _} = send_held(Conn, <<"first">>),
    Sender = send_all([Large, Large, Large], Held),
    set(Conn, #{held => false}),
    ?assertEqual({[<<"first">>], ok}, next_datagram()),
    ?assertEqual({[Large, Large], ok}, next_datagram()),
    ?assertEqual({[Large], ok}, next_datagram()),
    ?assertEqual(count(3), finish(Sender)).

%%% The largest datagram

%% A frame that does not fit a datagram on its own belongs on the stream,
%% which the caller writes.
too_large_test() ->
    Conn = conn(100),
    Large = frame(binary:copy(<<"x">>, 200)),
    ?assertMatch({stream, _}, trevrpc_datagram_sender:send(Large, infinity, sender(Conn))),
    nothing_else().

%% A caller whose frames all took the stream, so that no process keeps the
%% size fresh, asks the connection again before it gives up on the next,
%% once the size it knows is old enough (at once here).
size_grows_test() ->
    with_env(datagram_size_age, 0, fun() ->
        Conn = conn(100),
        Large = frame(binary:copy(<<"x">>, 200)),
        {stream, Sender} = trevrpc_datagram_sender:send(Large, infinity, sender(Conn)),
        set(Conn, #{max => 1200}),
        {sent, Next} = trevrpc_datagram_sender:send(Large, infinity, Sender),
        ?assertMatch({[_], ok}, next_datagram()),
        ?assertEqual(count(1), finish(Next))
    end).

%% A send refused for its size learns the new size and packs again, once.
shrunk_path_test() ->
    Conn = conn(1200),
    Medium = binary:copy(<<"m">>, 300),
    {Held, _} = send_held(Conn, <<"first">>),
    Sender = send_all([Medium, Medium], Held),
    set(Conn, #{script => [ok, {error, datagram_too_large_for_path}], max => 400, held => false}),
    ?assertEqual({[<<"first">>], ok}, next_datagram()),
    ?assertEqual({[Medium, Medium], {error, datagram_too_large_for_path}}, next_datagram()),
    ?assertEqual({[Medium], ok}, next_datagram()),
    ?assertEqual({[Medium], ok}, next_datagram()),
    ?assertEqual(count(3), finish(Sender)).

%%% Failed sends

%% The frames of a datagram the congestion window refused, or whose send
%% failed otherwise, go on the stream at once, uncounted.
failed_send_takes_the_stream_test() ->
    Conn = conn(1200),
    {Held, _} = send_held(Conn, <<"m1">>),
    Sender = send_all([<<"m2">>, <<"m3">>], Held),
    set(Conn, #{script => [{error, congestion_limited}, {error, closed}], held => false}),
    ?assertEqual({[<<"m1">>], {error, congestion_limited}}, next_datagram()),
    ?assertEqual({[<<"m1">>], ok}, next_write()),
    ?assertEqual({[<<"m2">>, <<"m3">>], {error, closed}}, next_datagram()),
    ?assertEqual({[<<"m2">>, <<"m3">>], ok}, next_write()),
    Next = send_all([<<"m4">>], Sender),
    ?assertEqual({[<<"m4">>], ok}, next_datagram()),
    ?assertEqual(count(1), finish(Next)),
    nothing_else().

%% A stream write that finds the send queue full asks the router to say
%% when it drained, and writes again then.
full_send_queue_test() ->
    Conn = conn(1200),
    set(Conn, #{
        script => [{error, congestion_limited}], stream_script => [{error, send_queue_full}]
    }),
    Sender = send_all([<<"late">>], sender(Conn)),
    ?assertEqual({[<<"late">>], {error, congestion_limited}}, next_datagram()),
    ?assertEqual({[<<"late">>], {error, send_queue_full}}, next_write()),
    receive
        {trevrpc_send_ready, Pid, Ref} -> Pid ! {trevrpc_stream, Ref, send_ready}
    after 5000 -> erlang:error(no_send_ready_request)
    end,
    ?assertEqual({[<<"late">>], ok}, next_write()),
    ?assertEqual(<<>>, finish(Sender)),
    nothing_else().

%% ... but not past the call's deadline, after which the frames are lost:
%% the router's wake-up for the wait that ended is dropped when it comes
%% late, and frames whose datagram fails once the deadline passed are not
%% written at all.
full_send_queue_deadline_test() ->
    Conn = conn(1200),
    set(Conn, #{
        script => [{error, congestion_limited}, ok, ok, {error, congestion_limited}],
        stream_script => [{error, send_queue_full}]
    }),
    Deadline = erlang:monotonic_time(millisecond) + 50,
    Sender = send_all([<<"late">>], Deadline, sender(Conn)),
    ?assertEqual({[<<"late">>], {error, congestion_limited}}, next_datagram()),
    ?assertEqual({[<<"late">>], {error, send_queue_full}}, next_write()),
    {Pid, Ref} =
        receive
            {trevrpc_send_ready, Waiting, WaitRef} -> {Waiting, WaitRef}
        after 5000 -> erlang:error(no_send_ready_request)
        end,
    %% Never told the queue drained, the process gives up at the deadline,
    %% and only then sends the next frame.
    Next = send_all([<<"next">>], Deadline, Sender),
    ?assertEqual({[<<"next">>], ok}, next_datagram()),
    ?assert(erlang:monotonic_time(millisecond) >= Deadline),
    %% The router's wake-up comes after the wait ended, before a frame.
    Pid ! {trevrpc_stream, Ref, send_ready},
    After = send_all([<<"after">>], Deadline, Next),
    ?assertEqual({[<<"after">>], ok}, next_datagram()),
    %% Its mailbox may still hold the reply to that datagram's send.
    {messages, Messages} = process_info(Pid, messages),
    ?assertEqual([], [Late || {trevrpc_stream, _, _} = Late <- Messages]),
    %% Past the deadline, a datagram that fails is not written.
    Skipped = send_all([<<"skipped">>], Deadline, After),
    ?assertEqual({[<<"skipped">>], {error, congestion_limited}}, next_datagram()),
    ?assertEqual(count(2), finish(Skipped)),
    nothing_else().

%%% The queue and the end of the direction

%% A caller past the queue's bound waits for it to drain, and drops the
%% frame when its deadline comes first.
full_queue_test() ->
    Conn = conn(1200),
    Messages = [integer_to_binary(N) || N <- lists:seq(1, 128)],
    {Held, _} = send_held(Conn, hd(Messages)),
    Sender = send_all(tl(Messages), Held),
    Deadline = erlang:monotonic_time(millisecond) + 50,
    ?assertMatch({sent, _}, trevrpc_datagram_sender:send(frame(<<"late">>), Deadline, Sender)),
    ?assert(erlang:monotonic_time(millisecond) >= Deadline),
    set(Conn, #{held => false}),
    {ok, Count} = trevrpc_frame:decode_datagram_count(strip_count(finish(Sender))),
    ?assertEqual(Messages, lists:append([element(1, next_datagram()) || _ <- lists:seq(1, Count)])),
    nothing_else().

%% The DATAGRAM_COUNT frame's payload.
strip_count(Frame) ->
    {ok, [{datagram_count, Payload}], _} = trevrpc_frame:feed(Frame, trevrpc_frame:reader(64)),
    Payload.

%% A direction that ends past its deadline stops the process, still
%% sending, and counts what was accepted; the caller lives on.
finish_timeout_test() ->
    Conn = conn(1200),
    {Sender, Pid} = send_held(Conn, <<"stuck">>),
    Deadline = erlang:monotonic_time(millisecond) + 50,
    ?assertEqual(<<>>, iolist_to_binary(trevrpc_datagram_sender:finish(Deadline, Sender))),
    ?assert(erlang:monotonic_time(millisecond) >= Deadline),
    await_exit(Pid, 1000),
    ?assertNot(is_process_alive(Pid)).

%% A direction that sent nothing ends with nothing.
nothing_sent_test() ->
    ?assertEqual([], trevrpc_datagram_sender:finish(infinity, undefined)),
    ?assertEqual(<<>>, finish(sender(conn(1200)))).

%%% The process's life

%% The process stops once it has nothing to send for its idle time (none
%% here); the next frame starts another, and the count goes on.
restarts_after_idle_test() ->
    with_env(datagram_sender_idle, 0, fun() ->
        Conn = conn(1200),
        Sender = send_all([<<"before">>], sender(Conn)),
        {[<<"before">>], ok, First} = next_datagram_from(),
        await_exit(First, 1000),
        Next = send_all([<<"after">>], Sender),
        {[<<"after">>], ok, Second} = next_datagram_from(),
        ?assertNotEqual(First, Second),
        ?assertEqual(count(2), finish(Next))
    end).

%% The process stops as soon as its caller is gone, well within its idle
%% time.
stops_with_the_caller_test() ->
    Conn = conn(1200),
    Caller = spawn(fun() ->
        _ = send_all([<<"orphan">>], sender(Conn)),
        receive
            stop -> ok
        end
    end),
    {[<<"orphan">>], ok, Pid} = next_datagram_from(),
    ?assert(is_process_alive(Pid)),
    Caller ! stop,
    await_exit(Pid, 500).

%% ... even while it waits for a full send queue to drain.
stops_with_the_caller_while_waiting_test() ->
    Conn = conn(1200),
    set(Conn, #{
        script => [{error, congestion_limited}], stream_script => [{error, send_queue_full}]
    }),
    Test = self(),
    Caller = spawn(fun() ->
        %% The test is the stream's router, asked when the queue drains.
        Stream = trevrpc_stream:new(
            {quic, Conn}, ?STREAM, make_ref(), Test, trevrpc_frame:reader(65536)
        ),
        _ = send_all([<<"orphan">>], trevrpc_datagram_sender:new(Stream, ?FLOW)),
        receive
            stop -> ok
        end
    end),
    ?assertEqual({[<<"orphan">>], {error, congestion_limited}}, next_datagram()),
    ?assertEqual({[<<"orphan">>], {error, send_queue_full}}, next_write()),
    Pid =
        receive
            {trevrpc_send_ready, Waiting, _Ref} -> Waiting
        after 5000 -> erlang:error(no_send_ready_request)
        end,
    exit(Caller, kill),
    await_exit(Pid, 500),
    nothing_else().
