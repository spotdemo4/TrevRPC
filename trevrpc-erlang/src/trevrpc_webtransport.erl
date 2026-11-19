%% @private The WebTransport session handler of a TrevRPC server: the
%% `webtransport_handler' of each session erlang-webtransport accepts, run
%% in the session's process, and the functions the rest of the server uses
%% to write to the session.
%%
%% The session's events go to the session's `trevrpc_server_conn' as
%% `{trevrpc_wt, Session, Event}' messages, with the events of the other
%% transports: `{stream_opened, Id}', `{stream_data, Id, Data, Fin}',
%% `{stream_reset, Id, Code}', `{stop_sending, Id, Code}' and
%% `{datagram, Payload}'. The first message, `{established, Transport}',
%% tells the connection process its transport.
%%
%% == Flow control ==
%%
%% erlang-webtransport 0.5.11 returns a stream's receive credit when the
%% handler's callback returns, and every stream of the session shares the
%% session's process. So the callback blocks while its stream holds more
%% than a stream window of bytes its call has not read (`consume/3'), or
%% the session more than a connection window: that bounds what a session
%% buffers, as on the other transports, but holds up the session's other
%% streams meanwhile. Asking upstream for credit that
%% follows the application's reads is tracked in TrevRPC #609.
%%
%% Nothing the server does waits on the session while the callback blocks,
%% or the two could wait on each other: writes are messages, handled by
%% {@link handle_info/2} as the session's actions, never calls into the
%% session. A writer waits for its write to be taken, so the callback also
%% stops blocking as soon as a write arrives, and takes it, at the cost of
%% one more chunk of credit for the stream it waited on. Writes could so
%% let a stream run ahead of its call without end, so a stream four stream
%% windows ahead, or one more than a window ahead while the session is four
%% connection windows ahead, fails its call with `resource_exhausted': the
%% connection process gets `{stream_overflow, Id}', and the stream's bytes
%% no longer count. Datagrams written while the callback blocks are
%% dropped, as when a QUIC connection's congestion window is full.
%%
%% == Stopping a stream ==
%%
%% Firefox tears down the whole stream when it gets STOP_SENDING, a STATUS
%% it has not read yet included. So a stop is deferred, as by the Go and Rust
%% servers: what the client still sends is read and dropped until it
%% finishes or resets its side, and STOP_SENDING goes out only after a
%% second, or a mebibyte, of it. The `webtransport_stop_timeout'
%% application variable of `trevrpc' changes the second, for tests.
%%
%% A write the session refuses because its send queue is full waits in its
%% stream's queue, behind which the stream's later writes wait, and is
%% tried again shortly after: erlang-webtransport tells a handler no more
%% than that it failed.
-module(trevrpc_webtransport).

-behaviour(webtransport_handler).

-export([
    init/3,
    origin_check/2,
    select_protocol/2,
    handle_stream/4,
    handle_stream_fin/4,
    handle_datagram/2,
    handle_stream_closed/3,
    handle_info/2,
    handle_action_failed/3,
    terminate/2
]).
-export([
    protocol/0,
    send/5,
    send_async/4,
    consume/3,
    forget/2,
    reset_stream/3,
    stop_sending/3,
    send_datagram/2,
    drain/1,
    close/1
]).

-export_type([options/0]).

%% `conn': the session's connection process; `window' and
%% `connection_window': the bytes a stream, and all of them, may hold unread
%% before the session waits for their calls; `quic': the QUIC connection;
%% `session_id': the CONNECT stream's ID.
-type options() :: #{
    conn := pid(),
    window := pos_integer(),
    connection_window := pos_integer(),
    quic := pid(),
    session_id := non_neg_integer()
}.

-record(stream, {
    %% Bytes passed to the connection process and not read by the call yet.
    unconsumed = 0 :: non_neg_integer(),
    %% false once the connection process has forgotten the stream: its
    %% bytes no longer hold the session back.
    tracked = true :: boolean(),
    %% The peer finished or reset its side; we finished or reset ours.
    peer_done = false :: boolean(),
    local_done = false :: boolean(),
    %% Writes waiting for the session's send queue, oldest first, with the
    %% alias of the writer waiting for each, if one does.
    queue = queue:new() :: queue:queue({binary(), boolean(), reference() | noack}),
    %% The session refused the stream's last write; the queue waits for
    %% the retry.
    blocked = false :: boolean(),
    %% A stop deferred while the client may still read: its code, and the
    %% bytes dropped since.
    stopping :: {non_neg_integer(), non_neg_integer()} | undefined
}).

-record(state, {
    conn :: pid(),
    %% How long a stopped stream is read before STOP_SENDING.
    stop_timeout :: non_neg_integer(),
    conn_monitor :: reference(),
    window :: pos_integer(),
    connection_window :: pos_integer(),
    %% The unconsumed bytes of every stream.
    unconsumed = 0 :: non_neg_integer(),
    streams = #{} :: #{non_neg_integer() => #stream{}},
    %% The timer of the next retry of refused writes.
    retry :: reference() | undefined
}).

-define(PROTOCOL, <<"trevrpc-1">>).
-define(CANCELLED, 1).
%% How long a write the session refused waits before it is tried again.
-define(RETRY_INTERVAL, 5).
%% How long, and how many bytes, a stopped stream is read before
%% STOP_SENDING.
-define(STOP_TIMEOUT, 1000).
-define(STOP_LIMIT, 1048576).
%% How many windows a stream, and all of them, may run ahead of their calls
%% before a stream's call fails.
-define(OVERFLOW, 4).

%%% Writing to a session

%% @doc The WebTransport protocol TrevRPC version 1 is negotiated as.
-spec protocol() -> binary().
protocol() -> ?PROTOCOL.

%% @doc Writes `Data' to stream `Id' of `Session', finishing it with `Fin',
%% and waits up to `Timeout' for the session to take it.
-spec send(pid(), non_neg_integer(), iodata(), boolean(), timeout()) -> ok | {error, term()}.
send(Session, Id, Data, Fin, Timeout) ->
    Alias = monitor(process, Session, [{alias, reply_demonitor}]),
    Session ! {trevrpc_wt, {send, Alias, Id, iolist_to_binary(Data), Fin}},
    receive
        {trevrpc_wt_sent, Alias, Result} ->
            Result;
        {'DOWN', Alias, process, _, _} ->
            {error, closed}
    after Timeout ->
        demonitor(Alias, [flush]),
        %% A write still queued is dropped; one the session took is sent.
        Session ! {trevrpc_wt, {cancel, Alias}},
        {error, timeout}
    end.

%% @doc Writes `Data' to stream `Id' without waiting.
-spec send_async(pid(), non_neg_integer(), iodata(), boolean()) -> ok.
send_async(Session, Id, Data, Fin) ->
    Session ! {trevrpc_wt, {send, noack, Id, iolist_to_binary(Data), Fin}},
    ok.

%% @doc The call read `Bytes' more of stream `Id'.
-spec consume(pid(), non_neg_integer(), non_neg_integer()) -> ok.
consume(_Session, _Id, 0) ->
    ok;
consume(Session, Id, Bytes) ->
    Session ! {trevrpc_wt, {consume, Id, Bytes}},
    ok.

%% @doc Nothing reads stream `Id' any more: what it holds no longer holds
%% the session back.
-spec forget(pid(), non_neg_integer()) -> ok.
forget(Session, Id) ->
    Session ! {trevrpc_wt, {forget, Id}},
    ok.

-spec reset_stream(pid(), non_neg_integer(), non_neg_integer()) -> ok.
reset_stream(Session, Id, Code) ->
    Session ! {trevrpc_wt, {reset_stream, Id, Code}},
    ok.

-spec stop_sending(pid(), non_neg_integer(), non_neg_integer()) -> ok.
stop_sending(Session, Id, Code) ->
    Session ! {trevrpc_wt, {stop_sending, Id, Code}},
    ok.

%% @doc Sends a session datagram, without waiting: one the connection
%% cannot send is dropped.
-spec send_datagram(pid(), iodata()) -> ok.
send_datagram(Session, Payload) ->
    Session ! {trevrpc_wt, {send_datagram, iolist_to_binary(Payload)}},
    ok.

%% @doc Tells the client that the session takes no more calls
%% (WT_DRAIN_SESSION).
-spec drain(pid()) -> ok.
drain(Session) ->
    Session ! {trevrpc_wt, drain},
    ok.

%% @doc Closes the session.
-spec close(pid()) -> ok.
close(Session) ->
    Session ! {trevrpc_wt, close},
    ok.

%%% webtransport_handler

-spec init(pid(), map(), options()) -> {ok, #state{}}.
init(Session, _Request, #{conn := Conn, window := Window} = Opts) ->
    Transport = {wt, Session, maps:get(quic, Opts), quarter_size(maps:get(session_id, Opts))},
    Conn ! {trevrpc_wt, Session, {established, Transport}},
    {ok, #state{
        conn = Conn,
        conn_monitor = monitor(process, Conn),
        window = Window,
        connection_window = maps:get(connection_window, Opts),
        stop_timeout = application:get_env(trevrpc, webtransport_stop_timeout, ?STOP_TIMEOUT)
    }}.

%% The bytes of the quarter stream ID that prefixes each datagram of the
%% session.
quarter_size(SessionId) ->
    byte_size(trevrpc_varint:encode(SessionId div 4)).

%% The server's admission decided on the request before erlang-webtransport
%% saw it; without this callback, erlang-webtransport would refuse every
%% request with an `Origin', which browsers always send.
-spec origin_check([{binary(), binary()}], options()) -> accept.
origin_check(_Headers, _Opts) ->
    accept.

%% A client that offers protocols must offer TrevRPC's; one that offers
%% none is a browser without the `protocols' option, taken as version 1.
-spec select_protocol([binary()], options()) -> {ok, binary()} | none | {reject, 400}.
select_protocol([], _Opts) ->
    none;
select_protocol(Offered, _Opts) ->
    case lists:member(?PROTOCOL, Offered) of
        true -> {ok, ?PROTOCOL};
        false -> {reject, 400}
    end.

-spec handle_stream(non_neg_integer(), bidi | uni, binary(), #state{}) -> result().
handle_stream(Id, Type, Data, State) ->
    deliver(Id, Type, Data, false, State).

-spec handle_stream_fin(non_neg_integer(), bidi | uni, binary(), #state{}) -> result().
handle_stream_fin(Id, Type, Data, State) ->
    deliver(Id, Type, Data, true, State).

-type result() :: {ok, #state{}} | {ok, #state{}, [webtransport_handler:action()]}.

-spec handle_datagram(binary(), #state{}) -> {ok, #state{}}.
handle_datagram(Payload, #state{conn = Conn} = State) ->
    Conn ! {trevrpc_wt, self(), {datagram, Payload}},
    {ok, State}.

-spec handle_stream_closed(non_neg_integer(), term(), #state{}) -> {ok, #state{}}.
handle_stream_closed(Id, {reset, Code}, #state{conn = Conn} = State) ->
    Conn ! {trevrpc_wt, self(), {stream_reset, Id, Code}},
    ok = settle_later(Id),
    {ok,
        update(Id, fun(Stream) -> Stream#stream{peer_done = true, stopping = undefined} end, State)};
handle_stream_closed(Id, {stop_sending, Code}, #state{conn = Conn} = State) ->
    Conn ! {trevrpc_wt, self(), {stop_sending, Id, Code}},
    ok = settle_later(Id),
    %% The peer reads no more: what waits to be written never will be.
    {ok, update(Id, fun(Stream) -> drop_queue(Stream#stream{local_done = true}) end, State)};
handle_stream_closed(_Id, _Reason, State) ->
    {ok, State}.

-spec handle_info(term(), #state{}) ->
    {ok, #state{}} | {ok, #state{}, [webtransport_handler:action()]}.
handle_info({trevrpc_wt, Op}, State) ->
    {Next, Actions} = op(Op, State),
    {ok, Next, Actions};
handle_info({timeout, Timer, retry}, #state{retry = Timer, streams = Streams} = State) ->
    Unblocked = maps:map(fun(_Id, Stream) -> Stream#stream{blocked = false} end, Streams),
    {Next, Actions} = flush(State#state{streams = Unblocked, retry = undefined}),
    {ok, Next, Actions};
handle_info({timeout, _Timer, {stop, Id}}, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #stream{stopping = {Code, _}, peer_done = false} = Stream} ->
            {ok, store(Id, Stream#stream{stopping = undefined}, State), [{stop_sending, Id, Code}]};
        #{} ->
            {ok, State}
    end;
handle_info({'DOWN', Monitor, process, _, _}, #state{conn_monitor = Monitor} = State) ->
    {ok, State, [{close_session, 0, <<>>}]};
handle_info(_Message, State) ->
    {ok, State}.

%% A write the session refused for its full send queue waits for the
%% retry, at the head of its stream's queue; its writer was told already.
%% Any other failure means the stream is gone. erlang-webtransport 0.5.11
%% runs a callback's actions as soon as the callback returns, calling this
%% for each that fails, before it handles another message: no later write
%% of the stream can go out ahead of the refused one.
-spec handle_action_failed(webtransport_handler:action(), term(), #state{}) -> {ok, #state{}}.
handle_action_failed({send, Id, Data}, Reason, State) ->
    {ok, refused(Id, Data, false, Reason, State)};
handle_action_failed({send, Id, Data, fin}, Reason, State) ->
    {ok, refused(Id, Data, true, Reason, State)};
handle_action_failed(_Action, _Reason, State) ->
    {ok, State}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, #state{streams = Streams}) ->
    _ = maps:map(fun(_Id, Stream) -> drop_queue(Stream) end, Streams),
    ok.

%%% Receiving

deliver(Id, uni, _Data, _Fin, State) ->
    %% TrevRPC uses no unidirectional streams.
    {ok, State, [{stop_sending, Id, ?CANCELLED}]};
deliver(Id, bidi, Data, Fin, #state{conn = Conn, streams = Streams} = State) ->
    Stream =
        case Streams of
            #{Id := Known} ->
                Known;
            #{} ->
                Conn ! {trevrpc_wt, self(), {stream_opened, Id}},
                #stream{}
        end,
    Conn ! {trevrpc_wt, self(), {stream_data, Id, Data, Fin}},
    ok =
        case Fin of
            true -> settle_later(Id);
            false -> ok
        end,
    {Stopping, Actions} = dropped(Id, byte_size(Data), Fin, Stream#stream.stopping),
    Delivered = Stream#stream{
        unconsumed = Stream#stream.unconsumed + byte_size(Data),
        peer_done = Stream#stream.peer_done orelse Fin,
        stopping = Stopping
    },
    Unconsumed =
        case Stream#stream.tracked of
            true -> State#state.unconsumed + byte_size(Data);
            false -> State#state.unconsumed
        end,
    Stored = store(Id, Delivered, State#state{unconsumed = Unconsumed}),
    Waited =
        case overflowed(Id, Stored) of
            true ->
                Conn ! {trevrpc_wt, self(), {stream_overflow, Id}},
                {ok, untrack(Id, Stored)};
            false ->
                wait(Id, Stored)
        end,
    case Waited of
        {ok, Next} when Actions =:= [] -> {ok, Next};
        {ok, Next} -> {ok, Next, Actions};
        {ok, Next, More} -> {ok, Next, Actions ++ More}
    end.

%% Whether stream `Id' ran too far ahead of its call.
overflowed(Id, #state{streams = Streams, window = Window} = State) ->
    case Streams of
        #{Id := #stream{tracked = true, unconsumed = Unconsumed}} ->
            Unconsumed > ?OVERFLOW * Window orelse
                (Unconsumed > Window andalso
                    State#state.unconsumed > ?OVERFLOW * State#state.connection_window);
        #{} ->
            false
    end.

%% Data on a stream whose stop is deferred: the client finished, or sent
%% too much to wait any longer.
dropped(_Id, _Bytes, _Fin, undefined) ->
    {undefined, []};
dropped(_Id, _Bytes, true, _Stopping) ->
    {undefined, []};
dropped(Id, Bytes, false, {Code, Dropped}) when Dropped + Bytes > ?STOP_LIMIT ->
    {undefined, [{stop_sending, Id, Code}]};
dropped(_Id, Bytes, false, {Code, Dropped}) ->
    {{Code, Dropped + Bytes}, []}.

%% Returns once stream `Id' holds no more than a window unread and the
%% session no more than a connection window, or some process may be waiting
%% on the session: a writer, whose write it takes, or one whose write waits
%% for the session's send queue.
wait(Id, State) ->
    case held(Id, State) of
        false ->
            {ok, State};
        true ->
            receive
                {trevrpc_wt, {consume, _, _} = Op} ->
                    wait(Id, element(1, op(Op, State)));
                {trevrpc_wt, {forget, _} = Op} ->
                    wait(Id, element(1, op(Op, State)));
                {trevrpc_wt, {cancel, _} = Op} ->
                    wait(Id, element(1, op(Op, State)));
                {trevrpc_wt, {send_datagram, _}} ->
                    %% Dropped: the session is busy.
                    wait(Id, State);
                {trevrpc_wt, {stop_sending, _, _} = Op} ->
                    {Next, Actions} = op(Op, State),
                    {ok, Next, Actions};
                {trevrpc_wt, {reset_stream, _, _} = Op} ->
                    {Next, Actions} = op(Op, State),
                    {ok, Next, Actions};
                {trevrpc_wt, Op} when Op =:= drain; Op =:= close ->
                    {Next, Actions} = op(Op, State),
                    {ok, Next, Actions};
                {trevrpc_wt, {send, _, _, _, _} = Op} ->
                    {Next, Actions} = op(Op, State),
                    {ok, Next, Actions};
                {trevrpc_wt, flush} ->
                    {Next, Actions} = flush(State),
                    {ok, Next, Actions};
                {timeout, Timer, retry} when Timer =:= State#state.retry ->
                    {ok, Next, Actions} = handle_info({timeout, Timer, retry}, State),
                    {ok, Next, Actions};
                {'DOWN', Monitor, process, _, _} = Down when Monitor =:= State#state.conn_monitor ->
                    self() ! Down,
                    {ok, State}
            end
    end.

held(_Id, #state{unconsumed = Unconsumed, connection_window = Window}) when Unconsumed > Window ->
    true;
held(Id, #state{streams = Streams, window = Window}) ->
    case Streams of
        #{Id := #stream{tracked = true, unconsumed = Unconsumed}} -> Unconsumed > Window;
        #{} -> false
    end.

%%% Operations

op({send, Ack, Id, Data, Fin}, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #stream{local_done = false, blocked = false} = Stream} ->
            case queue:is_empty(Stream#stream.queue) of
                true ->
                    reply(Ack, ok),
                    {store(Id, sent(Id, Fin, Stream), State), [send_action(Id, Data, Fin)]};
                false ->
                    %% Behind a write that the next flush takes.
                    {queue(Id, Data, Fin, Ack, Stream, State), []}
            end;
        #{Id := #stream{local_done = false} = Stream} ->
            {queue(Id, Data, Fin, Ack, Stream, State), []};
        #{} ->
            reply(Ack, {error, closed}),
            {State, []}
    end;
op({consume, Id, Bytes}, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #stream{tracked = true, unconsumed = Unconsumed} = Stream} ->
            Consumed = min(Bytes, Unconsumed),
            Next = State#state{unconsumed = State#state.unconsumed - Consumed},
            {store(Id, Stream#stream{unconsumed = Unconsumed - Consumed}, Next), []};
        #{} ->
            {State, []}
    end;
op({forget, Id}, State) ->
    ok = settle_later(Id),
    {untrack(Id, State), []};
op({cancel, Alias}, #state{streams = Streams} = State) ->
    Cancelled = maps:map(
        fun(_Id, #stream{queue = Queue} = Stream) ->
            Stream#stream{queue = queue:filter(fun({_, _, Ack}) -> Ack =/= Alias end, Queue)}
        end,
        Streams
    ),
    {State#state{streams = Cancelled}, []};
op({reset_stream, Id, Code}, State) ->
    ok = settle_later(Id),
    Done = update(Id, fun(Stream) -> drop_queue(Stream#stream{local_done = true}) end, State),
    {Done, [{reset_stream, Id, Code}]};
op({stop_sending, Id, Code}, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #stream{peer_done = false, stopping = undefined} = Stream} ->
            _ = erlang:start_timer(State#state.stop_timeout, self(), {stop, Id}),
            {store(Id, Stream#stream{stopping = {Code, 0}}, State), []};
        #{} ->
            %% The client finished or reset its side, or is being stopped.
            {State, []}
    end;
op({send_datagram, Payload}, State) ->
    {State, [{send_datagram, Payload}]};
op(drain, State) ->
    {State, [drain_session]};
op(close, State) ->
    {State, [{close_session, 0, <<"server shutdown">>}]};
op(flush, State) ->
    flush(State);
op({settle, Id}, State) ->
    {settle(Id, State), []}.

send_action(Id, Data, false) -> {send, Id, Data};
send_action(Id, Data, true) -> {send, Id, Data, fin}.

%% A FIN on its way: the stream may be done once the session took it.
sent(Id, true, Stream) ->
    ok = settle_later(Id),
    Stream#stream{local_done = true};
sent(_Id, false, Stream) ->
    Stream.

queue(Id, Data, Fin, Ack, #stream{queue = Queue} = Stream, State) ->
    store(Id, Stream#stream{queue = queue:in({Data, Fin, Ack}, Queue)}, State).

%% Writes one queued write of each stream the session is not refusing,
%% and comes back for the next once the session took them: two writes of a
%% stream in one batch could be taken out of order, the second fitting
%% where the first did not.
flush(#state{streams = Streams} = State) ->
    {Next, Actions} = maps:fold(
        fun
            (Id, #stream{blocked = false, queue = Queue} = Stream, {Acc, Writes}) ->
                case queue:out(Queue) of
                    {{value, {Data, Fin, Ack}}, Rest} ->
                        reply(Ack, ok),
                        Taken = sent(Id, Fin, Stream#stream{queue = Rest}),
                        {store(Id, Taken, Acc), [send_action(Id, Data, Fin) | Writes]};
                    {empty, _} ->
                        {Acc, Writes}
                end;
            (_Id, _Stream, Acc) ->
                Acc
        end,
        {State, []},
        Streams
    ),
    _ =
        case Actions of
            [] -> ok;
            _ -> self() ! {trevrpc_wt, flush}
        end,
    {Next, Actions}.

refused(Id, Data, Fin, Reason, #state{streams = Streams} = State) when
    Reason =:= send_queue_full; Reason =:= send_buffer_full
->
    case Streams of
        #{Id := #stream{queue = Queue} = Stream} ->
            Refused = Stream#stream{
                queue = queue:in_r({Data, Fin, noack}, Queue),
                blocked = true,
                %% The FIN is not out yet.
                local_done = false
            },
            retry(store(Id, Refused, State));
        #{} ->
            State
    end;
refused(Id, _Data, _Fin, _Reason, State) ->
    %% The stream is gone; so are its queued writes.
    ok = settle_later(Id),
    update(Id, fun(Stream) -> drop_queue(Stream#stream{local_done = true}) end, State).

retry(#state{retry = undefined} = State) ->
    State#state{retry = erlang:start_timer(?RETRY_INTERVAL, self(), retry)};
retry(State) ->
    State.

drop_queue(#stream{queue = Queue} = Stream) ->
    _ = [reply(Ack, {error, closed}) || {_Data, _Fin, Ack} <- queue:to_list(Queue)],
    Stream#stream{queue = queue:new(), blocked = false}.

reply(noack, _Result) ->
    ok;
reply(Alias, Result) ->
    Alias ! {trevrpc_wt_sent, Alias, Result},
    ok.

%%% Bookkeeping

update(Id, Fun, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := Stream} -> store(Id, Fun(Stream), State);
        #{} -> State
    end.

store(Id, Stream, #state{streams = Streams} = State) ->
    State#state{streams = Streams#{Id => Stream}}.

%% Asks to forget stream `Id' once the actions of this callback ran, if
%% both sides are finished by then with nothing left to write: a FIN the
%% session refused is written again first.
settle_later(Id) ->
    self() ! {trevrpc_wt, {settle, Id}},
    ok.

%% The session delivers nothing more for a stream whose both sides
%% finished.
settle(Id, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #stream{peer_done = true, local_done = true, blocked = false, queue = Queue}} ->
            case queue:is_empty(Queue) of
                true ->
                    Untracked = untrack(Id, State),
                    Untracked#state{streams = maps:remove(Id, Streams)};
                false ->
                    State
            end;
        #{} ->
            State
    end.

%% What stream `Id' holds unread no longer counts.
untrack(Id, #state{streams = Streams, unconsumed = Total} = State) ->
    case Streams of
        #{Id := #stream{tracked = true, unconsumed = Unconsumed} = Stream} ->
            store(Id, Stream#stream{tracked = false, unconsumed = 0}, State#state{
                unconsumed = Total - Unconsumed
            });
        #{} ->
            State
    end.
