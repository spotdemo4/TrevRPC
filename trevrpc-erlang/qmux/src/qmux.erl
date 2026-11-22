%% @doc A QMux session (draft-ietf-quic-qmux-02): QUIC's streams, flow
%% control, stream limits and datagrams, carried in records over a reliable
%% byte stream, such as TLS over TCP. Applications built on QUIC streams can
%% use it as a fallback for networks that block UDP.
%%
%% A session runs over a `qmux_transport' (`qmux_ssl', `qmux_tcp', or one
%% of the application's own) and frames its records with a `qmux_framing':
%% Size-prefixed by default, or one record per message, as QMux over
%% WebSocket does (draft-lcurley-qmux-websocket-00).
%%
%% Its API and the events it sends its owner mirror erlang_quic's, so an
%% application drives native QUIC and QMux the same way. Events go to the
%% owner as `{qmux, Session, Event}':
%%
%% <ul>
%%   <li>`{connected, #{send_queue => Queue}}': the peer's transport
%%       parameters arrived; `Queue' is the session's send queue, as
%%       {@link start/4} returns it.</li>
%%   <li>`{stream_opened, Id}': the peer opened a stream, bidirectional or
%%       unidirectional (`Id band 2 =/= 0').</li>
%%   <li>`{stream_data, Id, Bin, Fin}', `{stream_reset, Id, Code}',
%%       `{stop_sending, Id, Code}'.</li>
%%   <li>`{streams_available, bidi | uni}': an {@link open_stream/1} or
%%       {@link open_unidirectional_stream/1} refused for lack of stream
%%       credit would now succeed.</li>
%%   <li>`{datagram, Bin}': a DATAGRAM frame arrived (RFC 9221).</li>
%%   <li>`send_ready': the send queue drained, after
%%       {@link notify_send_ready/1}.</li>
%%   <li>`{closed, Reason}': the session ended.</li>
%% </ul>
%%
%% The session never blocks on a write: a writer process of its own writes
%% the records to the socket, so the session reads on while a write waits
%% for the peer to read. Two sessions that both send more than the
%% sockets' buffers hold, each blocked in a write the other no longer read,
%% would otherwise deadlock. At most ?WRITE_WINDOW bytes wait in the
%% writer; the rest stays in the streams until it wrote them.
%%
%% A stream earns the peer receive credit as erlang_quic's do: in `auto'
%% flow control (the default) as its data is delivered to the owner, while
%% the owner's mailbox holds no more than `max_owner_queue' messages; in
%% `manual' flow control only for the bytes returned with {@link consume/3},
%% so an owner that stops reading stops the peer after one window. Data,
%% and a FIN, go out in order: a FIN travels with a stream's last data, so
%% a reset's final size, the bytes actually sent, always agrees with what
%% the peer saw.
%%
%% Datagrams go each way only to an end that advertised
%% max_datagram_frame_size (RFC 9221 Section 3): a session sends them once
%% the peer did, and accepts them if it did itself. As over QUIC, one may be
%% dropped: when too many wait to be sent, or when the
%% owner's mailbox holds more than `max_owner_queue' messages as it
%% arrives. Unlike over QUIC, one that is sent arrives, in order.
-module(qmux).

-behaviour(gen_server).

-export([
    start/4,
    send_data/5,
    send_data_async/5,
    queued/1,
    set_stream_flow_control/3,
    consume/3,
    reset_stream/3,
    stop_sending/3,
    open_stream/1,
    open_unidirectional_stream/1,
    send_datagram/2,
    datagram_max_size/1,
    notify_send_ready/1,
    close/2,
    peername/1,
    flow/1
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([options/0, send_queue/0, flow_control/0]).

%% Session options:
%% <ul>
%%   <li>`parameters': the transport parameters to advertise.</li>
%%   <li>`keep_alive': milliseconds without sending after which a QX_PING
%%       goes out, or `infinity' (the default).</li>
%%   <li>`send_queue_limit': the buffered bytes past which send_data/5 is
%%       refused with `send_queue_full' (default 1 MiB).</li>
%%   <li>`handshake_timeout': milliseconds for the peer's
%%       QX_TRANSPORT_PARAMETERS to arrive (default 10 s).</li>
%%   <li>`send_timeout': milliseconds a write to a peer that stopped reading
%%       may block before the connection is lost (default 30 s).</li>
%%   <li>`framing': the `qmux_framing' module that carries records
%%       (default `qmux_stream_framing', Size-prefixed records).</li>
%%   <li>`received': bytes already read from the socket, which come before
%%       what it delivers (default none).</li>
%%   <li>`stream_flow_control': how new streams earn the peer credit,
%%       `auto' (the default) or `manual'; see
%%       {@link set_stream_flow_control/3}.</li>
%%   <li>`max_owner_queue': the owner's mailbox length past which `auto'
%%       streams hold their credit back and datagrams are dropped, or
%%       `infinity' (default 1000).</li>
%% </ul>
-type options() :: #{
    parameters := qmux_frame:parameters(),
    keep_alive => pos_integer() | infinity,
    send_queue_limit => pos_integer(),
    handshake_timeout => pos_integer(),
    send_timeout => pos_integer(),
    framing => module(),
    received => binary(),
    stream_flow_control => flow_control(),
    max_owner_queue => pos_integer() | infinity
}.

-type flow_control() :: auto | manual.

%% The session's send queue, as its writers see it: the bytes written and
%% not sent yet (?QUEUED), whether the session ended (?CLOSED), and the
%% limit past which writes are refused. Writers add what they write, so
%% they never wait for the session, and the session takes off what it sends
%% or drops.
-opaque send_queue() :: {atomics:atomics_ref(), pos_integer()}.

-define(QUEUED, 1).
-define(CLOSED, 2).

-record(stream, {
    id :: non_neg_integer(),
    %% Sending: data written and not sent yet, its size, the offset sent,
    %% and the peer's limit.
    send_buf = queue:new() :: queue:queue(binary()),
    send_size = 0 :: non_neg_integer(),
    sent = 0 :: non_neg_integer(),
    send_max :: non_neg_integer(),
    fin_requested = false :: boolean(),
    fin_sent = false :: boolean(),
    reset_sent = false :: boolean(),
    %% Receiving: the offset received, our limit and window, the offset
    %% consumed, and the final size once known.
    received = 0 :: non_neg_integer(),
    recv_max :: non_neg_integer(),
    recv_window :: non_neg_integer(),
    read = 0 :: non_neg_integer(),
    final_size :: non_neg_integer() | undefined,
    %% Credit comes back as data is delivered, not with consume/3.
    auto :: boolean(),
    %% The owner was given the stream's end: FIN or reset.
    recv_ended = false :: boolean(),
    %% The owner stopped reading: later data is consumed at once.
    read_stopped = false :: boolean()
}).

%% The streams of one direction: those we opened and may open, and whether
%% the owner waits for more; those the peer opened and may open, and
%% whether a MAX_STREAMS raising that limit is due.
-record(limits, {
    opened_local = 0 :: non_neg_integer(),
    max_local = 0 :: non_neg_integer(),
    want = false :: boolean(),
    opened_remote = 0 :: non_neg_integer(),
    max_remote = 0 :: non_neg_integer(),
    send_max = false :: boolean()
}).

-record(state, {
    transport :: module(),
    socket :: qmux_transport:socket() | undefined,
    %% The tags of the socket's data, closed and error messages.
    messages :: {atom(), atom(), atom()},
    %% The process that writes to the socket, its monitor, and the bytes
    %% handed to it and not written yet.
    writer :: pid() | undefined,
    writer_monitor :: reference() | undefined,
    unwritten = 0 :: non_neg_integer(),
    %% What the framing owes the peer for its latest message, such as a
    %% WebSocket Pong, waiting for room in the writer.
    pong = [] :: iodata(),
    role :: client | server,
    owner :: pid(),
    max_owner_queue :: pos_integer() | infinity,
    local :: qmux_frame:parameters(),
    peer :: qmux_frame:parameters() | undefined,
    max_record_size :: pos_integer(),
    framing :: qmux_framing:t(),
    %% Bytes read before the session took the socket over.
    received = <<>> :: binary(),
    streams = #{} :: #{non_neg_integer() => #stream{}},
    bidi :: #limits{},
    uni :: #limits{},
    auto :: boolean(),
    %% Auto streams whose credit waits for the owner to catch up, and the
    %% timer that checks again.
    withheld = #{} :: #{non_neg_integer() => true},
    credit_timer = false :: boolean(),
    %% Connection flow control.
    conn_send_max = 0 :: non_neg_integer(),
    conn_sent = 0 :: non_neg_integer(),
    conn_recv_max :: non_neg_integer(),
    conn_recv_window :: non_neg_integer(),
    conn_received = 0 :: non_neg_integer(),
    conn_consumed = 0 :: non_neg_integer(),
    send_max_data = false :: boolean(),
    %% Frames to send before stream data, oldest first, and streams with
    %% data or a FIN to send, in turn.
    control = [] :: [iodata()],
    ready = queue:new() :: queue:queue(non_neg_integer()),
    %% DATAGRAM frames to send, and how many; and whether stream data goes
    %% before them in the next record, as the two take turns.
    datagrams = queue:new() :: queue:queue(iodata()),
    datagrams_queued = 0 :: non_neg_integer(),
    streams_first = false :: boolean(),
    send_queue :: send_queue(),
    notify_ready = false :: boolean(),
    ping_sent = 0 :: non_neg_integer(),
    ping_received :: non_neg_integer() | undefined,
    send_ping = false :: boolean(),
    send_ping_reply = false :: boolean(),
    idle_timeout = infinity :: pos_integer() | infinity,
    keep_alive :: pos_integer() | infinity,
    handshake_timeout :: pos_integer(),
    send_timeout :: pos_integer(),
    last_received :: integer(),
    last_sent :: integer(),
    %% A `flush' message is on its way to this process.
    flush_pending = false :: boolean(),
    closing = false :: boolean()
}).

-define(MAX_SEND_RECORD, 65536).
-define(WRITE_WINDOW, (4 * ?MAX_SEND_RECORD)).
-define(QUANTUM, 16384).
-define(CLOSE_TIMEOUT, 1000).
-define(DEFAULT_QUEUE_LIMIT, 1048576).
-define(DEFAULT_MAX_OWNER_QUEUE, 1000).
%% The DATAGRAM frames waiting to be sent, past which new ones are dropped,
%% as RFC 9221 Section 5 lets a sender do.
-define(DATAGRAM_QUEUE, 128).
%% Milliseconds before withheld credit is looked at again.
-define(CREDIT_RETRY, 10).

%%% API

%% @doc Starts a session over `Socket', a connection of `Transport' (see
%% `qmux_transport') that is not in active mode, owned by `Owner', and
%% returns it with the send queue its writers pass to {@link send_data/5}.
%% The caller must be the socket's controlling process; the session takes
%% it over.
-spec start(qmux_transport:t(), client | server, pid(), options()) ->
    {ok, pid(), send_queue()} | {error, term()}.
start({Transport, Socket}, Role, Owner, Opts) ->
    Queue = {
        atomics:new(2, [{signed, true}]), maps:get(send_queue_limit, Opts, ?DEFAULT_QUEUE_LIMIT)
    },
    case gen_server:start(?MODULE, {Transport, Role, Owner, Queue, Opts}, []) of
        {ok, Pid} ->
            case Transport:controlling_process(Socket, Pid) of
                ok ->
                    gen_server:cast(Pid, {socket, Socket}),
                    {ok, Pid, Queue};
                {error, _} = Error ->
                    exit(Pid, kill),
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Writes data to a stream, finishing it with `Fin', without waiting
%% for the session. Refused with `send_queue_full' while the session's send
%% queue is over its limit, and with `closed' once the session ended. Data
%% written to a stream that is already reset or finished, or that only the
%% peer sends on, is dropped, as the peer would never read it.
-spec send_data(pid(), send_queue(), non_neg_integer(), iodata(), boolean()) ->
    ok | {error, send_queue_full | closed}.
send_data(Session, {Ref, Limit}, Id, Data, Fin) ->
    case atomics:get(Ref, ?CLOSED) of
        0 ->
            case reserve(Ref, Limit, iolist_size(Data)) of
                %% The writer copies its data into one binary, so the
                %% session does not.
                ok -> gen_server:cast(Session, {send, Id, iolist_to_binary(Data), Fin});
                full -> {error, send_queue_full}
            end;
        _ ->
            {error, closed}
    end.

%% Takes Size bytes into the queue while it is under Limit. A write is
%% taken only if the queue, with every write taken before it, is under the
%% limit, so concurrent writers overshoot it by one write at most, as when
%% the session checked each write itself; and a refused write never shows
%% in the queue, which the session reads to wake refused writers.
reserve(Ref, Limit, Size) ->
    case atomics:get(Ref, ?QUEUED) of
        Queued when Queued < Limit ->
            case atomics:compare_exchange(Ref, ?QUEUED, Queued, Queued + Size) of
                ok -> ok;
                _ -> reserve(Ref, Limit, Size)
            end;
        _ ->
            full
    end.

%% @doc The bytes written to the session and not sent yet.
-spec queued(send_queue()) -> non_neg_integer().
queued({Ref, _}) ->
    atomics:get(Ref, ?QUEUED).

%% @doc Writes data to a stream without waiting, whatever the send queue
%% holds.
-spec send_data_async(pid(), send_queue(), non_neg_integer(), iodata(), boolean()) -> ok.
send_data_async(Session, {Ref, _}, Id, Data, Fin) ->
    Bin = iolist_to_binary(Data),
    atomics:add(Ref, ?QUEUED, byte_size(Bin)),
    gen_server:cast(Session, {send, Id, Bin, Fin}).

%% @doc Chooses how a stream earns the peer receive credit. `auto' grants
%% it as data is delivered to the owner, while the owner's mailbox holds no
%% more than `max_owner_queue' messages; `manual' only for the bytes the
%% owner returns with {@link consume/3}. Bytes delivered before a switch to
%% `auto' are consumed at once.
-spec set_stream_flow_control(pid(), non_neg_integer(), flow_control()) ->
    ok | {error, unknown_stream | closed}.
set_stream_flow_control(Session, Id, Mode) when Mode =:= auto; Mode =:= manual ->
    call(Session, {flow_control, Id, Mode =:= auto}).

%% @doc Returns receive credit for `Bytes' of a `manual' stream the owner
%% processed; a no-op on an `auto' stream.
-spec consume(pid(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
consume(Session, Id, Bytes) ->
    call(Session, {consume, Id, Bytes}).

%% @doc Resets the sending side of a stream.
-spec reset_stream(pid(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
reset_stream(Session, Id, Code) ->
    call(Session, {reset_stream, Id, Code}).

%% @doc Stops reading a stream: its data is consumed as it arrives, and the
%% peer is asked to stop sending.
-spec stop_sending(pid(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
stop_sending(Session, Id, Code) ->
    call(Session, {stop_sending, Id, Code}).

%% @doc Opens a bidirectional stream. Refused with `stream_limit' without
%% stream credit; `{streams_available, bidi}' follows once there is some.
-spec open_stream(pid()) -> {ok, non_neg_integer()} | {error, term()}.
open_stream(Session) ->
    call(Session, {open_stream, bidi}).

%% @doc Opens a unidirectional stream, which only this end sends on.
%% Refused with `stream_limit' without stream credit;
%% `{streams_available, uni}' follows once there is some.
-spec open_unidirectional_stream(pid()) -> {ok, non_neg_integer()} | {error, term()}.
open_unidirectional_stream(Session) ->
    call(Session, {open_stream, uni}).

%% @doc Sends `Data' in a DATAGRAM frame (RFC 9221). Refused with
%% `datagrams_not_supported' unless the peer advertised
%% max_datagram_frame_size, and with `datagram_too_large' when it does not
%% fit in one (see {@link datagram_max_size/1}). Like a datagram over
%% QUIC, it may be dropped when too many wait to be sent.
-spec send_datagram(pid(), iodata()) ->
    ok | {error, datagrams_not_supported | datagram_too_large | not_connected | closed}.
send_datagram(Session, Data) ->
    call(Session, {send_datagram, iolist_to_binary(Data)}).

%% @doc The largest payload {@link send_datagram/2} accepts, or 0 when the
%% peer accepts no datagrams.
-spec datagram_max_size(pid()) -> non_neg_integer() | {error, closed}.
datagram_max_size(Session) ->
    call(Session, datagram_max_size).

%% @doc Asks for `send_ready' once the send queue is under half its limit
%% (at once if it already is).
-spec notify_send_ready(pid()) -> ok.
notify_send_ready(Session) ->
    gen_server:cast(Session, notify_send_ready).

%% @doc Closes the session with an application CONNECTION_CLOSE.
-spec close(pid(), non_neg_integer()) -> ok.
close(Session, Code) ->
    gen_server:cast(Session, {close, Code}).

-spec peername(pid()) -> {ok, {inet:ip_address(), inet:port_number()}} | {error, term()}.
peername(Session) ->
    call(Session, peername).

%% @doc The connection's receive flow control: the limit granted to the
%% peer, and the bytes received and returned as credit, for diagnostics.
-spec flow(pid()) ->
    #{recv_max := non_neg_integer(), received := non_neg_integer(), consumed := non_neg_integer()}
    | {error, closed}.
flow(Session) ->
    call(Session, flow).

call(Session, Request) ->
    try
        gen_server:call(Session, Request, infinity)
    catch
        exit:_ -> {error, closed}
    end.

%%% gen_server

-spec init({module(), client | server, pid(), send_queue(), options()}) -> {ok, #state{}}.
init({Transport, Role, Owner, Queue, Opts}) ->
    %% Every write of every stream queues here; under load the queue runs
    %% thousands deep, and kept off the heap it is not copied by each
    %% garbage collection.
    process_flag(message_queue_data, off_heap),
    monitor(process, Owner),
    Local = maps:get(parameters, Opts),
    Now = now_ms(),
    InitialData = maps:get(initial_max_data, Local, 0),
    MaxRecordSize = maps:get(max_record_size, Local, qmux_frame:default_max_record_size()),
    {ok, #state{
        transport = Transport,
        messages = Transport:messages(),
        role = Role,
        owner = Owner,
        max_owner_queue = maps:get(max_owner_queue, Opts, ?DEFAULT_MAX_OWNER_QUEUE),
        local = Local,
        max_record_size = MaxRecordSize,
        framing = qmux_framing:new(
            maps:get(framing, Opts, qmux_stream_framing), Role, MaxRecordSize
        ),
        received = maps:get(received, Opts, <<>>),
        bidi = #limits{max_remote = maps:get(initial_max_streams_bidi, Local, 0)},
        uni = #limits{max_remote = maps:get(initial_max_streams_uni, Local, 0)},
        auto = maps:get(stream_flow_control, Opts, auto) =:= auto,
        conn_recv_max = InitialData,
        conn_recv_window = InitialData,
        send_queue = Queue,
        keep_alive = maps:get(keep_alive, Opts, infinity),
        handshake_timeout = maps:get(handshake_timeout, Opts, 10000),
        send_timeout = maps:get(send_timeout, Opts, 30000),
        last_received = Now,
        last_sent = Now,
        control = [qmux_frame:transport_parameters(Local)]
    }}.

-spec handle_call(term(), gen_server:from(), #state{}) ->
    {reply, term(), #state{}} | {stop, term(), term(), #state{}}.
handle_call(_Request, _From, #state{closing = true} = State) ->
    {reply, {error, closed}, State};
handle_call({consume, Id, Bytes}, _From, State) ->
    case State#state.streams of
        #{Id := #stream{auto = true}} -> {reply, ok, State};
        #{Id := Stream} -> reply(ok, consumed(Bytes, Stream, State));
        #{} -> {reply, {error, unknown_stream}, State}
    end;
handle_call({flow_control, Id, Auto}, _From, State) ->
    case State#state.streams of
        #{Id := #stream{received = Received, read = Read} = Stream} when Auto ->
            reply(ok, consumed(Received - Read, Stream#stream{auto = true}, State));
        #{Id := Stream} ->
            {reply, ok, store(Stream#stream{auto = Auto}, State)};
        #{} ->
            {reply, {error, unknown_stream}, State}
    end;
handle_call({reset_stream, Id, Code}, _From, State) ->
    case State#state.streams of
        #{Id := Stream} -> reply(ok, reset_send(Code, Stream, State));
        #{} -> {reply, {error, unknown_stream}, State}
    end;
handle_call({stop_sending, Id, Code}, _From, State) ->
    case State#state.streams of
        #{Id := Stream} -> reply(ok, stop_reading(Code, Stream, State));
        #{} -> {reply, {error, unknown_stream}, State}
    end;
handle_call({open_stream, _Direction}, _From, #state{peer = undefined} = State) ->
    {reply, {error, not_connected}, State};
handle_call({open_stream, Direction}, _From, State) ->
    case open_local(Direction, State) of
        {ok, Id, Next} -> {reply, {ok, Id}, Next};
        {error, stream_limit, Next} -> {reply, {error, stream_limit}, Next}
    end;
handle_call({send_datagram, _Data}, _From, #state{peer = undefined} = State) ->
    {reply, {error, not_connected}, State};
handle_call({send_datagram, Data}, _From, State) ->
    case datagram_limit(State) of
        0 ->
            {reply, {error, datagrams_not_supported}, State};
        Max when byte_size(Data) > Max ->
            {reply, {error, datagram_too_large}, State};
        _ when State#state.datagrams_queued >= ?DATAGRAM_QUEUE ->
            {reply, ok, State};
        _ ->
            reply(ok, State#state{
                datagrams = queue:in(qmux_frame:datagram(Data), State#state.datagrams),
                datagrams_queued = State#state.datagrams_queued + 1
            })
    end;
handle_call(datagram_max_size, _From, #state{peer = undefined} = State) ->
    {reply, 0, State};
handle_call(datagram_max_size, _From, State) ->
    {reply, datagram_limit(State), State};
handle_call(flow, _From, State) ->
    {reply,
        #{
            recv_max => State#state.conn_recv_max,
            received => State#state.conn_received,
            consumed => State#state.conn_consumed
        },
        State};
handle_call(peername, _From, #state{transport = Transport, socket = Socket} = State) ->
    {reply, Transport:peername(Socket), State}.

reply(Reply, State) ->
    {reply, Reply, schedule_flush(State)}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}} | {stop, normal, #state{}}.
handle_cast(
    {socket, Socket},
    #state{transport = Transport, handshake_timeout = Handshake, send_timeout = Send} = State
) ->
    %% A write that blocks longer than the send timeout closes the socket, so
    %% a peer that stops reading cannot hold this process, its timers and its
    %% callers.
    case
        Transport:setopts(Socket, [
            {mode, binary}, {send_timeout, Send}, {send_timeout_close, true}
        ])
    of
        ok ->
            erlang:send_after(Handshake, self(), handshake_timeout),
            Session = self(),
            {Writer, Monitor} = spawn_opt(
                fun() -> writer(Session, Transport, Socket) end, [monitor]
            ),
            received(State#state.received, State#state{
                socket = Socket, received = <<>>, writer = Writer, writer_monitor = Monitor
            });
        {error, Reason} ->
            {stop, normal, lost({connection_lost, Reason}, State#state{socket = Socket})}
    end;
handle_cast(_Request, #state{closing = true} = State) ->
    {noreply, State};
handle_cast({send, Id, Bin, Fin}, State) ->
    noreply(write(Id, Bin, Fin, State));
handle_cast(notify_send_ready, State) ->
    noreply(maybe_send_ready(State#state{notify_ready = true}));
handle_cast({close, Code}, State) ->
    close_with(qmux_frame:application_close(Code, <<>>), normal, State).

-spec handle_info(term(), #state{}) -> {noreply, #state{}} | {stop, normal, #state{}}.
handle_info(
    {Tag, Socket, _Data},
    #state{socket = Socket, messages = {Tag, _, _}, closing = true} = State
) ->
    %% Draining after CONNECTION_CLOSE.
    {noreply, State};
handle_info({Tag, Socket, Data}, #state{socket = Socket, messages = {Tag, _, _}} = State) ->
    received(Data, State#state{last_received = now_ms()});
handle_info({Tag, Socket}, #state{socket = Socket, messages = {_, Tag, _}, closing = true} = State) ->
    {stop, normal, State};
handle_info({Tag, Socket}, #state{socket = Socket, messages = {_, Tag, _}} = State) ->
    {stop, normal, lost(connection_lost, State)};
handle_info({Tag, Socket, Reason}, #state{socket = Socket, messages = {_, _, Tag}} = State) ->
    {stop, normal, lost({connection_lost, Reason}, State)};
handle_info(flush, State) ->
    {noreply, flush(State#state{flush_pending = false})};
handle_info({written, Writer, Bytes}, #state{writer = Writer, unwritten = Unwritten} = State) ->
    %% Room in the writer: what waited for it goes now.
    noreply(State#state{unwritten = Unwritten - Bytes});
handle_info(
    {'DOWN', Monitor, process, _Writer, _Reason},
    #state{writer_monitor = Monitor, closing = true} = State
) ->
    {noreply, State#state{writer = undefined, writer_monitor = undefined}};
handle_info({'DOWN', Monitor, process, _Writer, Reason}, #state{writer_monitor = Monitor} = State) ->
    Lost =
        case Reason of
            {write_failed, Error} -> {connection_lost, Error};
            _ -> {connection_lost, Reason}
        end,
    {stop, normal, lost(Lost, State#state{writer = undefined, writer_monitor = undefined})};
handle_info(credit_retry, #state{closing = false} = State) ->
    noreply(release_credit(State#state{credit_timer = false}));
handle_info(tick, #state{closing = false} = State) ->
    tick(State);
handle_info(close_timeout, State) ->
    {stop, normal, State};
handle_info(handshake_timeout, #state{peer = undefined, closing = false} = State) ->
    %% The idle timer starts with the peer's parameters; until then this one
    %% bounds a peer that sends nothing.
    quiet_close(handshake_timeout, State);
handle_info(
    {'DOWN', _Monitor, process, Owner, _Reason}, #state{owner = Owner, closing = false} = State
) ->
    close_with(qmux_frame:application_close(0, <<>>), normal, State);
handle_info(_Message, State) ->
    {noreply, State}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, #state{socket = undefined} = State) ->
    mark_closed(State);
terminate(_Reason, #state{transport = Transport, socket = Socket} = State) ->
    mark_closed(State),
    %% What the writer holds, such as a WebSocket close, goes out first,
    %% for a moment at most. A write that still blocks then holds the
    %% close, as ssl closes behind its sender, until the send timeout.
    stop_writer(State),
    _ = Transport:close(Socket),
    ok.

stop_writer(#state{writer = undefined}) ->
    ok;
stop_writer(#state{writer = Writer, writer_monitor = Monitor}) ->
    Writer ! stop,
    receive
        {'DOWN', Monitor, process, Writer, _} -> ok
    after ?CLOSE_TIMEOUT -> ok
    end.

%% The writer: writes what the session hands it, in order, what piled up
%% meanwhile in one write, and tells the session how much it wrote. A write
%% that fails, or blocks past the send timeout, which closes the socket,
%% ends it, and the session learns of it from its exit. `shutdown' ends the
%% sending direction and `stop' the writer, after what was handed before.
writer(Session, Transport, Socket) ->
    writer_loop(Session, {Transport, Socket}, monitor(process, Session)).

writer_loop(Session, {Transport, Socket} = Out, Monitor) ->
    receive
        {write, Data, Size} ->
            write_batch(Session, Out, Monitor, [Data], Size);
        shutdown ->
            _ = Transport:shutdown(Socket, write),
            writer_loop(Session, Out, Monitor);
        stop ->
            ok;
        {'DOWN', Monitor, process, Session, _} ->
            ok
    end.

write_batch(Session, Out, Monitor, Batch, Size) when Size < ?WRITE_WINDOW ->
    receive
        {write, Data, More} -> write_batch(Session, Out, Monitor, [Data | Batch], Size + More)
    after 0 -> write_out(Session, Out, Monitor, Batch, Size)
    end;
write_batch(Session, Out, Monitor, Batch, Size) ->
    write_out(Session, Out, Monitor, Batch, Size).

write_out(Session, {Transport, Socket} = Out, Monitor, Batch, Size) ->
    case Transport:send(Socket, lists:reverse(Batch)) of
        ok ->
            Session ! {written, self(), Size},
            writer_loop(Session, Out, Monitor);
        {error, Reason} ->
            exit({write_failed, Reason})
    end.

noreply(State) ->
    {noreply, schedule_flush(State)}.

%% Writes are coalesced: whatever a request or a record queues to send goes
%% out with a `flush' message the session sends itself, which arrives after
%% every request and record already in its mailbox. Under load, one record,
%% one TLS record and one write carry the frames of every call queued by
%% then, instead of one each.
schedule_flush(#state{flush_pending = true} = State) ->
    State;
schedule_flush(State) ->
    self() ! flush,
    State#state{flush_pending = true}.

%%% Receiving

%% Handles bytes from the socket, then reads on.
received(Data, #state{transport = Transport} = State) ->
    {Records, Replies, Then, Framing} =
        case qmux_framing:decode(Data, State#state.framing) of
            {ok, R, P, F} -> {R, P, read_on, F};
            {closed, Why, R, P, F} -> {R, P, {closed, Why}, F};
            {error, Error, R, P, F} -> {R, P, {error, Error}, F}
        end,
    Result = records(Records, State#state{framing = Framing}),
    After =
        case Result of
            {ok, S} -> S;
            {_, _, S} -> S
        end,
    Next =
        case Then of
            read_on -> owe_pong(Replies, After);
            _ -> send_now(Replies, After)
        end,
    case {Result, Then} of
        {{error, {Code, FrameType, Reason}, _}, _} ->
            fail(Code, FrameType, Reason, Next);
        {{closed, Reason, _}, _} ->
            quiet_close(Reason, Next);
        {{ok, _}, {error, {Code, FrameType, Reason}}} ->
            fail(Code, FrameType, Reason, Next);
        {{ok, _}, {closed, Reason}} ->
            quiet_close(Reason, Next);
        {{ok, _}, read_on} ->
            case Transport:setopts(Next#state.socket, [{active, once}]) of
                ok -> noreply(Next);
                {error, Reason} -> {stop, normal, lost({connection_lost, Reason}, Next)}
            end
    end.

%% Sends what the framing answers the peer with as the session ends, at
%% once, whatever the writer holds.
send_now([], State) ->
    State;
send_now(Bytes, State) ->
    hand(Bytes, iolist_size(Bytes), State).

%% What the framing answers the peer's messages with, such as the Pongs that
%% answer WebSocket Pings, goes with the next flush that finds room in the
%% writer. Only the latest is owed, as the qmux_framing contract says (RFC
%% 6455 Section 5.5.3 for Pongs), so a peer that sends Pings and reads
%% nothing, once the writer holds its ?WRITE_WINDOW, adds one owed Pong, not
%% a queue of them, while the session reads on.
owe_pong([], State) ->
    State;
owe_pong(Pongs, State) ->
    State#state{pong = lists:last(Pongs)}.

records([], State) ->
    {ok, State};
records([Record | Rest], State) ->
    case
        qmux_frame:decode_frames(Record, maps:get(max_datagram_frame_size, State#state.local, 0))
    of
        {ok, Frames} ->
            case frames(Frames, State) of
                {ok, Next} -> records(Rest, Next);
                Other -> Other
            end;
        {error, Error} ->
            {error, Error, State}
    end.

frames([], State) ->
    {ok, State};
frames([{transport_parameters, Body} | Rest], #state{peer = undefined} = State) ->
    case qmux_frame:decode_parameters(Body) of
        {ok, Peer} -> frames(Rest, connected(Peer, State));
        {error, Error} -> {error, Error, State}
    end;
frames([Frame | _], #state{peer = undefined} = State) ->
    {error,
        {protocol_violation, frame_type(Frame), <<"first frame is not QX_TRANSPORT_PARAMETERS">>},
        State};
frames([Frame | Rest], State) ->
    case frame(Frame, State) of
        {ok, Next} -> frames(Rest, Next);
        Other -> Other
    end.

connected(Peer, #state{local = Local, owner = Owner, bidi = Bidi, uni = Uni} = State) ->
    Idle =
        case {maps:get(max_idle_timeout, Local, 0), maps:get(max_idle_timeout, Peer, 0)} of
            {0, 0} -> infinity;
            {0, Theirs} -> Theirs;
            {Ours, 0} -> Ours;
            {Ours, Theirs} -> min(Ours, Theirs)
        end,
    Owner ! {qmux, self(), {connected, #{send_queue => State#state.send_queue}}},
    Next = State#state{
        peer = Peer,
        bidi = Bidi#limits{max_local = maps:get(initial_max_streams_bidi, Peer, 0)},
        uni = Uni#limits{max_local = maps:get(initial_max_streams_uni, Peer, 0)},
        conn_send_max = maps:get(initial_max_data, Peer, 0),
        idle_timeout = Idle
    },
    schedule_tick(Next),
    Next.

frame({transport_parameters, _}, State) ->
    {error, {protocol_violation, 16#3f5153300d0a0d0a, <<"repeated QX_TRANSPORT_PARAMETERS">>},
        State};
frame({stream, Id, Offset, Data, Fin}, State) ->
    with_stream(Id, 16#08, recv, State, fun
        (undefined, S) when Data =:= <<>> -> {ok, S};
        (undefined, S) -> {error, {final_size, 16#08, <<"data beyond a stream's final size">>}, S};
        (Stream, S) -> stream_data(Stream, Offset, Data, Fin, S)
    end);
frame({reset_stream, Id, Code, Final}, State) ->
    with_stream(Id, 16#04, recv, State, fun
        (undefined, S) -> {ok, S};
        (Stream, S) -> reset_received(Stream, Code, Final, S)
    end);
frame({stop_sending, Id, Code}, State) ->
    with_stream(Id, 16#05, send, State, fun
        (undefined, S) ->
            {ok, S};
        (#stream{fin_sent = true}, S) ->
            {ok, S};
        (#stream{reset_sent = true}, S) ->
            {ok, S};
        (Stream, S) ->
            S#state.owner ! {qmux, self(), {stop_sending, Id, Code}},
            {ok, reset_send(Code, Stream, S)}
    end);
frame({max_data, Max}, #state{conn_send_max = Current} = State) when Max > Current ->
    %% Streams that stopped for lack of connection credit may go on.
    Blocked = [
        Id
     || Id := #stream{reset_sent = false, fin_sent = false} = Stream <- State#state.streams,
        Stream#stream.send_size > 0 orelse Stream#stream.fin_requested
    ],
    {ok, lists:foldl(fun ready/2, State#state{conn_send_max = Max}, Blocked)};
frame({max_data, _Max}, State) ->
    {ok, State};
frame({max_stream_data, Id, Max}, State) ->
    with_stream(Id, 16#11, send, State, fun
        (undefined, S) ->
            {ok, S};
        (#stream{send_max = Current} = Stream, S) when Max > Current ->
            {ok, ready(Id, store(Stream#stream{send_max = Max}, S))};
        (_Stream, S) ->
            {ok, S}
    end);
frame({max_streams, Direction, Max}, State) ->
    #limits{max_local = Current} = Limits = limits(Direction, State),
    case Max > qmux_frame:max_streams_limit() of
        true ->
            {error, {frame_encoding, max_streams_type(Direction), <<"MAX_STREAMS exceeds 2^60">>},
                State};
        false when Max > Current ->
            {ok, streams_available(Direction, Limits#limits{max_local = Max}, State)};
        false ->
            {ok, State}
    end;
frame({streams_blocked, _Direction, Max}, State) ->
    case Max > qmux_frame:max_streams_limit() of
        true -> {error, {frame_encoding, 16#16, <<"STREAMS_BLOCKED exceeds 2^60">>}, State};
        false -> {ok, State}
    end;
frame({data_blocked, _}, State) ->
    {ok, State};
frame({stream_data_blocked, Id, _}, State) ->
    with_stream(Id, 16#15, recv, State, fun(_, S) -> {ok, S} end);
frame({datagram, Data}, State) ->
    %% Datagrams are unreliable (RFC 9221 Section 5): one the owner has no
    %% room for is dropped, as nothing else stops a peer from filling its
    %% mailbox.
    _ =
        case owner_behind(State) of
            true -> dropped;
            false -> State#state.owner ! {qmux, self(), {datagram, Data}}
        end,
    {ok, State};
frame({connection_close, transport, Code, FrameType, Reason}, State) ->
    {closed, {peer_closed, transport, Code, FrameType, Reason}, State};
frame({connection_close, application, Code, Reason}, State) ->
    {closed, {peer_closed, application, Code, Reason}, State};
frame({ping, Sequence}, #state{ping_received = Last} = State) when
    Last =/= undefined, Sequence =< Last
->
    {error,
        {protocol_violation, 16#348c67529ef8c7bd, <<"QX_PING sequence number is not increasing">>},
        State};
frame({ping, Sequence}, State) ->
    {ok, State#state{ping_received = Sequence, send_ping_reply = true}};
frame({ping_response, Sequence}, #state{ping_sent = Sent} = State) when Sequence > Sent ->
    {error, {protocol_violation, 16#348c67529ef8c7be, <<"QX_PING response was never requested">>},
        State};
frame({ping_response, _}, State) ->
    {ok, State}.

frame_type({stream, _, _, _, _}) -> 16#08;
frame_type(_) -> 0.

max_streams_type(bidi) -> 16#12;
max_streams_type(uni) -> 16#13.

%% Looks a stream up for a frame about its `recv' or `send' side, opening
%% peer streams up to it. Calls Fun with the stream, or undefined for one
%% that already completed, and the state that opened it. A frame about a
%% side a unidirectional stream does not have is a STREAM_STATE_ERROR
%% (RFC 9000 Section 19).
with_stream(Id, FrameType, Side, State, Fun) ->
    Local = Id band 1 =:= role_bit(State#state.role),
    Direction = direction(Id),
    Number = Id bsr 2,
    #limits{opened_local = OpenedLocal, opened_remote = OpenedRemote, max_remote = MaxRemote} =
        limits(Direction, State),
    if
        Direction =:= uni, Local, Side =:= recv ->
            {error, {stream_state, FrameType, <<"frame for a send-only stream">>}, State};
        Direction =:= uni, not Local, Side =:= send ->
            {error, {stream_state, FrameType, <<"frame for a receive-only stream">>}, State};
        Local, Number >= OpenedLocal ->
            {error, {stream_state, FrameType, <<"stream was never opened">>}, State};
        Local ->
            Fun(maps:get(Id, State#state.streams, undefined), State);
        Number < OpenedRemote ->
            Fun(maps:get(Id, State#state.streams, undefined), State);
        Number >= MaxRemote ->
            {error, {stream_limit, FrameType, <<"stream exceeds the stream limit">>}, State};
        true ->
            Opened = open_remote(Number, Direction, State),
            with_stream(Id, FrameType, Side, Opened, Fun)
    end.

%% Opens the peer's streams of Direction up to Number.
open_remote(Number, Direction, #state{owner = Owner} = State) ->
    case limits(Direction, State) of
        #limits{opened_remote = Opened} when Opened > Number ->
            State;
        #limits{opened_remote = Opened} = Limits ->
            Id = stream_id(Opened, Direction, peer_role(State#state.role)),
            Stream =
                case Direction of
                    bidi ->
                        new_stream(
                            Id,
                            bidi,
                            maps:get(initial_max_stream_data_bidi_local, State#state.peer, 0),
                            maps:get(initial_max_stream_data_bidi_remote, State#state.local, 0),
                            State
                        );
                    uni ->
                        new_stream(
                            Id,
                            recv_only,
                            0,
                            maps:get(initial_max_stream_data_uni, State#state.local, 0),
                            State
                        )
                end,
            Owner ! {qmux, self(), {stream_opened, Id}},
            Next = set_limits(Direction, Limits#limits{opened_remote = Opened + 1}, State),
            open_remote(Number, Direction, Next#state{
                streams = (Next#state.streams)#{Id => Stream}
            })
    end.

%% Opens a stream of ours, if the peer allows another.
open_local(Direction, State) ->
    case limits(Direction, State) of
        #limits{opened_local = Opened, max_local = Max} = Limits when Opened < Max ->
            Id = stream_id(Opened, Direction, State#state.role),
            Stream =
                case Direction of
                    bidi ->
                        new_stream(
                            Id,
                            bidi,
                            maps:get(initial_max_stream_data_bidi_remote, State#state.peer, 0),
                            maps:get(initial_max_stream_data_bidi_local, State#state.local, 0),
                            State
                        );
                    uni ->
                        new_stream(
                            Id,
                            send_only,
                            maps:get(initial_max_stream_data_uni, State#state.peer, 0),
                            0,
                            State
                        )
                end,
            Next = set_limits(Direction, Limits#limits{opened_local = Opened + 1}, State),
            {ok, Id, Next#state{streams = (Next#state.streams)#{Id => Stream}}};
        Limits ->
            {error, stream_limit, set_limits(Direction, Limits#limits{want = true}, State)}
    end.

stream_data(
    #stream{id = Id, final_size = Final, received = Received} = Stream, Offset, Data, Fin, State
) ->
    End = Offset + byte_size(Data),
    if
        Final =/= undefined, End > Final orelse (Fin andalso End =/= Final) ->
            {error, {final_size, 16#08, <<"data beyond a stream's final size">>}, State};
        Offset =/= Received ->
            {error, {protocol_violation, 16#08, <<"stream data out of order">>}, State};
        End > Stream#stream.recv_max ->
            {error, {flow_control, 16#08, <<"stream flow control limit exceeded">>}, State};
        State#state.conn_received + byte_size(Data) > State#state.conn_recv_max ->
            {error, {flow_control, 16#08, <<"connection flow control limit exceeded">>}, State};
        true ->
            Updated = Stream#stream{
                received = End,
                final_size =
                    case Fin of
                        true -> End;
                        false -> Final
                    end
            },
            Counted = State#state{conn_received = State#state.conn_received + byte_size(Data)},
            case Updated#stream.read_stopped orelse Updated#stream.recv_ended of
                true ->
                    {ok, consumed(byte_size(Data), Updated, Counted)};
                false ->
                    State#state.owner ! {qmux, self(), {stream_data, Id, Data, Fin}},
                    {ok, delivered(Updated#stream{recv_ended = Fin}, Counted)}
            end
    end.

reset_received(
    #stream{id = Id, final_size = Final, received = Received} = Stream, Code, Size, State
) ->
    if
        Stream#stream.recv_ended, Final =:= Size ->
            {ok, State};
        Final =/= undefined, Final =/= Size; Size < Received ->
            {error, {final_size, 16#04, <<"stream reset with an inconsistent final size">>}, State};
        Size > Stream#stream.recv_max ->
            {error, {flow_control, 16#04, <<"stream final size exceeds its flow control limit">>},
                State};
        State#state.conn_received + (Size - Received) > State#state.conn_recv_max ->
            {error, {flow_control, 16#04, <<"connection flow control limit exceeded">>}, State};
        true ->
            case Stream#stream.read_stopped orelse Stream#stream.recv_ended of
                true ->
                    ok;
                false ->
                    State#state.owner ! {qmux, self(), {stream_reset, Id, Code}},
                    ok
            end,
            Counted = State#state{conn_received = State#state.conn_received + (Size - Received)},
            Ended = Stream#stream{received = Size, final_size = Size, recv_ended = true},
            %% Nothing will read the rest: its credit comes back now.
            {ok, consumed(Size - Stream#stream.read, Ended, Counted)}
    end.

%%% Flow control and stream state

%% Stores a stream after delivering data to the owner: an auto stream's
%% credit comes back at once, unless the owner is behind, when it waits.
delivered(#stream{auto = false} = Stream, State) ->
    store(Stream, State);
delivered(#stream{id = Id, received = Received, read = Read} = Stream, State) ->
    case owner_behind(State) of
        false ->
            consumed(Received - Read, Stream, State);
        true ->
            withhold(Id, store(Stream, State))
    end.

withhold(Id, #state{withheld = Withheld} = State) ->
    schedule_credit_retry(State#state{withheld = Withheld#{Id => true}}).

schedule_credit_retry(#state{credit_timer = true} = State) ->
    State;
schedule_credit_retry(State) ->
    erlang:send_after(?CREDIT_RETRY, self(), credit_retry),
    State#state{credit_timer = true}.

%% Grants what was withheld, or waits another round if the owner is still
%% behind.
release_credit(#state{withheld = Withheld} = State) when map_size(Withheld) =:= 0 ->
    State;
release_credit(#state{withheld = Withheld} = State) ->
    case owner_behind(State) of
        true ->
            schedule_credit_retry(State);
        false ->
            maps:fold(
                fun(Id, true, S) ->
                    case S#state.streams of
                        #{Id := #stream{auto = true, received = Received, read = Read} = Stream} ->
                            consumed(Received - Read, Stream, S);
                        #{} ->
                            S
                    end
                end,
                State#state{withheld = #{}},
                Withheld
            )
    end.

%% The owner is behind when its mailbox holds more than max_owner_queue
%% messages.
owner_behind(#state{max_owner_queue = infinity}) ->
    false;
owner_behind(#state{owner = Owner, max_owner_queue = Max}) when node(Owner) =:= node() ->
    case erlang:process_info(Owner, message_queue_len) of
        {message_queue_len, Length} -> Length > Max;
        undefined -> false
    end;
owner_behind(_State) ->
    false.

%% Returns credit for Bytes the owner (or the session itself) processed.
consumed(Bytes, #stream{read = Read, received = Received} = Stream, State) ->
    Taken = min(Bytes, Received - Read),
    NewRead = Read + Taken,
    Window = Stream#stream.recv_window,
    {Updated, Control} =
        case
            Stream#stream.final_size =:= undefined andalso
                Stream#stream.recv_max - NewRead =< Window div 2
        of
            true ->
                Max = NewRead + Window,
                {Stream#stream{read = NewRead, recv_max = Max}, [
                    qmux_frame:max_stream_data(Stream#stream.id, Max)
                ]};
            false ->
                {Stream#stream{read = NewRead}, []}
        end,
    connection_consumed(Taken, store(Updated, queue_control(Control, State))).

connection_consumed(
    Bytes, #state{conn_consumed = Consumed, conn_recv_max = Max, conn_recv_window = Window} = State
) ->
    NewConsumed = Consumed + Bytes,
    case Max - NewConsumed =< Window div 2 of
        true ->
            State#state{
                conn_consumed = NewConsumed,
                conn_recv_max = NewConsumed + Window,
                send_max_data = true
            };
        false ->
            State#state{conn_consumed = NewConsumed}
    end.

%% Resets our sending side: the data not sent yet is dropped, and the final
%% size is what was sent. A stream without a sending side has nothing to
%% reset.
reset_send(_Code, #stream{fin_sent = true}, State) ->
    State;
reset_send(_Code, #stream{reset_sent = true}, State) ->
    State;
reset_send(Code, #stream{id = Id, sent = Sent, send_size = Size} = Stream, State) ->
    Reset = Stream#stream{reset_sent = true, send_buf = queue:new(), send_size = 0},
    Next = queue_control([qmux_frame:reset_stream(Id, Code, Sent)], store(Reset, State)),
    unqueue(Size, Next),
    maybe_send_ready(Next).

%% Stops reading: what was received and not consumed comes back as credit,
%% and so will whatever still arrives. A stream without a receiving side has
%% nothing to stop.
stop_reading(_Code, #stream{read_stopped = true}, State) ->
    State;
stop_reading(
    _Code, #stream{recv_ended = true, final_size = Final, received = Final} = Stream, State
) ->
    consumed(Final, Stream#stream{read_stopped = true}, State);
stop_reading(Code, #stream{id = Id, received = Received, read = Read} = Stream, State) ->
    Stopped = Stream#stream{read_stopped = true},
    consumed(
        Received - Read, Stopped, queue_control([qmux_frame:stop_sending(Id, Code)], State)
    ).

%% Stores a stream, or removes it once all its sides ended: a completed
%% peer stream gives its stream credit back to the peer, and data received
%% and never consumed counts as consumed, so it does not hold back the
%% connection's window.
store(#stream{id = Id} = Stream, #state{streams = Streams} = State) ->
    case done(Stream) of
        false ->
            State#state{streams = Streams#{Id => Stream}};
        true ->
            Unread = Stream#stream.received - Stream#stream.read,
            Removed = connection_consumed(Unread, State#state{
                streams = maps:remove(Id, Streams), withheld = maps:remove(Id, State#state.withheld)
            }),
            case Id band 1 =:= role_bit(State#state.role) of
                true ->
                    Removed;
                false ->
                    Direction = direction(Id),
                    #limits{max_remote = Max} = Limits = limits(Direction, Removed),
                    set_limits(
                        Direction, Limits#limits{max_remote = Max + 1, send_max = true}, Removed
                    )
            end
    end.

done(#stream{
    fin_sent = FinSent,
    reset_sent = ResetSent,
    recv_ended = Ended,
    final_size = Final,
    received = Received,
    read_stopped = Stopped
}) ->
    SendDone = FinSent orelse ResetSent,
    RecvDone = (Ended orelse Stopped) andalso Final =/= undefined andalso Received =:= Final,
    SendDone andalso RecvDone.

%% A stream with the peer's limit SendMax and our window RecvWindow. The
%% side a unidirectional stream lacks starts out ended.
new_stream(Id, Kind, SendMax, RecvWindow, #state{auto = Auto}) ->
    Stream = #stream{
        id = Id,
        send_max = SendMax,
        recv_max = RecvWindow,
        recv_window = RecvWindow,
        auto = Auto
    },
    case Kind of
        bidi -> Stream;
        send_only -> Stream#stream{recv_ended = true, final_size = 0};
        recv_only -> Stream#stream{fin_requested = true, fin_sent = true}
    end.

streams_available(Direction, #limits{want = true} = Limits, #state{owner = Owner} = State) ->
    Owner ! {qmux, self(), {streams_available, Direction}},
    set_limits(Direction, Limits#limits{want = false}, State);
streams_available(Direction, Limits, State) ->
    set_limits(Direction, Limits, State).

limits(bidi, #state{bidi = Limits}) -> Limits;
limits(uni, #state{uni = Limits}) -> Limits.

set_limits(bidi, Limits, State) -> State#state{bidi = Limits};
set_limits(uni, Limits, State) -> State#state{uni = Limits}.

direction(Id) when Id band 2 =:= 0 -> bidi;
direction(_Id) -> uni.

%% The ID of the Number-th stream of Direction that Role opens.
stream_id(Number, Direction, Role) ->
    DirectionBit =
        case Direction of
            bidi -> 0;
            uni -> 2
        end,
    (Number bsl 2) bor DirectionBit bor role_bit(Role).

%%% Sending

%% Queues a write, which its writer counted in the send queue already;
%% one to a stream that is gone, reset, finished or receive-only is dropped.
write(Id, Bin, Fin, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #stream{reset_sent = false, fin_requested = false} = Stream} ->
            #stream{send_buf = Buf, send_size = Size} = Stream,
            Updated = Stream#stream{
                send_buf =
                    case Bin of
                        <<>> -> Buf;
                        _ -> queue:in(Bin, Buf)
                    end,
                send_size = Size + byte_size(Bin),
                fin_requested = Fin
            },
            ready(Id, store(Updated, State));
        #{} ->
            unqueue(byte_size(Bin), State),
            maybe_send_ready(State)
    end.

%% Takes bytes sent or dropped off the send queue.
unqueue(0, _State) ->
    ok;
unqueue(Bytes, #state{send_queue = {Ref, _}}) ->
    atomics:sub(Ref, ?QUEUED, Bytes).

%% Tells writers the session ended: their writes are refused from now on.
mark_closed(#state{send_queue = {Ref, _}}) ->
    atomics:put(Ref, ?CLOSED, 1).

ready(Id, #state{ready = Ready} = State) ->
    case queue:member(Id, Ready) of
        true -> State;
        false -> State#state{ready = queue:in(Id, Ready)}
    end.

queue_control([], State) ->
    State;
queue_control(Frames, #state{control = Control} = State) ->
    State#state{control = Control ++ Frames}.

%% Hands the writer everything that can go: control frames, flow control
%% updates, datagrams and stream data as credit allows, in records the peer
%% accepts, until the writer holds ?WRITE_WINDOW bytes. The rest goes once
%% it wrote some.
flush(#state{writer = undefined} = State) ->
    State;
flush(#state{closing = true} = State) ->
    State;
flush(#state{unwritten = Unwritten} = State) when Unwritten >= ?WRITE_WINDOW ->
    State;
flush(#state{pong = []} = State) ->
    flush(State, [], 0);
flush(#state{pong = Pong} = State) ->
    flush(State#state{pong = []}, [Pong], iolist_size(Pong)).

flush(#state{unwritten = Unwritten} = State, Records, Size) when
    Unwritten + Size >= ?WRITE_WINDOW
->
    hand(lists:reverse(Records), Size, State);
flush(State, Records, Size) ->
    case build(State) of
        {[], Next} ->
            hand(lists:reverse(Records), Size, Next);
        {Frames, Next} ->
            Record = qmux_framing:encode(Frames, Next#state.framing),
            flush(Next, [Record | Records], Size + iolist_size(Record))
    end.

hand(_Records, 0, State) ->
    maybe_send_ready(State);
hand(_Records, _Size, #state{writer = undefined} = State) ->
    %% The writer is gone, and with it the connection.
    State;
hand(Records, Size, #state{writer = Writer, unwritten = Unwritten} = State) ->
    Writer ! {write, Records, Size},
    maybe_send_ready(State#state{unwritten = Unwritten + Size, last_sent = now_ms()}).

%% The size of the records to send, once the peer's parameters arrived.
limit(#state{peer = Peer}) ->
    min(
        maps:get(max_record_size, Peer, qmux_frame:default_max_record_size()),
        ?MAX_SEND_RECORD
    ).

%% The largest datagram payload that fits in a DATAGRAM frame the peer
%% accepts and in one record, or 0 when the peer accepts none.
datagram_limit(#state{peer = Peer} = State) ->
    case maps:get(max_datagram_frame_size, Peer, 0) of
        0 -> 0;
        FrameSize -> qmux_frame:max_datagram_payload(min(FrameSize, limit(State)))
    end.

%% One record's frames, and the state after sending them.
build(#state{peer = undefined, control = Control} = State) ->
    %% Before the peer's parameters, only ours may go.
    {Control, State#state{control = []}};
build(State) ->
    Limit = limit(State),
    {Control, Size1, State1} = take_control(State#state.control, Limit, [], 0, State),
    {Flow, Size2, State2} = flow_control(Size1, Limit, State1),
    %% While both wait, datagrams and stream data take turns at the front of
    %% a record, so neither starves the other.
    BothWait =
        State2#state.datagrams_queued > 0 andalso not queue:is_empty(State2#state.ready),
    {Data, State3} =
        case State2#state.streams_first of
            true ->
                {Streams, Size3, S} = stream_data(Size2, Limit, State2),
                {Datagrams, _, S1} = datagram_frames(Size3, Limit, S),
                {Streams ++ Datagrams, S1};
            false ->
                {Datagrams, Size3, S} = datagram_frames(Size2, Limit, State2),
                {Streams, _, S1} = stream_data(Size3, Limit, S),
                {Datagrams ++ Streams, S1}
        end,
    {Control ++ Flow ++ Data, State3#state{
        streams_first = State3#state.streams_first xor BothWait
    }}.

take_control([Frame | Rest], Limit, Acc, Size, State) ->
    FrameSize = iolist_size(Frame),
    case Size + FrameSize =< Limit of
        true -> take_control(Rest, Limit, [Frame | Acc], Size + FrameSize, State);
        false -> {lists:reverse(Acc), Size, State#state{control = [Frame | Rest]}}
    end;
take_control([], _Limit, Acc, Size, State) ->
    {lists:reverse(Acc), Size, State#state{control = []}}.

flow_control(Size, Limit, State) when Size + 60 > Limit ->
    {[], Size, State};
flow_control(Size, _Limit, State) ->
    {MaxData, S1} =
        case State#state.send_max_data of
            true ->
                {[qmux_frame:max_data(State#state.conn_recv_max)], State#state{
                    send_max_data = false
                }};
            false ->
                {[], State}
        end,
    {MaxBidi, S2} = max_streams(bidi, S1),
    {MaxUni, S3} = max_streams(uni, S2),
    {Reply, S4} =
        case S3#state.send_ping_reply of
            true ->
                {[qmux_frame:ping_response(S3#state.ping_received)], S3#state{
                    send_ping_reply = false
                }};
            false ->
                {[], S3}
        end,
    {Ping, S5} =
        case S4#state.send_ping of
            true ->
                Sequence = S4#state.ping_sent + 1,
                {[qmux_frame:ping(Sequence)], S4#state{
                    send_ping = false, ping_sent = Sequence
                }};
            false ->
                {[], S4}
        end,
    Frames = MaxData ++ MaxBidi ++ MaxUni ++ Reply ++ Ping,
    {Frames, Size + iolist_size(Frames), S5}.

%% The MAX_STREAMS frame of Direction that is due, if any.
max_streams(Direction, State) ->
    case limits(Direction, State) of
        #limits{send_max = true, max_remote = Max} = Limits ->
            {
                [qmux_frame:max_streams(Direction, Max)],
                set_limits(Direction, Limits#limits{send_max = false}, State)
            };
        #limits{} ->
            {[], State}
    end.

%% The queued DATAGRAM frames that fit in the rest of the record, in order.
datagram_frames(Size, Limit, State) ->
    datagram_frames(Size, Limit, [], State).

datagram_frames(Size, Limit, Acc, #state{datagrams = Datagrams} = State) ->
    case queue:peek(Datagrams) of
        {value, Frame} ->
            FrameSize = iolist_size(Frame),
            case Size + FrameSize =< Limit of
                true ->
                    datagram_frames(Size + FrameSize, Limit, [Frame | Acc], State#state{
                        datagrams = queue:drop(Datagrams),
                        datagrams_queued = State#state.datagrams_queued - 1
                    });
                false ->
                    {lists:reverse(Acc), Size, State}
            end;
        empty ->
            {lists:reverse(Acc), Size, State}
    end.

%% Stream data in turn: each ready stream sends up to a quantum, as its own
%% and the connection's credit allow.
stream_data(Size, Limit, #state{ready = Ready} = State) ->
    stream_data(queue:to_list(Ready), Size, Limit, [], queue:new(), State).

stream_data([], Size, _Limit, Acc, Again, State) ->
    {lists:reverse(Acc), Size, State#state{ready = queue:join(Again, queue:new())}};
stream_data([_ | _] = Ids, Size, Limit, Acc, Again, State) when Limit - Size < 32 ->
    {lists:reverse(Acc), Size, State#state{ready = queue:join(queue:from_list(Ids), Again)}};
stream_data([Id | Rest], Size, Limit, Acc, Again, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #stream{reset_sent = false} = Stream} ->
            Pending = Stream#stream.send_size,
            Credit = min(
                Stream#stream.send_max - Stream#stream.sent,
                State#state.conn_send_max - State#state.conn_sent
            ),
            Room = max(
                0,
                Limit - Size -
                    qmux_frame:stream_overhead(
                        Id, Stream#stream.sent, min(Pending, ?QUANTUM)
                    )
            ),
            N = lists:min([Pending, Credit, ?QUANTUM, Room]),
            Fin = Stream#stream.fin_requested andalso N =:= Pending,
            if
                N =:= 0, not Fin ->
                    %% Blocked on credit, or nothing to send: it comes back
                    %% when credit arrives or more is written.
                    stream_data(Rest, Size, Limit, Acc, Again, State);
                true ->
                    {Data, Buf} = take(N, Stream#stream.send_buf),
                    Frame = qmux_frame:stream(Id, Stream#stream.sent, Data, Fin),
                    Sent = Stream#stream{
                        send_buf = Buf,
                        send_size = Pending - N,
                        sent = Stream#stream.sent + N,
                        fin_sent = Fin
                    },
                    unqueue(N, State),
                    State1 = store(Sent, State#state{conn_sent = State#state.conn_sent + N}),
                    More = Pending - N > 0 orelse (Stream#stream.fin_requested andalso not Fin),
                    Again1 =
                        case More of
                            true -> queue:in(Id, Again);
                            false -> Again
                        end,
                    stream_data(
                        Rest, Size + iolist_size(Frame), Limit, [Frame | Acc], Again1, State1
                    )
            end;
        #{} ->
            stream_data(Rest, Size, Limit, Acc, Again, State)
    end.

%% The first N bytes of a queue of binaries.
take(N, Buf) ->
    take(N, Buf, []).

take(0, Buf, Acc) ->
    {lists:reverse(Acc), Buf};
take(N, Buf, Acc) ->
    {{value, Bin}, Rest} = queue:out(Buf),
    case byte_size(Bin) of
        Size when Size =< N ->
            take(N - Size, Rest, [Bin | Acc]);
        _ ->
            <<Head:N/binary, Tail/binary>> = Bin,
            {lists:reverse(Acc, [Head]), queue:in_r(Tail, Rest)}
    end.

maybe_send_ready(
    #state{notify_ready = true, send_queue = {Ref, Limit}, owner = Owner} = State
) ->
    case atomics:get(Ref, ?QUEUED) < Limit div 2 of
        true ->
            Owner ! {qmux, self(), send_ready},
            State#state{notify_ready = false};
        false ->
            State
    end;
maybe_send_ready(State) ->
    State.

%%% Timers and closing

schedule_tick(#state{idle_timeout = infinity, keep_alive = infinity}) ->
    ok;
schedule_tick(#state{idle_timeout = Idle, keep_alive = KeepAlive}) ->
    Tick = lists:min([T || T <- [quarter(Idle), half(KeepAlive)], is_integer(T)]),
    erlang:send_after(max(10, Tick), self(), tick),
    ok.

quarter(infinity) -> infinity;
quarter(Idle) -> Idle div 4.

half(infinity) -> infinity;
half(KeepAlive) -> KeepAlive div 2.

tick(
    #state{idle_timeout = Idle, keep_alive = KeepAlive, last_received = Received, last_sent = Sent} =
        State
) ->
    Now = now_ms(),
    Active = max(Received, Sent),
    %% Keep-alives come at least twice per idle timeout.
    Interval =
        case {KeepAlive, Idle} of
            {infinity, infinity} -> infinity;
            {infinity, _} -> infinity;
            {_, infinity} -> KeepAlive;
            _ -> min(KeepAlive, Idle div 2)
        end,
    if
        Idle =/= infinity, Now - Active >= Idle ->
            quiet_close(idle_timeout, State);
        Interval =/= infinity, Now - Sent >= Interval ->
            schedule_tick(State),
            noreply(State#state{send_ping = true});
        true ->
            schedule_tick(State),
            {noreply, State}
    end.

%% Ends the session for a protocol error the peer made.
fail(Code, FrameType, Reason, State) ->
    Cut = binary:part(Reason, 0, min(256, byte_size(Reason))),
    Frame = qmux_frame:transport_close(qmux_frame:error_code(Code), FrameType, Cut),
    close_with(Frame, {transport_error, Code, Reason}, State).

%% Sends CONNECTION_CLOSE after everything already queued that fits, ends
%% our sending direction, and waits briefly for the peer to close.
close_with(Frame, Reason, #state{transport = Transport, socket = Socket, owner = Owner} = State0) ->
    mark_closed(State0),
    Owner ! {qmux, self(), {closed, Reason}},
    case State0#state.writer of
        undefined ->
            {stop, normal, State0};
        Writer ->
            %% What was written before the close, and not flushed yet, goes
            %% first, whatever the writer holds.
            State = flush_all(State0),
            Closed = send_now(qmux_framing:close(Frame, State#state.framing), State),
            Writer ! shutdown,
            erlang:send_after(?CLOSE_TIMEOUT, self(), close_timeout),
            _ = Transport:setopts(Socket, [{active, true}]),
            {noreply, Closed#state{closing = true}}
    end.

%% Hands the writer everything that can go, past its window.
flush_all(State) ->
    case build(State) of
        {[], Next} ->
            Next;
        {Frames, Next} ->
            Record = qmux_framing:encode(Frames, Next#state.framing),
            flush_all(hand(Record, iolist_size(Record), Next))
    end.

%% The peer closed, or the idle timeout expired: end without frames, but
%% with whatever the framing ends a connection with, such as a WebSocket
%% Close.
quiet_close(Reason, #state{owner = Owner} = State) ->
    mark_closed(State),
    Owner ! {qmux, self(), {closed, Reason}},
    Closed = send_now(qmux_framing:close(none, State#state.framing), State),
    _ =
        case Closed#state.writer of
            %% It failed during a close the peer now answers.
            undefined -> ok;
            Writer -> Writer ! shutdown
        end,
    {stop, normal, Closed}.

lost(Reason, #state{owner = Owner} = State) ->
    mark_closed(State),
    Owner ! {qmux, self(), {closed, Reason}},
    State#state{closing = true}.

role_bit(client) -> 0;
role_bit(server) -> 1.

peer_role(client) -> server;
peer_role(server) -> client.

now_ms() -> erlang:monotonic_time(millisecond).
