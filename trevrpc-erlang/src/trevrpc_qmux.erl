%% @doc A QMux session (draft-ietf-quic-qmux-02) over a TLS connection: QUIC's
%% bidirectional streams, flow control and stream limits, carried in
%% Size-prefixed records over a reliable byte stream.
%%
%% Its API and the events it sends its owner mirror erlang_quic's, so the
%% rest of the runtime drives native QUIC and QMux the same way (see
%% `trevrpc_transport'). Events go to the owner as
%% `{trevrpc_qmux, Session, Event}':
%%
%% <ul>
%%   <li>`{connected, #{}}': the peer's transport parameters arrived.</li>
%%   <li>`{stream_opened, Id}', `{stream_data, Id, Bin, Fin}',
%%       `{stream_reset, Id, Code}', `{stop_sending, Id, Code}'.</li>
%%   <li>`{streams_available, bidi}': an {@link open_stream/1} refused for
%%       lack of stream credit would now succeed.</li>
%%   <li>`send_ready': the send queue drained, after
%%       {@link notify_send_ready/1}.</li>
%%   <li>`{closed, Reason}': the session ended.</li>
%% </ul>
%%
%% Streams are always in manual flow control: the peer may send one window
%% past the bytes returned with {@link consume/3}. Data, and a FIN, go out in
%% order: a FIN travels with a stream's last data, so a reset's final size,
%% the bytes actually sent, always agrees with what the peer saw.
-module(trevrpc_qmux).

-behaviour(gen_server).

-export([
    start/4,
    send_data/4,
    send_data_async/4,
    consume/3,
    reset_stream/3,
    stop_sending/3,
    open_stream/1,
    notify_send_ready/1,
    close/2,
    peername/1
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([options/0]).

%% Session options:
%% <ul>
%%   <li>`parameters': the transport parameters to advertise.</li>
%%   <li>`keep_alive': milliseconds without sending after which a QX_PING
%%       goes out, or `infinity' (the default).</li>
%%   <li>`send_queue_limit': the buffered bytes past which send_data/4 is
%%       refused with `send_queue_full' (default 1 MiB).</li>
%%   <li>`handshake_timeout': milliseconds for the peer's
%%       QX_TRANSPORT_PARAMETERS to arrive (default 10 s).</li>
%%   <li>`send_timeout': milliseconds a write to a peer that stopped reading
%%       may block before the connection is lost (default 30 s).</li>
%% </ul>
-type options() :: #{
    parameters := trevrpc_qmux_frame:parameters(),
    keep_alive => pos_integer() | infinity,
    send_queue_limit => pos_integer(),
    handshake_timeout => pos_integer(),
    send_timeout => pos_integer()
}.

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
    %% Receiving: the offset received, our limit and window, the offset the
    %% owner consumed, and the final size once known.
    received = 0 :: non_neg_integer(),
    recv_max :: non_neg_integer(),
    recv_window :: non_neg_integer(),
    read = 0 :: non_neg_integer(),
    final_size :: non_neg_integer() | undefined,
    %% The owner was given the stream's end: FIN or reset.
    recv_ended = false :: boolean(),
    %% The owner stopped reading: later data is consumed at once.
    read_stopped = false :: boolean()
}).

-record(state, {
    socket :: ssl:sslsocket() | undefined,
    role :: client | server,
    owner :: pid(),
    local :: trevrpc_qmux_frame:parameters(),
    peer :: trevrpc_qmux_frame:parameters() | undefined,
    max_record_size :: pos_integer(),
    buffer = <<>> :: binary(),
    streams = #{} :: #{non_neg_integer() => #stream{}},
    %% Streams we opened and may open; streams the peer opened and may open.
    opened_local = 0 :: non_neg_integer(),
    max_local = 0 :: non_neg_integer(),
    want_streams = false :: boolean(),
    opened_remote = 0 :: non_neg_integer(),
    max_remote :: non_neg_integer(),
    send_max_streams = false :: boolean(),
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
    queued = 0 :: non_neg_integer(),
    queue_limit :: pos_integer(),
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
    closing = false :: boolean()
}).

-define(MAX_SEND_RECORD, 65536).
-define(QUANTUM, 16384).
-define(CLOSE_TIMEOUT, 1000).
-define(DEFAULT_QUEUE_LIMIT, 1048576).

%%% API

%% @doc Starts a session over `Socket', a TLS connection whose handshake
%% completed, owned by `Owner'. The caller must be the socket's controlling
%% process; the session takes it over.
-spec start(ssl:sslsocket(), client | server, pid(), options()) -> {ok, pid()} | {error, term()}.
start(Socket, Role, Owner, Opts) ->
    case gen_server:start(?MODULE, {Role, Owner, Opts}, []) of
        {ok, Pid} ->
            case ssl:controlling_process(Socket, Pid) of
                ok ->
                    gen_server:cast(Pid, {socket, Socket}),
                    {ok, Pid};
                {error, _} = Error ->
                    exit(Pid, kill),
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Writes data to a stream, finishing it with `Fin'. Refused with
%% `send_queue_full' while the session's send queue is over its limit.
-spec send_data(pid(), non_neg_integer(), iodata(), boolean()) -> ok | {error, term()}.
send_data(Session, Id, Data, Fin) ->
    call(Session, {send, Id, Data, Fin, limited}).

%% @doc Writes data to a stream without waiting, whatever the send queue
%% holds.
-spec send_data_async(pid(), non_neg_integer(), iodata(), boolean()) -> ok.
send_data_async(Session, Id, Data, Fin) ->
    gen_server:cast(Session, {send, Id, Data, Fin, unlimited}).

%% @doc Returns receive credit for `Bytes' of a stream the owner processed.
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
    call(Session, open_stream).

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

call(Session, Request) ->
    try
        gen_server:call(Session, Request, infinity)
    catch
        exit:_ -> {error, closed}
    end.

%%% gen_server

-spec init({client | server, pid(), options()}) -> {ok, #state{}}.
init({Role, Owner, Opts}) ->
    monitor(process, Owner),
    Local = maps:get(parameters, Opts),
    Now = now_ms(),
    InitialData = maps:get(initial_max_data, Local, 0),
    {ok, #state{
        role = Role,
        owner = Owner,
        local = Local,
        max_record_size = maps:get(
            max_record_size, Local, trevrpc_qmux_frame:default_max_record_size()
        ),
        max_remote = maps:get(initial_max_streams_bidi, Local, 0),
        conn_recv_max = InitialData,
        conn_recv_window = InitialData,
        queue_limit = maps:get(send_queue_limit, Opts, ?DEFAULT_QUEUE_LIMIT),
        keep_alive = maps:get(keep_alive, Opts, infinity),
        handshake_timeout = maps:get(handshake_timeout, Opts, 10000),
        send_timeout = maps:get(send_timeout, Opts, 30000),
        last_received = Now,
        last_sent = Now,
        control = [trevrpc_qmux_frame:transport_parameters(Local)]
    }}.

-spec handle_call(term(), gen_server:from(), #state{}) ->
    {reply, term(), #state{}} | {stop, term(), term(), #state{}}.
handle_call(_Request, _From, #state{closing = true} = State) ->
    {reply, {error, closed}, State};
handle_call({send, Id, Data, Fin, Limit}, _From, State) ->
    {Reply, Next} = write(Id, Data, Fin, Limit, State),
    reply(Reply, Next);
handle_call({consume, Id, Bytes}, _From, State) ->
    case State#state.streams of
        #{Id := Stream} -> reply(ok, consumed(Bytes, Stream, State));
        #{} -> {reply, {error, unknown_stream}, State}
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
handle_call(open_stream, _From, #state{peer = undefined} = State) ->
    {reply, {error, not_connected}, State};
handle_call(open_stream, _From, #state{opened_local = Opened, max_local = Max} = State) when
    Opened < Max
->
    Id = (Opened bsl 2) bor role_bit(State#state.role),
    Stream = new_stream(
        Id,
        maps:get(initial_max_stream_data_bidi_remote, State#state.peer, 0),
        maps:get(initial_max_stream_data_bidi_local, State#state.local, 0)
    ),
    {reply, {ok, Id}, State#state{
        opened_local = Opened + 1, streams = (State#state.streams)#{Id => Stream}
    }};
handle_call(open_stream, _From, State) ->
    {reply, {error, stream_limit}, State#state{want_streams = true}};
handle_call(peername, _From, #state{socket = Socket} = State) ->
    {reply, ssl:peername(Socket), State}.

reply(Reply, State) ->
    case flush(State) of
        {ok, Next} -> {reply, Reply, Next};
        {error, Reason, Next} -> {stop, normal, Reply, lost(Reason, Next)}
    end.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}} | {stop, normal, #state{}}.
handle_cast({socket, Socket}, #state{handshake_timeout = Handshake, send_timeout = Send} = State) ->
    %% A write that blocks longer than the send timeout closes the socket, so
    %% a peer that stops reading cannot hold this process, its timers and its
    %% callers.
    case ssl:setopts(Socket, [{active, once}, {send_timeout, Send}, {send_timeout_close, true}]) of
        ok ->
            erlang:send_after(Handshake, self(), handshake_timeout),
            noreply(State#state{socket = Socket});
        {error, Reason} ->
            {stop, normal, lost({connection_lost, Reason}, State#state{socket = Socket})}
    end;
handle_cast(_Request, #state{closing = true} = State) ->
    {noreply, State};
handle_cast({send, Id, Data, Fin, Limit}, State) ->
    {_Reply, Next} = write(Id, Data, Fin, Limit, State),
    noreply(Next);
handle_cast(notify_send_ready, State) ->
    noreply(maybe_send_ready(State#state{notify_ready = true}));
handle_cast({close, Code}, State) ->
    close_with(trevrpc_qmux_frame:application_close(Code, <<>>), normal, State).

-spec handle_info(term(), #state{}) -> {noreply, #state{}} | {stop, normal, #state{}}.
handle_info({ssl, Socket, _Data}, #state{socket = Socket, closing = true} = State) ->
    %% Draining after CONNECTION_CLOSE.
    {noreply, State};
handle_info({ssl, Socket, Data}, #state{socket = Socket} = State) ->
    case
        records(<<(State#state.buffer)/binary, Data/binary>>, State#state{last_received = now_ms()})
    of
        {ok, Next} ->
            case ssl:setopts(Socket, [{active, once}]) of
                ok -> noreply(Next);
                {error, Reason} -> {stop, normal, lost({connection_lost, Reason}, Next)}
            end;
        {error, {Code, FrameType, Reason}, Next} ->
            fail(Code, FrameType, Reason, Next);
        {closed, Reason, Next} ->
            quiet_close(Reason, Next)
    end;
handle_info({ssl_closed, Socket}, #state{socket = Socket, closing = true} = State) ->
    {stop, normal, State};
handle_info({ssl_closed, Socket}, #state{socket = Socket} = State) ->
    {stop, normal, lost(connection_lost, State)};
handle_info({ssl_error, Socket, Reason}, #state{socket = Socket} = State) ->
    {stop, normal, lost({connection_lost, Reason}, State)};
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
    close_with(trevrpc_qmux_frame:application_close(0, <<>>), normal, State);
handle_info(_Message, State) ->
    {noreply, State}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, #state{socket = undefined}) ->
    ok;
terminate(_Reason, #state{socket = Socket}) ->
    _ = ssl:close(Socket),
    ok.

noreply(State) ->
    case flush(State) of
        {ok, Next} -> {noreply, Next};
        {error, Reason, Next} -> {stop, normal, lost(Reason, Next)}
    end.

%%% Receiving

records(Bin, State) ->
    case trevrpc_qmux_frame:decode_record(Bin, State#state.max_record_size) of
        more ->
            {ok, State#state{buffer = Bin}};
        {error, Error} ->
            {error, Error, State};
        {ok, Record, Rest} ->
            case trevrpc_qmux_frame:decode_frames(Record) of
                {ok, Frames} ->
                    case frames(Frames, State) of
                        {ok, Next} -> records(Rest, Next);
                        Other -> Other
                    end;
                {error, Error} ->
                    {error, Error, State}
            end
    end.

frames([], State) ->
    {ok, State};
frames([{transport_parameters, Body} | Rest], #state{peer = undefined} = State) ->
    case trevrpc_qmux_frame:decode_parameters(Body) of
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

connected(Peer, #state{local = Local, owner = Owner} = State) ->
    Idle =
        case {maps:get(max_idle_timeout, Local, 0), maps:get(max_idle_timeout, Peer, 0)} of
            {0, 0} -> infinity;
            {0, Theirs} -> Theirs;
            {Ours, 0} -> Ours;
            {Ours, Theirs} -> min(Ours, Theirs)
        end,
    Owner ! {trevrpc_qmux, self(), {connected, #{}}},
    Next = State#state{
        peer = Peer,
        max_local = maps:get(initial_max_streams_bidi, Peer, 0),
        conn_send_max = maps:get(initial_max_data, Peer, 0),
        idle_timeout = Idle
    },
    schedule_tick(Next),
    Next.

frame({transport_parameters, _}, State) ->
    {error, {protocol_violation, 16#3f5153300d0a0d0a, <<"repeated QX_TRANSPORT_PARAMETERS">>},
        State};
frame({stream, Id, Offset, Data, Fin}, State) ->
    with_stream(Id, 16#08, State, fun
        (undefined, S) when Data =:= <<>> -> {ok, S};
        (undefined, S) -> {error, {final_size, 16#08, <<"data beyond a stream's final size">>}, S};
        (Stream, S) -> stream_data(Stream, Offset, Data, Fin, S)
    end);
frame({reset_stream, Id, Code, Final}, State) ->
    with_stream(Id, 16#04, State, fun
        (undefined, S) -> {ok, S};
        (Stream, S) -> reset_received(Stream, Code, Final, S)
    end);
frame({stop_sending, Id, Code}, State) ->
    with_stream(Id, 16#05, State, fun
        (undefined, S) ->
            {ok, S};
        (#stream{fin_sent = true}, S) ->
            {ok, S};
        (#stream{reset_sent = true}, S) ->
            {ok, S};
        (Stream, S) ->
            S#state.owner ! {trevrpc_qmux, self(), {stop_sending, Id, Code}},
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
    with_stream(Id, 16#11, State, fun
        (undefined, S) ->
            {ok, S};
        (#stream{send_max = Current} = Stream, S) when Max > Current ->
            {ok, ready(Id, store(Stream#stream{send_max = Max}, S))};
        (_Stream, S) ->
            {ok, S}
    end);
frame({max_streams, Direction, Max}, State) ->
    case Max > trevrpc_qmux_frame:max_streams_limit() of
        true ->
            {error, {frame_encoding, 16#12, <<"MAX_STREAMS exceeds 2^60">>}, State};
        false when Direction =:= bidi, Max > State#state.max_local ->
            {ok, streams_available(State#state{max_local = Max})};
        false ->
            {ok, State}
    end;
frame({streams_blocked, _Direction, Max}, State) ->
    case Max > trevrpc_qmux_frame:max_streams_limit() of
        true -> {error, {frame_encoding, 16#16, <<"STREAMS_BLOCKED exceeds 2^60">>}, State};
        false -> {ok, State}
    end;
frame({data_blocked, _}, State) ->
    {ok, State};
frame({stream_data_blocked, Id, _}, State) ->
    with_stream(Id, 16#15, State, fun(_, S) -> {ok, S} end);
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

%% Looks a stream up for a frame, opening peer streams up to it. Calls Fun
%% with the stream, or undefined for one that already completed, and the
%% state that opened it.
with_stream(Id, FrameType, State, Fun) ->
    Local = Id band 1 =:= role_bit(State#state.role),
    Number = Id bsr 2,
    if
        Id band 2 =/= 0, Local ->
            {error, {stream_state, FrameType, <<"unidirectional stream was never opened">>}, State};
        Id band 2 =/= 0 ->
            {error, {stream_limit, FrameType, <<"unidirectional streams are not allowed">>}, State};
        Local, Number >= State#state.opened_local ->
            {error, {stream_state, FrameType, <<"stream was never opened">>}, State};
        Local ->
            Fun(maps:get(Id, State#state.streams, undefined), State);
        Number < State#state.opened_remote ->
            Fun(maps:get(Id, State#state.streams, undefined), State);
        Number >= State#state.max_remote ->
            {error, {stream_limit, FrameType, <<"stream exceeds the stream limit">>}, State};
        true ->
            Opened = open_remote(Number, State),
            with_stream(Id, FrameType, Opened, Fun)
    end.

open_remote(Number, #state{opened_remote = Opened} = State) when Opened > Number ->
    State;
open_remote(Number, #state{opened_remote = Opened, owner = Owner} = State) ->
    Id = (Opened bsl 2) bor role_bit(peer_role(State#state.role)),
    Stream = new_stream(
        Id,
        maps:get(initial_max_stream_data_bidi_local, State#state.peer, 0),
        maps:get(initial_max_stream_data_bidi_remote, State#state.local, 0)
    ),
    Owner ! {trevrpc_qmux, self(), {stream_opened, Id}},
    open_remote(Number, State#state{
        opened_remote = Opened + 1, streams = (State#state.streams)#{Id => Stream}
    }).

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
                    State#state.owner ! {trevrpc_qmux, self(), {stream_data, Id, Data, Fin}},
                    {ok, store(Updated#stream{recv_ended = Fin}, Counted)}
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
                    State#state.owner ! {trevrpc_qmux, self(), {stream_reset, Id, Code}},
                    ok
            end,
            Counted = State#state{conn_received = State#state.conn_received + (Size - Received)},
            Ended = Stream#stream{received = Size, final_size = Size, recv_ended = true},
            %% Nothing will read the rest: its credit comes back now.
            {ok, consumed(Size - Stream#stream.read, Ended, Counted)}
    end.

%%% Flow control and stream state

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
                    trevrpc_qmux_frame:max_stream_data(Stream#stream.id, Max)
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
%% size is what was sent.
reset_send(_Code, #stream{fin_sent = true}, State) ->
    State;
reset_send(_Code, #stream{reset_sent = true}, State) ->
    State;
reset_send(Code, #stream{id = Id, sent = Sent, send_size = Size} = Stream, State) ->
    Reset = Stream#stream{reset_sent = true, send_buf = queue:new(), send_size = 0},
    Next = queue_control([trevrpc_qmux_frame:reset_stream(Id, Code, Sent)], store(Reset, State)),
    maybe_send_ready(Next#state{queued = Next#state.queued - Size}).

%% Stops reading: what was received and not consumed comes back as credit,
%% and so will whatever still arrives.
stop_reading(_Code, #stream{read_stopped = true}, State) ->
    State;
stop_reading(
    _Code, #stream{recv_ended = true, final_size = Final, received = Final} = Stream, State
) ->
    consumed(Final, Stream#stream{read_stopped = true}, State);
stop_reading(Code, #stream{id = Id, received = Received, read = Read} = Stream, State) ->
    Stopped = Stream#stream{read_stopped = true},
    consumed(
        Received - Read, Stopped, queue_control([trevrpc_qmux_frame:stop_sending(Id, Code)], State)
    ).

%% Stores a stream, or removes it once both its sides ended: a completed
%% peer stream gives its stream credit back to the peer, and data received
%% and never consumed counts as consumed, so it does not hold back the
%% connection's window.
store(#stream{id = Id} = Stream, #state{streams = Streams} = State) ->
    case done(Stream) of
        false ->
            State#state{streams = Streams#{Id => Stream}};
        true ->
            Unread = Stream#stream.received - Stream#stream.read,
            Removed = connection_consumed(Unread, State#state{streams = maps:remove(Id, Streams)}),
            case Id band 1 =:= role_bit(State#state.role) of
                true ->
                    Removed;
                false ->
                    Removed#state{
                        max_remote = Removed#state.max_remote + 1, send_max_streams = true
                    }
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

new_stream(Id, SendMax, RecvWindow) ->
    #stream{id = Id, send_max = SendMax, recv_max = RecvWindow, recv_window = RecvWindow}.

streams_available(#state{want_streams = true, owner = Owner} = State) ->
    Owner ! {trevrpc_qmux, self(), {streams_available, bidi}},
    State#state{want_streams = false};
streams_available(State) ->
    State.

%%% Sending

write(_Id, _Data, _Fin, limited, #state{queued = Queued, queue_limit = Limit} = State) when
    Queued >= Limit
->
    {{error, send_queue_full}, State};
write(Id, Data, Fin, _Limit, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #stream{reset_sent = true}} ->
            {{error, stream_closed}, State};
        #{Id := #stream{fin_requested = true}} ->
            {{error, stream_closed}, State};
        #{Id := #stream{send_buf = Buf, send_size = Size} = Stream} ->
            Bin = iolist_to_binary(Data),
            Updated = Stream#stream{
                send_buf =
                    case Bin of
                        <<>> -> Buf;
                        _ -> queue:in(Bin, Buf)
                    end,
                send_size = Size + byte_size(Bin),
                fin_requested = Fin
            },
            {ok,
                ready(
                    Id, store(Updated, State#state{queued = State#state.queued + byte_size(Bin)})
                )};
        #{} ->
            {{error, unknown_stream}, State}
    end.

ready(Id, #state{ready = Ready} = State) ->
    case queue:member(Id, Ready) of
        true -> State;
        false -> State#state{ready = queue:in(Id, Ready)}
    end.

queue_control([], State) ->
    State;
queue_control(Frames, #state{control = Control} = State) ->
    State#state{control = Control ++ Frames}.

%% Sends everything that can go: control frames, flow control updates and
%% stream data as credit allows, in records the peer accepts.
flush(#state{socket = undefined} = State) ->
    {ok, State};
flush(#state{closing = true} = State) ->
    {ok, State};
flush(State) ->
    flush(State, []).

flush(State, Records) ->
    {Frames, Next} = build(State),
    case Frames of
        [] ->
            send_records(lists:reverse(Records), Next);
        _ ->
            Batch = [trevrpc_qmux_frame:record(Frames) | Records],
            case iolist_size(Batch) >= 4 * ?MAX_SEND_RECORD of
                true ->
                    case send_records(lists:reverse(Batch), Next) of
                        {ok, Sent} -> flush(Sent, []);
                        Error -> Error
                    end;
                false ->
                    flush(Next, Batch)
            end
    end.

send_records([], State) ->
    {ok, maybe_send_ready(State)};
send_records(Records, #state{socket = Socket} = State) ->
    case ssl:send(Socket, Records) of
        ok -> {ok, maybe_send_ready(State#state{last_sent = now_ms()})};
        {error, Reason} -> {error, Reason, State}
    end.

%% The size of the records to send, once the peer's parameters arrived.
limit(#state{peer = Peer}) ->
    min(
        maps:get(max_record_size, Peer, trevrpc_qmux_frame:default_max_record_size()),
        ?MAX_SEND_RECORD
    ).

%% One record's frames, and the state after sending them.
build(#state{peer = undefined, control = Control} = State) ->
    %% Before the peer's parameters, only ours may go.
    {Control, State#state{control = []}};
build(State) ->
    Limit = limit(State),
    {Control, Size1, State1} = take_control(State#state.control, Limit, [], 0, State),
    {Flow, Size2, State2} = flow_control(Size1, Limit, State1),
    {Data, _Size3, State3} = stream_data(Size2, Limit, State2),
    {Control ++ Flow ++ Data, State3}.

take_control([Frame | Rest], Limit, Acc, Size, State) ->
    FrameSize = iolist_size(Frame),
    case Size + FrameSize =< Limit of
        true -> take_control(Rest, Limit, [Frame | Acc], Size + FrameSize, State);
        false -> {lists:reverse(Acc), Size, State#state{control = [Frame | Rest]}}
    end;
take_control([], _Limit, Acc, Size, State) ->
    {lists:reverse(Acc), Size, State#state{control = []}}.

flow_control(Size, Limit, State) when Size + 40 > Limit ->
    {[], Size, State};
flow_control(Size, _Limit, State) ->
    {MaxData, S1} =
        case State#state.send_max_data of
            true ->
                {[trevrpc_qmux_frame:max_data(State#state.conn_recv_max)], State#state{
                    send_max_data = false
                }};
            false ->
                {[], State}
        end,
    {MaxStreams, S2} =
        case S1#state.send_max_streams of
            true ->
                {[trevrpc_qmux_frame:max_streams_bidi(S1#state.max_remote)], S1#state{
                    send_max_streams = false
                }};
            false ->
                {[], S1}
        end,
    {Reply, S3} =
        case S2#state.send_ping_reply of
            true ->
                {[trevrpc_qmux_frame:ping_response(S2#state.ping_received)], S2#state{
                    send_ping_reply = false
                }};
            false ->
                {[], S2}
        end,
    {Ping, S4} =
        case S3#state.send_ping of
            true ->
                Sequence = S3#state.ping_sent + 1,
                {[trevrpc_qmux_frame:ping(Sequence)], S3#state{
                    send_ping = false, ping_sent = Sequence
                }};
            false ->
                {[], S3}
        end,
    Frames = MaxData ++ MaxStreams ++ Reply ++ Ping,
    {Frames, Size + iolist_size(Frames), S4}.

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
                    trevrpc_qmux_frame:stream_overhead(
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
                    Frame = trevrpc_qmux_frame:stream(Id, Stream#stream.sent, Data, Fin),
                    Sent = Stream#stream{
                        send_buf = Buf,
                        send_size = Pending - N,
                        sent = Stream#stream.sent + N,
                        fin_sent = Fin
                    },
                    State1 = store(Sent, State#state{
                        conn_sent = State#state.conn_sent + N, queued = State#state.queued - N
                    }),
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
    #state{notify_ready = true, queued = Queued, queue_limit = Limit, owner = Owner} = State
) when
    Queued < Limit div 2
->
    Owner ! {trevrpc_qmux, self(), send_ready},
    State#state{notify_ready = false};
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
    Frame = trevrpc_qmux_frame:transport_close(trevrpc_qmux_frame:error_code(Code), FrameType, Cut),
    close_with(Frame, {transport_error, Code, Reason}, State).

%% Sends CONNECTION_CLOSE after everything already queued that fits, ends
%% our sending direction, and waits briefly for the peer to close.
close_with(Frame, Reason, #state{socket = Socket, owner = Owner} = State) ->
    Owner ! {trevrpc_qmux, self(), {closed, Reason}},
    case Socket of
        undefined ->
            {stop, normal, State};
        _ ->
            _ = ssl:send(Socket, trevrpc_qmux_frame:record(Frame)),
            _ = ssl:shutdown(Socket, write),
            erlang:send_after(?CLOSE_TIMEOUT, self(), close_timeout),
            _ = ssl:setopts(Socket, [{active, true}]),
            {noreply, State#state{closing = true}}
    end.

%% The peer closed, or the idle timeout expired: end without frames.
quiet_close(Reason, #state{socket = Socket, owner = Owner} = State) ->
    Owner ! {trevrpc_qmux, self(), {closed, Reason}},
    _ = ssl:shutdown(Socket, write),
    {stop, normal, State}.

lost(Reason, #state{owner = Owner} = State) ->
    Owner ! {trevrpc_qmux, self(), {closed, Reason}},
    State#state{closing = true}.

role_bit(client) -> 0;
role_bit(server) -> 1.

peer_role(client) -> server;
peer_role(server) -> client.

now_ms() -> erlang:monotonic_time(millisecond).
