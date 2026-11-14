%% @private The owner of one server-side QUIC connection.
%%
%% It receives every event of the connection. For each stream it reads the
%% REQUEST frame, checks it, and either answers with a lone STATUS or starts
%% a call process (`trevrpc_server_call') to run the handler, then routes
%% the stream's events to that process.
%%
%% It also owns each call's lifecycle: the initial request timeout, the
%% call's deadline, the peer's cancellation, and shutdown all end a call by
%% stopping its process, whether or not the handler cooperates. A call
%% process asks for permission ({@link finish/3}) before it writes its
%% terminal STATUS, which settles the race between a handler that finishes
%% and a deadline or cancellation that ends it: exactly one of them writes
%% the call's end.
-module(trevrpc_server_conn).

-behaviour(gen_server).

-export([start/2, start_qmux/3, shutdown/1, finish/3, reserve/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(entry, {
    %% reading: waiting for the REQUEST frame; running: the call process
    %% runs; finishing: it is writing its terminal STATUS; expiring: it was
    %% stopped at its deadline; cancelling: the peer abandoned the call;
    %% shutdown: the server is shutting down.
    state = reading :: reading | running | finishing | expiring | cancelling | shutdown,
    ref = make_ref() :: reference(),
    reader :: trevrpc_frame:reader(),
    %% The peer finished the request side.
    fin = false :: boolean(),
    %% The initial request timer while reading, the deadline timer after.
    timer :: reference() | undefined,
    pid :: pid() | undefined,
    monitor :: reference() | undefined,
    %% Whether the stream still holds its concurrency slots.
    counted = true :: boolean(),
    %% Stream compression memory the call holds, given back when it ends.
    reserved = 0 :: non_neg_integer()
}).

-record(state, {
    %% The connection: native QUIC, or a QMux session once its socket came.
    transport :: trevrpc_transport:t() | undefined,
    %% The QMux session's monitor: a session that dies without its closed
    %% event still ends the connection.
    session_monitor :: reference() | undefined,
    config :: trevrpc_server:config(),
    streams = #{} :: #{non_neg_integer() => #entry{}},
    monitors = #{} :: #{reference() => non_neg_integer()},
    %% Streams counted against the per-connection limit: reading or with a
    %% call that has not released its slots.
    active = 0 :: non_neg_integer(),
    %% Processes waiting for the send queue to drain.
    send_waiters = [] :: [{pid(), reference()}],
    stopping = false :: boolean()
}).

-define(CANCELLED, 1).

%% @doc Starts the owner of the QUIC connection `Conn'. Called by the
%% listener, which makes it the connection's owner before the connection
%% sees its first packet.
-spec start(pid(), trevrpc_server:config()) -> {ok, pid()} | ignore | {error, term()}.
start(Conn, Config) ->
    gen_server:start(?MODULE, {{quic, Conn}, Config}, []).

%% @doc Starts the owner of a QMux session over the TLS connection `Socket',
%% whose handshake completed, with the server's session options and `Opts'
%% (how its records travel: see {@link trevrpc_qmux:options()}).
%% The caller must be the socket's controlling process; the session takes
%% it over.
-spec start_qmux(ssl:sslsocket(), map(), trevrpc_server:config()) ->
    {ok, pid()} | {error, term()}.
start_qmux(Socket, Opts, Config) ->
    case gen_server:start(?MODULE, {undefined, Config}, []) of
        {ok, Pid} ->
            case ssl:controlling_process(Socket, Pid) of
                ok ->
                    gen_server:cast(Pid, {qmux, Socket, Opts}),
                    {ok, Pid};
                {error, _} = Error ->
                    exit(Pid, kill),
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Cancels every call, waits for their handlers up to the graceful
%% shutdown timeout, then closes the connection.
-spec shutdown(pid()) -> ok.
shutdown(Pid) ->
    gen_server:cast(Pid, shutdown).

%% @doc Reserves `Cost' bytes of stream compression memory for call `Ref'
%% on stream `Id', until the call ends.
-spec reserve(pid(), non_neg_integer(), reference(), non_neg_integer()) -> boolean().
reserve(Pid, Id, Ref, Cost) ->
    try
        gen_server:call(Pid, {reserve, Id, Ref, Cost}, infinity)
    catch
        exit:_ -> false
    end.

%% @doc Asks to end the call on stream `Id' with its own terminal STATUS.
%% `ok' releases its concurrency slots, after which only the call writes to
%% the stream; `cancelled' means the call was already ended for it.
-spec finish(pid(), non_neg_integer(), reference()) -> ok | cancelled.
finish(Pid, Id, Ref) ->
    try
        gen_server:call(Pid, {finish, Id, Ref}, infinity)
    catch
        exit:_ -> cancelled
    end.

%%% gen_server

-spec init({trevrpc_transport:t() | undefined, trevrpc_server:config()}) -> {ok, #state{}}.
init({Transport, Config}) ->
    %% Call processes are linked, so they die with this process however it
    %% exits; their own exits arrive as messages, and as the monitors' DOWN.
    process_flag(trap_exit, true),
    %% Exits with the server.
    monitor(process, maps:get(server, Config)),
    {ok, #state{transport = Transport, config = Config}}.

-spec handle_call(term(), gen_server:from(), #state{}) -> {reply, term(), #state{}}.
handle_call({reserve, Id, Ref, Cost}, _From, #state{streams = Streams, config = Config} = State) ->
    case Streams of
        #{Id := #entry{ref = Ref, pid = Pid, reserved = Reserved} = Entry} when is_pid(Pid) ->
            case trevrpc_zstd_stream:reserve(maps:get(stream_budget, Config), Cost) of
                true ->
                    Reserving = Entry#entry{reserved = Reserved + Cost},
                    {reply, true, State#state{streams = Streams#{Id := Reserving}}};
                false ->
                    {reply, false, State}
            end;
        _ ->
            {reply, false, State}
    end;
handle_call({finish, Id, Ref}, _From, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #entry{state = running, ref = Ref} = Entry} ->
            cancel_timer(Entry),
            {reply, ok, release(Id, Entry#entry{state = finishing, timer = undefined}, State)};
        _ ->
            {reply, cancelled, State}
    end.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}} | {stop, normal, #state{}}.
handle_cast({qmux, Socket, Opts}, #state{config = Config} = State) ->
    case trevrpc_qmux:start(Socket, server, self(), maps:merge(maps:get(qmux, Config), Opts)) of
        {ok, Session} ->
            {noreply, State#state{
                transport = {qmux, Session}, session_monitor = monitor(process, Session)
            }};
        {error, _Reason} ->
            {stop, normal, State}
    end;
handle_cast(shutdown, #state{stopping = true} = State) ->
    {noreply, State};
handle_cast(shutdown, #state{config = #{graceful_shutdown_timeout := Grace}} = State) ->
    State1 = maps:fold(fun shutdown_stream/3, State#state{stopping = true}, State#state.streams),
    erlang:send_after(Grace, self(), kill_calls),
    maybe_close(State1).

-spec handle_info(term(), #state{}) -> {noreply, #state{}} | {stop, normal, #state{}}.
handle_info({Tag, Pid, Event}, #state{transport = {_, Pid}} = State) when
    Tag =:= quic; Tag =:= trevrpc_qmux
->
    quic_event(Event, State);
handle_info(
    {trevrpc_send_ready, Pid, Ref}, #state{send_waiters = Waiters, transport = Transport} = State
) ->
    case Waiters of
        [] -> trevrpc_transport:notify_send_ready(Transport);
        _ -> ok
    end,
    {noreply, State#state{send_waiters = [{Pid, Ref} | Waiters]}};
handle_info({timeout, Timer, {initial, Id}}, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #entry{state = reading, timer = Timer} = Entry} ->
            Status = trevrpc_status:new(deadline_exceeded, <<"initial request frame timeout">>),
            {noreply, refuse(Id, Entry, Status, State)};
        _ ->
            {noreply, State}
    end;
handle_info({timeout, Timer, {deadline, Id}}, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #entry{state = running, timer = Timer, pid = Pid} = Entry} ->
            exit(Pid, kill),
            {noreply, release(Id, Entry#entry{state = expiring, timer = undefined}, State)};
        _ ->
            {noreply, State}
    end;
handle_info({'DOWN', Monitor, process, _Pid, Reason}, #state{session_monitor = Monitor} = State) ->
    quic_event({closed, {session_down, Reason}}, State#state{session_monitor = undefined});
handle_info({'DOWN', Monitor, process, _Pid, Reason}, #state{monitors = Monitors} = State) ->
    case maps:take(Monitor, Monitors) of
        {Id, Rest} -> call_down(Id, Reason, State#state{monitors = Rest});
        error -> server_down(Monitor, State)
    end;
handle_info(kill_calls, #state{streams = Streams} = State) ->
    _ = [exit(Pid, kill) || #entry{pid = Pid} <- maps:values(Streams), is_pid(Pid)],
    {noreply, State};
handle_info(_Message, State) ->
    {noreply, State}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, #state{streams = Streams} = State) ->
    %% Calls still in flight, as when the connection closes under them,
    %% give their slots in the server's request limit back.
    _ = maps:fold(fun release/3, State, Streams),
    _ = [exit(Pid, kill) || #entry{pid = Pid} <- maps:values(Streams), is_pid(Pid)],
    Budget = maps:get(stream_budget, State#state.config),
    _ = [
        trevrpc_zstd_stream:release(Budget, Reserved)
     || #entry{reserved = Reserved} <- maps:values(Streams)
    ],
    ok.

%%% Connection events

quic_event({stream_opened, Id}, State) ->
    {noreply, stream_opened(Id, State)};
quic_event({stream_data, Id, Bin, Fin}, State) ->
    {noreply, stream_data(Id, Bin, Fin, State)};
quic_event({stream_reset, Id, _Code}, State) ->
    {noreply, abandoned(Id, State)};
quic_event({stop_sending, Id, _Code}, State) ->
    {noreply, abandoned(Id, State)};
quic_event(send_ready, #state{send_waiters = Waiters} = State) ->
    _ = [Pid ! {trevrpc_stream, Ref, send_ready} || {Pid, Ref} <- Waiters],
    {noreply, State#state{send_waiters = []}};
quic_event({closed, _Reason}, State) ->
    {stop, normal, State};
quic_event(_Event, State) ->
    %% connected, datagrams (not used yet), timers, transport errors.
    {noreply, State}.

stream_opened(Id, #state{transport = Transport} = State) when Id band 2 =/= 0 ->
    %% TrevRPC uses no unidirectional streams.
    _ = trevrpc_transport:stop_sending(Transport, Id, ?CANCELLED),
    State;
stream_opened(Id, #state{stopping = true} = State) ->
    lone_status(Id, false, trevrpc_status:new(unavailable, <<"server is shutting down">>), State),
    State;
stream_opened(Id, #state{active = Active, config = Config} = State) ->
    #{max_streams_per_connection := Max} = Config,
    case Active >= Max of
        true ->
            lone_status(
                Id, false, trevrpc_status:new(unavailable, <<"too many concurrent RPCs">>), State
            ),
            State;
        false ->
            ok = trevrpc_transport:manual_flow_control(State#state.transport, Id),
            #{
                max_frame_size := MaxFrame,
                max_stream_body_size := MaxBody,
                initial_request_timeout := Initial
            } =
                Config,
            Entry = #entry{
                reader = trevrpc_frame:reader(MaxFrame, #{max_bytes => MaxBody}),
                timer = start_timer(Initial, {initial, Id})
            },
            State#state{streams = (State#state.streams)#{Id => Entry}, active = Active + 1}
    end.

stream_data(Id, Bin, Fin, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #entry{state = reading} = Entry} ->
            read_request(Id, Entry, Bin, Fin, State);
        #{Id := #entry{state = running, ref = Ref, pid = Pid} = Entry} ->
            Pid ! {trevrpc_stream, Ref, {data, Bin, Fin}},
            case Fin of
                true -> State#state{streams = Streams#{Id := Entry#entry{fin = true}}};
                false -> State
            end;
        #{Id := Entry} ->
            %% The call is ending; nothing reads its requests any more, so
            %% their credit goes back at once instead of waiting for the
            %% client to reset the stream.
            _ = trevrpc_transport:consume(State#state.transport, Id, byte_size(Bin)),
            State#state{streams = Streams#{Id := Entry#entry{fin = Entry#entry.fin orelse Fin}}};
        _ ->
            _ = trevrpc_transport:consume(State#state.transport, Id, byte_size(Bin)),
            State
    end.

%% The peer reset its request side or stopped reading: it abandoned the call.
abandoned(Id, #state{streams = Streams, transport = Transport} = State) ->
    case Streams of
        #{Id := #entry{state = reading} = Entry} ->
            cancel_timer(Entry),
            _ = trevrpc_transport:reset_stream(Transport, Id, ?CANCELLED),
            _ = trevrpc_transport:stop_sending(Transport, Id, ?CANCELLED),
            remove(Id, release(Id, Entry, State));
        #{Id := #entry{state = running, pid = Pid} = Entry} ->
            exit(Pid, kill),
            cancel_timer(Entry),
            _ = trevrpc_transport:reset_stream(Transport, Id, ?CANCELLED),
            _ = trevrpc_transport:stop_sending(Transport, Id, ?CANCELLED),
            release(Id, Entry#entry{state = cancelling, timer = undefined}, State);
        _ ->
            State
    end.

%%% Reading the REQUEST

read_request(Id, #entry{reader = Reader} = Entry, Bin, Fin, #state{transport = Transport} = State) ->
    _ = trevrpc_transport:consume(Transport, Id, byte_size(Bin)),
    case trevrpc_frame:feed(Bin, Reader) of
        {ok, [], Next} when Fin ->
            Message =
                case trevrpc_frame:at_boundary(Next) of
                    true -> <<"request stream ended before REQUEST">>;
                    false -> <<"request stream ended inside a frame">>
                end,
            refuse(
                Id, Entry#entry{fin = true}, trevrpc_status:new(invalid_argument, Message), State
            );
        {ok, [], Next} ->
            State#state{streams = (State#state.streams)#{Id := Entry#entry{reader = Next}}};
        {ok, [{request, Payload} | Rest], Next} ->
            cancel_timer(Entry),
            dispatch(
                Id, Entry#entry{reader = Next, fin = Fin, timer = undefined}, Payload, Rest, State
            );
        {ok, [{Type, _} | _], _} ->
            Message = iolist_to_binary(
                io_lib:format("request stream must start with REQUEST, not ~s", [frame_name(Type)])
            ),
            refuse(
                Id, Entry#entry{fin = Fin}, trevrpc_status:new(invalid_argument, Message), State
            );
        {error, Reason} ->
            refuse(Id, Entry#entry{fin = Fin}, read_status(Reason, State), State)
    end.

read_status({frame_too_large, Length}, #state{config = #{max_frame_size := Max}}) ->
    trevrpc_status:new(
        resource_exhausted,
        iolist_to_binary(io_lib:format("frame length ~b exceeds maximum ~b", [Length, Max]))
    );
read_status({body_too_large, Max}, _State) ->
    trevrpc_status:new(
        resource_exhausted,
        iolist_to_binary(
            io_lib:format("request stream exceeded maximum body size of ~b bytes", [Max])
        )
    ).

dispatch(Id, Entry, Payload, Rest, #state{config = Config} = State) ->
    case check_request(Payload, Config) of
        {ok, Request, Route, Metadata, Timeout} ->
            admit(Id, Entry, Request, Route, Metadata, Timeout, Rest, State);
        {error, Status} ->
            refuse(Id, Entry, Status, State)
    end.

%% Checks a REQUEST in the order the Go and Rust servers do, with their
%% messages.
check_request(Payload, #{routes := Routes}) ->
    maybe
        {ok, Request} ?= decode_request(Payload),
        #{method := Method, kind := Kind, stream_compression := Compression} = Request,
        ok ?= check_method(Method),
        ok ?= check_kind(Kind),
        ok ?= check_stream_compression(Compression),
        {ok, Metadata} ?= merge_metadata(Request),
        ok ?= validate_metadata(Metadata),
        {ok, Timeout} ?= timeout(Request),
        {ok, Route} ?= route(Method, Kind, Routes),
        {ok, Request, Route, Metadata, Timeout}
    end.

decode_request(Payload) ->
    case trevrpc_wire:decode_request(Payload) of
        {ok, Request} -> {ok, Request};
        error -> invalid(<<"invalid RPC request frame: malformed RpcRequest">>)
    end.

check_method(<<$/, Rest/binary>>) ->
    case binary:split(Rest, <<"/">>) of
        [Service, Method] when Service =/= <<>>, Method =/= <<>> ->
            case binary:match(Method, <<"/">>) of
                nomatch -> ok;
                _ -> malformed_method()
            end;
        _ ->
            malformed_method()
    end;
check_method(_) ->
    malformed_method().

malformed_method() ->
    invalid(<<"malformed RPC method path; expected /<service>/<method>">>).

check_kind({unknown, Value}) ->
    invalid(iolist_to_binary(io_lib:format("unsupported TrevRPC RPC kind ~b", [Value])));
check_kind(_) ->
    ok.

check_stream_compression({unknown, Value}) ->
    invalid(iolist_to_binary(io_lib:format("unsupported TrevRPC stream compression ~b", [Value])));
check_stream_compression(_) ->
    ok.

%% This server stores no connection metadata sets yet. An offer serves its
%% own call and is never confirmed, as on the ordinary HTTP mapping, so the
%% client keeps offering; a reference names a set it does not hold.
merge_metadata(#{metadata := Own, connection_metadata := undefined}) ->
    {ok, Own};
merge_metadata(#{metadata := Own, connection_metadata := #{id := Id, metadata := Set}}) ->
    case map_size(Set) of
        0 -> invalid(iolist_to_binary(io_lib:format("unknown connection metadata set ~b", [Id])));
        _ -> {ok, trevrpc_metadata:merge(Set, Own)}
    end.

validate_metadata(Metadata) ->
    case trevrpc_metadata:validate(Metadata) of
        ok -> ok;
        {error, Message} -> invalid(Message)
    end.

%% Erlang timers take up to 2^32 - 1 ms; a longer timeout is rejected rather
%% than truncated.
timeout(#{timeout_nanos := 0}) ->
    {ok, infinity};
timeout(#{timeout_nanos := Nanos}) ->
    case (Nanos + 999999) div 1000000 of
        Millis when Millis =< 16#FFFFFFFF -> {ok, Millis};
        _ -> invalid(<<"RPC timeout is too large">>)
    end.

route(Method, Kind, Routes) ->
    case Routes of
        #{Method := #{kind := Kind} = Route} ->
            {ok, Route};
        #{Method := #{kind := Registered}} ->
            invalid(
                iolist_to_binary(
                    io_lib:format("streaming RPC kind mismatch for ~ts: expected ~b, got ~b", [
                        Method, trevrpc_wire:kind_to_int(Registered), trevrpc_wire:kind_to_int(Kind)
                    ])
                )
            );
        #{} ->
            {error,
                trevrpc_status:new(
                    unimplemented,
                    iolist_to_binary(io_lib:format("unknown RPC method ~ts", [Method]))
                )}
    end.

%% Whether the call decodes stream-compressed requests: the client offered
%% stream compression, the route allows it for requests it streams, and the
%% budget has room for the decompressing context, which the call holds until
%% it ends. An optimistic call it cannot decode is refused before any
%% application code runs, and the client replays it without stream
%% compression.
stream_requests(#{stream_compression := Offer, kind := Kind}, Route, Budget) ->
    {Requests, _} = trevrpc_zstd:directions(maps:get(compression, Route, none)),
    Decode =
        Offer =/= none andalso Requests =:= stream andalso trevrpc_wire:streams_requests(Kind) andalso
            trevrpc_zstd_stream:reserve(Budget, trevrpc_zstd_stream:decoder_cost()),
    case {Decode, Offer} of
        {true, _} ->
            {ok, true};
        {false, optimistic} ->
            {error, (trevrpc_status:new(
                resource_exhausted, <<"stream-compressed requests refused">>
            ))#{
                refused_stream_compression => true
            }};
        {false, _} ->
            {ok, false}
    end.

invalid(Message) ->
    {error, trevrpc_status:new(invalid_argument, Message)}.

admit(Id, Entry, Request, Route, Metadata, Timeout, Rest, #state{config = Config} = State) ->
    #{requests := Requests, max_concurrent_requests := Max} = Config,
    case counters:get(Requests, 1) >= Max of
        true ->
            refuse(
                Id, Entry, trevrpc_status:new(unavailable, <<"too many concurrent RPCs">>), State
            );
        false ->
            case stream_requests(Request, Route, maps:get(stream_budget, Config)) of
                {ok, Decode} ->
                    start_call(Id, Entry, Request, Route, Metadata, Timeout, Rest, Decode, State);
                {error, Status} ->
                    refuse(Id, Entry, Status, State)
            end
    end.

start_call(Id, Entry, Request, Route, Metadata, Timeout, Rest, Decode, State) ->
    #state{config = #{requests := Requests} = Config} = State,
    counters:add(Requests, 1, 1),
    Deadline =
        case Timeout of
            infinity -> infinity;
            _ -> erlang:monotonic_time(millisecond) + Timeout
        end,
    Spec = #{
        transport => State#state.transport,
        id => Id,
        ref => Entry#entry.ref,
        router => self(),
        reader => Entry#entry.reader,
        frames => Rest,
        fin => Entry#entry.fin,
        request => Request,
        metadata => Metadata,
        route => Route,
        deadline => Deadline,
        config => Config,
        stream_requests => Decode
    },
    {Pid, Monitor} = trevrpc_server_call:start(Spec),
    Running = Entry#entry{
        state = running,
        pid = Pid,
        monitor = Monitor,
        timer = start_timer(Timeout, {deadline, Id}),
        reserved =
            case Decode of
                true -> trevrpc_zstd_stream:decoder_cost();
                false -> 0
            end
    },
    State#state{
        streams = (State#state.streams)#{Id := Running},
        monitors = (State#state.monitors)#{Monitor => Id}
    }.

%% Ends a stream that has no call yet with a lone STATUS.
refuse(Id, Entry, Status, State) ->
    cancel_timer(Entry),
    lone_status(Id, Entry#entry.fin, Status, State),
    remove(Id, release(Id, Entry, State)).

%% Writes a terminal STATUS from this process, which must not block, so it
%% is queued without the send queue's ceiling. A request side the peer has
%% not finished is stopped after it.
lone_status(Id, Fin, Status, #state{transport = Transport, config = #{max_frame_size := Max}}) ->
    ok = trevrpc_transport:send_data_async(
        Transport, Id, trevrpc_server_call:status_frame(Status, Max), true
    ),
    _ =
        case Fin of
            true -> ok;
            false -> trevrpc_transport:stop_sending(Transport, Id, ?CANCELLED)
        end,
    ok.

%%% Call processes

call_down(Id, Reason, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #entry{state = finishing}} ->
            maybe_close(remove(Id, State));
        #{Id := #entry{state = expiring, fin = Fin}} ->
            lone_status(
                Id, Fin, trevrpc_status:new(deadline_exceeded, <<"deadline exceeded">>), State
            ),
            maybe_close(remove(Id, State));
        #{Id := #entry{state = cancelling}} ->
            maybe_close(remove(Id, State));
        #{Id := #entry{state = shutdown}} ->
            cut_off(Id, State),
            maybe_close(remove(Id, State));
        #{Id := #entry{state = running, fin = Fin} = Entry} ->
            %% The call process failed outside its handler, which catches
            %% handler failures itself.
            logger:error("trevrpc call process on stream ~b exited: ~p", [Id, Reason]),
            cancel_timer(Entry),
            State1 = release(Id, Entry, State),
            lone_status(Id, Fin, trevrpc_status:new(internal, <<"internal server error">>), State1),
            maybe_close(remove(Id, State1));
        _ ->
            maybe_close(State)
    end.

server_down(Monitor, State) ->
    %% The only other monitor is the server's.
    _ = Monitor,
    handle_cast(shutdown, State).

shutdown_stream(Id, #entry{state = reading} = Entry, State) ->
    refuse(Id, Entry, trevrpc_status:new(unavailable, <<"server is shutting down">>), State);
shutdown_stream(Id, #entry{state = running, pid = Pid} = Entry, State) ->
    %% A handler that traps exits gets the grace period to return.
    exit(Pid, shutdown),
    cancel_timer(Entry),
    release(Id, Entry#entry{state = shutdown, timer = undefined}, State);
shutdown_stream(_Id, _Entry, State) ->
    State.

%% A call cut off by shutdown: the client sees its stream reset.
cut_off(Id, #state{transport = Transport}) ->
    _ = trevrpc_transport:reset_stream(Transport, Id, ?CANCELLED),
    _ = trevrpc_transport:stop_sending(Transport, Id, ?CANCELLED),
    ok.

maybe_close(#state{stopping = true, streams = Streams, transport = Transport} = State) ->
    Calls = [Id || Id := #entry{pid = Pid} <- Streams, is_pid(Pid)],
    case Calls of
        [] ->
            ok = close_transport(Transport),
            {stop, normal, State};
        _ ->
            {noreply, State}
    end;
maybe_close(State) ->
    {noreply, State}.

close_transport(undefined) -> ok;
close_transport(Transport) -> trevrpc_transport:close(Transport, 0).

%%% Bookkeeping

%% Stores the stream's entry, releasing its slot in the per-connection
%% limit and, for a call that was started, its slot in the server's request
%% limit, unless it released them already.
release(Id, #entry{counted = false} = Entry, State) ->
    State#state{streams = (State#state.streams)#{Id => Entry}};
release(Id, #entry{pid = Pid} = Entry, #state{active = Active, config = Config} = State) ->
    case Pid of
        undefined -> ok;
        _ -> counters:sub(maps:get(requests, Config), 1, 1)
    end,
    State#state{
        streams = (State#state.streams)#{Id => Entry#entry{counted = false}}, active = Active - 1
    }.

%% Forgets the stream, giving back the stream compression memory its call
%% held.
remove(Id, #state{streams = Streams, config = Config} = State) ->
    case Streams of
        #{Id := #entry{reserved = Reserved}} ->
            trevrpc_zstd_stream:release(maps:get(stream_budget, Config), Reserved);
        #{} ->
            ok
    end,
    State#state{streams = maps:remove(Id, Streams)}.

start_timer(infinity, _Message) ->
    undefined;
start_timer(Timeout, Message) ->
    erlang:start_timer(Timeout, self(), Message).

cancel_timer(#entry{timer = undefined}) ->
    ok;
cancel_timer(#entry{timer = Timer}) ->
    _ = erlang:cancel_timer(Timer, [{async, true}, {info, false}]),
    ok.

frame_name(message) -> "MESSAGE";
frame_name(request) -> "REQUEST";
frame_name(headers) -> "HEADERS";
frame_name(status) -> "STATUS";
frame_name(zstd_message) -> "ZSTD_MESSAGE";
frame_name(zstd_stream_message) -> "ZSTD_STREAM_MESSAGE";
frame_name(answer) -> "ANSWER".
