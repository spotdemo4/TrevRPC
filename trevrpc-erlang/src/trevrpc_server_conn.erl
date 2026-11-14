%% @private The owner of one server-side connection: native QUIC, QMux, or
%% the HTTP mapping over HTTP/3 or HTTP/2.
%%
%% It receives every event of the connection. For each stream it reads the
%% REQUEST frame, checks it, and either answers with a lone STATUS or starts
%% a call process (`trevrpc_server_call') to run the handler, then routes
%% the stream's events to that process. An HTTP request first passes the
%% HTTP mapping's checks ({@link trevrpc_http}): a request they refuse gets
%% an HTTP error, and an accepted one a `200', after which its bodies are
%% the stream as on the other transports.
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

-export([start/2, start_qmux/3, start_h2/2, serve_h3/3, shutdown/1, finish/3, reserve/4]).
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
    stopping = false :: boolean(),
    %% The client's address, for the HTTP admission: looked up at the first
    %% request over HTTP/3.
    peer :: {inet:ip_address(), inet:port_number()} | undefined | unknown,
    %% When an HTTP/2 connection last had a stream, to close it once idle.
    active_at = 0 :: integer(),
    %% Lone STATUS frames quic_h3 refused with a full send queue, written
    %% again when it says the stream's writer may (`send_ready').
    pending = #{} :: #{non_neg_integer() => binary()}
}).

-define(CANCELLED, 1).
-define(H3_NO_ERROR, 16#100).
%% HTTP/2 windows are at most 2^31 - 1 bytes.
-define(MAX_H2_WINDOW, 2147483647).

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

%% @doc Starts the owner of an HTTP/2 connection over the TLS connection
%% `Socket', whose handshake completed. The caller must be the socket's
%% controlling process; the HTTP/2 connection takes it over.
-spec start_h2(ssl:sslsocket(), trevrpc_server:config()) -> {ok, pid()} | {error, term()}.
start_h2(Socket, Config) ->
    case gen_server:start(?MODULE, {undefined, Config}, []) of
        {ok, Pid} ->
            Peer =
                case ssl:peername(Socket) of
                    {ok, Address} -> Address;
                    {error, _} -> unknown
                end,
            case ssl:controlling_process(Socket, Pid) of
                ok ->
                    gen_server:cast(Pid, {h2, Socket, Peer}),
                    {ok, Pid};
                {error, _} = Error ->
                    exit(Pid, kill),
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Makes the owner `Pid' of the QUIC connection `Quic', which
%% negotiated `h3', serve HTTP/3 through `H3', the connection's `quic_h3'
%% process, whose events it receives from now on. Called in the QUIC
%% connection's ALPN hook, before any stream event.
-spec serve_h3(pid(), pid(), pid()) -> ok.
serve_h3(Pid, H3, Quic) ->
    gen_server:call(Pid, {h3, H3, Quic}).

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
    %% With HTTP, the QUIC connection's ALPN hook looks its owner up here.
    %% Registered before this process can stop, and removed when it does
    %% if the hook never ran.
    _ =
        case {Transport, Config} of
            {{quic, Conn}, #{owners := Owners}} when Owners =/= undefined ->
                ets:insert(Owners, {Conn, self()});
            _ ->
                true
        end,
    {ok, #state{transport = Transport, config = Config}}.

-spec handle_call(term(), gen_server:from(), #state{}) -> {reply, term(), #state{}}.
handle_call({h3, H3, Quic}, _From, #state{transport = {quic, Quic}} = State) ->
    {reply, ok, State#state{transport = {h3, H3, Quic}}};
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
handle_cast({h2, Socket, Peer}, #state{config = Config} = State) ->
    case start_h2_connection(Socket, Config) of
        {ok, Conn} ->
            arm_idle(maps:get(idle_timeout, maps:get(http, Config))),
            {noreply, State#state{
                transport = {h2, Conn},
                peer = Peer,
                active_at = erlang:monotonic_time(millisecond)
            }};
        {error, _Reason} ->
            _ = ssl:close(Socket),
            {stop, normal, State}
    end;
handle_cast(shutdown, #state{stopping = true} = State) ->
    {noreply, State};
handle_cast(shutdown, #state{config = #{graceful_shutdown_timeout := Grace}} = State) ->
    %% HTTP clients send no more requests; QUIC and QMux streams that still
    %% open are refused one by one.
    _ =
        case State#state.transport of
            undefined -> ok;
            Transport -> trevrpc_transport:goaway(Transport)
        end,
    State1 = maps:fold(fun shutdown_stream/3, State#state{stopping = true}, State#state.streams),
    erlang:send_after(Grace, self(), kill_calls),
    maybe_close(State1).

-spec handle_info(term(), #state{}) -> {noreply, #state{}} | {stop, normal, #state{}}.
handle_info({Tag, Pid, Event}, #state{transport = Transport} = State) when
    is_tuple(Transport),
    element(2, Transport) =:= Pid,
    (Tag =:= quic orelse Tag =:= trevrpc_qmux orelse Tag =:= quic_h3 orelse Tag =:= h2)
->
    quic_event(Event, State);
handle_info({'EXIT', Pid, _Reason}, #state{transport = {h2, Pid}} = State) ->
    %% The HTTP/2 connection, linked, ended.
    {stop, normal, State};
handle_info({timeout, _Timer, idle}, State) ->
    idle(State);
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
    %% A STATUS that a client which stopped reading still has not taken
    %% holds the connection no longer.
    maybe_close(State#state{pending = #{}});
handle_info(_Message, State) ->
    {noreply, State}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, #state{streams = Streams} = State) ->
    %% A QUIC connection that never got to its ALPN hook.
    _ =
        case {State#state.transport, State#state.config} of
            {{quic, Conn}, #{owners := Owners}} when Owners =/= undefined ->
                ets:delete(Owners, Conn);
            _ ->
                true
        end,
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
quic_event({request, Id, Method, Target, Headers}, State) ->
    {noreply, http_request(Id, Method, Target, Headers, State)};
quic_event({data, Id, Bin, Fin}, State) ->
    {noreply, stream_data(Id, Bin, Fin, State)};
quic_event({trailers, Id, _Trailers}, State) ->
    %% The HTTP mapping has no request trailers: the client broke it.
    maybe_close(abandoned(Id, State));
quic_event({stream_reset, Id, _Code}, State) ->
    %% The STATUS a stopping connection waited on may have gone with it.
    maybe_close(abandoned(Id, State));
quic_event({stop_sending, Id, _Code}, State) ->
    maybe_close(abandoned(Id, State));
quic_event({send_ready, Id}, #state{transport = {h3, _, _}, pending = Pending} = State) ->
    case Pending of
        #{Id := Frame} ->
            case trevrpc_transport:send_data_async(State#state.transport, Id, Frame, true) of
                %% Refused again: quic_h3 tells this process once more.
                {error, send_queue_full} -> {noreply, State};
                ok -> maybe_close(State#state{pending = maps:remove(Id, Pending)})
            end;
        #{} ->
            {noreply, State}
    end;
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
    lone_status(Id, false, trevrpc_status:new(unavailable, <<"server is shutting down">>), State);
stream_opened(Id, State) ->
    case full(State) of
        true ->
            lone_status(
                Id, false, trevrpc_status:new(unavailable, <<"too many concurrent RPCs">>), State
            );
        false ->
            ok = trevrpc_transport:manual_flow_control(State#state.transport, Id),
            open_stream(Id, State)
    end.

full(#state{active = Active, config = #{max_streams_per_connection := Max}}) ->
    Active >= Max.

%% Reads the REQUEST of the stream that opened.
open_stream(Id, #state{active = Active, config = Config} = State) ->
    #{
        max_frame_size := MaxFrame,
        max_stream_body_size := MaxBody,
        initial_request_timeout := Initial
    } = Config,
    Entry = #entry{
        reader = trevrpc_frame:reader(MaxFrame, #{max_bytes => MaxBody}),
        timer = start_timer(Initial, {initial, Id})
    },
    State#state{streams = (State#state.streams)#{Id => Entry}, active = Active + 1}.

%%% HTTP requests

http_request(Id, Method, Target, Headers, #state{transport = Transport} = State0) ->
    {Peer, State} = peer(State0),
    Version =
        case trevrpc_transport:kind(Transport) of
            h3 -> <<"HTTP/3.0">>;
            h2 -> <<"HTTP/2.0">>
        end,
    Http = http_options(State),
    Checked =
        case trevrpc_http:check(Version, Method, Target, Headers, Peer, Http) of
            ok when State#state.stopping ->
                {refuse, 503, [], <<"server is shutting down">>};
            ok ->
                case full(State) of
                    true -> {refuse, 503, [], <<"too many concurrent RPCs on HTTP connection">>};
                    false -> ok
                end;
            Refusal ->
                Refusal
        end,
    case Checked of
        ok ->
            accept_http(Id, Http, State);
        {refuse, Status, Extra, Message} ->
            refuse_http(Id, Status, trevrpc_http:refusal_headers(Extra, Http), Message, State),
            State
    end.

%% The client's address: from the socket over HTTP/2, from the QUIC
%% connection over HTTP/3, the first time it is asked.
peer(#state{peer = undefined, transport = {h3, _H3, Quic}} = State) ->
    Peer =
        case quic:peername(Quic) of
            {ok, Address} -> Address;
            {error, _} -> unknown
        end,
    peer(State#state{peer = Peer});
peer(#state{peer = Peer} = State) when is_tuple(Peer) ->
    {Peer, State};
peer(State) ->
    {undefined, State}.

%% HTTP/2 responses advertise HTTP/3; HTTP/3 ones need not.
http_options(#state{transport = {h2, _}, config = #{http := Http}}) -> Http;
http_options(#state{config = #{http := Http}}) -> maps:remove(alt_svc, Http).

%% Accepts the call: the stream's data comes here from now on, credited as
%% it is read, and the call starts once its REQUEST arrived.
accept_http(Id, Http, #state{transport = Transport} = State) ->
    case accept_stream(Transport, Id, trevrpc_http:accepted_headers(Http)) of
        ok ->
            open_stream(Id, State#state{active_at = erlang:monotonic_time(millisecond)});
        {error, _} ->
            %% The stream is gone already.
            _ = trevrpc_transport:reset_stream(Transport, Id, ?CANCELLED),
            State
    end.

accept_stream({h3, H3, _Quic}, Id, Headers) ->
    maybe
        ok ?= h3_handler(H3, Id),
        ok ?= quic_h3:send_response(H3, Id, 200, Headers),
        %% quic_h3 holds a response's HEADERS back until its first DATA;
        %% the client hears of its acceptance at once, as from Go and Rust.
        _ = quic_h3:send_data(H3, Id, <<>>, false),
        ok
    end;
accept_stream({h2, Conn}, Id, Headers) ->
    maybe
        ok ?= h2:set_stream_handler(Conn, Id, self(), #{flow_control => manual}),
        h2:send_response(Conn, Id, 200, Headers)
    end.

%% What arrived before is sent here as messages, after this.
h3_handler(H3, Id) ->
    case
        quic_h3:set_stream_handler(H3, Id, self(), #{flow_control => manual, drain_buffer => false})
    of
        ok -> ok;
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.

%% Answers a request the mapping refused, and stops reading its body.
refuse_http(Id, Status, Headers, Message, #state{transport = {h3, H3, Quic}}) ->
    _ = quic_h3:respond(H3, Id, Status, Headers, <<Message/binary, $\n>>),
    _ = quic:stop_sending(Quic, Id, ?H3_NO_ERROR),
    ok;
refuse_http(Id, Status, Headers, Message, #state{transport = {h2, Conn}}) ->
    %% The response is written whole, so the stream can end (RFC 9113
    %% Section 8.1): erlang_h2 would otherwise hold the rest of the body.
    _ = h2:respond(Conn, Id, Status, Headers, <<Message/binary, $\n>>),
    _ = h2:cancel(Conn, Id, no_error),
    ok.

%% HTTP/2 connections without a stream for the idle timeout are closed, as
%% the Go server's are. QUIC's idle timeout covers HTTP/3.
arm_idle(infinity) ->
    ok;
arm_idle(Timeout) ->
    _ = erlang:start_timer(Timeout, self(), idle),
    ok.

idle(#state{config = #{http := #{idle_timeout := Timeout}}, streams = Streams} = State) when
    map_size(Streams) > 0
->
    arm_idle(Timeout),
    {noreply, State};
idle(#state{config = #{http := #{idle_timeout := Timeout}}, active_at = At} = State) ->
    case erlang:monotonic_time(millisecond) - At of
        Idle when Idle >= Timeout ->
            ok = close_transport(State#state.transport),
            {stop, normal, State};
        Idle ->
            arm_idle(Timeout - Idle),
            {noreply, State}
    end.

start_h2_connection(Socket, Config) ->
    #{max_frame_size := MaxFrame, max_streams_per_connection := Max} = Config,
    Opts = #{
        settings => #{
            initial_window_size => min(trevrpc_quic:stream_window(MaxFrame), ?MAX_H2_WINDOW),
            %% One spare stream lets an RPC past the limit still get its 503.
            max_concurrent_streams => Max + 1
        },
        connection_window_size => max(
            65535, min(trevrpc_quic:connection_window(MaxFrame, Max), ?MAX_H2_WINDOW)
        )
    },
    maybe
        {ok, Conn} ?= h2_connection:start_link(server, Socket, self(), Opts),
        ok ?= transfer(Socket, Conn),
        ok ?= h2_connection:activate(Conn),
        {ok, Conn}
    end.

transfer(Socket, Conn) ->
    case ssl:controlling_process(Socket, Conn) of
        ok ->
            ok;
        {error, _} = Error ->
            exit(Conn, kill),
            Error
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
            %% A STATUS still waiting to be written goes with its stream.
            State#state{pending = maps:remove(Id, State#state.pending)}
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
                io_lib:format("request stream must start with REQUEST, not ~s", [
                    trevrpc_frame:name(Type)
                ])
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
    remove(Id, release(Id, Entry, lone_status(Id, Entry#entry.fin, Status, State))).

%% Writes a terminal STATUS from this process, which must not block, so it
%% is queued without the send queue's ceiling, or over HTTP/3, which has
%% none, written again once quic_h3's queue drained. A request side the
%% peer has not finished is stopped after it.
lone_status(
    Id, Fin, Status, #state{transport = Transport, config = #{max_frame_size := Max}} = State
) ->
    Frame = iolist_to_binary(trevrpc_server_call:status_frame(Status, Max)),
    State1 =
        case trevrpc_transport:send_data_async(Transport, Id, Frame, true) of
            ok -> State;
            {error, send_queue_full} -> State#state{pending = (State#state.pending)#{Id => Frame}}
        end,
    _ =
        case Fin of
            true -> ok;
            false -> trevrpc_transport:stop_sending(Transport, Id, ?CANCELLED)
        end,
    State1.

%%% Call processes

call_down(Id, Reason, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #entry{state = finishing}} ->
            maybe_close(remove(Id, State));
        #{Id := #entry{state = expiring, fin = Fin}} ->
            State1 = lone_status(
                Id, Fin, trevrpc_status:new(deadline_exceeded, <<"deadline exceeded">>), State
            ),
            maybe_close(remove(Id, State1));
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
            State1 = lone_status(
                Id,
                Fin,
                trevrpc_status:new(internal, <<"internal server error">>),
                release(Id, Entry, State)
            ),
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
    case Calls =:= [] andalso map_size(State#state.pending) =:= 0 of
        true ->
            ok = close_transport(Transport),
            {stop, normal, State};
        false ->
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
    State#state{streams = maps:remove(Id, Streams), active_at = erlang:monotonic_time(millisecond)}.

start_timer(infinity, _Message) ->
    undefined;
start_timer(Timeout, Message) ->
    erlang:start_timer(Timeout, self(), Message).

cancel_timer(#entry{timer = undefined}) ->
    ok;
cancel_timer(#entry{timer = Timer}) ->
    _ = erlang:cancel_timer(Timer, [{async, true}, {info, false}]),
    ok.
