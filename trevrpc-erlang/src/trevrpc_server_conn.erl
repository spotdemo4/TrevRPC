%% @private The owner of one server-side connection: native QUIC, QMux,
%% HTTP/3, HTTP/2 or HTTP/1.1, or a WebTransport session.
%%
%% It receives every event of the connection. For each stream it starts a
%% call process (`trevrpc_server_call') at once and routes the stream's
%% events to it; the call reads and checks the REQUEST itself, and asks to
%% be admitted ({@link admit/4}) before its handler runs, so the work of
%% each call, and its blocking calls into the transport, stay out of this
%% process. An HTTP request first passes the HTTP mapping's checks
%% ({@link trevrpc_http}): a request they refuse gets an HTTP error, and an
%% accepted one a `200', after which its bodies are the stream as on the
%% other transports.
%%
%% An HTTP request that TrevRPC does not claim goes to the HTTP fallback,
%% when the server has one: it takes a stream slot like a call, and its
%% handler runs in a request process of its own
%% ({@link trevrpc_http_request}), to which this process routes the
%% request body. HTTP/1.1 carries only the fallback's requests, one at a
%% time, and WebSocket upgrades at the HTTP path, after which the
%% connection is a QMux session.
%%
%% A WebTransport CONNECT on an HTTP/3 connection that passes its checks
%% becomes a session of erlang-webtransport, with a connection process of
%% its own, which this one starts, and waits for when it shuts down. The
%% session's handler, {@link trevrpc_webtransport}, sends that process the
%% session's events.
%%
%% It also owns each call's lifecycle and the resources it holds: its slots
%% in the per-connection and server-wide limits, its datagram flow, its
%% stream compression memory. The call's deadline, the peer's cancellation,
%% and shutdown all end a call by stopping its process, whether or not the
%% handler cooperates. A call process asks for permission
%% ({@link finish/3}) before it writes its terminal STATUS, which settles
%% the race between a call that finishes and a deadline or cancellation
%% that ends it: exactly one of them writes the call's end.
-module(trevrpc_server_conn).

-behaviour(gen_server).

-export([start/2, start_qmux/3, start_http/3, serve_h3/3, shutdown/1, admit/4, finish/3, reserve/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(entry, {
    %% A call, or a request of the HTTP fallback, which is running until its
    %% process ends, or cancelling once the peer abandoned it.
    kind = call :: call | fallback,
    %% starting: the call process reads and checks the REQUEST; running: it
    %% was admitted and runs its handler; finishing: it is writing its
    %% terminal STATUS; expiring: it was stopped at its deadline;
    %% overflowing: it was stopped for reading too slowly (WebTransport);
    %% cancelling: the peer abandoned the call; shutdown: the server is
    %% shutting down.
    state = starting ::
        starting | running | finishing | expiring | overflowing | cancelling | shutdown,
    ref :: reference(),
    %% The peer finished the request side.
    fin = false :: boolean(),
    %% The deadline timer, once admitted.
    timer :: reference() | undefined,
    pid :: pid(),
    monitor :: reference(),
    %% Whether the stream still holds its concurrency slots, and whether
    %% one of them is in the server's request limit.
    counted = true :: boolean(),
    admitted = false :: boolean(),
    %% Stream compression memory the call holds, given back when it ends.
    reserved = 0 :: non_neg_integer(),
    %% The call's datagram flow, registered until the stream is forgotten.
    flow :: pos_integer() | undefined
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
    %% Streams counted against the per-connection limit: those whose call
    %% has not released its slots.
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
    pending = #{} :: #{non_neg_integer() => binary()},
    %% The datagram flows of the calls, on native QUIC and WebTransport.
    flows :: trevrpc_datagram_flows:flows(),
    %% The connection processes of the WebTransport sessions of an HTTP/3
    %% connection, by their monitors.
    sessions = #{} :: #{reference() => pid()}
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

%% @doc Starts the owner of an HTTP/2 or HTTP/1.1 connection over the TLS
%% connection `Socket', whose handshake completed. The caller must be the
%% socket's controlling process; the HTTP connection takes it over.
-spec start_http(h2 | h1, ssl:sslsocket(), trevrpc_server:config()) ->
    {ok, pid()} | {error, term()}.
start_http(Kind, Socket, Config) ->
    case gen_server:start(?MODULE, {undefined, Config}, []) of
        {ok, Pid} ->
            Peer =
                case ssl:peername(Socket) of
                    {ok, Address} -> Address;
                    {error, _} -> unknown
                end,
            case ssl:controlling_process(Socket, Pid) of
                ok ->
                    gen_server:cast(Pid, {Kind, Socket, Peer}),
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

%% @doc Admits the call `Ref' on stream `Id', whose REQUEST passed its
%% checks: takes its slot in the server's request limit and its datagram
%% flow, reserves the memory to decode stream-compressed requests if the
%% call may, and starts its deadline. `{refuse, Status}' ends the call with
%% that STATUS, which the call writes; `cancelled' means it was ended
%% already.
%%
%% `Admission' holds the datagram flow the call asks for (0 for none), its
%% kind and stream compression offer, how its route compresses requests,
%% and its timeout.
-spec admit(pid(), non_neg_integer(), reference(), map()) ->
    {ok, #{
        flow := {pos_integer(), trevrpc_datagram_flows:credit()} | undefined,
        stream_requests := boolean()
    }}
    | {refuse, trevrpc_status:status()}
    | cancelled.
admit(Pid, Id, Ref, Admission) ->
    try
        gen_server:call(Pid, {admit, Id, Ref, Admission}, infinity)
    catch
        exit:_ -> cancelled
    end.

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
    %% Every event of the connection queues here; a deep queue kept off
    %% the heap is not copied by each garbage collection.
    process_flag(message_queue_data, off_heap),
    %% Exits with the server.
    monitor(process, maps:get(server, Config)),
    Flows = trevrpc_datagram_flows:new(server, 2 * maps:get(max_streams_per_connection, Config)),
    {ok, #state{transport = Transport, config = Config, flows = Flows}}.

-spec handle_call(term(), gen_server:from(), #state{}) -> {reply, term(), #state{}}.
handle_call({h3, H3, Quic}, _From, #state{transport = {quic, Quic}} = State) ->
    {reply, ok, State#state{transport = {h3, H3, Quic}}};
handle_call({admit, Id, Ref, Admission}, {Pid, _}, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #entry{state = starting, ref = Ref, pid = Pid} = Entry} ->
            case admission(Admission, State) of
                {ok, Flow, Decode} ->
                    admitted(Id, Entry, Admission, Flow, Decode, State);
                {error, Status} ->
                    %% The call writes the refusal.
                    Refused = release(Id, Entry#entry{state = finishing}, State),
                    {reply, {refuse, Status}, Refused}
            end;
        _ ->
            {reply, cancelled, State}
    end;
handle_call({reserve, Id, Ref, Cost}, _From, #state{streams = Streams, config = Config} = State) ->
    case Streams of
        #{Id := #entry{state = running, ref = Ref, reserved = Reserved} = Entry} ->
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
        #{Id := #entry{state = Running, ref = Ref} = Entry} when
            Running =:= starting; Running =:= running
        ->
            cancel_timer(Entry),
            {reply, ok, release(Id, Entry#entry{state = finishing, timer = undefined}, State)};
        _ ->
            {reply, cancelled, State}
    end.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}} | {stop, normal, #state{}}.
handle_cast({qmux, Socket, Opts}, #state{config = Config} = State) ->
    case trevrpc_qmux:start(Socket, server, self(), maps:merge(maps:get(qmux, Config), Opts)) of
        {ok, Session, Queue} ->
            {noreply, State#state{
                transport = {qmux, Session, Queue}, session_monitor = monitor(process, Session)
            }};
        {error, _Reason} ->
            {stop, normal, State}
    end;
handle_cast({Kind, Socket, Peer}, #state{config = Config} = State) when
    Kind =:= h2; Kind =:= h1
->
    case start_http_connection(Kind, Socket, Config) of
        {ok, Conn} ->
            {noreply, State#state{
                transport = arm_idle({Kind, Conn}, Config),
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
    %% A WebTransport session shuts down as a connection of its own.
    _ = [shutdown(Pid) || Pid <- maps:values(State#state.sessions)],
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
handle_info(
    {trevrpc_wt, Session, {established, {wt, Session, _, _} = Transport}},
    #state{transport = undefined} = State
) ->
    %% The first event of the session whose connection this is.
    {noreply, State#state{transport = Transport, session_monitor = monitor(process, Session)}};
handle_info({Tag, Pid, Event}, #state{transport = Transport} = State) when
    is_tuple(Transport),
    element(2, Transport) =:= Pid,
    (Tag =:= quic orelse Tag =:= trevrpc_qmux orelse Tag =:= quic_h3 orelse Tag =:= h2 orelse
        Tag =:= h1 orelse Tag =:= trevrpc_wt)
->
    quic_event(Event, State);
handle_info({'EXIT', Pid, _Reason}, #state{transport = {Kind, Pid}} = State) when
    Kind =:= h2; Kind =:= h1
->
    %% The HTTP connection, linked, ended; an HTTP/1.1 connection that
    %% upgraded to a WebSocket is not the transport any more.
    {stop, normal, State};
handle_info({timeout, _Timer, idle}, #state{transport = {Kind, _}} = State) when
    Kind =:= h2; Kind =:= h1
->
    idle(State);
handle_info({timeout, _Timer, trevrpc_datagram_hold}, #state{flows = Flows} = State) ->
    Now = erlang:monotonic_time(millisecond),
    {noreply, State#state{flows = trevrpc_datagram_flows:expire(Now, Flows)}};
handle_info({trevrpc_await_datagrams, Pid, Ref, Flow, Count}, #state{flows = Flows} = State) ->
    {noreply, State#state{flows = trevrpc_datagram_flows:await(Flow, Pid, Ref, Count, Flows)}};
handle_info(
    {trevrpc_send_ready, Pid, Ref}, #state{send_waiters = Waiters, transport = Transport} = State
) ->
    case Waiters of
        [] -> trevrpc_transport:notify_send_ready(Transport);
        _ -> ok
    end,
    {noreply, State#state{send_waiters = [{Pid, Ref} | Waiters]}};
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
handle_info({'DOWN', Monitor, process, _Pid, _Reason}, #state{sessions = Sessions} = State) when
    is_map_key(Monitor, Sessions)
->
    maybe_close(State#state{sessions = maps:remove(Monitor, Sessions)});
handle_info({'DOWN', Monitor, process, _Pid, Reason}, #state{monitors = Monitors} = State) ->
    case maps:take(Monitor, Monitors) of
        {Id, Rest} -> call_down(Id, Reason, State#state{monitors = Rest});
        error -> server_down(Monitor, State)
    end;
handle_info(kill_calls, #state{streams = Streams} = State) ->
    _ = [exit(Pid, kill) || #entry{pid = Pid} <- maps:values(Streams)],
    %% A STATUS that a client which stopped reading still has not taken
    %% holds the connection no longer.
    maybe_close(State#state{pending = #{}});
handle_info(_Message, State) ->
    {noreply, State}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, #state{streams = Streams, sessions = Sessions} = State) ->
    %% Calls still in flight, as when the connection closes under them,
    %% give their slots in the server's request limit back.
    _ = maps:fold(fun release/3, State, Streams),
    %% Writers waiting for the send queue that outlive their call, as a
    %% call's datagram sender may, stop waiting.
    _ = [Pid ! {trevrpc_stream, Ref, closed} || {Pid, Ref} <- State#state.send_waiters],
    _ = [shutdown(Pid) || Pid <- maps:values(Sessions)],
    _ = [exit(Pid, kill) || #entry{pid = Pid} <- maps:values(Streams)],
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
    {noreply, http_request(Id, Method, Target, Headers, requested(State))};
quic_event({upgrade, Id, _Protocol, Method, Target, Headers}, State) ->
    upgrade(Id, Method, Target, Headers, requested(State));
quic_event({data, Id, Bin, Fin}, State) ->
    {noreply, stream_data(Id, Bin, Fin, State)};
quic_event({trailers, Id, _Trailers}, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #entry{kind = fallback}} ->
            %% They end the request body, and the fallback does not read
            %% them.
            {noreply, stream_data(Id, <<>>, true, State)};
        #{} ->
            %% The HTTP mapping has no request trailers: the client broke it.
            maybe_close(abandoned(Id, State))
    end;
quic_event({stream_reset, Id, _Code}, State) ->
    %% The STATUS a stopping connection waited on may have gone with it.
    maybe_close(abandoned(Id, State));
quic_event({stop_sending, Id, _Code}, State) ->
    maybe_close(abandoned(Id, State));
quic_event({stream_overflow, Id}, State) ->
    {noreply, overflowed(Id, State)};
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
quic_event({datagram, Datagram}, #state{transport = Transport, flows = Flows} = State) when
    element(1, Transport) =:= quic; element(1, Transport) =:= wt
->
    Now = erlang:monotonic_time(millisecond),
    {noreply, State#state{flows = trevrpc_datagram_flows:dispatch(Datagram, Now, Flows)}};
quic_event({closed, _Reason}, State) ->
    {stop, normal, State};
quic_event(_Event, State) ->
    %% connected, timers, transport errors.
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
            %% Over native QUIC the stream is held in manual flow control
            %% already; QMux streams always are, and HTTP request streams
            %% from their acceptance.
            open_stream(Id, State)
    end.

full(#state{active = Active, config = #{max_streams_per_connection := Max}}) ->
    Active >= Max.

%% Starts the call of the stream that opened, which reads its REQUEST.
open_stream(Id, #state{active = Active, config = Config} = State) ->
    Ref = make_ref(),
    {Pid, Monitor} = trevrpc_server_call:start(#{
        transport => State#state.transport, id => Id, ref => Ref, router => self(), config => Config
    }),
    Entry = #entry{ref = Ref, pid = Pid, monitor = Monitor},
    State#state{
        streams = (State#state.streams)#{Id => Entry},
        monitors = (State#state.monitors)#{Monitor => Id},
        active = Active + 1
    }.

%%% HTTP requests

%% TrevRPC claims the HTTP path while it serves the HTTP mapping or
%% browsers; every other request goes to the fallback, CONNECTs included,
%% as on the Go server.
http_request(Id, Method, Target, Headers, #state{config = #{http_path := HttpPath}} = State) ->
    case trevrpc_http:path(Target) of
        HttpPath -> claimed(Id, Method, Target, Headers, State);
        _ -> unclaimed(Id, Method, Target, Headers, State)
    end.

claimed(Id, _Method, _Target, _Headers, #state{transport = {h1, _}} = State) ->
    %% WebSocket upgrades come as upgrade events.
    Message = <<"TrevRPC over HTTP requires HTTP/2 or HTTP/3">>,
    refuse_http(Id, 505, refusal_headers([], State), Message, State),
    State;
claimed(Id, <<"CONNECT">>, Target, Headers, #state{transport = {h3, _, _}} = State) when
    map_get(webtransport, State#state.config) =/= undefined
->
    connect(Id, Target, Headers, State);
claimed(Id, _Method, _Target, _Headers, #state{config = #{http := undefined}} = State) ->
    %% A server that serves WebTransport but not the HTTP mapping.
    refuse_http(Id, 404, refusal_headers([], State), <<"not found">>, State),
    State;
claimed(Id, Method, Target, Headers, #state{transport = Transport} = State0) ->
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

%% A request for the fallback, which takes a stream slot like a call.
unclaimed(Id, _Method, _Target, _Headers, #state{config = #{fallback := undefined}} = State) ->
    refuse_http(Id, 404, refusal_headers([], State), <<"not found">>, State),
    State;
unclaimed(Id, _Method, _Target, _Headers, #state{stopping = true} = State) ->
    refuse_http(Id, 503, refusal_headers([], State), <<"server is shutting down">>, State),
    State;
unclaimed(Id, Method, Target, Headers, State0) ->
    case full(State0) of
        true ->
            Message = <<"too many concurrent requests on HTTP connection">>,
            refuse_http(Id, 503, refusal_headers([], State0), Message, State0),
            State0;
        false ->
            {Peer, State} = peer(State0),
            start_fallback(Id, Method, Target, Headers, Peer, State)
    end.

%% The request's body comes here from now on, credited as the request's
%% process reads it.
start_fallback(Id, Method, Target, Headers, Peer, State) ->
    #state{transport = Transport, config = Config, active = Active} = State,
    case claim_stream(Transport, Id) of
        ok ->
            Ref = make_ref(),
            {Pid, Monitor} = trevrpc_http_request:start(#{
                transport => Transport,
                id => Id,
                ref => Ref,
                router => self(),
                handler => maps:get(fallback, Config),
                alt_svc => maps:get(alt_svc, Config),
                request => #{
                    method => Method,
                    target => Target,
                    headers => Headers,
                    protocol => protocol(Transport),
                    peer => Peer
                }
            }),
            Entry = #entry{
                kind = fallback, state = running, ref = Ref, pid = Pid, monitor = Monitor
            },
            State#state{
                streams = (State#state.streams)#{Id => Entry},
                monitors = (State#state.monitors)#{Monitor => Id},
                active = Active + 1,
                active_at = erlang:monotonic_time(millisecond)
            };
        {error, _} ->
            %% The stream is gone already.
            _ = trevrpc_transport:reset_stream(Transport, Id, ?CANCELLED),
            State
    end.

protocol({h3, _, _}) -> <<"HTTP/3.0">>;
protocol({h2, _}) -> <<"HTTP/2.0">>;
protocol({h1, _}) -> <<"HTTP/1.1">>.

%% An HTTP/1.1 upgrade request: a WebSocket session at the HTTP path, if it
%% passes the checks; off the path, a request for the fallback, whose
%% response declines the upgrade.
upgrade(Id, Method, Target, Headers, #state{config = #{http_path := HttpPath} = Config} = State0) ->
    Claimed = trevrpc_http:path(Target) =:= HttpPath,
    case {Claimed, Config} of
        {false, _} ->
            {noreply, unclaimed(Id, Method, Target, Headers, State0)};
        {true, #{webtransport := undefined}} ->
            {noreply, claimed(Id, Method, Target, Headers, State0)};
        {true, #{webtransport := WebTransport}} ->
            {Peer, State} = peer(State0),
            Checked =
                case trevrpc_http1:check_upgrade(Method, Target, Headers, Peer, WebTransport) of
                    {ok, _, Request} when State#state.stopping ->
                        {refuse, 503, <<"server is shutting down">>, Request};
                    Result ->
                        Result
                end,
            case Checked of
                {ok, Accept, Accepted} ->
                    accept_websocket(Id, Accept, Accepted, State);
                {refuse, Status, Message, Refused} ->
                    logger:warning("trevrpc: refused WebSocket upgrade from ~s with ~b: ~s", [
                        trevrpc_http1:describe(Refused), Status, Message
                    ]),
                    refuse_http(Id, Status, refusal_headers([], State), Message, State),
                    {noreply, State}
            end
    end.

%% Answers the upgrade, after which this process owns the socket and the
%% connection is a QMux session, as one over TLS would be.
accept_websocket(Id, Accept, Request, #state{transport = {h1, Conn}, config = Config} = State) ->
    Upgraded =
        try
            h1_connection:accept_upgrade(Conn, Id, Accept)
        catch
            exit:Exit -> {error, Exit}
        end,
    case Upgraded of
        {ok, Socket, Received} ->
            Opts = maps:merge(maps:get(qmux, Config), #{framing => websocket, received => Received}),
            case trevrpc_qmux:start(Socket, server, self(), Opts) of
                {ok, Session, Queue} ->
                    logger:info("trevrpc: WebSocket session from ~s", [
                        trevrpc_http1:describe(Request)
                    ]),
                    {noreply, State#state{
                        transport = {qmux, Session, Queue},
                        session_monitor = monitor(process, Session)
                    }};
                {error, Reason} ->
                    logger:warning("trevrpc: refused WebSocket session from ~s: ~p", [
                        trevrpc_http1:describe(Request), Reason
                    ]),
                    ok = trevrpc_http1:refuse_session(Socket),
                    {stop, normal, State}
            end;
        {error, Reason} ->
            logger:warning("trevrpc: WebSocket upgrade from ~s failed: ~p", [
                trevrpc_http1:describe(Request), Reason
            ]),
            {stop, normal, State}
    end.

%% A WebTransport CONNECT: a session if it passes the checks, the ones of
%% erlang-webtransport included, of which there is one per connection, as
%% from the Rust server.
connect(Id, Target, Headers, #state{config = #{webtransport := WebTransport}} = State0) ->
    {Peer, State} = peer(State0),
    Checked =
        case trevrpc_http:check_connect(Target, Headers, Peer, WebTransport) of
            ok when State#state.stopping ->
                {refuse, 503, [], <<"server is shutting down">>};
            ok when map_size(State#state.sessions) > 0 ->
                {refuse, 503, [], <<"one WebTransport session per connection">>};
            Result ->
                Result
        end,
    case Checked of
        ok ->
            accept_session(Id, Headers, WebTransport, State);
        {refuse, Status, Extra, Message} ->
            refuse_http(Id, Status, trevrpc_http:refusal_headers(Extra, #{}), Message, State),
            State
    end.

%% Starts the session's connection process, then the session, which tells
%% that process it started. erlang-webtransport 0.5.13 answers the CONNECT,
%% refusals included: `webtransport:accept/4' returns an error only after it
%% refused or reset the request, and `{ok, Session}' once the session's
%% `init/3' ran and the `200' went out. A session that ends later takes
%% its connection process with it, which ends its entry here.
accept_session(Id, Headers, #{session := SessionOpts}, State) ->
    #state{transport = {h3, H3, Quic}, config = Config, sessions = Sessions} = State,
    #{max_frame_size := MaxFrame, max_streams_per_connection := MaxStreams} = Config,
    {ok, Conn} = gen_server:start(?MODULE, {undefined, Config}, []),
    Monitor = monitor(process, Conn),
    Opts = SessionOpts#{
        transport => h3,
        handler => trevrpc_webtransport,
        handler_opts => #{
            conn => Conn,
            window => trevrpc_quic:stream_window(MaxFrame),
            connection_window => trevrpc_quic:connection_window(MaxFrame, 4 * MaxStreams),
            quic => Quic,
            session_id => Id
        }
    },
    case webtransport:accept(H3, Id, Headers, Opts) of
        {ok, _Session} ->
            State#state{sessions = Sessions#{Monitor => Conn}};
        {error, _Reason} ->
            demonitor(Monitor, [flush]),
            exit(Conn, kill),
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

%% HTTP/2 and HTTP/1.1 responses advertise HTTP/3; HTTP/3 ones need not.
http_options(#state{transport = {h3, _, _}, config = #{http := Http}}) ->
    maps:remove(alt_svc, Http);
http_options(#state{config = #{http := Http}}) ->
    Http.

refusal_headers(Extra, #state{transport = {h3, _, _}}) ->
    trevrpc_http:refusal_headers(Extra, #{});
refusal_headers(Extra, #state{config = #{alt_svc := undefined}}) ->
    trevrpc_http:refusal_headers(Extra, #{});
refusal_headers(Extra, #state{config = #{alt_svc := AltSvc}}) ->
    trevrpc_http:refusal_headers(Extra, #{alt_svc => AltSvc}).

%% Accepts the call: the stream's data comes here from now on, credited as
%% it is read, and the call starts once its REQUEST arrived.
accept_http(Id, Http, #state{transport = Transport} = State) ->
    Accepted =
        maybe
            ok ?= claim_stream(Transport, Id),
            trevrpc_transport:send_response(
                Transport, Id, 200, trevrpc_http:accepted_headers(Http)
            )
        end,
    case Accepted of
        ok ->
            open_stream(Id, State#state{active_at = erlang:monotonic_time(millisecond)});
        {error, _} ->
            %% The stream is gone already.
            _ = trevrpc_transport:reset_stream(Transport, Id, ?CANCELLED),
            State
    end.

%% Makes the stream's data come here, in manual flow control; what arrived
%% before is sent here as messages, after this. An HTTP/1.1 connection
%% sends every request's data here, and is in manual flow control from the
%% start.
claim_stream({h3, H3, _Quic}, Id) ->
    case
        quic_h3:set_stream_handler(H3, Id, self(), #{flow_control => manual, drain_buffer => false})
    of
        ok -> ok;
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end;
claim_stream({h2, Conn}, Id) ->
    h2:set_stream_handler(Conn, Id, self(), #{flow_control => manual});
claim_stream({h1, _Conn}, _Id) ->
    ok.

%% Answers a request that is refused, and stops reading its body.
refuse_http(Id, Status, Headers, Message, #state{transport = Transport}) ->
    _ = trevrpc_transport:respond(Transport, Id, Status, Headers, <<Message/binary, $\n>>),
    stop_request(Transport, Id).

stop_request({h3, _H3, Quic}, Id) ->
    _ = quic:stop_sending(Quic, Id, ?H3_NO_ERROR),
    ok;
stop_request({h2, Conn}, Id) ->
    %% The response is written whole, so the stream can end (RFC 9113
    %% Section 8.1): erlang_h2 would otherwise hold the rest of the body.
    _ = h2:cancel(Conn, Id, no_error),
    ok;
stop_request({h1, _Conn}, _Id) ->
    %% erlang_h1 drains a body left unread for a second after the response,
    %% then closes the connection.
    ok.

%% HTTP/2 connections without a stream for the connection idle timeout
%% are closed, as the Go server's are, and HTTP/1.1 ones that send no
%% request head within the TLS handshake timeout, after the handshake or
%% the request before, as the Rust server's are. A request in flight holds
%% neither back, however long its body takes. QUIC's idle timeout covers
%% HTTP/3. Returns the transport.
arm_idle(Transport, Config) ->
    _ = start_timer(idle_timeout(Transport, Config), idle),
    Transport.

idle_timeout({h1, _}, #{h1_idle_timeout := Timeout}) -> Timeout;
idle_timeout(_Transport, #{idle_timeout := Timeout}) -> Timeout.

%% A request arrived: the idle time starts again once it is answered.
requested(State) ->
    State#state{active_at = erlang:monotonic_time(millisecond)}.

idle(#state{transport = Transport, config = Config, streams = Streams} = State) when
    map_size(Streams) > 0
->
    _ = arm_idle(Transport, Config),
    {noreply, State};
idle(#state{transport = Transport, config = Config, active_at = At} = State) ->
    Timeout = idle_timeout(Transport, Config),
    case erlang:monotonic_time(millisecond) - At of
        Idle when Idle >= Timeout ->
            ok = close_transport(Transport),
            {stop, normal, State};
        Idle ->
            _ = start_timer(Timeout - Idle, idle),
            {noreply, State}
    end.

start_http_connection(h1, Socket, #{h1 := Opts}) ->
    maybe
        {ok, Conn} ?= h1_connection:start_link(server, Socket, self(), Opts),
        ok ?= transfer(Socket, Conn),
        ok ?= h1_connection:activate(Conn),
        {ok, Conn}
    end;
start_http_connection(h2, Socket, Config) ->
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
        #{Id := #entry{state = Running, ref = Ref, pid = Pid} = Entry} when
            Running =:= starting; Running =:= running
        ->
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
        #{Id := #entry{state = Running, pid = Pid} = Entry} when
            Running =:= starting; Running =:= running
        ->
            exit(Pid, kill),
            cancel_timer(Entry),
            _ = trevrpc_transport:reset_stream(Transport, Id, ?CANCELLED),
            _ = trevrpc_transport:stop_sending(Transport, Id, ?CANCELLED),
            release(Id, Entry#entry{state = cancelling, timer = undefined}, State);
        _ ->
            %% A STATUS still waiting to be written goes with its stream.
            State#state{pending = maps:remove(Id, State#state.pending)}
    end.

%% A WebTransport stream ran too far ahead of its call, which its session
%% could not hold back (see trevrpc_webtransport): the call ends with
%% `resource_exhausted' once its process is gone.
overflowed(Id, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := #entry{state = Running, pid = Pid} = Entry} when
            Running =:= starting; Running =:= running
        ->
            exit(Pid, kill),
            cancel_timer(Entry),
            release(Id, Entry#entry{state = overflowing, timer = undefined}, State);
        _ ->
            State
    end.

%%% Admission

%% Whether the server takes the call: a slot in its request limit, the
%% datagram flow the call asks for, and the memory to decode its requests
%% if it streams them compressed.
admission(#{flow := Flow, kind := Kind, offer := Offer, requests := Requests}, State) ->
    #state{config = #{requests := Counter, max_concurrent_requests := Max} = Config} = State,
    case counters:get(Counter, 1) >= Max of
        true ->
            {error, trevrpc_status:new(unavailable, <<"too many concurrent RPCs">>)};
        false ->
            maybe
                {ok, Taken} ?= flow(Flow, State),
                {ok, Decode} ?=
                    stream_requests(Offer, Requests, Kind, maps:get(stream_budget, Config)),
                {ok, Taken, Decode}
            end
    end.

%% Whether the call decodes stream-compressed requests: the client offered
%% stream compression, the route allows it for requests it streams, and the
%% budget has room for the decompressing context, which the call holds until
%% it ends. An optimistic call it cannot decode is refused before any
%% application code runs, and the client replays it without stream
%% compression.
stream_requests(Offer, Requests, Kind, Budget) ->
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

%% The call's datagram flow, on native QUIC and WebTransport, which alone
%% carry datagrams here: another transport ignores it. A flow that is in use
%% fails the call.
flow(Flow, #state{transport = Transport, flows = Flows}) when
    Flow > 0, element(1, Transport) =:= quic orelse element(1, Transport) =:= wt
->
    case trevrpc_datagram_flows:in_use(Flow, Flows) of
        false ->
            {ok, Flow};
        true ->
            Message = io_lib:format("datagram flow ~b is already in use", [Flow]),
            {error, trevrpc_status:new(invalid_argument, iolist_to_binary(Message))}
    end;
flow(_Flow, _State) ->
    {ok, undefined}.

%% Takes what admission found free for the call, and starts its deadline.
admitted(Id, #entry{ref = Ref, pid = Pid} = Entry, Admission, Flow, Decode, State) ->
    #{kind := Kind, timeout := Timeout} = Admission,
    #state{config = #{requests := Requests}, flows = Flows} = State,
    counters:add(Requests, 1, 1),
    %% The call's flow is its own from now on: the datagrams that arrived
    %% for it before its REQUEST go to it now.
    {Spec, Registered} =
        case Flow of
            undefined ->
                {undefined, Flows};
            _ ->
                Credit = trevrpc_datagram_flows:credit(),
                Receives = trevrpc_wire:streams_requests(Kind),
                {ok, R} = trevrpc_datagram_flows:register(Flow, Pid, Ref, Credit, Receives, Flows),
                {{Flow, Credit}, R}
        end,
    Running = Entry#entry{
        state = running,
        admitted = true,
        flow = Flow,
        timer = start_timer(Timeout, {deadline, Id}),
        reserved =
            case Decode of
                true -> trevrpc_zstd_stream:decoder_cost();
                false -> 0
            end
    },
    {reply, {ok, #{flow => Spec, stream_requests => Decode}}, State#state{
        streams = (State#state.streams)#{Id := Running}, flows = Registered
    }}.

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
        #{Id := #entry{kind = fallback, state = Running} = Entry} ->
            %% A request process ends its response itself, failing handlers
            %% included; one that was killed, as at the end of the graceful
            %% shutdown, has its stream reset.
            _ =
                case Running =:= running andalso Reason =/= normal of
                    true -> cut_off(Id, State);
                    false -> ok
                end,
            maybe_close(remove(Id, release(Id, Entry, State)));
        #{Id := #entry{state = starting} = Entry} when Reason =:= normal ->
            %% Its stream went before its REQUEST did.
            maybe_close(remove(Id, release(Id, Entry, State)));
        #{Id := #entry{state = finishing}} ->
            maybe_close(remove(Id, State));
        #{Id := #entry{state = expiring, fin = Fin}} ->
            State1 = lone_status(
                Id, Fin, trevrpc_status:new(deadline_exceeded, <<"deadline exceeded">>), State
            ),
            maybe_close(remove(Id, State1));
        #{Id := #entry{state = overflowing, fin = Fin}} ->
            Status = trevrpc_status:new(resource_exhausted, <<"requests read too slowly">>),
            maybe_close(remove(Id, lone_status(Id, Fin, Status, State)));
        #{Id := #entry{state = cancelling}} ->
            maybe_close(remove(Id, State));
        #{Id := #entry{state = shutdown}} ->
            cut_off(Id, State),
            maybe_close(remove(Id, State));
        #{Id := #entry{state = Running, fin = Fin} = Entry} when
            Running =:= starting; Running =:= running
        ->
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

shutdown_stream(_Id, #entry{kind = fallback}, State) ->
    %% A request of the fallback gets the grace period to finish.
    State;
shutdown_stream(Id, #entry{state = starting, pid = Pid, fin = Fin} = Entry, State) ->
    %% Nothing of the call ran yet; the stream ends as one refused.
    exit(Pid, kill),
    Status = trevrpc_status:new(unavailable, <<"server is shutting down">>),
    remove(Id, release(Id, Entry, lone_status(Id, Fin, Status, State)));
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
    case
        map_size(Streams) =:= 0 andalso map_size(State#state.pending) =:= 0 andalso
            map_size(State#state.sessions) =:= 0
    of
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
release(Id, #entry{admitted = Admitted} = Entry, #state{active = Active, config = Config} = State) ->
    case Admitted of
        true -> counters:sub(maps:get(requests, Config), 1, 1);
        false -> ok
    end,
    State#state{
        streams = (State#state.streams)#{Id => Entry#entry{counted = false}}, active = Active - 1
    }.

%% Forgets the stream, giving back the stream compression memory its call
%% held, and its datagram flow.
remove(Id, #state{streams = Streams, config = Config, flows = Flows} = State) ->
    ok = forget(Id, State),
    Released =
        case Streams of
            #{Id := #entry{reserved = Reserved, flow = Flow}} ->
                trevrpc_zstd_stream:release(maps:get(stream_budget, Config), Reserved),
                case Flow of
                    undefined -> Flows;
                    _ -> trevrpc_datagram_flows:release(Flow, Flows)
                end;
            #{} ->
                Flows
        end,
    State#state{
        streams = maps:remove(Id, Streams),
        active_at = erlang:monotonic_time(millisecond),
        flows = Released
    }.

%% Over WebTransport, what the call left unread no longer holds the session
%% back, and over HTTP/1.1 the connection reads and drops the rest of a
%% request body. The other transports need no call: this process makes
%% none into them for a call that ends normally.
forget(Id, #state{transport = {wt, _, _, _} = Transport}) ->
    trevrpc_transport:forget(Transport, Id);
forget(Id, #state{transport = {h1, _} = Transport}) ->
    trevrpc_transport:forget(Transport, Id);
forget(_Id, _State) ->
    ok.

start_timer(infinity, _Message) ->
    undefined;
start_timer(Timeout, Message) ->
    erlang:start_timer(Timeout, self(), Message).

cancel_timer(#entry{timer = undefined}) ->
    ok;
cancel_timer(#entry{timer = Timer}) ->
    _ = erlang:cancel_timer(Timer, [{async, true}, {info, false}]),
    ok.
