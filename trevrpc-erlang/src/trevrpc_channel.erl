%% @doc A long-lived client channel to one TrevRPC server over native QUIC.
%%
%% ```
%% {ok, Channel} = trevrpc_channel:connect(#{
%%     host => "localhost", port => 4433, cacertfile => "ca.pem"
%% }, 5000),
%% {ok, Reply, _Metadata} = trevrpc_client:unary(Channel, <<"/pkg.Svc/Method">>, Request, #{}).
%% '''
%%
%% The channel reconnects on its own. Each ready connection is a generation:
%% a call stays on the generation it started on, fails if that generation
%% dies, and is never retried or moved. A call made while the channel is
%% reconnecting fails at once with `unavailable'; {@link wait_until_ready/2}
%% waits for the next generation. Reconnects back off from 100 ms, doubling
%% each attempt up to 30 s, with 20 percent jitter, and start over once a
%% generation is ready.
%%
%% The channel process owns the QUIC connection and routes each stream's
%% events to the process that made the call, which reads and writes the
%% stream itself; see {@link trevrpc_client}.
-module(trevrpc_channel).

-behaviour(gen_server).

-export([
    start_link/1,
    start/1,
    connect/2,
    wait_until_ready/2,
    generation/1,
    subscribe/1,
    unsubscribe/1,
    close/1
]).
-export([open_stream/2, stream_done/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([options/0, stream_info/0, state/0]).

%% A channel's state: ready with the number of its generation, connecting
%% (before the first generation and between generations), or closed for
%% good.
-type state() :: {ready, pos_integer()} | connecting | closed.

-type options() :: #{
    host := inet:hostname() | inet:ip_address() | binary(),
    port := inet:port_number(),
    server_name => string(),
    verify => boolean(),
    cacertfile => file:filename_all(),
    cacerts => [public_key:der_encoded()],
    max_frame_size => pos_integer(),
    connect_timeout => pos_integer(),
    idle_timeout => pos_integer()
}.

%% What a calling process needs to use a stream it opened.
-type stream_info() :: #{
    conn := pid(),
    id := non_neg_integer(),
    ref := reference(),
    generation := pos_integer(),
    max_frame_size := pos_integer()
}.

-define(INITIAL_DELAY, 100).
-define(MAX_DELAY, 30000).
-define(CANCELLED, 1).

-record(state, {
    host :: inet:hostname() | inet:ip_address(),
    port :: inet:port_number(),
    quic_opts :: map(),
    max_frame_size :: pos_integer(),
    connect_timeout :: pos_integer(),
    status = connecting :: connecting | ready,
    conn :: pid() | undefined,
    %% Tags the current connection attempt, so stale results are ignored.
    attempt :: reference() | undefined,
    %% The process that started the current connection, which lives as
    %% long as the connection should (see attempt/1).
    parent :: pid() | undefined,
    generation = 0 :: non_neg_integer(),
    delay = ?INITIAL_DELAY :: pos_integer(),
    ready_waiters = [] :: [{gen_server:from(), reference()}],
    %% Stream ID => {calling process, routing tag, monitor}.
    streams = #{} :: #{non_neg_integer() => {pid(), reference(), reference()}},
    monitors = #{} :: #{reference() => non_neg_integer()},
    %% Opens refused for lack of stream credit, waiting for more.
    pending_opens = [] :: [{gen_server:from(), pid(), reference()}],
    send_waiters = [] :: [{pid(), reference()}],
    %% Processes told of every change of state.
    subscribers = #{} :: #{reference() => pid()}
}).

%% @doc Starts a channel linked to the caller. It connects in the
%% background.
-spec start_link(options()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

%% @doc Starts a channel. It connects in the background.
-spec start(options()) -> {ok, pid()} | ignore | {error, term()}.
start(Opts) ->
    gen_server:start(?MODULE, Opts, []).

%% @doc Starts a channel linked to the caller and waits up to `Timeout'
%% milliseconds for its first connection.
-spec connect(options(), timeout()) -> {ok, pid()} | {error, trevrpc_status:status()}.
connect(Opts, Timeout) ->
    {ok, Channel} = start_link(Opts),
    case wait_until_ready(Channel, Timeout) of
        ok ->
            {ok, Channel};
        {error, Status} ->
            close(Channel),
            {error, Status}
    end.

%% @doc Waits up to `Timeout' milliseconds for the channel to be ready.
-spec wait_until_ready(pid(), timeout()) -> ok | {error, trevrpc_status:status()}.
wait_until_ready(Channel, Timeout) ->
    try
        gen_server:call(Channel, {wait_until_ready, Timeout}, infinity)
    catch
        exit:_ -> {error, closed_status()}
    end.

%% @doc The number of the current generation, 0 before the first.
-spec generation(pid()) -> non_neg_integer().
generation(Channel) ->
    gen_server:call(Channel, generation).

%% @doc Sends the calling process the channel's state now and after every
%% change, as `{trevrpc_channel, Channel, State}' messages, until it
%% unsubscribes or exits; `closed' is the last. Subscribing again only sends
%% the current state again.
-spec subscribe(pid()) -> ok.
subscribe(Channel) ->
    gen_server:call(Channel, {subscribe, self()}).

%% @doc Stops the state messages to the calling process.
-spec unsubscribe(pid()) -> ok.
unsubscribe(Channel) ->
    gen_server:call(Channel, {unsubscribe, self()}).

%% @doc Closes the channel for good: its connection closes, its calls fail,
%% and it does not reconnect.
-spec close(pid()) -> ok.
close(Channel) ->
    try
        gen_server:call(Channel, close, infinity)
    catch
        exit:_ -> ok
    end.

%% @private Opens a stream for the calling process, waiting until
%% `Deadline' for stream credit if the server's limit is reached.
-spec open_stream(pid(), trevrpc_stream:deadline()) ->
    {ok, stream_info()} | {error, trevrpc_status:status()}.
open_stream(Channel, Deadline) ->
    try
        gen_server:call(Channel, {open_stream, self(), Deadline}, infinity)
    catch
        exit:_ -> {error, closed_status()}
    end.

%% @private The calling process is done with stream `Id'.
-spec stream_done(pid(), non_neg_integer(), reference()) -> ok.
stream_done(Channel, Id, Ref) ->
    gen_server:cast(Channel, {stream_done, Id, Ref}).

%%% gen_server

-spec init(options()) -> {ok, #state{}}.
init(Opts) ->
    _ = application:ensure_all_started(quic),
    MaxFrame = maps:get(max_frame_size, Opts, trevrpc:default_max_frame_size()),
    Tls = maps:with([server_name, cacertfile, cacerts], Opts),
    QuicOpts = maps:merge(
        trevrpc_quic:transport_opts(MaxFrame, 64, maps:get(idle_timeout, Opts, 30000)),
        Tls#{verify => maps:get(verify, Opts, true)}
    ),
    State = #state{
        host = host(maps:get(host, Opts)),
        port = maps:get(port, Opts),
        quic_opts = QuicOpts,
        max_frame_size = MaxFrame,
        connect_timeout = maps:get(connect_timeout, Opts, 10000)
    },
    {ok, attempt(State)}.

host(Host) when is_binary(Host) -> binary_to_list(Host);
host(Host) -> Host.

-spec handle_call(term(), gen_server:from(), #state{}) ->
    {reply, term(), #state{}} | {noreply, #state{}} | {stop, normal, ok, #state{}}.
handle_call({wait_until_ready, _Timeout}, _From, #state{status = ready} = State) ->
    {reply, ok, State};
handle_call({wait_until_ready, Timeout}, From, #state{ready_waiters = Waiters} = State) ->
    Timer =
        case Timeout of
            infinity -> make_ref();
            _ -> erlang:start_timer(Timeout, self(), {ready_timeout, From})
        end,
    {noreply, State#state{ready_waiters = [{From, Timer} | Waiters]}};
handle_call(generation, _From, #state{generation = Generation} = State) ->
    {reply, Generation, State};
handle_call({subscribe, Pid}, _From, #state{subscribers = Subscribers} = State) ->
    Pid ! {trevrpc_channel, self(), channel_state(State)},
    %% Subscribing again only repeats the current state.
    case lists:member(Pid, maps:values(Subscribers)) of
        true -> {reply, ok, State};
        false -> {reply, ok, State#state{subscribers = Subscribers#{monitor(process, Pid) => Pid}}}
    end;
handle_call({unsubscribe, Pid}, _From, #state{subscribers = Subscribers} = State) ->
    Gone = [Monitor || Monitor := Subscriber <- Subscribers, Subscriber =:= Pid],
    _ = [demonitor(Monitor, [flush]) || Monitor <- Gone],
    {reply, ok, State#state{subscribers = maps:without(Gone, Subscribers)}};
handle_call(close, _From, State) ->
    {stop, normal, ok, State};
handle_call({open_stream, _Owner, _Deadline}, _From, #state{status = connecting} = State) ->
    {reply, {error, trevrpc_status:new(unavailable, <<"channel is reconnecting">>)}, State};
handle_call({open_stream, Owner, Deadline}, From, State) ->
    open(From, Owner, Deadline, State).

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast({stream_done, Id, Ref}, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := {_Pid, Ref, Monitor}} ->
            demonitor(Monitor, [flush]),
            {noreply, forget(Id, Monitor, State)};
        _ ->
            {noreply, State}
    end.

-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info({quic, Conn, Event}, #state{conn = Conn} = State) when Conn =/= undefined ->
    quic_event(Event, State);
handle_info(
    {quic, Conn, {Outcome, _} = Event}, #state{conn = undefined, attempt = Attempt} = State
) when
    Attempt =/= undefined, (Outcome =:= connected orelse Outcome =:= error)
->
    %% quic:connect/4 returns at once for an address, so the handshake can
    %% end before the attempt reports the connection.
    quic_event(Event, State#state{conn = Conn});
handle_info({quic, _OldConn, _Event}, State) ->
    {noreply, State};
handle_info({connect_result, Attempt, Result}, #state{attempt = Attempt} = State) ->
    case {Result, State#state.conn} of
        {{ok, Conn}, undefined} ->
            {noreply, State#state{conn = Conn}};
        {{ok, Conn}, Conn} ->
            {noreply, State};
        {{ok, Other}, _Adopted} ->
            _ = catch quic:close(Other, 0),
            {noreply, State};
        {{error, _Reason}, _} ->
            {noreply, retry(State)}
    end;
handle_info({connect_result, _Stale, {ok, Conn}}, #state{conn = Current} = State) when
    Conn =/= Current
->
    %% An attempt that timed out connected after all.
    _ = catch quic:close(Conn, 0),
    {noreply, State};
handle_info(
    {timeout, _Timer, {connect_timeout, Attempt}},
    #state{attempt = Attempt, status = connecting} = State
) ->
    case State#state.conn of
        undefined -> ok;
        Conn -> _ = catch quic:close(Conn, 0)
    end,
    {noreply, retry(State#state{conn = undefined})};
handle_info({timeout, _Timer, reconnect}, #state{status = connecting, attempt = undefined} = State) ->
    {noreply, attempt(State)};
handle_info({timeout, Timer, {ready_timeout, From}}, #state{ready_waiters = Waiters} = State) ->
    case lists:keytake(From, 1, Waiters) of
        {value, {From, Timer}, Rest} ->
            gen_server:reply(
                From, {error, trevrpc_status:new(unavailable, <<"channel is not ready">>)}
            ),
            {noreply, State#state{ready_waiters = Rest}};
        _ ->
            {noreply, State}
    end;
handle_info({timeout, _Timer, {open_timeout, From}}, #state{pending_opens = Pending} = State) ->
    case lists:keytake(From, 1, Pending) of
        {value, _, Rest} ->
            gen_server:reply(
                From, {error, trevrpc_status:new(deadline_exceeded, <<"deadline exceeded">>)}
            ),
            {noreply, State#state{pending_opens = Rest}};
        false ->
            {noreply, State}
    end;
handle_info({trevrpc_send_ready, Pid, Ref}, #state{send_waiters = Waiters, conn = Conn} = State) ->
    _ =
        case {Waiters, Conn} of
            {_, undefined} -> Pid ! {trevrpc_stream, Ref, closed};
            {[], _} -> quic_connection:notify_send_ready(Conn);
            _ -> ok
        end,
    {noreply, State#state{send_waiters = [{Pid, Ref} | Waiters]}};
handle_info(
    {'DOWN', Monitor, process, _Pid, _Reason}, #state{subscribers = Subscribers} = State
) when
    is_map_key(Monitor, Subscribers)
->
    {noreply, State#state{subscribers = maps:remove(Monitor, Subscribers)}};
handle_info(
    {'DOWN', Monitor, process, _Pid, _Reason},
    #state{monitors = Monitors, streams = Streams} = State
) ->
    %% The calling process exited without finishing its call: cancel it.
    case maps:find(Monitor, Monitors) of
        {ok, Id} ->
            #{Id := _} = Streams,
            _ = catch quic:reset_stream(State#state.conn, Id, ?CANCELLED),
            _ = catch quic:stop_sending(State#state.conn, Id, ?CANCELLED),
            {noreply, forget(Id, Monitor, State)};
        error ->
            {noreply, State}
    end;
handle_info(_Message, State) ->
    {noreply, State}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, #state{conn = Conn, subscribers = Subscribers} = State) ->
    _ = [Pid ! {trevrpc_channel, self(), closed} || Pid <- maps:values(Subscribers)],
    fail_streams(State),
    case Conn of
        undefined -> ok;
        _ -> _ = catch quic:close(Conn, 0)
    end,
    ok.

%%% Connecting

%% quic:connect/4 blocks while it races the addresses of a host name, so it
%% runs in a process of its own; the channel owns the connection either way.
%% The connection is linked to the process that started it and stops when
%% that process exits, so that process stays alive as its parent: it exits
%% when told to, or with the channel, and takes the connection with it.
attempt(#state{host = Host, port = Port, quic_opts = QuicOpts, connect_timeout = Timeout} = State) ->
    stop_parent(State),
    Attempt = make_ref(),
    Self = self(),
    Opts = QuicOpts#{connect_timeout => Timeout},
    Parent = spawn_link(fun() ->
        process_flag(trap_exit, true),
        Self ! {connect_result, Attempt, catch_connect(Host, Port, Opts, Self)},
        parent_loop(Self)
    end),
    erlang:start_timer(Timeout, self(), {connect_timeout, Attempt}),
    State#state{attempt = Attempt, conn = undefined, parent = Parent}.

parent_loop(Channel) ->
    receive
        stop -> ok;
        {'EXIT', Channel, _Reason} -> ok;
        %% The connection exited; the channel hears of it from the connection.
        {'EXIT', _Other, _Reason} -> parent_loop(Channel)
    end.

stop_parent(#state{parent = undefined}) ->
    ok;
stop_parent(#state{parent = Parent}) ->
    Parent ! stop,
    ok.

catch_connect(Host, Port, Opts, Owner) ->
    try
        quic:connect(Host, Port, Opts, Owner)
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

%% Schedules the next attempt after the backoff delay, with jitter.
retry(#state{delay = Delay} = State) ->
    stop_parent(State),
    Jittered = round(Delay * (0.8 + 0.4 * rand:uniform())),
    erlang:start_timer(Jittered, self(), reconnect),
    State#state{
        status = connecting,
        conn = undefined,
        attempt = undefined,
        parent = undefined,
        delay = min(?MAX_DELAY, Delay * 2)
    }.

quic_event({connected, #{alpn := Alpn}}, #state{status = connecting} = State) ->
    case Alpn =:= trevrpc:alpn() of
        true ->
            _ = [gen_server:reply(From, ok) || {From, _Timer} <- State#state.ready_waiters],
            {noreply,
                notify(State#state{
                    status = ready,
                    generation = State#state.generation + 1,
                    delay = ?INITIAL_DELAY,
                    attempt = undefined,
                    ready_waiters = []
                })};
        false ->
            _ = catch quic:close(State#state.conn, 0),
            {noreply, retry(State)}
    end;
quic_event({connected, _Info}, #state{status = connecting, conn = Conn} = State) ->
    %% No application protocol: not a TrevRPC server.
    _ = catch quic:close(Conn, 0),
    {noreply, retry(State)};
quic_event({stream_data, Id, Bin, Fin}, State) ->
    route(Id, {data, Bin, Fin}, State);
quic_event({stream_reset, Id, Code}, State) ->
    route(Id, {reset, Code}, State);
quic_event({stop_sending, Id, Code}, State) ->
    route(Id, {stop_sending, Code}, State);
quic_event({stream_opened, Id}, #state{conn = Conn} = State) ->
    %% Servers open no streams in TrevRPC.
    _ = catch quic:stop_sending(Conn, Id, ?CANCELLED),
    {noreply, State};
quic_event({streams_available, bidi}, State) ->
    retry_opens(State);
quic_event(send_ready, #state{send_waiters = Waiters} = State) ->
    _ = [Pid ! {trevrpc_stream, Ref, send_ready} || {Pid, Ref} <- Waiters],
    {noreply, State#state{send_waiters = []}};
quic_event({closed, _Reason}, State) ->
    {noreply, lost(State)};
quic_event({error, _Reason}, State) ->
    {noreply, lost(State)};
quic_event(_Event, State) ->
    {noreply, State}.

%% The connection is gone: fail its calls and reconnect.
lost(#state{status = ready} = State) ->
    reconnect(notify(State#state{status = connecting}));
lost(State) ->
    reconnect(State).

%% Tells the subscribers the channel's state.
notify(#state{subscribers = Subscribers} = State) ->
    Message = {trevrpc_channel, self(), channel_state(State)},
    _ = [Pid ! Message || Pid <- maps:values(Subscribers)],
    State.

channel_state(#state{status = ready, generation = Generation}) -> {ready, Generation};
channel_state(#state{status = connecting}) -> connecting.

reconnect(State) ->
    fail_streams(State),
    _ = [
        gen_server:reply(From, {error, trevrpc_status:new(unavailable, <<"connection closed">>)})
     || {From, _Owner, _Timer} <- State#state.pending_opens
    ],
    _ = [demonitor(Monitor, [flush]) || Monitor <- maps:keys(State#state.monitors)],
    retry(State#state{streams = #{}, monitors = #{}, pending_opens = [], send_waiters = []}).

fail_streams(#state{streams = Streams, send_waiters = Waiters}) ->
    _ = [Pid ! {trevrpc_stream, Ref, closed} || {Pid, Ref, _Monitor} <- maps:values(Streams)],
    _ = [Pid ! {trevrpc_stream, Ref, closed} || {Pid, Ref} <- Waiters],
    ok.

route(Id, Event, #state{streams = Streams} = State) ->
    _ =
        case Streams of
            #{Id := {Pid, Ref, _Monitor}} -> Pid ! {trevrpc_stream, Ref, Event};
            _ -> ok
        end,
    {noreply, State}.

%%% Streams

open(From, Owner, Deadline, #state{conn = Conn} = State) ->
    case catch quic:open_stream(Conn) of
        {ok, Id} ->
            _ = catch quic:set_stream_flow_control(Conn, Id, manual),
            Ref = make_ref(),
            Monitor = monitor(process, Owner),
            Info = #{
                conn => Conn,
                id => Id,
                ref => Ref,
                generation => State#state.generation,
                max_frame_size => State#state.max_frame_size
            },
            {reply, {ok, Info}, State#state{
                streams = (State#state.streams)#{Id => {Owner, Ref, Monitor}},
                monitors = (State#state.monitors)#{Monitor => Id}
            }};
        {error, stream_limit} ->
            Timer =
                case Deadline of
                    infinity ->
                        make_ref();
                    _ ->
                        Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
                        erlang:start_timer(Timeout, self(), {open_timeout, From})
                end,
            {noreply, State#state{
                pending_opens = State#state.pending_opens ++ [{From, Owner, Timer}]
            }};
        _ ->
            {reply, {error, trevrpc_status:new(unavailable, <<"could not open a stream">>)}, State}
    end.

%% Stream credit came back: open the waiting streams, in order, while it
%% lasts.
retry_opens(#state{pending_opens = []} = State) ->
    {noreply, State};
retry_opens(#state{pending_opens = [{From, Owner, Timer} | Rest]} = State) ->
    case open(From, Owner, infinity, State#state{pending_opens = []}) of
        {reply, Reply, Opened} ->
            _ = erlang:cancel_timer(Timer, [{async, true}, {info, false}]),
            gen_server:reply(From, Reply),
            retry_opens(Opened#state{pending_opens = Rest});
        {noreply, _StillFull} ->
            {noreply, State}
    end.

forget(Id, Monitor, #state{streams = Streams, monitors = Monitors} = State) ->
    State#state{streams = maps:remove(Id, Streams), monitors = maps:remove(Monitor, Monitors)}.

closed_status() ->
    trevrpc_status:new(unavailable, <<"channel is closed">>).
