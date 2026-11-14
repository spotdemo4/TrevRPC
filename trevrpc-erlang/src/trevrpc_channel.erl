%% @doc A long-lived client channel to one TrevRPC server, over native QUIC
%% with QMux over TLS as the fallback for networks that block UDP.
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
%% With the `auto' transport (the default) each connection races the two
%% transports, as the Go and Rust channels do: QUIC starts first, QMux over
%% TLS after the fallback delay (300 ms) or as soon as QUIC fails, and the
%% first to be ready wins while the other is closed. Once QMux won,
%% reconnects use it directly until the reprobe interval (5 minutes) has
%% passed, then race again. `quic' and `tcp' pin one transport.
%%
%% The channel process owns the connection and routes each stream's events
%% to the process that made the call, which reads and writes the stream
%% itself; see {@link trevrpc_client}.
-module(trevrpc_channel).

-behaviour(gen_server).

-export([
    start_link/1,
    start/1,
    connect/2,
    wait_until_ready/2,
    generation/1,
    transport/1,
    subscribe/1,
    unsubscribe/1,
    close/1
]).
-export([open_stream/2, stream_done/3, learn/3, reserve/4, release/4, optimism/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([options/0, stream_info/0, state/0]).

%% A channel's state: ready with the number of its generation, connecting
%% (before the first generation and between generations), or closed for
%% good.
-type state() :: {ready, pos_integer()} | connecting | closed.

%% `tcp_host' and `tcp_port' address the server's TCP listener, by default
%% the same host and port as QUIC.
-type options() :: #{
    host := inet:hostname() | inet:ip_address() | binary(),
    port := inet:port_number(),
    tcp_host => inet:hostname() | inet:ip_address() | binary(),
    tcp_port => inet:port_number(),
    transport => auto | quic | tcp,
    fallback_delay => non_neg_integer(),
    reprobe_interval => non_neg_integer(),
    server_name => string(),
    verify => boolean(),
    cacertfile => file:filename_all(),
    cacerts => [public_key:der_encoded()],
    max_frame_size => pos_integer(),
    connect_timeout => pos_integer(),
    idle_timeout => pos_integer(),
    compression => trevrpc_zstd:settings(),
    stream_compression_memory => non_neg_integer()
}.

%% What a calling process needs to use a stream it opened.
-type stream_info() :: #{
    transport := trevrpc_transport:t(),
    id := non_neg_integer(),
    ref := reference(),
    generation := pos_integer(),
    max_frame_size := pos_integer(),
    compression := trevrpc_zstd:config(),
    %% The dictionaries the connection shares with the server, once a call
    %% learned them from the server's answer.
    shared := [non_neg_integer()] | undefined,
    %% The channel's stream compression budget, and whether calls on the
    %% connection compress their requests optimistically.
    stream_budget := trevrpc_zstd_stream:budget() | undefined,
    optimistic := boolean()
}.

-define(INITIAL_DELAY, 100).
-define(MAX_DELAY, 30000).
-define(CANCELLED, 1).
-define(STREAM_COMPRESSION_MEMORY, 67108864).

%% A connection attempt: its transport kind, its connection once known, and
%% for QUIC the process the connection is linked to (see quic_attempt/2).
-type attempt() :: #{
    kind := trevrpc_transport:kind(),
    transport := trevrpc_transport:t() | undefined,
    parent := pid() | undefined
}.

-record(state, {
    host :: inet:hostname() | inet:ip_address(),
    port :: inet:port_number(),
    tcp_host :: inet:hostname() | inet:ip_address(),
    tcp_port :: inet:port_number(),
    policy :: auto | quic | tcp,
    fallback_delay :: non_neg_integer(),
    reprobe_interval :: non_neg_integer(),
    %% Until when reconnects use QMux directly, after it won a race.
    fallback_until :: integer() | undefined,
    quic_opts :: map(),
    tls_opts :: [ssl:tls_client_option()],
    qmux_opts :: trevrpc_qmux:options(),
    max_frame_size :: pos_integer(),
    connect_timeout :: pos_integer(),
    compression :: trevrpc_zstd:config(),
    %% The current connection's shared dictionaries, once known.
    shared :: [non_neg_integer()] | undefined,
    %% Stream compression: the channel's budget, whether the connection's
    %% calls are optimistic (until its server refuses one, and again once a
    %% call is answered), and what each stream holds from the budget.
    stream_budget :: trevrpc_zstd_stream:budget() | undefined,
    optimistic = true :: boolean(),
    reserved = #{} :: #{non_neg_integer() => non_neg_integer()},
    status = connecting :: connecting | ready,
    transport :: trevrpc_transport:t() | undefined,
    %% The current race, its attempts, whether it started with QUIC, and
    %% whether its QMux attempt started.
    round :: reference() | undefined,
    attempts = #{} :: #{reference() => attempt()},
    raced = false :: boolean(),
    fallback_started = false :: boolean(),
    %% The ready connection's monitor, and for QUIC the process it is linked
    %% to (see quic_attempt/1).
    conn_monitor :: reference() | undefined,
    parent :: pid() | undefined,
    %% Connections the channel closed, monitored until they exit: their late
    %% events are ignored, so a closing one cannot pass for a new attempt's.
    retired = #{} :: #{pid() => reference()},
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
    case start_link(Opts) of
        {ok, Channel} ->
            case wait_until_ready(Channel, Timeout) of
                ok ->
                    {ok, Channel};
                {error, Status} ->
                    close(Channel),
                    {error, Status}
            end;
        {error, {compression, Message}} ->
            {error, trevrpc_status:new(invalid_argument, Message)}
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

%% @doc The transport of the current generation, or `none' while connecting.
-spec transport(pid()) -> trevrpc_transport:kind() | none.
transport(Channel) ->
    gen_server:call(Channel, transport).

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

%% @private A call on generation `Generation' learned the dictionaries the
%% connection shares with the server from its answer. The first answer
%% wins; they all come from one configuration.
-spec learn(pid(), pos_integer(), [non_neg_integer()]) -> ok.
learn(Channel, Generation, Shared) ->
    gen_server:cast(Channel, {learn, Generation, Shared}).

%% @private Reserves `Cost' bytes of the channel's stream compression
%% memory for the calling process's stream `Id' (routed with `Ref'), held
%% until the stream is done.
-spec reserve(pid(), non_neg_integer(), reference(), non_neg_integer()) -> boolean().
reserve(Channel, Id, Ref, Cost) ->
    try
        gen_server:call(Channel, {reserve, Id, Ref, Cost}, infinity)
    catch
        exit:_ -> false
    end.

%% @private Gives back `Cost' bytes stream `Id' reserved.
-spec release(pid(), non_neg_integer(), reference(), non_neg_integer()) -> ok.
release(Channel, Id, Ref, Cost) ->
    gen_server:cast(Channel, {release, Id, Ref, Cost}).

%% @private Whether the calls on generation `Generation' compress their
%% requests optimistically: not after its server refused a call, and again
%% once a call got a stream compression answer.
-spec optimism(pid(), pos_integer(), boolean()) -> ok.
optimism(Channel, Generation, Optimistic) ->
    gen_server:cast(Channel, {optimism, Generation, Optimistic}).

%% @private The calling process is done with stream `Id'.
-spec stream_done(pid(), non_neg_integer(), reference()) -> ok.
stream_done(Channel, Id, Ref) ->
    gen_server:cast(Channel, {stream_done, Id, Ref}).

%%% gen_server

%% Invalid settings fail the start without taking a linked caller down.
-spec init(options()) -> {ok, #state{}} | {error, {compression, binary()}}.
init(Opts) ->
    case trevrpc_zstd:config(maps:get(compression, Opts, #{})) of
        {ok, Compression} -> init(Opts, Compression);
        {error, Message} -> {error, {compression, Message}}
    end.

init(Opts, Compression) ->
    _ = application:ensure_all_started(quic),
    _ = application:ensure_all_started(ssl),
    process_flag(trap_exit, true),
    MaxFrame = maps:get(max_frame_size, Opts, trevrpc:default_max_frame_size()),
    Idle = maps:get(idle_timeout, Opts, 30000),
    Host = host(maps:get(host, Opts)),
    Port = maps:get(port, Opts),
    QuicTls = maps:with([server_name, cacertfile, cacerts], Opts),
    QuicOpts = maps:merge(
        trevrpc_quic:transport_opts(MaxFrame, 64, Idle), QuicTls#{
            verify => maps:get(verify, Opts, true)
        }
    ),
    TcpHost = host(maps:get(tcp_host, Opts, Host)),
    State = #state{
        host = Host,
        port = Port,
        tcp_host = TcpHost,
        tcp_port = maps:get(tcp_port, Opts, Port),
        policy = maps:get(transport, Opts, auto),
        fallback_delay = maps:get(fallback_delay, Opts, 300),
        reprobe_interval = maps:get(reprobe_interval, Opts, 300000),
        quic_opts = QuicOpts,
        tls_opts = tls_opts(Opts, TcpHost),
        qmux_opts = #{
            parameters => trevrpc_quic:qmux_parameters(MaxFrame, 64, 0, Idle),
            keep_alive => max(1, Idle div 2),
            handshake_timeout => maps:get(connect_timeout, Opts, 10000),
            send_timeout => Idle
        },
        max_frame_size = MaxFrame,
        connect_timeout = maps:get(connect_timeout, Opts, 10000),
        compression = Compression,
        stream_budget = trevrpc_zstd_stream:budget(
            maps:get(stream_compression_memory, Opts, ?STREAM_COMPRESSION_MEMORY)
        )
    },
    {ok, race(State)}.

host(Host) when is_binary(Host) -> binary_to_list(Host);
host(Host) -> Host.

%% TLS for QMux: the trust the options name, and for a host name, SNI and a
%% check of the certificate against it. For an IP address only the chain is
%% checked.
tls_opts(Opts, Host) ->
    Name =
        case Opts of
            #{server_name := ServerName} -> ServerName;
            #{} when is_list(Host) -> Host;
            #{} -> undefined
        end,
    DnsName =
        case Name =:= undefined orelse inet:parse_address(Name) =/= {error, einval} of
            true -> undefined;
            false -> Name
        end,
    Trust =
        case Opts of
            #{verify := false} ->
                [{verify, verify_none}];
            #{cacerts := CaCerts} ->
                pinned(CaCerts, DnsName);
            #{cacertfile := File} ->
                case trevrpc_quic:read_certificates(File) of
                    {ok, CaCerts} -> pinned(CaCerts, DnsName);
                    %% Each attempt fails with ssl's own error.
                    {error, _} -> [{verify, verify_peer}, {cacertfile, File}]
                end;
            #{} ->
                [{verify, verify_peer}, {cacerts, public_key:cacerts_get()}]
        end,
    Identity =
        case DnsName of
            undefined ->
                [{server_name_indication, disable}];
            _ ->
                [
                    {server_name_indication, DnsName},
                    {customize_hostname_check, [
                        {match_fun, public_key:pkix_verify_hostname_match_fun(https)}
                    ]}
                ]
        end,
    [
        binary,
        {active, false},
        {nodelay, true},
        {versions, ['tlsv1.3', 'tlsv1.2']},
        {alpn_advertised_protocols, [trevrpc:qmux_alpn()]}
        | Trust ++ Identity
    ].

%% Trust in the certificates `Trusted', which may include a self-signed
%% server certificate itself, as Go and Rust clients allow. ssl refuses every
%% self-signed peer certificate unless the verify fun accepts it, and then
%% skips its host name check, so the fun checks, for a DNS name, that the
%% certificate names the host, and its validity again.
pinned(Trusted, DnsName) ->
    Verify = fun
        (Cert, {bad_cert, selfsigned_peer} = Reason, State) ->
            Der = public_key:pkix_encode('OTPCertificate', Cert, otp),
            case lists:member(Der, Trusted) andalso valid_pinned(Der, DnsName) of
                true -> {valid, State};
                false -> {fail, Reason}
            end;
        (_Cert, {bad_cert, _} = Reason, _State) ->
            {fail, Reason};
        (_Cert, {extension, _}, State) ->
            {unknown, State};
        (_Cert, _Valid, State) ->
            {valid, State}
    end,
    [{verify, verify_peer}, {cacerts, Trusted}, {verify_fun, {Verify, []}}].

valid_pinned(Der, DnsName) ->
    Valid = element(1, public_key:pkix_path_validation(Der, [Der], [])) =:= ok,
    Valid andalso
        (DnsName =:= undefined orelse
            public_key:pkix_verify_hostname(Der, [{dns_id, DnsName}], [
                {match_fun, public_key:pkix_verify_hostname_match_fun(https)}
            ])).

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
handle_call(transport, _From, #state{status = ready, transport = Transport} = State) ->
    {reply, trevrpc_transport:kind(Transport), State};
handle_call(transport, _From, State) ->
    {reply, none, State};
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
    open(From, Owner, Deadline, State);
handle_call({reserve, Id, Ref, Cost}, _From, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := {_Pid, Ref, _Monitor}} ->
            case trevrpc_zstd_stream:reserve(State#state.stream_budget, Cost) of
                true ->
                    Reserved = State#state.reserved,
                    {reply, true, State#state{
                        reserved = Reserved#{Id => maps:get(Id, Reserved, 0) + Cost}
                    }};
                false ->
                    {reply, false, State}
            end;
        #{} ->
            {reply, false, State}
    end.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast({stream_done, Id, Ref}, #state{streams = Streams} = State) ->
    case Streams of
        #{Id := {_Pid, Ref, Monitor}} ->
            demonitor(Monitor, [flush]),
            {noreply, forget(Id, Monitor, State)};
        _ ->
            {noreply, State}
    end;
handle_cast(
    {learn, Generation, Shared}, #state{generation = Generation, shared = undefined} = State
) ->
    {noreply, State#state{shared = Shared}};
handle_cast({learn, _Generation, _Shared}, State) ->
    {noreply, State};
handle_cast({release, Id, Ref, Cost}, #state{streams = Streams, reserved = Reserved} = State) ->
    case {Streams, Reserved} of
        {#{Id := {_Pid, Ref, _Monitor}}, #{Id := Held}} ->
            Given = min(Cost, Held),
            trevrpc_zstd_stream:release(State#state.stream_budget, Given),
            {noreply, State#state{reserved = Reserved#{Id := Held - Given}}};
        _ ->
            {noreply, State}
    end;
handle_cast({optimism, Generation, Optimistic}, #state{generation = Generation} = State) ->
    {noreply, State#state{optimistic = Optimistic}};
handle_cast({optimism, _Generation, _Optimistic}, State) ->
    {noreply, State}.

-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info({Tag, Pid, Event}, #state{status = ready, transport = {_, Pid}} = State) when
    Tag =:= quic; Tag =:= trevrpc_qmux
->
    connection_event(Event, State);
handle_info({Tag, Pid, _Event}, #state{retired = Retired} = State) when
    (Tag =:= quic orelse Tag =:= trevrpc_qmux) andalso is_map_key(Pid, Retired)
->
    {noreply, State};
handle_info({Tag, Pid, Event}, State) when Tag =:= quic; Tag =:= trevrpc_qmux ->
    case find_attempt(Pid, State) of
        {ok, Ref} ->
            attempt_event(Ref, Event, State);
        error when Tag =:= quic ->
            adopt(quic, Pid, Event, State);
        error ->
            adopt(qmux, Pid, Event, State)
    end;
handle_info({connect_result, Ref, Result}, #state{attempts = Attempts} = State) ->
    case {Attempts, Result} of
        {#{Ref := #{transport := undefined} = Attempt}, {ok, Transport}} ->
            {noreply, State#state{attempts = Attempts#{Ref := Attempt#{transport := Transport}}}};
        {#{Ref := #{transport := Adopted}}, {ok, Transport}} when Adopted =/= Transport ->
            %% The attempt adopted another connection from its events.
            {noreply, retire(Transport, State)};
        {#{Ref := _}, {ok, _}} ->
            {noreply, State};
        {#{Ref := _}, {error, _Reason}} ->
            {noreply, attempt_failed(Ref, State)};
        {#{}, {ok, Transport}} ->
            %% A stale attempt connected after all, unless a later round
            %% adopted its connection.
            case
                Transport =:= State#state.transport orelse
                    find_attempt(trevrpc_transport:pid(Transport), State) =/= error
            of
                true -> {noreply, State};
                false -> {noreply, retire(Transport, State)}
            end;
        {#{}, {error, _}} ->
            {noreply, State}
    end;
handle_info({timeout, _Timer, {attempt_timeout, Ref}}, #state{attempts = Attempts} = State) ->
    case Attempts of
        #{Ref := _} -> {noreply, attempt_failed(Ref, State)};
        #{} -> {noreply, State}
    end;
handle_info(
    {timeout, _Timer, {fallback, Round}}, #state{round = Round, status = connecting} = State
) ->
    {noreply, start_fallback(State)};
handle_info({timeout, _Timer, reconnect}, #state{status = connecting, round = undefined} = State) ->
    {noreply, race(State)};
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
handle_info({trevrpc_send_ready, Pid, Ref}, #state{send_waiters = Waiters} = State) ->
    _ =
        case {Waiters, State#state.status} of
            {_, connecting} -> Pid ! {trevrpc_stream, Ref, closed};
            {[], ready} -> trevrpc_transport:notify_send_ready(State#state.transport);
            _ -> ok
        end,
    {noreply, State#state{send_waiters = [{Pid, Ref} | Waiters]}};
handle_info({'DOWN', Monitor, process, Pid, _Reason}, #state{retired = Retired} = State) when
    map_get(Pid, Retired) =:= Monitor
->
    {noreply, State#state{retired = maps:remove(Pid, Retired)}};
handle_info({'DOWN', Monitor, process, _Pid, _Reason}, #state{conn_monitor = Monitor} = State) ->
    %% The connection's process died without telling: as if it closed.
    {noreply, lost(State#state{conn_monitor = undefined})};
handle_info(
    {'DOWN', Monitor, process, _Pid, _Reason}, #state{subscribers = Subscribers} = State
) when
    is_map_key(Monitor, Subscribers)
->
    {noreply, State#state{subscribers = maps:remove(Monitor, Subscribers)}};
handle_info({'DOWN', Monitor, process, _Pid, _Reason}, #state{monitors = Monitors} = State) ->
    %% The calling process exited without finishing its call: cancel it.
    case maps:find(Monitor, Monitors) of
        {ok, Id} ->
            _ = trevrpc_transport:reset_stream(State#state.transport, Id, ?CANCELLED),
            _ = trevrpc_transport:stop_sending(State#state.transport, Id, ?CANCELLED),
            {noreply, forget(Id, Monitor, State)};
        error ->
            {noreply, State}
    end;
handle_info({'EXIT', _Pid, _Reason}, State) ->
    %% Connect helpers and the processes QUIC connections are linked to.
    {noreply, State};
handle_info(_Message, State) ->
    {noreply, State}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, #state{subscribers = Subscribers} = State) ->
    _ = [Pid ! {trevrpc_channel, self(), closed} || Pid <- maps:values(Subscribers)],
    fail_streams(State),
    _ = release_connection(stop_attempts(State)),
    ok.

%%% Racing

%% Starts a round of connection attempts: QMux alone while it won recently,
%% else QUIC, with QMux to follow.
race(#state{policy = Policy, fallback_until = Until} = State) ->
    Round = make_ref(),
    UseFallback =
        Policy =:= tcp orelse
            (Policy =:= auto andalso Until =/= undefined andalso now_ms() < Until),
    Started = State#state{
        round = Round, attempts = #{}, raced = not UseFallback, fallback_started = UseFallback
    },
    case UseFallback of
        true ->
            tcp_attempt(Started);
        false ->
            _ =
                case Policy of
                    auto ->
                        erlang:start_timer(State#state.fallback_delay, self(), {fallback, Round});
                    quic ->
                        ok
                end,
            quic_attempt(Started)
    end.

%% QMux joins a round once, when the fallback delay passes or QUIC fails,
%% whichever comes first.
start_fallback(#state{policy = auto, fallback_started = false} = State) ->
    tcp_attempt(State#state{fallback_started = true});
start_fallback(State) ->
    State.

%% quic:connect/4 blocks while it races the addresses of a host name, so it
%% runs in a process of its own; the channel owns the connection either way.
%% The connection is linked to the process that started it and stops when
%% that process exits, so that process stays alive as its parent: it exits
%% when told to, or with the channel, and takes the connection with it.
quic_attempt(
    #state{host = Host, port = Port, quic_opts = QuicOpts, connect_timeout = Timeout} = State
) ->
    Ref = make_ref(),
    Self = self(),
    Opts = QuicOpts#{connect_timeout => Timeout},
    Parent = spawn_link(fun() ->
        process_flag(trap_exit, true),
        Result =
            try quic:connect(Host, Port, Opts, Self) of
                {ok, Conn} -> {ok, {quic, Conn}};
                Error -> Error
            catch
                Class:Reason -> {error, {Class, Reason}}
            end,
        Self ! {connect_result, Ref, Result},
        parent_loop(Self)
    end),
    erlang:start_timer(Timeout, self(), {attempt_timeout, Ref}),
    add_attempt(Ref, #{kind => quic, transport => undefined, parent => Parent}, State).

parent_loop(Channel) ->
    receive
        stop -> ok;
        {'EXIT', Channel, _Reason} -> ok;
        %% The connection exited; the channel hears of it from the connection.
        {'EXIT', _Other, _Reason} -> parent_loop(Channel)
    end.

%% A TLS connection whose handshake picked the QMux ALPN, then a QMux
%% session over it, owned by the channel.
tcp_attempt(
    #state{
        tcp_host = Host,
        tcp_port = Port,
        tls_opts = TlsOpts,
        qmux_opts = QmuxOpts,
        connect_timeout = Timeout
    } = State
) ->
    Ref = make_ref(),
    Self = self(),
    _ = spawn_link(fun() ->
        Result =
            case ssl:connect(Host, Port, TlsOpts, Timeout) of
                {ok, Socket} ->
                    Qmux = trevrpc:qmux_alpn(),
                    case ssl:negotiated_protocol(Socket) of
                        {ok, Qmux} ->
                            case trevrpc_qmux:start(Socket, client, Self, QmuxOpts) of
                                {ok, Session} ->
                                    {ok, {qmux, Session}};
                                Error ->
                                    _ = ssl:close(Socket),
                                    Error
                            end;
                        _ ->
                            _ = ssl:close(Socket),
                            {error, no_application_protocol}
                    end;
                Error ->
                    Error
            end,
        Self ! {connect_result, Ref, Result}
    end),
    erlang:start_timer(Timeout, self(), {attempt_timeout, Ref}),
    add_attempt(Ref, #{kind => qmux, transport => undefined, parent => undefined}, State).

add_attempt(Ref, Attempt, #state{attempts = Attempts} = State) ->
    State#state{attempts = Attempts#{Ref => Attempt}}.

find_attempt(Pid, #state{attempts = Attempts}) ->
    case [Ref || Ref := #{transport := {_, P}} <- Attempts, P =:= Pid] of
        [Ref | _] -> {ok, Ref};
        [] -> error
    end.

%% A connection can be ready, or already gone, before its attempt reports
%% it: quic:connect/4 returns at once for an address, and a QMux session can
%% get the server's parameters, or see it close, before its helper sends the
%% result. Take it from its events; the channel's own closed connections are
%% retired, so these come from the round's attempts.
adopt(Kind, Pid, {Outcome, _} = Event, #state{attempts = Attempts} = State) when
    Outcome =:= connected; Outcome =:= closed; Kind =:= quic andalso Outcome =:= error
->
    case [Ref || Ref := #{kind := K, transport := undefined} <- Attempts, K =:= Kind] of
        [Ref | _] ->
            Adopted = State#state{
                attempts = Attempts#{Ref := (maps:get(Ref, Attempts))#{transport := {Kind, Pid}}}
            },
            attempt_event(Ref, Event, Adopted);
        [] ->
            {noreply, State}
    end;
adopt(_Kind, _Pid, _Event, State) ->
    {noreply, State}.

attempt_event(Ref, {connected, Info}, #state{attempts = Attempts} = State) ->
    #{Ref := #{kind := Kind, transport := Transport}} = Attempts,
    case Kind =:= qmux orelse maps:get(alpn, Info, undefined) =:= trevrpc:alpn() of
        true -> {noreply, won(Ref, Transport, Kind, State)};
        %% No or another application protocol: not a TrevRPC server.
        false -> {noreply, attempt_failed(Ref, State)}
    end;
attempt_event(Ref, {Outcome, _Reason}, State) when Outcome =:= closed; Outcome =:= error ->
    {noreply, attempt_failed(Ref, State)};
attempt_event(_Ref, _Event, State) ->
    {noreply, State}.

%% The first ready connection becomes the generation; the race's other
%% attempts close.
won(Ref, Transport, Kind, #state{attempts = Attempts} = State) ->
    #{Ref := #{parent := Parent}} = Attempts,
    Stopped = stop_attempts(State#state{attempts = maps:remove(Ref, Attempts)}),
    _ = [gen_server:reply(From, ok) || {From, _Timer} <- State#state.ready_waiters],
    %% QMux winning a race against QUIC opens the window in which reconnects
    %% skip QUIC; reconnects within it leave it as it is.
    FallbackUntil =
        case {State#state.policy, Kind, State#state.raced} of
            {auto, qmux, true} -> now_ms() + State#state.reprobe_interval;
            {auto, qmux, false} -> State#state.fallback_until;
            _ -> undefined
        end,
    notify(Stopped#state{
        status = ready,
        transport = Transport,
        generation = State#state.generation + 1,
        shared = undefined,
        optimistic = true,
        delay = ?INITIAL_DELAY,
        round = undefined,
        attempts = #{},
        ready_waiters = [],
        fallback_until = FallbackUntil,
        conn_monitor = monitor(process, trevrpc_transport:pid(Transport)),
        parent = Parent
    }).

attempt_failed(Ref, #state{attempts = Attempts} = State) ->
    #{Ref := #{kind := Kind} = Attempt} = Attempts,
    Next = close_attempt(Attempt, State#state{attempts = maps:remove(Ref, Attempts)}),
    %% QUIC failing starts the fallback at once.
    Fallback =
        case Kind of
            quic -> start_fallback(Next);
            qmux -> Next
        end,
    case map_size(Fallback#state.attempts) of
        0 -> retry(Fallback);
        _ -> Fallback
    end.

%% Closes the attempts; the caller forgets them.
stop_attempts(#state{attempts = Attempts} = State) ->
    lists:foldl(fun close_attempt/2, State, maps:values(Attempts)).

close_attempt(#{transport := Transport, parent := Parent}, State) ->
    _ =
        case Parent of
            undefined -> ok;
            _ -> Parent ! stop
        end,
    retire(Transport, State).

%% Closes a connection, and ignores what it sends until it exits.
retire(undefined, State) ->
    State;
retire(Transport, #state{retired = Retired} = State) ->
    ok = trevrpc_transport:close(Transport, 0),
    Pid = trevrpc_transport:pid(Transport),
    case Retired of
        #{Pid := _} -> State;
        #{} -> State#state{retired = Retired#{Pid => monitor(process, Pid)}}
    end.

%% Schedules the next race after the backoff delay, with jitter.
retry(#state{delay = Delay} = State0) ->
    State = stop_attempts(State0),
    Jittered = round(Delay * (0.8 + 0.4 * rand:uniform())),
    erlang:start_timer(Jittered, self(), reconnect),
    State#state{
        status = connecting,
        transport = undefined,
        round = undefined,
        attempts = #{},
        delay = min(?MAX_DELAY, Delay * 2)
    }.

%%% The ready connection

connection_event({stream_data, Id, Bin, Fin}, State) ->
    route(Id, {data, Bin, Fin}, State);
connection_event({stream_reset, Id, Code}, State) ->
    route(Id, {reset, Code}, State);
connection_event({stop_sending, Id, Code}, State) ->
    route(Id, {stop_sending, Code}, State);
connection_event({stream_opened, Id}, #state{transport = Transport} = State) ->
    %% Servers open no streams in TrevRPC.
    _ = trevrpc_transport:stop_sending(Transport, Id, ?CANCELLED),
    {noreply, State};
connection_event({streams_available, bidi}, State) ->
    retry_opens(State);
connection_event(send_ready, #state{send_waiters = Waiters} = State) ->
    _ = [Pid ! {trevrpc_stream, Ref, send_ready} || {Pid, Ref} <- Waiters],
    {noreply, State#state{send_waiters = []}};
connection_event({closed, _Reason}, State) ->
    {noreply, lost(State)};
connection_event({error, _Reason}, State) ->
    {noreply, lost(State)};
connection_event(_Event, State) ->
    {noreply, State}.

%% The connection is gone: fail its calls and reconnect.
lost(State) ->
    Connecting = notify((release_connection(State))#state{status = connecting}),
    fail_streams(Connecting),
    _ = [
        gen_server:reply(From, {error, trevrpc_status:new(unavailable, <<"connection closed">>)})
     || {From, _Owner, _Timer} <- Connecting#state.pending_opens
    ],
    _ = [demonitor(Monitor, [flush]) || Monitor <- maps:keys(Connecting#state.monitors)],
    _ = [
        trevrpc_zstd_stream:release(Connecting#state.stream_budget, Held)
     || _Id := Held <- Connecting#state.reserved
    ],
    retry(Connecting#state{
        streams = #{}, monitors = #{}, reserved = #{}, pending_opens = [], send_waiters = []
    }).

%% Lets the ready connection go: closes it, in case it reported an error
%% rather than closing, and stops the process a QUIC connection is linked to.
release_connection(#state{transport = Transport, conn_monitor = Monitor, parent = Parent} = State) ->
    _ =
        case Monitor of
            undefined -> ok;
            _ -> demonitor(Monitor, [flush])
        end,
    _ =
        case Parent of
            undefined -> ok;
            _ -> Parent ! stop
        end,
    (retire(Transport, State))#state{conn_monitor = undefined, parent = undefined}.

%% Tells the subscribers the channel's state.
notify(#state{subscribers = Subscribers} = State) ->
    Message = {trevrpc_channel, self(), channel_state(State)},
    _ = [Pid ! Message || Pid <- maps:values(Subscribers)],
    State.

channel_state(#state{status = ready, generation = Generation}) -> {ready, Generation};
channel_state(#state{status = connecting}) -> connecting.

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

open(From, Owner, Deadline, #state{transport = Transport} = State) ->
    case trevrpc_transport:open_stream(Transport) of
        {ok, Id} ->
            ok = trevrpc_transport:manual_flow_control(Transport, Id),
            Ref = make_ref(),
            Monitor = monitor(process, Owner),
            Info = #{
                transport => Transport,
                id => Id,
                ref => Ref,
                generation => State#state.generation,
                max_frame_size => State#state.max_frame_size,
                compression => State#state.compression,
                shared => State#state.shared,
                stream_budget => State#state.stream_budget,
                optimistic => State#state.optimistic
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

%% Forgets stream `Id', giving back what it held of the stream compression
%% budget.
forget(Id, Monitor, #state{streams = Streams, monitors = Monitors, reserved = Reserved} = State) ->
    trevrpc_zstd_stream:release(State#state.stream_budget, maps:get(Id, Reserved, 0)),
    State#state{
        streams = maps:remove(Id, Streams),
        monitors = maps:remove(Monitor, Monitors),
        reserved = maps:remove(Id, Reserved)
    }.

closed_status() ->
    trevrpc_status:new(unavailable, <<"channel is closed">>).

now_ms() -> erlang:monotonic_time(millisecond).
