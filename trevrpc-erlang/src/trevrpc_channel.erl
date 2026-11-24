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
%% Over native QUIC, a generation keeps a pool of connections: at least
%% `min_quic_connections' (1 by default), and under load up to
%% `max_quic_connections' (the schedulers online, from one to four, by
%% default), as trevrpc_quic_pool describes. The generation's own
%% connection is the pool's first and decides its life; the others, its
%% extras, are dialled when it is ready, or when the pool grows. An extra
%% that is lost fails only its own calls, and one of the minimum is dialled
%% again; one the pool added closes once it went unused for
%% `quic_connection_idle_timeout' (30 s by default). When the generation
%% ends its extras drain: they take no new calls and close after their
%% last.
%%
%% The channel process owns the connection and routes each stream's events
%% to the process that made the call, which reads and writes the stream
%% itself; see {@link trevrpc_client}. A calling process opens its streams
%% on the connection itself too, once it knows the connection, and only
%% registers them with the channel, so the channel, which every call goes
%% through, never waits on the connection for them.
-module(trevrpc_channel).

-behaviour(gen_server).

-ifdef(TEST).
-export([tls_opts/2]).
-endif.

-export([
    start_link/1,
    start/1,
    connect/2,
    wait_until_ready/2,
    generation/1,
    transport/1,
    datagram_stats/1,
    subscribe/1,
    unsubscribe/1,
    close/1,
    set_connection_metadata/2
]).
-export([open_stream/2, open_stream/3, open_stream/4]).
-export([stream_done/3, learn/3, reserve/4, release/4, optimism/3, confirm_metadata/3]).
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
    %% How long data sent on the QMux over TLS connection may stay
    %% unacknowledged before the kernel drops it (TCP_USER_TIMEOUT, Linux
    %% only), so that a server that vanishes is noticed; idle_timeout by
    %% default.
    tcp_user_timeout => pos_integer() | infinity,
    compression => trevrpc_zstd:settings(),
    stream_compression_memory => non_neg_integer(),
    min_quic_connections => pos_integer(),
    max_quic_connections => pos_integer(),
    %% Calls and bytes per second, per connection, the pool spreads and
    %% grows at (5000 and 20 MB/s by default).
    quic_connection_call_rate => pos_integer(),
    quic_connection_byte_rate => pos_integer(),
    %% How long a streaming call that finds its connection crowded waits for
    %% the connection the pool dials, in milliseconds (100 by default).
    quic_crowded_dial_wait => non_neg_integer(),
    quic_connection_idle_timeout => pos_integer(),
    %% Request metadata every call carries, sent once per connection; see
    %% set_connection_metadata/2.
    connection_metadata => trevrpc_metadata:metadata(),
    %% Checks the requests of calls before they are sent; see
    %% trevrpc_validator.
    validator => trevrpc_validator:validator()
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
    optimistic := boolean(),
    %% The stream's datagram flow and its credit, when the calling process
    %% asked for one and the connection carries datagrams.
    flow := {pos_integer(), trevrpc_datagram_flows:credit()} | undefined,
    %% The generation's connections, numbered, as of a version, with the
    %% pool's limits, and the counters callers read to open their own
    %% streams (see open_stream/3).
    pool := {non_neg_integer(), [{pos_integer(), trevrpc_transport:t()}], pool_limits()},
    counters := trevrpc_quic_pool:counters(),
    %% The channel's connection metadata set, and whether the stream's
    %% connection confirmed it.
    connection_metadata := connection_set() | undefined,
    metadata_confirmed := boolean()
}.

%% A version of the channel's connection metadata: its ID, which changes
%% with it, and its entries.
-type connection_set() :: {pos_integer(), trevrpc_metadata:metadata()}.

%% The pool's limits, and how long a streaming call waits for a connection
%% the pool dials.
-type pool_limits() :: {trevrpc_quic_pool:limits(), non_neg_integer()}.

%% Whether a call asks for a datagram flow, and whether it reads a stream of
%% responses on it.
-type datagrams() :: boolean() | undefined.

%% The process that opens a stream, whether its call asks for a datagram
%% flow, and whether it is streaming.
-type opener() :: {pid(), datagrams(), boolean()}.

-define(INITIAL_DELAY, 100).
-define(MAX_DELAY, 30000).
-define(CANCELLED, 1).
-define(STREAM_COMPRESSION_MEMORY, 67108864).
-define(CALL_RATE, 5000).
%% A fifth of what one erlang_quic connection carried on 4 KiB bidirectional
%% streams, and more than a 100 Mbit/s link can.
-define(BYTE_RATE, 20000000).
-define(CROWDED_DIAL_WAIT, 100).
-define(POOL_IDLE_TIMEOUT, 30000).
%% How long the pool waits after a failed dial before it grows again.
-define(GROW_RETRY, 5000).
%% How long the channel waits for its connections' bytes and round trips.
-define(SAMPLE_TIMEOUT, 1000).
%% The highest number of the extras of the pool's minimum: those above it
%% the pool added as it grew.
-define(MIN_SLOT(Limits), trevrpc_quic_pool:min(Limits)).

%% A stream, by its connection's process and its ID on that connection.
-type key() :: {pid(), non_neg_integer()}.

%% A connection attempt: its transport kind, its connection once known, and
%% for QUIC the process the connection is linked to (see quic_attempt/2).
-type attempt() :: #{
    kind := trevrpc_transport:kind(),
    transport := trevrpc_transport:t() | undefined,
    parent := pid() | undefined
}.

-type extra() :: #{
    transport := trevrpc_transport:t(),
    slot := pos_integer(),
    monitor := reference(),
    parent := pid()
}.

-type extra_attempt() :: #{
    slot := pos_integer(),
    generation := non_neg_integer(),
    transport := trevrpc_transport:t() | undefined,
    parent := pid()
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
    qmux_opts :: qmux:options(),
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
    reserved = #{} :: #{key() => non_neg_integer()},
    status = connecting :: connecting | ready,
    transport :: trevrpc_transport:t() | undefined,
    %% The current race, its attempts, whether it started with QUIC, and
    %% whether its QMux attempt started.
    round :: reference() | undefined,
    %% The only map in the state keyed by references whose values have a
    %% `kind': trevrpc_e2e_tests finds it by that shape.
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
    %% Stream => {calling process, routing tag, monitor}, and the streams by
    %% routing tag and by monitor.
    streams = #{} :: #{key() => {pid(), reference(), reference()}},
    refs = #{} :: #{reference() => key()},
    monitors = #{} :: #{reference() => key()},
    %% Opens refused for lack of stream credit, waiting for more.
    pending_opens = [] :: [{gen_server:from(), opener(), reference()}],
    %% The counters callers that open their own streams read (see
    %% trevrpc_quic_pool).
    counters :: trevrpc_quic_pool:counters(),
    %% The datagram flows of the connection's calls, the flow of each
    %% stream that has one, and whether the connection carries datagrams,
    %% once a call asked.
    flows = trevrpc_datagram_flows:new(client, 0) :: trevrpc_datagram_flows:flows(),
    stream_flows = #{} :: #{key() => pos_integer()},
    datagrams :: boolean() | undefined,
    %% Processes waiting for each connection's send queue to drain.
    send_waiters = #{} :: #{pid() => [{pid(), reference()}]},
    %% Over native QUIC, the generation's own connection is number 1 of its
    %% pool, and up to quic_limits' maximum - 1 more, the extras, join it:
    %% each with its number, monitor and parent, while they are dialled, and
    %% with the delay before one of the minimum, lost, is dialled again.
    %% Extras of a generation that ended drain: they take no new calls and
    %% close after their last. The current generation's limits are QMux's
    %% single connection over QMux.
    quic_limits :: trevrpc_quic_pool:limits(),
    limits :: trevrpc_quic_pool:limits(),
    crowded_dial_wait = ?CROWDED_DIAL_WAIT :: non_neg_integer(),
    pool_idle_timeout = ?POOL_IDLE_TIMEOUT :: pos_integer(),
    version = 0 :: non_neg_integer(),
    extras = #{} :: #{pid() => extra()},
    draining = #{} :: #{pid() => extra()},
    extra_attempts = #{} :: #{reference() => extra_attempt()},
    extra_delays = #{} :: #{pos_integer() => pos_integer()},
    %% Events of connections no extra's attempt knows yet, until the
    %% attempt's result names the connection, and whether a new generation
    %% still dials extras, which holds back wait_until_ready/2.
    held_events = #{} :: #{pid() => [term()]},
    pool_settling = false :: boolean(),
    %% The calls in flight on each connection, and the streaming calls.
    loads = #{} :: #{pid() => non_neg_integer()},
    streaming = #{} :: #{key() => true},
    %% Opens of streaming calls that found their connection crowded, held
    %% for the connection the pool dials, the extras the pool added that
    %% took a call since the last idle check, and whether that check is
    %% armed.
    crowded_opens = [] :: [{gen_server:from(), opener(), trevrpc_stream:deadline(), reference()}],
    used = #{} :: #{pid() => true},
    idle_check = false :: boolean(),
    %% The measure of the connections' bytes and round trips under way, by
    %% its tag, process and timer, and the bytes each had moved, and when,
    %% as last measured.
    sampling = false :: false | {reference(), pid(), reference()},
    sampled_at :: integer() | undefined,
    sampled_bytes = #{} :: #{pid() => non_neg_integer()},
    %% Processes told of every change of state.
    subscribers = #{} :: #{reference() => pid()},
    %% Connection metadata: the channel's current set, the ID of the last
    %% one (numbered from 1), and the set the server confirmed on each
    %% connection of the pool.
    connection_metadata :: connection_set() | undefined,
    metadata_ids = 0 :: non_neg_integer(),
    confirmed = #{} :: #{pid() => pos_integer()}
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
            {error, trevrpc_status:new(invalid_argument, Message)};
        {error, {connection_metadata, Message}} ->
            {error, trevrpc_status:new(invalid_argument, Message)};
        {error, {invalid_option, Name}} ->
            {error,
                trevrpc_status:new(invalid_argument, <<"invalid ", (atom_to_binary(Name))/binary>>)}
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

%% @doc The datagram counters of the current connection, when it is native
%% QUIC: those it delivered, dropped on receipt, sent and dropped on
%% sending. `none' on QMux and while connecting.
-spec datagram_stats(pid()) -> #{atom() => non_neg_integer()} | none.
datagram_stats(Channel) ->
    gen_server:call(Channel, datagram_stats).

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

%% @doc Replaces the channel's connection metadata: request metadata that
%% every call carries, such as a bearer token, which crosses each connection
%% in its first calls instead of in every call. Calls that start afterwards
%% carry the new metadata, and calls in flight keep theirs; each connection
%% offers it until its server confirms it stored it, and refers to it by ID
%% after that. The server merges it into each call's metadata before
%% validation, the call's own entry winning for a key both have. Empty
%% metadata removes it.
-spec set_connection_metadata(pid(), trevrpc_metadata:metadata()) ->
    ok | {error, trevrpc_status:status()}.
set_connection_metadata(Channel, Metadata) ->
    case trevrpc_metadata:validate(Metadata) of
        ok ->
            try
                gen_server:call(Channel, {set_connection_metadata, Metadata}, infinity)
            catch
                exit:_ -> {error, closed_status()}
            end;
        {error, Message} ->
            {error, trevrpc_status:new(invalid_argument, Message)}
    end.

%% @private Opens a stream for the calling process, waiting until
%% `Deadline' for stream credit if the server's limit is reached.
-spec open_stream(pid(), trevrpc_stream:deadline()) ->
    {ok, stream_info()} | {error, trevrpc_status:status()}.
open_stream(Channel, Deadline) ->
    open_stream(Channel, Deadline, undefined, false).

%% @private Opens a stream for a call that is not streaming, as {@link
%% open_stream/4} does.
-spec open_stream(pid(), trevrpc_stream:deadline(), datagrams()) ->
    {ok, stream_info()} | {error, trevrpc_status:status()}.
open_stream(Channel, Deadline, Datagrams) ->
    open_stream(Channel, Deadline, Datagrams, false).

%% @private Opens a stream as {@link open_stream/2} does, giving it a
%% datagram flow if `Datagrams' asks for one (`true' when the call reads a
%% stream of responses, `false' when it reads one response) and the
%% connection carries datagrams. `Streaming' says whether the call streams
%% requests or responses, which crowd a connection (see trevrpc_quic_pool).
%%
%% The calling process remembers the channel's connections and their
%% generation from its last stream, opens the stream itself on the one
%% trevrpc_quic_pool:choose/5 picks, and registers it with the channel
%% before it writes to it, so no event of the stream can reach the channel
%% first. The channel opens it instead when the process knows no connection
%% yet, its connections changed, the server's stream limit is reached (the
%% channel waits for credit, and the process waits behind the opens already
%% waiting), the connection is gone or was replaced, or the process waited
%% for a connection the pool dialled.
-spec open_stream(pid(), trevrpc_stream:deadline(), datagrams(), boolean()) ->
    {ok, stream_info()} | {error, trevrpc_status:status()}.
open_stream(Channel, Deadline, Datagrams, Streaming) ->
    Opener = {self(), Datagrams, Streaming},
    case get({?MODULE, Channel}) of
        {Generation, Version, Pool, Counters, {Limits, Wait}} ->
            case
                trevrpc_quic_pool:queued(Counters) =:= 0 andalso
                    trevrpc_quic_pool:version(Counters) =:= Version
            of
                true ->
                    case trevrpc_quic_pool:choose(Pool, Counters, Limits, Datagrams, Streaming) of
                        {_Transport, _Grow, true, Sample} when Wait > 0 ->
                            %% Crowded: the channel holds the open for the
                            %% connection the pool dials.
                            sample(Channel, Sample),
                            channel_open(Channel, Deadline, Opener, true);
                        {Transport, Grow, _Crowded, Sample} ->
                            sample(Channel, Sample),
                            grow(Channel, Counters, Grow),
                            own_open(Channel, Generation, Transport, Deadline, Opener)
                    end;
                false ->
                    channel_open(Channel, Deadline, Opener, false)
            end;
        undefined ->
            channel_open(Channel, Deadline, Opener, false)
    end.

%% Asks the channel to measure its connections' bytes and round trips, once
%% a call ended the window the rates are measured over.
sample(Channel, true) -> gen_server:cast(Channel, sample);
sample(_Channel, false) -> ok.

%% Asks the channel to dial another connection, unless the pool is growing
%% already.
grow(Channel, Counters, true) ->
    case trevrpc_quic_pool:claim_growth(Counters) of
        true -> gen_server:cast(Channel, grow);
        false -> ok
    end;
grow(_Channel, _Counters, false) ->
    ok.

%% Opens the stream on the remembered connection and registers it. A
%% caller killed in between leaves a stream nobody registered. A QMux
%% server, which opens the streams below the next one it hears of, ends it
%% once its REQUEST does not come, and the channel cancels it, as any
%% stream it gets data for and does not know. A QUIC server never hears of
%% it, so it keeps one stream credit until the connection closes.
own_open(Channel, Generation, Transport, Deadline, Opener) ->
    case trevrpc_transport:open_stream(Transport) of
        {ok, Id} ->
            ok = trevrpc_transport:manual_flow_control(Transport, Id),
            Conn = trevrpc_transport:pid(Transport),
            case register_stream(Channel, Generation, Conn, Id, Opener) of
                {ok, Info} ->
                    {ok, Info};
                stale ->
                    cancel_stream(Transport, Id),
                    channel_open(Channel, Deadline, Opener, true)
            end;
        {error, _} ->
            channel_open(Channel, Deadline, Opener, true)
    end.

%% Has the channel open the stream, and remembers its connections. A
%% channel that is gone is forgotten. Counted says whether the call was
%% counted toward the pool's call rate already.
channel_open(Channel, Deadline, Opener, Counted) ->
    _ = erase({?MODULE, Channel}),
    try gen_server:call(Channel, {open_stream, Opener, Deadline, Counted}, infinity) of
        {ok, #{generation := Generation, pool := {Version, Pool, Limits}, counters := Counters}} =
                Opened ->
            put({?MODULE, Channel}, {Generation, Version, Pool, Counters, Limits}),
            Opened;
        Error ->
            Error
    catch
        exit:_ -> {error, closed_status()}
    end.

register_stream(Channel, Generation, Conn, Id, Opener) ->
    try
        gen_server:call(Channel, {register_stream, Generation, Conn, Id, Opener}, infinity)
    catch
        exit:_ -> stale
    end.

%% @private A call on generation `Generation' learned the dictionaries the
%% connection shares with the server from its answer. The first answer
%% wins; they all come from one configuration.
-spec learn(pid(), pos_integer(), [non_neg_integer()]) -> ok.
learn(Channel, Generation, Shared) ->
    gen_server:cast(Channel, {learn, Generation, Shared}).

%% @private A call that offered connection metadata set `Id' on connection
%% `Conn' learned from its ANSWER that the server stored it: the
%% connection's calls refer to it from now on, while it is the channel's
%% current set.
-spec confirm_metadata(pid(), pid(), pos_integer()) -> ok.
confirm_metadata(Channel, Conn, Id) ->
    gen_server:cast(Channel, {confirm_metadata, Conn, Id}).

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
-spec init(options()) ->
    {ok, #state{}}
    | {error, {compression | connection_metadata, binary()} | {invalid_option, atom()}}.
init(Opts) ->
    maybe
        ok ?= trevrpc_socket:check_user_timeout(Opts),
        {ok, Compression} ?=
            case trevrpc_zstd:config(maps:get(compression, Opts, #{})) of
                {error, Message} -> {error, {compression, Message}};
                Config -> Config
            end,
        ConnectionMetadata = maps:get(connection_metadata, Opts, #{}),
        ok ?=
            case trevrpc_metadata:validate(ConnectionMetadata) of
                ok -> ok;
                {error, Invalid} -> {error, {connection_metadata, Invalid}}
            end,
        Validator = maps:get(validator, Opts, undefined),
        ok ?=
            case trevrpc_validator:check(Validator) of
                ok -> ok;
                error -> {error, {invalid_option, validator}}
            end,
        {ok, State} = init(Opts, Compression),
        ok = trevrpc_validator:install(Validator),
        {ok, replace_metadata(ConnectionMetadata, State)}
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
    QuicLimits = trevrpc_quic_pool:limits(
        maps:get(min_quic_connections, Opts, 1),
        maps:get(max_quic_connections, Opts, default_max_quic_connections()),
        maps:get(quic_connection_call_rate, Opts, ?CALL_RATE),
        maps:get(quic_connection_byte_rate, Opts, ?BYTE_RATE)
    ),
    State = #state{
        quic_limits = QuicLimits,
        limits = trevrpc_quic_pool:single(),
        crowded_dial_wait = maps:get(quic_crowded_dial_wait, Opts, ?CROWDED_DIAL_WAIT),
        pool_idle_timeout = max(
            1, maps:get(quic_connection_idle_timeout, Opts, ?POOL_IDLE_TIMEOUT)
        ),
        counters = trevrpc_quic_pool:new(trevrpc_quic_pool:max(QuicLimits)),
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
            send_timeout => Idle,
            %% Calls return the credit for what they read.
            stream_flow_control => manual
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

%% The schedulers online, from one to four. An erlang_quic connection is
%% one process, so connections run in parallel only on schedulers that run
%% them. On four schedulers, 64 concurrent streams of 4 KiB messages ran 3.5
%% times as fast over four connections as over one, and 2 times as fast as
%% over two: unlike the Rust channel's, whose connections contend for
%% threads, the Erlang channel's are each slow to queue many streams' data.
default_max_quic_connections() ->
    max(1, min(4, erlang:system_info(schedulers_online))).

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
        | Trust ++ Identity ++
            trevrpc_socket:user_timeout(
                maps:get(tcp_user_timeout, Opts, maps:get(idle_timeout, Opts, 30000))
            )
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
handle_call(
    {wait_until_ready, _Timeout}, _From, #state{status = ready, pool_settling = false} = State
) ->
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
handle_call(datagram_stats, _From, #state{status = ready, transport = {quic, Conn}} = State) ->
    Stats =
        try quic:datagram_stats(Conn) of
            Counters when is_map(Counters) -> Counters
        catch
            _:_ -> none
        end,
    {reply, Stats, State};
handle_call(datagram_stats, _From, State) ->
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
handle_call({set_connection_metadata, Metadata}, _From, State) ->
    {reply, ok, replace_metadata(Metadata, State)};
handle_call({open_stream, _Owner, _Deadline, _Counted}, _From, #state{status = connecting} = State) ->
    {reply, {error, trevrpc_status:new(unavailable, <<"channel is reconnecting">>)}, State};
handle_call({open_stream, Owner, Deadline, false}, From, State) ->
    open(From, Owner, Deadline, wait, State);
handle_call({open_stream, Owner, Deadline, true}, From, State) ->
    open(From, Owner, Deadline, wait_counted, State);
handle_call(
    {register_stream, Generation, Conn, Id, Opener},
    _From,
    #state{status = ready, generation = Generation} = State
) ->
    case slot(Conn, State) of
        undefined ->
            %% A connection of the generation that is gone.
            {reply, stale, State};
        _ ->
            {Info, Registered} = opened(Conn, Id, Opener, State),
            {reply, {ok, Info}, Registered}
    end;
handle_call({register_stream, _Generation, _Conn, _Id, _Opener}, _From, State) ->
    %% The connection the stream is on is no longer the channel's.
    {reply, stale, State};
handle_call({reserve, Id, Ref, Cost}, _From, State) ->
    case stream_key(Id, Ref, State) of
        {ok, Key} ->
            case trevrpc_zstd_stream:reserve(State#state.stream_budget, Cost) of
                true ->
                    Reserved = State#state.reserved,
                    {reply, true, State#state{
                        reserved = Reserved#{Key => maps:get(Key, Reserved, 0) + Cost}
                    }};
                false ->
                    {reply, false, State}
            end;
        error ->
            {reply, false, State}
    end.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast({stream_done, Id, Ref}, State) ->
    case stream_key(Id, Ref, State) of
        {ok, Key} ->
            {_Pid, Ref, Monitor} = maps:get(Key, State#state.streams),
            demonitor(Monitor, [flush]),
            {noreply, forget(Key, State)};
        error ->
            {noreply, State}
    end;
handle_cast(
    {learn, Generation, Shared}, #state{generation = Generation, shared = undefined} = State
) ->
    {noreply, State#state{shared = Shared}};
handle_cast({learn, _Generation, _Shared}, State) ->
    {noreply, State};
handle_cast({release, Id, Ref, Cost}, #state{reserved = Reserved} = State) ->
    case stream_key(Id, Ref, State) of
        {ok, Key} when is_map_key(Key, Reserved) ->
            Held = map_get(Key, Reserved),
            Given = min(Cost, Held),
            trevrpc_zstd_stream:release(State#state.stream_budget, Given),
            {noreply, State#state{reserved = Reserved#{Key := Held - Given}}};
        _ ->
            {noreply, State}
    end;
handle_cast({optimism, Generation, Optimistic}, #state{generation = Generation} = State) ->
    {noreply, State#state{optimistic = Optimistic}};
handle_cast({optimism, _Generation, _Optimistic}, State) ->
    {noreply, State};
handle_cast({confirm_metadata, Conn, Id}, State) ->
    case State#state.connection_metadata of
        %% A confirmation of a set the channel replaced since makes no call
        %% refer to the new one.
        {Id, _} when State#state.status =:= ready ->
            case slot(Conn, State) of
                undefined ->
                    {noreply, State};
                _ ->
                    {noreply, State#state{confirmed = (State#state.confirmed)#{Conn => Id}}}
            end;
        _ ->
            {noreply, State}
    end;
handle_cast(grow, State) ->
    {noreply, grow_pool(State)};
handle_cast(sample, State) ->
    {noreply, sample_pool(State)}.

-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info({Tag, Pid, Event}, #state{status = ready, transport = Transport} = State) when
    (Tag =:= quic orelse Tag =:= qmux), element(2, Transport) =:= Pid
->
    connection_event(Pid, Event, State);
handle_info({quic, Pid, Event}, #state{extras = Extras, draining = Draining} = State) when
    is_map_key(Pid, Extras); is_map_key(Pid, Draining)
->
    connection_event(Pid, Event, State);
handle_info({Tag, Pid, _Event}, #state{retired = Retired} = State) when
    (Tag =:= quic orelse Tag =:= qmux) andalso is_map_key(Pid, Retired)
->
    {noreply, State};
handle_info({quic, Pid, Event}, #state{status = ready} = State) ->
    %% Only an extra's attempt dials while the generation is ready.
    {noreply, extra_attempt_event(Pid, Event, State)};
handle_info({Tag, Pid, Event}, State) when Tag =:= quic; Tag =:= qmux ->
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
handle_info({extra_result, Ref, Result}, State) ->
    {noreply, extra_result(Ref, Result, State)};
handle_info({timeout, _Timer, {extra_timeout, Ref}}, #state{extra_attempts = Attempts} = State) ->
    case Attempts of
        #{Ref := _} -> {noreply, extra_failed(Ref, State)};
        #{} -> {noreply, State}
    end;
handle_info({timeout, _Timer, {grow_retry, Generation}}, #state{generation = Generation} = State) ->
    trevrpc_quic_pool:end_growth(State#state.counters),
    {noreply, State};
handle_info({timeout, _Timer, {grow_retry, _Generation}}, State) ->
    {noreply, State};
handle_info({timeout, _Timer, {idle_check, Generation}}, #state{generation = Generation} = State) ->
    {noreply, idle_check(State#state{idle_check = false})};
handle_info({timeout, _Timer, {idle_check, _Generation}}, State) ->
    {noreply, State};
handle_info({timeout, Timer, {crowded_timeout, From}}, #state{crowded_opens = Held} = State) ->
    case lists:keytake(From, 1, Held) of
        {value, {From, _Opener, _Deadline, Timer} = Open, Rest} ->
            {From, Opener, Deadline, _Timer} = Open,
            case open(From, Opener, Deadline, no_wait, State#state{crowded_opens = Rest}) of
                {reply, Reply, Next} ->
                    gen_server:reply(From, Reply),
                    {noreply, Next};
                {noreply, Next} ->
                    {noreply, Next}
            end;
        _ ->
            {noreply, State}
    end;
handle_info(
    {pool_sample, Ref, Generation, At, Samples}, #state{sampling = {Ref, _Pid, Timer}} = State
) ->
    _ = erlang:cancel_timer(Timer, [{async, true}, {info, false}]),
    {noreply, settle(sampled(Generation, At, Samples, State#state{sampling = false}))};
handle_info({pool_sample, _Ref, _Generation, _At, _Samples}, State) ->
    {noreply, State};
handle_info({timeout, _Timer, {sample_timeout, Ref}}, #state{sampling = {Ref, Pid, _}} = State) ->
    %% A connection that does not answer: the pool goes without this measure.
    exit(Pid, kill),
    {noreply, settle(State#state{sampling = false})};
handle_info({timeout, _Timer, {sample_timeout, _Ref}}, State) ->
    {noreply, State};
handle_info(
    {timeout, _Timer, {redial, Slot, Generation}},
    #state{status = ready, generation = Generation} = State
) ->
    {noreply, dial_extra(Slot, State)};
handle_info({timeout, _Timer, {redial, _Slot, _Generation}}, State) ->
    {noreply, State};
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
            {noreply, pending(Rest, State)};
        false ->
            {noreply, State}
    end;
handle_info({trevrpc_await_datagrams, Pid, Ref, Flow, Count}, #state{flows = Flows} = State) ->
    {noreply, State#state{flows = trevrpc_datagram_flows:await(Flow, Pid, Ref, Count, Flows)}};
handle_info({trevrpc_send_ready, Pid, Ref}, #state{send_waiters = Waiters, refs = Refs} = State) ->
    case Refs of
        #{Ref := {Conn, _Id}} ->
            ConnWaiters = maps:get(Conn, Waiters, []),
            _ =
                case ConnWaiters of
                    [] -> trevrpc_transport:notify_send_ready(conn_transport(Conn, State));
                    _ -> ok
                end,
            {noreply, State#state{send_waiters = Waiters#{Conn => [{Pid, Ref} | ConnWaiters]}}};
        #{} ->
            %% Its stream is gone, with its connection.
            Pid ! {trevrpc_stream, Ref, closed},
            {noreply, State}
    end;
handle_info({'DOWN', Monitor, process, Pid, _Reason}, #state{retired = Retired} = State) when
    map_get(Pid, Retired) =:= Monitor
->
    {noreply, State#state{retired = maps:remove(Pid, Retired)}};
handle_info({'DOWN', Monitor, process, _Pid, _Reason}, #state{conn_monitor = Monitor} = State) ->
    %% The connection's process died without telling: as if it closed.
    {noreply, lost(State#state{conn_monitor = undefined})};
handle_info(
    {'DOWN', _Monitor, process, Pid, _Reason}, #state{extras = Extras, draining = Draining} = State
) when
    is_map_key(Pid, Extras); is_map_key(Pid, Draining)
->
    {noreply, extra_lost(Pid, State)};
handle_info(
    {'DOWN', Monitor, process, _Pid, _Reason}, #state{subscribers = Subscribers} = State
) when
    is_map_key(Monitor, Subscribers)
->
    {noreply, State#state{subscribers = maps:remove(Monitor, Subscribers)}};
handle_info({'DOWN', Monitor, process, _Pid, _Reason}, #state{monitors = Monitors} = State) ->
    %% The calling process exited without finishing its call: cancel it.
    case maps:find(Monitor, Monitors) of
        {ok, {Conn, Id} = Key} ->
            cancel_stream(conn_transport(Conn, State), Id),
            {noreply, forget(Key, State)};
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
    ok = trevrpc_validator:uninstall(),
    _ = [Pid ! {trevrpc_channel, self(), closed} || Pid <- maps:values(Subscribers)],
    fail_streams(fun(_Conn) -> true end, State),
    Stopped = stop_extra_attempts(stop_attempts(State)),
    Closed = lists:foldl(
        fun close_extra/2,
        Stopped,
        maps:values(Stopped#state.extras) ++ maps:values(Stopped#state.draining)
    ),
    _ = release_connection(Closed),
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
quic_attempt(#state{connect_timeout = Timeout} = State) ->
    Ref = make_ref(),
    Parent = quic_connect(connect_result, Ref, State),
    erlang:start_timer(Timeout, self(), {attempt_timeout, Ref}),
    add_attempt(Ref, #{kind => quic, transport => undefined, parent => Parent}, State).

%% Dials a QUIC connection the channel owns, from a parent process that
%% sends the channel `{Tag, Ref, Result}'.
quic_connect(
    Tag, Ref, #state{host = Host, port = Port, quic_opts = QuicOpts, connect_timeout = Timeout}
) ->
    Self = self(),
    Opts = QuicOpts#{connect_timeout => Timeout},
    spawn_link(fun() ->
        process_flag(trap_exit, true),
        Result =
            try quic:connect(Host, Port, Opts, Self) of
                {ok, Conn} -> {ok, {quic, Conn}};
                Error -> Error
            catch
                Class:Reason -> {error, {Class, Reason}}
            end,
        Self ! {Tag, Ref, Result},
        parent_loop(Self)
    end).

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
                            case qmux:start({qmux_ssl, Socket}, client, Self, QmuxOpts) of
                                {ok, Session, Queue} ->
                                    {ok, {qmux, Session, Queue}};
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
    case
        [
            Ref
         || Ref := #{transport := Transport} <- Attempts,
            Transport =/= undefined,
            trevrpc_transport:pid(Transport) =:= Pid
        ]
    of
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
                attempts = Attempts#{
                    Ref := (maps:get(Ref, Attempts))#{transport := adopted(Kind, Pid, Event)}
                }
            },
            attempt_event(Ref, Event, Adopted);
        [] ->
            {noreply, State}
    end;
adopt(_Kind, _Pid, _Event, State) ->
    {noreply, State}.

%% The connection an attempt adopts. A QMux session's writers need its send
%% queue, which its connected event carries; one that closed first is left
%% to its helper's result, which retires it.
adopted(quic, Pid, _Event) -> {quic, Pid};
adopted(qmux, Pid, {connected, #{send_queue := Queue}}) -> {qmux, Pid, Queue};
adopted(qmux, _Pid, _Event) -> undefined.

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
    %% QMux winning a race against QUIC opens the window in which reconnects
    %% skip QUIC; reconnects within it leave it as it is.
    FallbackUntil =
        case {State#state.policy, Kind, State#state.raced} of
            {auto, qmux, true} -> now_ms() + State#state.reprobe_interval;
            {auto, qmux, false} -> State#state.fallback_until;
            _ -> undefined
        end,
    Conn = trevrpc_transport:pid(Transport),
    notify(
        start_pool(Kind, Stopped#state{
            pool_settling = true,
            status = ready,
            transport = Transport,
            generation = State#state.generation + 1,
            shared = undefined,
            optimistic = true,
            delay = ?INITIAL_DELAY,
            round = undefined,
            attempts = #{},
            fallback_until = FallbackUntil,
            conn_monitor = monitor(process, trevrpc_transport:pid(Transport)),
            parent = Parent,
            flows = trevrpc_datagram_flows:new(client, 0),
            datagrams = undefined,
            loads = (Stopped#state.loads)#{Conn => 0},
            extra_delays = #{}
        })
    ).

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

%%% The ready connections

%% An event of connection Conn: the generation's own, an extra, or one that
%% drains.
connection_event(Conn, {stream_data, Id, Bin, Fin}, State) ->
    route(Conn, Id, {data, Bin, Fin}, State);
connection_event(Conn, {stream_reset, Id, Code}, State) ->
    route(Conn, Id, {reset, Code}, State);
connection_event(Conn, {stop_sending, Id, Code}, State) ->
    route(Conn, Id, {stop_sending, Code}, State);
connection_event(Conn, {stream_opened, Id}, State) ->
    %% Servers open no streams in TrevRPC.
    _ = trevrpc_transport:stop_sending(conn_transport(Conn, State), Id, ?CANCELLED),
    {noreply, State};
connection_event(_Conn, {streams_available, bidi}, State) ->
    retry_opens(State);
connection_event(Conn, {datagram, Datagram}, #state{flows = Flows} = State) ->
    %% Calls with datagram flows are on the generation's own connection.
    case own(Conn, State) of
        true ->
            Now = erlang:monotonic_time(millisecond),
            {noreply, State#state{flows = trevrpc_datagram_flows:dispatch(Datagram, Now, Flows)}};
        false ->
            {noreply, State}
    end;
connection_event(Conn, send_ready, #state{send_waiters = Waiters} = State) ->
    _ = [Pid ! {trevrpc_stream, Ref, send_ready} || {Pid, Ref} <- maps:get(Conn, Waiters, [])],
    {noreply, State#state{send_waiters = maps:remove(Conn, Waiters)}};
connection_event(Conn, {Outcome, _Reason}, State) when Outcome =:= closed; Outcome =:= error ->
    case own(Conn, State) of
        true -> {noreply, lost(State)};
        false -> {noreply, extra_lost(Conn, State)}
    end;
connection_event(_Conn, _Event, State) ->
    {noreply, State}.

own(Conn, #state{transport = Transport}) ->
    Transport =/= undefined andalso trevrpc_transport:pid(Transport) =:= Conn.

%% The generation's own connection is gone: fail its calls and reconnect.
%% Its extras take no new calls and close after their last.
lost(#state{transport = Transport} = State0) ->
    Own = trevrpc_transport:pid(Transport),
    State1 = drain_extras(stop_extra_attempts(State0)),
    Connecting = notify((release_connection(State1))#state{status = connecting}),
    fail_streams(fun(Conn) -> Conn =:= Own end, Connecting),
    _ = [
        gen_server:reply(From, {error, trevrpc_status:new(unavailable, <<"connection closed">>)})
     || {From, _Owner, _Timer} <- Connecting#state.pending_opens
    ],
    Forgotten = forget_streams(Own, Connecting),
    retry((pending([], Forgotten))#state{
        send_waiters = maps:remove(Own, Forgotten#state.send_waiters),
        flows = trevrpc_datagram_flows:new(client, 0),
        stream_flows = #{},
        loads = maps:remove(Own, Forgotten#state.loads)
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

%% Tells the calls on the connections Which accepts that they closed.
fail_streams(Which, #state{streams = Streams} = State) ->
    _ = [
        Pid ! {trevrpc_stream, Ref, closed}
     || {Conn, _Id} := {Pid, Ref, _Monitor} <- Streams, Which(Conn)
    ],
    fail_send_waiters(Which, State).

%% Tells the writers waiting for the send queues of the connections Which
%% accepts that their streams closed.
fail_send_waiters(Which, #state{send_waiters = Waiters}) ->
    _ = [
        Pid ! {trevrpc_stream, Ref, closed}
     || Conn := ConnWaiters <- Waiters, Which(Conn), {Pid, Ref} <- ConnWaiters
    ],
    ok.

%% Forgets the streams of connection Conn, which is gone.
forget_streams(Conn, #state{streams = Streams} = State) ->
    lists:foldl(
        fun({Key, {_Pid, _Ref, Monitor}}, Acc) ->
            demonitor(Monitor, [flush]),
            forget(Key, Acc)
        end,
        State,
        [{Key, Value} || {C, _} = Key := Value <- Streams, C =:= Conn]
    ).

route(Conn, Id, Event, #state{streams = Streams} = State) ->
    _ =
        case {Streams, Event} of
            {#{{Conn, Id} := {Pid, Ref, _Monitor}}, _} ->
                Pid ! {trevrpc_stream, Ref, Event};
            {#{}, {data, _, _}} ->
                %% A stream no call reads: one whose caller is done, or
                %% exited before it registered the stream it opened.
                cancel_stream(conn_transport(Conn, State), Id);
            _ ->
                ok
        end,
    {noreply, State}.

%% Cancels both sides of a stream nobody uses.
cancel_stream(Transport, Id) ->
    _ = trevrpc_transport:reset_stream(Transport, Id, ?CANCELLED),
    _ = trevrpc_transport:stop_sending(Transport, Id, ?CANCELLED),
    ok.

%%% Streams

%% Opens a stream for a call, or holds the open: for stream credit, or for
%% the connection the pool dials because the call is streaming and found
%% its connection crowded. Wait says whether it may wait for that: `wait'
%% for an open that did not yet, which the open counts toward the pool's
%% call rate, `wait_counted' for one its caller counted, {waited, Timer}
%% for one held before, whose timer bounds its wait for all the connections
%% it waits for, or `no_wait'. Only `wait' counts the call: the others were
%% counted when the caller or the channel first picked for them.
open(From, Opener, Deadline, _Wait, #state{pending_opens = [_ | _]} = State) ->
    %% Opens wait for stream credit: this one waits behind them, even if
    %% credit came back and the channel has not heard of it yet.
    queue_open(From, Opener, Deadline, State);
open(From, {_Owner, Datagrams, Streaming} = Opener, Deadline, Wait, State0) ->
    %% The connection trevrpc_quic_pool:choose/5 picks, then the others, by
    %% their calls in flight, should the server's stream limit refuse it.
    Pool = pool(State0),
    #state{counters = Counters, limits = Limits} = State0,
    {Chosen, Grow, Crowded, Sample} =
        trevrpc_quic_pool:choose(Pool, Counters, Limits, Datagrams, Streaming, Wait =:= wait),
    State1 =
        case Sample of
            true -> sample_pool(State0);
            false -> State0
        end,
    State =
        case Grow andalso trevrpc_quic_pool:claim_growth(Counters) of
            true -> grow_pool(State1);
            false -> State1
        end,
    Candidates =
        case Datagrams of
            undefined ->
                Others = lists:sort([
                    {load(Slot, State), Slot, T}
                 || {Slot, T} <- Pool, T =/= Chosen
                ]),
                [Chosen | [T || {_, _, T} <- Others]];
            _ ->
                [Chosen]
        end,
    MayWait = Wait =/= no_wait andalso State#state.crowded_dial_wait > 0,
    case {Crowded andalso MayWait andalso growing_attempt(State), Wait} of
        {true, New} when New =:= wait; New =:= wait_counted ->
            Timer = erlang:start_timer(
                State#state.crowded_dial_wait, self(), {crowded_timeout, From}
            ),
            {noreply, hold_crowded(From, Opener, Deadline, Timer, State)};
        {true, {waited, Timer}} ->
            {noreply, hold_crowded(From, Opener, Deadline, Timer, State)};
        {false, {waited, Timer}} ->
            _ = erlang:cancel_timer(Timer, [{async, true}, {info, false}]),
            open_on(Candidates, From, Opener, Deadline, false, State);
        {false, _} ->
            open_on(Candidates, From, Opener, Deadline, false, State)
    end.

hold_crowded(From, Opener, Deadline, Timer, #state{crowded_opens = Held} = State) ->
    State#state{crowded_opens = Held ++ [{From, Opener, Deadline, Timer}]}.

open_on([Transport | Rest], From, Opener, Deadline, Limited, State0) ->
    case trevrpc_transport:open_stream(Transport) of
        {ok, Id} ->
            ok = trevrpc_transport:manual_flow_control(Transport, Id),
            {Info, State} = opened(trevrpc_transport:pid(Transport), Id, Opener, State0),
            {reply, {ok, Info}, State};
        {error, stream_limit} ->
            open_on(Rest, From, Opener, Deadline, true, State0);
        _ ->
            open_on(Rest, From, Opener, Deadline, Limited, State0)
    end;
open_on([], From, Opener, Deadline, true, State) ->
    queue_open(From, Opener, Deadline, State);
open_on([], _From, _Opener, _Deadline, false, State) ->
    {reply, {error, trevrpc_status:new(unavailable, <<"could not open a stream">>)}, State}.

%% Routes the events of stream `Id', open on connection Conn of the current
%% generation, to the process that opened it, and what that process needs
%% to use it.
opened(Conn, Id, {Owner, Datagrams, Streaming}, State0) ->
    Key = {Conn, Id},
    Ref = make_ref(),
    Monitor = monitor(process, Owner),
    {Flow, State} =
        case own(Conn, State0) of
            true -> flow(Key, Owner, Ref, Datagrams, State0);
            false -> {undefined, State0}
        end,
    Info = #{
        flow => Flow,
        transport => conn_transport(Conn, State),
        id => Id,
        ref => Ref,
        generation => State#state.generation,
        max_frame_size => State#state.max_frame_size,
        compression => State#state.compression,
        shared => State#state.shared,
        stream_budget => State#state.stream_budget,
        optimistic => State#state.optimistic,
        pool => {State#state.version, pool(State), {
            State#state.limits, State#state.crowded_dial_wait
        }},
        counters => State#state.counters,
        connection_metadata => State#state.connection_metadata,
        metadata_confirmed =>
            case State#state.connection_metadata of
                {SetId, _} -> maps:get(Conn, State#state.confirmed, 0) =:= SetId;
                undefined -> false
            end
    },
    Loaded = add_load(Conn, 1, State#state{
        streams = (State#state.streams)#{Key => {Owner, Ref, Monitor}},
        refs = (State#state.refs)#{Ref => Key},
        monitors = (State#state.monitors)#{Monitor => Key},
        used = used(Conn, State)
    }),
    {Info,
        case Streaming of
            true ->
                add_streams(Key, 1, Loaded#state{streaming = (Loaded#state.streaming)#{Key => true}});
            false ->
                Loaded
        end}.

%% Marks an extra the pool added as used since the last idle check.
used(Conn, #state{used = Used, extras = Extras, limits = Limits}) ->
    case Extras of
        #{Conn := #{slot := Slot}} ->
            case Slot > ?MIN_SLOT(Limits) andalso not is_map_key(Conn, Used) of
                true -> Used#{Conn => true};
                false -> Used
            end;
        #{} ->
            Used
    end.

%% Counts N more streaming calls in flight on stream Key's connection.
add_streams({Conn, _Id}, N, #state{counters = Counters} = State) ->
    _ =
        case slot(Conn, State) of
            undefined -> ok;
            Slot -> trevrpc_quic_pool:add_streams(Counters, Slot, N)
        end,
    State.

%% The stream of a call, by its ID and routing tag.
stream_key(Id, Ref, #state{refs = Refs}) ->
    case Refs of
        #{Ref := {_Conn, Id} = Key} -> {ok, Key};
        #{} -> error
    end.

%% Holds an open until stream credit comes back, or its deadline passes.
queue_open(From, Opener, Deadline, State) ->
    Timer =
        case Deadline of
            infinity ->
                make_ref();
            _ ->
                Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
                erlang:start_timer(Timeout, self(), {open_timeout, From})
        end,
    {noreply, pending(State#state.pending_opens ++ [{From, Opener, Timer}], State)}.

%% A datagram flow for stream Key, if its call asked for one and the
%% connection carries datagrams: native QUIC whose server takes them, which
%% is asked once per connection.
flow(_Key, _Owner, _Ref, undefined, State) ->
    {undefined, State};
flow(Key, Owner, Ref, Receives, #state{datagrams = undefined, transport = Transport} = State) ->
    flow(Key, Owner, Ref, Receives, State#state{
        datagrams = trevrpc_transport:datagram_max_size(Transport) > 0
    });
flow(_Key, _Owner, _Ref, _Receives, #state{datagrams = false} = State) ->
    {undefined, State};
flow(Key, Owner, Ref, Receives, #state{flows = Flows, stream_flows = StreamFlows} = State) ->
    {Flow, Credit, Allocated} = trevrpc_datagram_flows:allocate(Owner, Ref, Receives, Flows),
    {{Flow, Credit}, State#state{flows = Allocated, stream_flows = StreamFlows#{Key => Flow}}}.

%% Stream credit came back: open the waiting streams, in order, while it
%% lasts.
retry_opens(#state{pending_opens = []} = State) ->
    {noreply, State};
retry_opens(#state{pending_opens = [{From, Owner, Timer} | Rest]} = State) ->
    case open(From, Owner, infinity, no_wait, State#state{pending_opens = []}) of
        {reply, Reply, Opened} ->
            _ = erlang:cancel_timer(Timer, [{async, true}, {info, false}]),
            gen_server:reply(From, Reply),
            retry_opens(pending(Rest, Opened));
        {noreply, _StillFull} ->
            %% The open, refused, did not join the queue it came from.
            {noreply, pending(State#state.pending_opens, State)}
    end.

%% Sets the opens waiting for stream credit, and publishes their number.
pending(Pending, #state{counters = Counters} = State) ->
    trevrpc_quic_pool:set_queued(Counters, length(Pending)),
    State#state{pending_opens = Pending}.

%% Forgets stream Key, giving back what it held of the stream compression
%% budget, its datagram flow, and its place on its connection.
forget({Conn, _Id} = Key, #state{streams = Streams, reserved = Reserved} = State) ->
    {_Pid, Ref, Monitor} = maps:get(Key, Streams),
    trevrpc_zstd_stream:release(State#state.stream_budget, maps:get(Key, Reserved, 0)),
    {Flows, StreamFlows} =
        case maps:take(Key, State#state.stream_flows) of
            {Flow, Rest} -> {trevrpc_datagram_flows:release(Flow, State#state.flows), Rest};
            error -> {State#state.flows, State#state.stream_flows}
        end,
    Unstreamed =
        case maps:take(Key, State#state.streaming) of
            {true, Streaming} -> add_streams(Key, -1, State#state{streaming = Streaming});
            error -> State
        end,
    add_load(Conn, -1, Unstreamed#state{
        streams = maps:remove(Key, Streams),
        refs = maps:remove(Ref, State#state.refs),
        monitors = maps:remove(Monitor, State#state.monitors),
        reserved = maps:remove(Key, Reserved),
        flows = Flows,
        stream_flows = StreamFlows
    }).

%%% The pool of QUIC connections

%% The generation's connections, by number: its own, then its extras.
pool(#state{status = connecting}) ->
    [];
pool(#state{transport = Transport, extras = Extras}) ->
    [{1, Transport} | lists:sort([{Slot, T} || _ := #{slot := Slot, transport := T} <- Extras])].

%% The number of connection Conn in the pool: undefined for one that is not
%% in it, such as one that drains.
slot(Conn, #state{extras = Extras} = State) ->
    case {own(Conn, State), Extras} of
        {true, _} -> 1;
        {false, #{Conn := #{slot := Slot}}} -> Slot;
        {false, #{}} -> undefined
    end.

%% The connection Conn: the generation's own, an extra, or one that drains.
conn_transport(Conn, #state{extras = Extras, draining = Draining} = State) ->
    case {own(Conn, State), Extras, Draining} of
        {true, _, _} -> State#state.transport;
        {false, #{Conn := #{transport := Transport}}, _} -> Transport;
        {false, _, #{Conn := #{transport := Transport}}} -> Transport
    end.

load(Slot, #state{counters = Counters}) ->
    trevrpc_quic_pool:load(Counters, Slot).

%% Counts N more calls in flight on connection Conn; one that drains closes
%% after its last.
add_load(Conn, N, #state{loads = Loads, counters = Counters} = State) ->
    Load = maps:get(Conn, Loads, 0) + N,
    _ =
        case slot(Conn, State) of
            undefined -> ok;
            Slot -> trevrpc_quic_pool:add_load(Counters, Slot, N)
        end,
    case State#state.draining of
        #{Conn := Extra} when Load =< 0 ->
            %% Its last call ended; a writer still waiting for its send
            %% queue has no call left on it.
            fail_send_waiters(fun(C) -> C =:= Conn end, State),
            close_extra(Extra, State#state{
                draining = maps:remove(Conn, State#state.draining),
                loads = maps:remove(Conn, Loads),
                send_waiters = maps:remove(Conn, State#state.send_waiters)
            });
        _ ->
            State#state{loads = Loads#{Conn => Load}}
    end.

%% Publishes that the pool changed: callers that remember it ask again. A
%% connection that left the pool forgets the set its server confirmed: a new
%% one starts with nothing confirmed.
bump(#state{version = Version, counters = Counters, confirmed = Confirmed} = State) ->
    trevrpc_quic_pool:set_version(Counters, Version + 1),
    State#state{
        version = Version + 1,
        confirmed = maps:filter(fun(Conn, _) -> slot(Conn, State) =/= undefined end, Confirmed)
    }.

%% Makes `Metadata' the channel's connection metadata, as a set with a new
%% ID, which no connection confirmed yet; empty metadata removes it. IDs
%% wrap after 2^32 - 1 sets, past 0, which means none.
replace_metadata(Metadata, State) when map_size(Metadata) =:= 0 ->
    State#state{connection_metadata = undefined, confirmed = #{}};
replace_metadata(Metadata, #state{metadata_ids = Ids} = State) ->
    Id = Ids rem 16#FFFFFFFF + 1,
    State#state{connection_metadata = {Id, Metadata}, metadata_ids = Id, confirmed = #{}}.

%% A generation starts with its own connection alone, and over native QUIC
%% dials the extras of its minimum, and measures its round trip.
start_pool(Kind, #state{quic_limits = QuicLimits, counters = Counters} = State) ->
    trevrpc_quic_pool:reset(Counters, trevrpc_quic_pool:max(QuicLimits)),
    Limits =
        case Kind of
            quic -> QuicLimits;
            qmux -> trevrpc_quic_pool:single()
        end,
    Bumped = bump(State#state{
        limits = Limits,
        used = #{},
        idle_check = false,
        sampled_bytes = #{},
        sampled_at = undefined
    }),
    Dialled =
        case Kind of
            quic ->
                Slots = lists:seq(2, trevrpc_quic_pool:min(Limits)),
                sample_pool(lists:foldl(fun dial_extra/2, Bumped, Slots));
            qmux ->
                Bumped
        end,
    settle(Dialled).

%% The channel is ready for wait_until_ready/2 once a new generation dials
%% no extra (each joined or failed) and measured its round trips, which
%% crowded streams need (see trevrpc_quic_pool). A failed extra does not
%% hold it up, and is dialled again, though a redial that starts while
%% another extra is still being dialled is waited for too, a connect
%% timeout at most.
settle(#state{pool_settling = true, extra_attempts = Attempts, sampling = false} = State) when
    map_size(Attempts) =:= 0
->
    _ = [gen_server:reply(From, ok) || {From, _Timer} <- State#state.ready_waiters],
    State#state{pool_settling = false, ready_waiters = [], held_events = #{}};
settle(#state{extra_attempts = Attempts} = State) when map_size(Attempts) =:= 0 ->
    State#state{held_events = #{}};
settle(State) ->
    State.

%% Dials the extra numbered Slot, unless it is up or being dialled.
dial_extra(Slot, #state{extras = Extras, extra_attempts = Attempts} = State) ->
    Taken = [S || _ := #{slot := S} <- Extras] ++ [S || _ := #{slot := S} <- Attempts],
    case lists:member(Slot, Taken) of
        true ->
            State;
        false ->
            Ref = make_ref(),
            Parent = quic_connect(extra_result, Ref, State),
            erlang:start_timer(State#state.connect_timeout, self(), {extra_timeout, Ref}),
            Attempt = #{
                slot => Slot,
                generation => State#state.generation,
                transport => undefined,
                parent => Parent
            },
            State#state{extra_attempts = Attempts#{Ref => Attempt}}
    end.

extra_result(Ref, Result, #state{extra_attempts = Attempts} = State) ->
    case {Attempts, Result} of
        {#{Ref := #{transport := undefined} = Attempt}, {ok, Transport}} ->
            %% Its connection's events that came first go now.
            Conn = trevrpc_transport:pid(Transport),
            {Held, Rest} =
                case maps:take(Conn, State#state.held_events) of
                    {Events, Others} -> {Events, Others};
                    error -> {[], State#state.held_events}
                end,
            Known = State#state{
                extra_attempts = Attempts#{Ref := Attempt#{transport := Transport}},
                held_events = Rest
            },
            lists:foldl(fun(Event, Acc) -> extra_attempt_event(Conn, Event, Acc) end, Known, Held);
        {#{Ref := #{transport := Adopted}}, {ok, Transport}} when Adopted =/= Transport ->
            retire(Transport, State);
        {#{Ref := _}, {ok, _}} ->
            State;
        {#{Ref := _}, {error, _}} ->
            extra_failed(Ref, State);
        {#{}, {ok, Transport}} ->
            %% It joined the pool from its events already, or its attempt
            %% was given up.
            case slot(trevrpc_transport:pid(Transport), State) of
                undefined -> retire(Transport, State);
                _ -> State
            end;
        {#{}, {error, _}} ->
            State
    end.

%% An event of a connection an extra's attempt dialled. One that comes
%% before the attempt's result names the connection is held until then:
%% the extras are dialled in parallel, so which attempt it belongs to is
%% not known before.
extra_attempt_event(Conn, {Outcome, _} = Event, #state{extra_attempts = Attempts} = State) when
    Outcome =:= connected; Outcome =:= closed; Outcome =:= error
->
    Known = [Ref || Ref := #{transport := {quic, C}} <- Attempts, C =:= Conn],
    case Known of
        [Ref | _] ->
            extra_attempt_outcome(Ref, Event, State);
        [] when map_size(Attempts) > 0 ->
            Held = State#state.held_events,
            State#state{held_events = Held#{Conn => maps:get(Conn, Held, []) ++ [Event]}};
        [] ->
            State
    end;
extra_attempt_event(_Conn, _Event, State) ->
    State.

extra_attempt_outcome(Ref, {connected, Info}, State) ->
    case maps:get(alpn, Info, undefined) =:= trevrpc:alpn() of
        true -> join(Ref, State);
        false -> extra_failed(Ref, State)
    end;
extra_attempt_outcome(Ref, _Failed, State) ->
    extra_failed(Ref, State).

%% The extra an attempt dialled joins the pool, and takes calls from now on.
join(Ref, #state{extra_attempts = Attempts} = State) ->
    {#{slot := Slot, transport := Transport, parent := Parent}, Rest} = maps:take(Ref, Attempts),
    Conn = trevrpc_transport:pid(Transport),
    Extra = #{
        transport => Transport, slot => Slot, monitor => monitor(process, Conn), parent => Parent
    },
    Counters = State#state.counters,
    trevrpc_quic_pool:reset_slot(Counters, Slot),
    %% Its round trip, until measured, is the generation's own connection's:
    %% the same path to the same server.
    trevrpc_quic_pool:set_rtt(Counters, Slot, trevrpc_quic_pool:rtt(Counters, 1)),
    Joined = bump(State#state{
        extra_attempts = Rest,
        extras = (State#state.extras)#{Conn => Extra},
        loads = (State#state.loads)#{Conn => 0},
        extra_delays = maps:remove(Slot, State#state.extra_delays)
    }),
    Grown =
        case Slot > ?MIN_SLOT(State#state.limits) of
            %% It counts as used until the second idle check, so it takes
            %% calls before it can close.
            true ->
                arm_idle_check(
                    growth_ended(Joined#state{used = (Joined#state.used)#{Conn => true}})
                );
            false ->
                Joined
        end,
    settle(sample_pool(Grown)).

%% An extra's attempt failed: it is dialled again after a backoff.
extra_failed(Ref, #state{extra_attempts = Attempts} = State) ->
    {#{slot := Slot, transport := Transport, parent := Parent}, Rest} = maps:take(Ref, Attempts),
    Parent ! stop,
    Retired = retire(Transport, State#state{extra_attempts = Rest}),
    case Slot > ?MIN_SLOT(State#state.limits) of
        true ->
            %% The pool grows again after a pause.
            erlang:start_timer(?GROW_RETRY, self(), {grow_retry, State#state.generation}),
            settle(open_crowded(Retired));
        false ->
            settle(redial(Slot, Retired))
    end.

%% Dials the extra numbered Slot again after its backoff, which doubles each
%% time, as the channel's reconnects do.
redial(Slot, #state{extra_delays = Delays, generation = Generation} = State) ->
    Delay = maps:get(Slot, Delays, ?INITIAL_DELAY),
    Jittered = round(Delay * (0.8 + 0.4 * rand:uniform())),
    erlang:start_timer(Jittered, self(), {redial, Slot, Generation}),
    State#state{extra_delays = Delays#{Slot => min(?MAX_DELAY, Delay * 2)}}.

%% An extra, or one that drains, is gone: its own calls fail, and an extra
%% of the current generation is dialled again.
extra_lost(Conn, #state{extras = Extras, draining = Draining} = State0) ->
    {Extra, Current} =
        case Extras of
            #{Conn := E} -> {E, true};
            #{} -> {maps:get(Conn, Draining), false}
        end,
    State1 = State0#state{
        extras = maps:remove(Conn, Extras), draining = maps:remove(Conn, Draining)
    },
    fail_streams(fun(C) -> C =:= Conn end, State1),
    State2 = forget_streams(Conn, State1),
    State3 = close_extra(Extra, State2#state{
        send_waiters = maps:remove(Conn, State2#state.send_waiters),
        loads = maps:remove(Conn, State2#state.loads)
    }),
    #{slot := Slot} = Extra,
    case Current andalso Slot =< ?MIN_SLOT(State3#state.limits) of
        true ->
            trevrpc_quic_pool:reset_slot(State3#state.counters, Slot),
            redial(Slot, bump(State3));
        false when Current ->
            %% One the pool added is not dialled again until the pool grows.
            trevrpc_quic_pool:reset_slot(State3#state.counters, Slot),
            bump(State3#state{used = maps:remove(Conn, State3#state.used)});
        false ->
            State3
    end.

%% The generation ended: its extras take no new calls, and close after
%% their last.
drain_extras(#state{extras = Extras, loads = Loads} = State) ->
    Drained = maps:fold(
        fun(Conn, Extra, Acc) ->
            case maps:get(Conn, Loads, 0) of
                0 ->
                    close_extra(Extra, Acc#state{loads = maps:remove(Conn, Acc#state.loads)});
                _ ->
                    Acc#state{draining = (Acc#state.draining)#{Conn => Extra}}
            end
        end,
        State#state{extras = #{}},
        Extras
    ),
    bump(Drained).

stop_extra_attempts(#state{extra_attempts = Attempts} = State) ->
    maps:fold(
        fun(_Ref, #{transport := Transport, parent := Parent}, Acc) ->
            Parent ! stop,
            retire(Transport, Acc)
        end,
        fail_crowded_opens(State#state{
            extra_attempts = #{}, held_events = #{}, pool_settling = false, used = #{}
        }),
        Attempts
    ).

%%% Growing the pool

%% Dials another connection, numbered with the lowest number free above the
%% pool's minimum, unless the pool has all its maximum. The caller marked
%% the pool growing (trevrpc_quic_pool:claim_growth/1).
grow_pool(#state{status = ready, transport = {quic, _}, limits = Limits} = State) ->
    Taken = [S || {S, _} <- pool(State)] ++ [S || _ := #{slot := S} <- State#state.extra_attempts],
    Free = lists:seq(?MIN_SLOT(Limits) + 1, trevrpc_quic_pool:max(Limits)) -- Taken,
    case Free of
        [Slot | _] -> dial_extra(Slot, State);
        [] -> growth_ended(State)
    end;
grow_pool(State) ->
    growth_ended(State).

%% Whether the pool is dialling a connection to grow.
growing_attempt(#state{extra_attempts = Attempts, limits = Limits}) ->
    Min = ?MIN_SLOT(Limits),
    lists:any(fun(#{slot := Slot}) -> Slot > Min end, maps:values(Attempts)).

growth_ended(#state{counters = Counters} = State) ->
    trevrpc_quic_pool:end_growth(Counters),
    open_crowded(State).

%% The pool's dial ended: the opens held for it open now, unless they find
%% their connection crowded still and the pool dials another, which they
%% wait for in turn, within what is left of their wait: a burst of streams
%% may need several.
open_crowded(#state{crowded_opens = Held} = State) ->
    lists:foldl(
        fun({From, Opener, Deadline, Timer}, Acc) ->
            case open(From, Opener, Deadline, {waited, Timer}, Acc) of
                {reply, Reply, Next} ->
                    gen_server:reply(From, Reply),
                    Next;
                {noreply, Next} ->
                    Next
            end
        end,
        State#state{crowded_opens = []},
        Held
    ).

%% The generation ended with opens held for its pool's dial: they fail, as
%% the opens held for stream credit do.
fail_crowded_opens(#state{crowded_opens = Held} = State) ->
    _ = [
        begin
            _ = erlang:cancel_timer(Timer, [{async, true}, {info, false}]),
            gen_server:reply(
                From, {error, trevrpc_status:new(unavailable, <<"connection closed">>)}
            )
        end
     || {From, _Opener, _Deadline, Timer} <- Held
    ],
    State#state{crowded_opens = []}.

arm_idle_check(#state{idle_check = false} = State) ->
    erlang:start_timer(
        State#state.pool_idle_timeout, self(), {idle_check, State#state.generation}
    ),
    State#state{idle_check = true};
arm_idle_check(State) ->
    State.

%% Closes each extra the pool added that took no call since the last check
%% and has none in flight, and checks again while any is left.
idle_check(#state{extras = Extras, used = Used, limits = Limits} = State0) ->
    Min = ?MIN_SLOT(Limits),
    Idle = [
        {Conn, Extra}
     || Conn := #{slot := Slot} = Extra <- Extras,
        Slot > Min,
        not is_map_key(Conn, Used),
        maps:get(Conn, State0#state.loads, 0) =:= 0
    ],
    State1 = lists:foldl(
        fun({Conn, Extra}, Acc) ->
            close_extra(Extra, Acc#state{
                extras = maps:remove(Conn, Acc#state.extras),
                loads = maps:remove(Conn, Acc#state.loads),
                send_waiters = maps:remove(Conn, Acc#state.send_waiters)
            })
        end,
        State0#state{used = #{}},
        Idle
    ),
    State2 =
        case Idle of
            [] -> State1;
            _ -> bump(State1)
        end,
    case [S || _ := #{slot := S} <- State2#state.extras, S > Min] of
        [] -> State2;
        _ -> arm_idle_check(State2)
    end.

%% Measures the bytes the pool's connections moved and their round trips,
%% unless a measure is under way already, or the pool cannot grow, which
%% needs no measure. erlang_quic answers from each connection's process,
%% with no timeout, so the measure runs in a process of its own, which the
%% channel gives up on after ?SAMPLE_TIMEOUT.
sample_pool(
    #state{sampling = false, status = ready, transport = {quic, _}, limits = Limits} = State
) ->
    case trevrpc_quic_pool:max(Limits) > trevrpc_quic_pool:min(Limits) of
        true ->
            Channel = self(),
            Ref = make_ref(),
            Generation = State#state.generation,
            Conns = [trevrpc_transport:pid(T) || {_, T} <- pool(State)],
            Pid = spawn(fun() ->
                At = erlang:monotonic_time(millisecond),
                Channel ! {pool_sample, Ref, Generation, At, [measure(Conn) || Conn <- Conns]}
            end),
            Timer = erlang:start_timer(?SAMPLE_TIMEOUT, self(), {sample_timeout, Ref}),
            State#state{sampling = {Ref, Pid, Timer}};
        false ->
            State
    end;
sample_pool(State) ->
    State.

measure(Conn) ->
    Bytes =
        try quic:get_stats(Conn) of
            {ok, #{data_sent := Sent, data_received := Received}} -> Sent + Received;
            _ -> undefined
        catch
            _:_ -> undefined
        end,
    %% In microseconds, of whole milliseconds, as erlang_quic measures them.
    Rtt =
        try quic:get_path_stats(Conn) of
            {ok, #{srtt := Srtt}} -> Srtt;
            _ -> undefined
        catch
            _:_ -> undefined
        end,
    {Conn, Bytes, Rtt}.

%% Publishes the round trips of the connections still in the pool, and the
%% bytes per second they moved since the last measure.
sampled(Generation, At, Samples, #state{generation = Generation, status = ready} = State) ->
    Counters = State#state.counters,
    Last = State#state.sampled_bytes,
    _ = [
        trevrpc_quic_pool:set_rtt(Counters, Slot, Rtt)
     || {Conn, _, Rtt} <- Samples, is_integer(Rtt), Slot <- [slot(Conn, State)], Slot =/= undefined
    ],
    Bytes = #{Conn => B || {Conn, B, _} <- Samples, is_integer(B)},
    Moved = lists:sum([
        max(0, B - map_get(Conn, Last))
     || Conn := B <- Bytes, is_map_key(Conn, Last)
    ]),
    _ =
        case State#state.sampled_at of
            undefined ->
                ok;
            Before when At > Before ->
                trevrpc_quic_pool:set_byte_rate(Counters, Moved * 1000 div (At - Before));
            _ ->
                ok
        end,
    State#state{sampled_bytes = Bytes, sampled_at = At};
sampled(_Generation, _At, _Samples, #state{sampled_at = undefined} = State) ->
    %% A measure of an earlier generation held up the current one's first.
    sample_pool(State);
sampled(_Generation, _At, _Samples, State) ->
    State.

close_extra(#{transport := Transport, monitor := Monitor, parent := Parent}, State) ->
    demonitor(Monitor, [flush]),
    Parent ! stop,
    retire(Transport, State).

closed_status() ->
    trevrpc_status:new(unavailable, <<"channel is closed">>).

now_ms() -> erlang:monotonic_time(millisecond).
