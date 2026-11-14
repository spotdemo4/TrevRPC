%% @doc A TrevRPC server over native QUIC (ALPN `trevrpc/1'), with QMux over
%% TLS on a TCP listener for clients that cannot reach UDP.
%%
%% ```
%% Routes = #{
%%     <<"/example.greeter.Greeter/SayHello">> => #{
%%         kind => unary,
%%         handler => fun(Request, _Call) -> {ok, reply(Request)} end
%%     }
%% },
%% {ok, Server} = trevrpc_server:start_link(#{
%%     port => 4433, certfile => "cert.pem", keyfile => "key.pem", routes => Routes
%% }),
%% {ok, Port} = trevrpc_server:port(Server).
%% '''
%%
%% With `http => true', the server also serves the HTTP mapping, for
%% clients such as Cronet: a `POST' to the HTTP path (`http_path',
%% `/trevrpc' by default) over HTTP/3 on the UDP socket and over HTTP/2 on
%% the TCP listener, for the requests that `http_admission' accepts (all of
%% them by default).
%%
%% With `webtransport => true', the TCP listener also serves browsers: QMux
%% over WebSocket at the HTTP path, for the sessions that
%% `webtransport_admission' accepts (see {@link trevrpc_admission}).
%% WebTransport itself is not served yet.
%%
%% Messages are opaque binaries here: a handler decodes its request and
%% encodes its response, usually through generated code. See
%% {@link trevrpc_call} for the handler API.
%%
%% Every connection is owned by a `trevrpc_server_conn' process, which
%% reads each stream's REQUEST, checks it and runs its handler in a call
%% process of its own. That process can be stopped at any moment, which is
%% how deadlines, cancellation and shutdown end a handler, whether or not it
%% cooperates.
-module(trevrpc_server).

-behaviour(gen_server).

-export([start_link/1, start/1, port/1, tcp_port/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([options/0, routes/0, route/0, handler/0, result/0, config/0]).

%% A handler's outcome. `ok' (streaming responses) and `{ok, Response}'
%% (single responses) end the call with OK; `{error, Status}' with that
%% status.
-type result() :: ok | {ok, iodata()} | {error, trevrpc_status:status()}.

%% Unary and server-streaming handlers get the request message;
%% client-streaming and bidirectional ones read requests from the call.
-type handler() ::
    fun((binary(), trevrpc_call:call()) -> result())
    | fun((trevrpc_call:call()) -> result()).

%% `request_type' and `response_type' are the protobuf full names of the
%% method's messages, which pick their compression dictionaries, and
%% `compression' how its messages may be compressed without one (see
%% {@link trevrpc_zstd:directions()}).
-type route() :: #{
    kind := trevrpc_wire:kind(),
    handler := handler(),
    request_type => binary(),
    response_type => binary(),
    compression => trevrpc_zstd:directions()
}.

%% Method paths (`/package.Service/Method') to their routes.
-type routes() :: #{binary() => route()}.

-type options() :: #{
    routes := routes(),
    port => inet:port_number(),
    ip => inet:ip_address(),
    cert => public_key:der_encoded(),
    cert_chain => [public_key:der_encoded()],
    key => term(),
    certfile => file:filename_all(),
    keyfile => file:filename_all(),
    tcp => boolean(),
    tcp_port => inet:port_number(),
    tls_handshake_timeout => pos_integer(),
    max_tls_handshakes => pos_integer(),
    max_frame_size => pos_integer(),
    max_connections => pos_integer(),
    max_streams_per_connection => pos_integer(),
    max_concurrent_requests => pos_integer(),
    max_stream_messages => pos_integer() | infinity,
    max_stream_body_size => pos_integer() | infinity,
    initial_request_timeout => timeout(),
    connection_idle_timeout => pos_integer() | infinity,
    graceful_shutdown_timeout => non_neg_integer(),
    compression => trevrpc_zstd:settings(),
    stream_compression_memory => non_neg_integer(),
    http => boolean(),
    http_admission => trevrpc_admission:admission(),
    webtransport => boolean(),
    webtransport_admission => trevrpc_admission:admission(),
    http_path => binary()
}.

%% What each connection process needs, shared by all of them.
-type config() :: #{
    server := pid(),
    routes := routes(),
    max_frame_size := pos_integer(),
    max_streams_per_connection := pos_integer(),
    max_concurrent_requests := pos_integer(),
    max_stream_messages := pos_integer() | infinity,
    max_stream_body_size := pos_integer() | infinity,
    initial_request_timeout := timeout(),
    graceful_shutdown_timeout := non_neg_integer(),
    %% Index 1: requests running on the whole server.
    requests := counters:counters_ref(),
    compression := trevrpc_zstd:config(),
    %% What stream compression contexts may hold on the whole server.
    stream_budget := trevrpc_zstd_stream:budget() | undefined,
    %% The options of QMux sessions.
    qmux := trevrpc_qmux:options(),
    %% The HTTP mapping's options, with the idle timeout of HTTP/2
    %% connections, unless HTTP is off.
    http := (trevrpc_http:options() | #{idle_timeout := timeout()}) | undefined,
    %% Over HTTP, the owner of each QUIC connection until its ALPN hook ran.
    owners := ets:tid() | undefined
}.

-record(state, {
    name :: atom(),
    config :: config(),
    %% Index 1: connections; index 2: 1 once shutdown started.
    counters :: counters:counters_ref(),
    connections = #{} :: #{reference() => pid()},
    %% The TCP listener, unless TCP is off.
    tcp :: pid() | undefined
}).

-define(CONNECTIONS, 1).
-define(STOPPING, 2).
-define(TLS_HANDSHAKE_TIMEOUT, 10000).
-define(STREAM_COMPRESSION_MEMORY, 268435456).
-define(HTTP_PATH, <<"/trevrpc">>).
-define(H3, <<"h3">>).

%% @doc Starts a server linked to the caller, listening once it returns.
-spec start_link(options()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

%% @doc Starts a server, listening once it returns.
-spec start(options()) -> {ok, pid()} | ignore | {error, term()}.
start(Opts) ->
    gen_server:start(?MODULE, Opts, []).

%% @doc The UDP port the server listens on.
-spec port(pid()) -> {ok, inet:port_number()} | {error, term()}.
port(Server) ->
    gen_server:call(Server, port).

%% @doc The TCP port the server listens on for QMux over TLS.
-spec tcp_port(pid()) -> {ok, inet:port_number()} | {error, term()}.
tcp_port(Server) ->
    gen_server:call(Server, tcp_port).

%% @doc Shuts the server down gracefully: it stops taking connections and
%% calls, cancels the calls in flight, gives their handlers up to the
%% graceful shutdown timeout to exit, then closes every connection.
-spec stop(pid()) -> ok.
stop(Server) ->
    gen_server:call(Server, stop, infinity).

%%% gen_server

%% A failed start returns its error without taking a linked caller down.
-spec init(options()) -> {ok, #state{}} | {error, term()}.
init(Opts) ->
    process_flag(trap_exit, true),
    _ = application:ensure_all_started(quic),
    _ = application:ensure_all_started(ssl),
    _ = application:ensure_all_started(h1),
    _ = application:ensure_all_started(h2),
    maybe
        {ok, Credentials} ?= trevrpc_quic:credentials(Opts),
        {ok, Compression} ?= compression(Opts),
        ok ?= check_routes(maps:get(routes, Opts)),
        ok ?= check_browsers(Opts),
        listen(Opts, Credentials, Compression)
    else
        {error, Reason} -> {error, Reason}
    end.

compression(Opts) ->
    case trevrpc_zstd:config(maps:get(compression, Opts, #{})) of
        {ok, Compression} -> {ok, Compression};
        {error, Message} -> {error, {compression, Message}}
    end.

%% A route's compression options are checked once, so that a mistake fails
%% the start rather than every call.
check_routes(Routes) ->
    Invalid = [
        {route, Method, Reason}
     || Method := Route <- Routes,
        Reason <- route_errors(Route)
    ],
    case Invalid of
        [] -> ok;
        [Error | _] -> {error, Error}
    end.

check_browsers(Opts) ->
    case Opts of
        #{http := Enabled} when not is_boolean(Enabled) ->
            {error, {invalid_option, http}};
        #{http_admission := Admission} when not is_function(Admission, 1) ->
            {error, {invalid_option, http_admission}};
        #{webtransport := Enabled} when not is_boolean(Enabled) ->
            {error, {invalid_option, webtransport}};
        #{webtransport_admission := Admission} when not is_function(Admission, 1) ->
            {error, {invalid_option, webtransport_admission}};
        #{http_path := <<"/", _/binary>>} ->
            ok;
        #{http_path := _} ->
            {error, {invalid_option, http_path}};
        #{} ->
            ok
    end.

route_errors(Route) ->
    Directions =
        try trevrpc_zstd:directions(maps:get(compression, Route, none)) of
            _ -> []
        catch
            error:_ -> [{invalid_compression, maps:get(compression, Route)}]
        end,
    Types = [
        {invalid_type, Key}
     || Key <- [request_type, response_type],
        not is_binary(maps:get(Key, Route, <<>>))
    ],
    Directions ++ Types.

listen(Opts, Credentials, Compression) ->
    Name = list_to_atom("trevrpc_server_" ++ integer_to_list(erlang:unique_integer([positive]))),
    Counters = counters:new(2, [write_concurrency]),
    Config = config(Opts, Compression),
    MaxConnections = maps:get(max_connections, Opts, 256),
    Streams = maps:get(max_streams_per_connection, Config),
    Start = fun(Conn) ->
        admit(
            fun() -> trevrpc_server_conn:start(Conn, Config) end, Config, Counters, MaxConnections
        )
    end,
    QuicOpts0 = maps:merge(
        trevrpc_quic:transport_opts(
            maps:get(max_frame_size, Config),
            %% Stream credit comes back to a client about two round trips after
            %% it opened the stream, so QUIC allows four times the limit, which
            %% the server enforces itself.
            4 * Streams,
            maps:get(connection_idle_timeout, Opts, 30000)
        ),
        Credentials
    ),
    QuicOpts = maps:merge(
        QuicOpts0#{extra_socket_opts => [{ip, maps:get(ip, Opts, {0, 0, 0, 0})}]},
        quic_handlers(Start, Config)
    ),
    case quic:start_server(Name, maps:get(port, Opts, 0), QuicOpts) of
        {ok, _} ->
            {ok, UdpPort} = quic:get_server_port(Name),
            State = #state{name = Name, config = alt_svc(Config, UdpPort), counters = Counters},
            case listen_tcp(Opts, Credentials, State, MaxConnections) of
                {ok, Tcp} ->
                    {ok, State#state{tcp = Tcp}};
                {error, Reason} ->
                    _ = quic:stop_server(Name),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% Who owns each QUIC connection: the process `Start' starts for it. With
%% HTTP, a connection that negotiated `h3' is HTTP/3, whose `quic_h3'
%% process the owner hands the connection to in the ALPN hook, which runs
%% in the connection's process; a table, where each owner registers itself,
%% tells the hook the owner.
quic_handlers(Start, #{owners := undefined}) ->
    #{connection_handler => Start};
quic_handlers(Start, #{owners := Owners}) ->
    #{
        alpn => [trevrpc:alpn(), ?H3],
        %% The control and QPACK streams of HTTP/3, and room for the
        %% unidirectional streams clients send to grease the protocol. They
        %% apply to native connections too, which stop each unidirectional
        %% stream and drop what it carried.
        max_streams_uni => 8,
        max_stream_data_uni => 16384,
        %% The owner registers itself in the table as it starts.
        connection_handler => Start,
        alpn_handler => fun(Conn, Alpn) ->
            case ets:take(Owners, Conn) of
                [{_, Owner}] when Alpn =:= ?H3 -> serve_h3(Conn, Owner);
                [{_, Owner}] -> {ok, Owner};
                [] -> {error, no_owner}
            end
        end
    }.

%% Starts HTTP/3 on the QUIC connection `Conn', whose events go to `Owner'
%% from now on.
serve_h3(Conn, Owner) ->
    maybe
        %% No handler: the owner answers each request.
        {ok, H3} ?= quic_h3:accept(Conn, #{owner => Owner, handler => undefined}),
        ok ?= trevrpc_server_conn:serve_h3(Owner, H3, Conn),
        {ok, H3}
    end.

%% HTTP/2 responses advertise HTTP/3 on the UDP socket.
alt_svc(#{http := undefined} = Config, _UdpPort) ->
    Config;
alt_svc(#{http := Http} = Config, UdpPort) ->
    AltSvc = iolist_to_binary(io_lib:format("h3=\":~b\"; ma=86400", [UdpPort])),
    Config#{http := Http#{alt_svc => AltSvc}}.

%% The TCP listener for QMux over TLS, by default on the UDP socket's port
%% number, or any free port if that one is taken and none was asked for.
listen_tcp(#{tcp := false}, _Credentials, _State, _MaxConnections) ->
    {ok, undefined};
listen_tcp(
    Opts, Credentials, #state{name = Name, config = Config, counters = Counters}, MaxConnections
) ->
    Qmux = fun(Session) ->
        fun(Socket) ->
            case
                admit(
                    fun() -> trevrpc_server_conn:start_qmux(Socket, Session, Config) end,
                    Config,
                    Counters,
                    MaxConnections
                )
            of
                {ok, _Pid} -> ok;
                {error, _} = Error -> Error
            end
        end
    end,
    Handshake = maps:get(tls_handshake_timeout, Opts, ?TLS_HANDSHAKE_TIMEOUT),
    Accept0 =
        case maps:get(http, Config) of
            undefined ->
                #{qmux => Qmux(#{})};
            _ ->
                H2 = fun(Socket) ->
                    case
                        admit(
                            fun() -> trevrpc_server_conn:start_h2(Socket, Config) end,
                            Config,
                            Counters,
                            MaxConnections
                        )
                    of
                        {ok, _Pid} -> ok;
                        {error, _} = Error -> Error
                    end
                end,
                #{qmux => Qmux(#{}), h2 => H2}
        end,
    Accept =
        case maps:get(webtransport, Opts, false) of
            true ->
                Http = #{
                    path => maps:get(http_path, Opts, ?HTTP_PATH),
                    admission => maps:get(
                        webtransport_admission, Opts, fun trevrpc_admission:no_origin/1
                    ),
                    timeout => Handshake,
                    room => fun() -> room(Counters, MaxConnections) end
                },
                WebSocket = fun(Socket, Received) ->
                    (Qmux(#{framing => websocket, received => Received}))(Socket)
                end,
                Accept0#{http1 => fun(Socket) -> trevrpc_http1:serve(Socket, Http, WebSocket) end};
            false ->
                Accept0
        end,
    {ok, UdpPort} = quic:get_server_port(Name),
    Listen = #{
        ip => maps:get(ip, Opts, {0, 0, 0, 0}),
        port => maps:get(tcp_port, Opts, UdpPort),
        handshake_timeout => Handshake,
        max_handshakes => maps:get(max_tls_handshakes, Opts, 256)
    },
    case trevrpc_tcp_listener:start_link(Listen, Credentials, Accept) of
        {error, eaddrinuse} when not is_map_key(tcp_port, Opts) ->
            trevrpc_tcp_listener:start_link(Listen#{port => 0}, Credentials, Accept);
        Result ->
            Result
    end.

config(Opts, Compression) ->
    #{
        compression => Compression,
        stream_budget => trevrpc_zstd_stream:budget(
            maps:get(stream_compression_memory, Opts, ?STREAM_COMPRESSION_MEMORY)
        ),
        server => self(),
        routes => maps:get(routes, Opts),
        max_frame_size => maps:get(max_frame_size, Opts, trevrpc:default_max_frame_size()),
        max_streams_per_connection => maps:get(max_streams_per_connection, Opts, 64),
        max_concurrent_requests => maps:get(max_concurrent_requests, Opts, 1024),
        max_stream_messages => maps:get(max_stream_messages, Opts, infinity),
        max_stream_body_size => maps:get(max_stream_body_size, Opts, infinity),
        initial_request_timeout => maps:get(initial_request_timeout, Opts, 10000),
        graceful_shutdown_timeout => maps:get(graceful_shutdown_timeout, Opts, 30000),
        requests => counters:new(1, [write_concurrency]),
        qmux => qmux_options(Opts),
        http => http_options(Opts),
        owners =>
            case Opts of
                #{http := true} ->
                    ets:new(trevrpc_owners, [public, set, {write_concurrency, true}]);
                #{} ->
                    undefined
            end
    }.

http_options(#{http := true} = Opts) ->
    #{
        path => maps:get(http_path, Opts, ?HTTP_PATH),
        admission => maps:get(http_admission, Opts, undefined),
        idle_timeout => maps:get(connection_idle_timeout, Opts, 30000)
    };
http_options(#{}) ->
    undefined.

%% QMux sessions get the windows and limits of QUIC connections, and
%% keep-alives at half the idle timeout.
qmux_options(Opts) ->
    Streams = 4 * maps:get(max_streams_per_connection, Opts, 64),
    Idle = maps:get(connection_idle_timeout, Opts, 30000),
    #{
        %% The QMux handshake follows the TLS one under the same bound, and a
        %% client that stops reading is lost after the idle timeout.
        handshake_timeout => maps:get(tls_handshake_timeout, Opts, ?TLS_HANDSHAKE_TIMEOUT),
        send_timeout =>
            case Idle of
                infinity -> 30000;
                _ -> Idle
            end,
        parameters => trevrpc_quic:qmux_parameters(
            maps:get(max_frame_size, Opts, trevrpc:default_max_frame_size()), Streams, Streams, Idle
        ),
        keep_alive =>
            case Idle of
                infinity -> infinity;
                _ -> max(1, Idle div 2)
            end
    }.

%% Runs for every new connection, in the QUIC listener or in a TLS
%% handshake's process: refuses it when the server is full or stopping,
%% otherwise starts the process that will own it.
admit(Start, #{server := Server}, Counters, MaxConnections) ->
    case room(Counters, MaxConnections) of
        ok ->
            counters:add(Counters, ?CONNECTIONS, 1),
            case Start() of
                {ok, Pid} ->
                    gen_server:cast(Server, {connection, Pid}),
                    {ok, Pid};
                Error ->
                    counters:sub(Counters, ?CONNECTIONS, 1),
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% Whether the server takes another connection now.
room(Counters, MaxConnections) ->
    case counters:get(Counters, ?STOPPING) of
        1 ->
            {error, shutting_down};
        0 ->
            case counters:get(Counters, ?CONNECTIONS) >= MaxConnections of
                true -> {error, too_many_connections};
                false -> ok
            end
    end.

-spec handle_call(term(), gen_server:from(), #state{}) -> {reply, term(), #state{}}.
handle_call(port, _From, #state{name = Name} = State) ->
    {reply, quic:get_server_port(Name), State};
handle_call(tcp_port, _From, #state{tcp = undefined} = State) ->
    {reply, {error, disabled}, State};
handle_call(tcp_port, _From, #state{tcp = Tcp} = State) ->
    {reply, trevrpc_tcp_listener:port(Tcp), State};
handle_call(stop, _From, State) ->
    {reply, ok, shutdown(State)}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast({connection, Pid}, #state{connections = Connections} = State) ->
    {noreply, State#state{connections = Connections#{monitor(process, Pid) => Pid}}}.

-spec handle_info(term(), #state{}) -> {noreply, #state{}} | {stop, term(), #state{}}.
handle_info({'DOWN', Monitor, process, _Pid, _Reason}, #state{connections = Connections} = State) ->
    case maps:take(Monitor, Connections) of
        {_, Rest} ->
            counters:sub(State#state.counters, ?CONNECTIONS, 1),
            {noreply, State#state{connections = Rest}};
        error ->
            {noreply, State}
    end;
handle_info({'EXIT', Tcp, Reason}, #state{tcp = Tcp} = State) ->
    %% A server that silently stopped serving TCP would strand the clients
    %% QUIC cannot reach: stop it all instead.
    logger:error("trevrpc: the TCP listener exited: ~p", [Reason]),
    {stop, {tcp_listener, Reason}, State#state{tcp = undefined}};
handle_info(_Message, State) ->
    {noreply, State}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, State) ->
    _ = shutdown(State),
    ok.

%% Stops new connections, has every connection cancel its calls and close,
%% and waits for them, then stops the listener.
shutdown(#state{counters = Counters, connections = Connections, name = Name} = State) ->
    case counters:get(Counters, ?STOPPING) of
        1 ->
            State;
        0 ->
            counters:put(Counters, ?STOPPING, 1),
            case State#state.tcp of
                undefined -> ok;
                Tcp -> trevrpc_tcp_listener:stop(Tcp)
            end,
            #{graceful_shutdown_timeout := Grace} = State#state.config,
            _ = [trevrpc_server_conn:shutdown(Pid) || Pid <- maps:values(Connections)],
            %% Each connection gives its handlers the grace period, then kills
            %% them; allow it a little longer to close before giving up.
            Deadline = erlang:monotonic_time(millisecond) + Grace + 5000,
            Left = await_connections(Connections, Deadline),
            _ = [exit(Pid, kill) || Pid <- maps:values(Left)],
            _ = quic:stop_server(Name),
            State#state{connections = #{}}
    end.

await_connections(Connections, _Deadline) when map_size(Connections) =:= 0 ->
    Connections;
await_connections(Connections, Deadline) ->
    Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {'DOWN', Monitor, process, _Pid, _Reason} when is_map_key(Monitor, Connections) ->
            await_connections(maps:remove(Monitor, Connections), Deadline)
    after Timeout ->
        Connections
    end.
