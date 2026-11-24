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
%% With `webtransport => true', the server also serves browsers, at the
%% HTTP path, for the sessions that `webtransport_admission' accepts (see
%% {@link trevrpc_admission}): WebTransport over HTTP/3 on the UDP socket,
%% and QMux over WebSocket on the TCP listener. Each HTTP/3 connection
%% carries at most one WebTransport session, whose calls count against
%% `max_streams_per_connection' as a connection's would. A session handles
%% its streams' data one chunk at a time, and holds back the chunks of its
%% other calls while one call is a window behind on reading its own (see
%% {@link trevrpc_webtransport}).
%%
%% With `http_fallback => Handler', every other HTTP request on the same
%% sockets goes to `Handler', over HTTP/3, HTTP/2 and HTTP/1.1, such as the
%% static content of a web application: see {@link trevrpc_http_request}.
%% TrevRPC claims the HTTP path while `http' or `webtransport' is on, and
%% setting a fallback serves those HTTP versions even with both off. HTTP/2
%% and HTTP/1.1 responses advertise HTTP/3 through `Alt-Svc'.
%%
%% An HTTP request whose host is another name than the TLS server name (SNI)
%% its client sent for the connection is answered `421 Misdirected Request',
%% over every HTTP version and whatever its path, before admission, RPCs,
%% browser sessions or the fallback see it. Browsers send such requests
%% when the certificate covers both names (connection coalescing), and
%% retry them on a new connection after a `421', which a proxy that routes
%% by SNI then sends to the right server. The port, a trailing dot and case
%% do not count, and a connection without SNI, as a client opens to an IP
%% address, or a request for an IP address is always served; an SNI that
%% is an IP address counts as none. Each `421' is logged at debug level.
%% `misdirected_request_check => false' serves every host, as a server
%% whose clients reach it through a proxy that terminates TLS and sends a
%% server name of its own needs.
%%
%% Messages are opaque binaries here: a handler decodes its request and
%% encodes its response, usually through generated code. See
%% {@link trevrpc_call} for the handler API.
%%
%% With a `validator', request messages are checked after they are decoded
%% and before a handler sees them, so that a request that breaks its rules,
%% such as its `buf.validate' ones, fails with `invalid_argument' and a
%% `buf.validate.Violations' detail: see {@link trevrpc_validator}. Without
%% one, requests are not checked.
%%
%% Every connection is owned by a `trevrpc_server_conn' process, which
%% reads each stream's REQUEST, checks it and runs its handler in a call
%% process of its own. That process can be stopped at any moment, which is
%% how deadlines, cancellation and shutdown end a handler, whether or not it
%% cooperates.
-module(trevrpc_server).

-behaviour(gen_server).

-ifdef(TEST).
-export([tcp_listener_options/2]).
-endif.

-export([start_link/1, start/1, port/1, tcp_port/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([options/0, routes/0, route/0, handler/0, result/0, config/0]).

%% A handler's outcome. `ok' (streaming responses) and `{ok, Response}'
%% (single responses) end the call with OK; `{error, Status}' with that
%% status. A response is the message's bytes, or a term the route's codec
%% encodes.
-type result() :: ok | {ok, term()} | {error, trevrpc_status:status()}.

%% Unary and server-streaming handlers get the request message;
%% client-streaming and bidirectional ones read requests from the call.
-type handler() ::
    fun((term(), trevrpc_call:call()) -> result())
    | fun((trevrpc_call:call()) -> result()).

%% `request_type' and `response_type' are the protobuf full names of the
%% method's messages, which pick their compression dictionaries, and
%% `compression' how its messages may be compressed without one (see
%% {@link trevrpc_zstd:directions()}). With a `codec', handlers get and give
%% decoded messages; without one, their bytes.
%%
%% `unreliable' (see {@link trevrpc_datagram:unreliable()}) lets the
%% responses of a server-streaming or bidirectional method travel as
%% datagrams, on calls whose client gave them a datagram flow over native
%% QUIC: they can be lost or reordered, but are never duplicated. Each goes
%% on the stream instead when it does not fit a datagram. Request datagrams
%% are accepted on every call with a flow, so `requests' only turns the
%% route's stream compression of requests into per-message compression, as
%% the client compresses them. Generated routes carry the method's schema
%% option.
-type route() :: #{
    kind := trevrpc_wire:kind(),
    handler := handler(),
    request_type => binary(),
    response_type => binary(),
    compression => trevrpc_zstd:directions(),
    codec => trevrpc_codec:codec(),
    unreliable => trevrpc_datagram:unreliable()
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
    %% How long data sent on a TCP connection may stay unacknowledged before
    %% the kernel drops it (TCP_USER_TIMEOUT, Linux only), so that a client
    %% that vanishes is let go; connection_idle_timeout by default.
    tcp_user_timeout => pos_integer() | infinity,
    graceful_shutdown_timeout => non_neg_integer(),
    compression => trevrpc_zstd:settings(),
    stream_compression_memory => non_neg_integer(),
    http => boolean(),
    http_admission => trevrpc_admission:admission(),
    webtransport => boolean(),
    webtransport_admission => trevrpc_admission:admission(),
    http_path => binary(),
    http_fallback => trevrpc_http_request:handler(),
    misdirected_request_check => boolean(),
    validator => trevrpc_validator:validator()
}.

%% What each connection process needs, shared by all of them.
-type config() :: #{
    server := pid(),
    routes := routes(),
    %% What checks the decoded requests of each route that has a check, by
    %% method path; see trevrpc_validator.
    validators := #{binary() => trevrpc_validator:validate()},
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
    qmux := qmux:options(),
    %% The HTTP mapping's options, unless HTTP is off.
    http := trevrpc_http:options() | undefined,
    %% How long an HTTP/2 or HTTP/1.1 connection may go without a request.
    idle_timeout := timeout(),
    %% How WebTransport sessions are admitted, the limits each session
    %% grants its client, and erlang-webtransport's HTTP/3 options for them,
    %% unless WebTransport is off.
    webtransport :=
        #{
            path := binary(),
            admission := trevrpc_admission:admission(),
            session := map(),
            h3 := map()
        }
        | undefined,
    %% The HTTP path TrevRPC claims, while it serves the HTTP mapping or
    %% browsers: other paths go to the fallback.
    http_path := binary() | undefined,
    %% The handler of the requests TrevRPC does not claim.
    fallback := trevrpc_http_request:handler() | undefined,
    %% The `Alt-Svc' value HTTP/2 and HTTP/1.1 responses advertise HTTP/3
    %% with, while HTTP/2 is served.
    alt_svc := binary() | undefined,
    %% erlang_h1's options for HTTP/1.1 connections, and how long one may
    %% go without a request in flight.
    h1 := map(),
    h1_idle_timeout := timeout(),
    %% Whether HTTP requests for another name than their connection's TLS
    %% server name are answered `421'.
    misdirected_request_check := boolean()
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
%% Where a QUIC connection's process keeps the client's server name from
%% the SNI hook to the ALPN hook.
-define(SERVER_NAME_KEY, {?MODULE, server_name}).
%% How long a stopping server waits at most for its QUIC connections to
%% drain before it closes the listener's socket; see close_quic/2.
-define(QUIC_DRAIN_WAIT, 2000).

%% @doc Starts a server linked to the caller, listening once it returns.
-spec start_link(options()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

%% @doc Starts a server, listening once it returns.
-spec start(options()) -> {ok, pid()} | ignore | {error, term()}.
start(Opts) ->
    gen_server:start(?MODULE, Opts, []).

%% @doc The UDP port the server listens on. Like every call to the server,
%% it exits (`noproc') once the server is down, and with the server's exit
%% reason when the server goes down while it waits, as during a stop.
-spec port(pid()) -> {ok, inet:port_number()} | {error, term()}.
port(Server) ->
    gen_server:call(Server, port).

%% @doc The TCP port the server listens on for QMux over TLS, or `{error,
%% disabled}' with TCP off. It exits once the server is down, as port/1
%% does.
-spec tcp_port(pid()) -> {ok, inet:port_number()} | {error, term()}.
tcp_port(Server) ->
    gen_server:call(Server, tcp_port).

%% @doc Shuts the server down gracefully: it stops taking connections and
%% calls, cancels the calls in flight, gives their handlers up to the
%% graceful shutdown timeout to exit, then closes every connection, and
%% exits `normal'. Returns once the server is down; exits (`noproc') if it
%% already is, as gen_server:stop/1 does, so a server is stopped once.
-spec stop(pid()) -> ok.
stop(Server) ->
    gen_server:call(Server, stop, infinity).

%%% gen_server

%% A failed start returns its error without taking a linked caller down.
%% The server traps exits so that the TCP listener's exit is a message; see
%% handle_info/2 for the others.
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
        ok ?= trevrpc_socket:check_user_timeout(Opts),
        {ok, Validators} ?= validators(Opts),
        ok = webtransport_routers(Opts),
        listen(Opts, Credentials, Compression, Validators)
    else
        {error, Reason} -> {error, Reason}
    end.

compression(Opts) ->
    case trevrpc_zstd:config(maps:get(compression, Opts, #{})) of
        {ok, Compression} -> {ok, Compression};
        {error, Message} -> {error, {compression, Message}}
    end.

%% A route's options are checked once, so that a mistake fails
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

%% Each route's check is bound once, so that rules that cannot be enforced
%% fail the start rather than every call.
validators(Opts) ->
    Validator = maps:get(validator, Opts, undefined),
    case trevrpc_validator:check(Validator) of
        ok -> trevrpc_validator:bind(Validator, maps:get(routes, Opts));
        error -> {error, {invalid_option, validator}}
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
        #{http_fallback := Fallback} when not is_function(Fallback, 1) ->
            {error, {invalid_option, http_fallback}};
        #{misdirected_request_check := Enabled} when not is_boolean(Enabled) ->
            {error, {invalid_option, misdirected_request_check}};
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
    Codec =
        case trevrpc_codec:check(maps:get(codec, Route, undefined)) of
            ok -> [];
            error -> [{invalid_codec, maps:get(codec, Route)}]
        end,
    Unreliable =
        case trevrpc_datagram:check_unreliable(maps:get(unreliable, Route, #{})) of
            ok -> [];
            error -> [{invalid_unreliable, maps:get(unreliable, Route)}]
        end,
    Directions ++ Types ++ Codec ++ Unreliable.

%% erlang-webtransport finds the stream router of each HTTP/3 connection in
%% a named table, which the first process that needs it creates, and which
%% goes with that process: here it is a process of its own, which outlives
%% every server of the node.
webtransport_routers(#{webtransport := true}) ->
    case ets:whereis(webtransport_routers) of
        undefined ->
            Parent = self(),
            {Pid, Monitor} = spawn_monitor(fun() -> own_webtransport_routers(Parent) end),
            receive
                {Pid, ready} -> demonitor(Monitor, [flush]);
                {'DOWN', Monitor, process, Pid, _} -> ok
            end,
            ok;
        _ ->
            ok
    end;
webtransport_routers(#{}) ->
    ok.

own_webtransport_routers(Parent) ->
    try ets:new(webtransport_routers, [named_table, public, set]) of
        _ ->
            Parent ! {self(), ready},
            receive
            after infinity -> ok
            end
    catch
        %% Created by another process meanwhile.
        error:badarg -> ok
    end.

%% Directions whose messages travel as datagrams are never stream-compressed,
%% which needs a stream's ordered delivery: their messages are compressed
%% one by one instead.
datagram_route(#{unreliable := Unreliable, kind := Kind} = Route) ->
    Directions = trevrpc_zstd:directions(maps:get(compression, Route, none)),
    {Requests, Responses} = trevrpc_datagram:compression(
        Directions, trevrpc_datagram:effective(Unreliable, Kind)
    ),
    Route#{compression => #{requests => Requests, responses => Responses}};
datagram_route(Route) ->
    Route.

listen(Opts, Credentials, Compression, Validators) ->
    Counters = counters:new(2, [write_concurrency]),
    Config = config(Opts, Compression, Validators),
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
        maps:merge(
            QuicOpts0#{extra_socket_opts => [{ip, maps:get(ip, Opts, {0, 0, 0, 0})}]},
            webtransport_quic(Config)
        ),
        quic_handlers(Start, Credentials, Config)
    ),
    case start_quic(maps:get(port, Opts, 0), QuicOpts) of
        {ok, Name} ->
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

%% Starts the QUIC listener under the first name no running listener holds.
%% `quic:start_server/3' registers each listener under an atom, and atoms are
%% never freed, so names are reused: a node makes no more of them than it ever
%% ran servers at once.
start_quic(Port, Opts) ->
    start_quic(1, Port, Opts).

start_quic(N, Port, Opts) ->
    Name = list_to_atom("trevrpc_server_" ++ integer_to_list(N)),
    %% The registry forgets a listener once it has seen it exit, so a name
    %% it no longer holds is free to take. Taken names are skipped here,
    %% cheaply, rather than through the option checks of a start.
    case quic:get_server_info(Name) of
        {ok, _} ->
            start_quic(N + 1, Port, Opts);
        {error, _} ->
            case quic:start_server(Name, Port, Opts) of
                {ok, _} ->
                    {ok, Name};
                %% Taken by a listener starting or stopping meanwhile.
                {error, {already_started, _}} ->
                    start_quic(N + 1, Port, Opts);
                {error, already_present} ->
                    start_quic(N + 1, Port, Opts);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% The QUIC options WebTransport needs (draft-ietf-webtrans-http3 Section
%% 3.1): RESET_STREAM_AT, and resets that carry their stream's final size,
%% which sessions with flow control count. QUIC datagrams are on already.
webtransport_quic(#{webtransport := undefined}) ->
    #{};
webtransport_quic(#{webtransport := _}) ->
    #{reset_stream_at => true, stream_reset_final_size => true}.

%% Who owns each QUIC connection: the process `Start' starts for it. With
%% HTTP, WebTransport or the fallback, a connection that negotiated `h3' is
%% HTTP/3, whose `quic_h3' process the owner hands the connection to in the
%% ALPN hook, which runs in the connection's process.
quic_handlers(Start, _Credentials, #{
    http := undefined, webtransport := undefined, fallback := undefined
}) ->
    #{connection_handler => Start};
quic_handlers(Start, Credentials, Config) ->
    #{
        alpn => [trevrpc:alpn(), ?H3],
        %% The control and QPACK streams of HTTP/3, and room for the
        %% unidirectional streams clients send to grease the protocol. They
        %% apply to native connections too, which stop each unidirectional
        %% stream and drop what it carried.
        max_streams_uni => 8,
        max_stream_data_uni => 16384,
        connection_handler => Start,
        %% erlang_quic tells the ALPN hook nothing of the server name the
        %% client sent, which HTTP/3 requests must name (see
        %% trevrpc_http:misdirected/2). The SNI hook runs just before it in
        %% the same connection process, so it leaves the name there, and
        %% presents the server's certificate as without it.
        sni_callback => fun(ServerName) ->
            _ = put(?SERVER_NAME_KEY, ServerName),
            {ok, Credentials}
        end,
        alpn_handler => fun
            (Conn, ?H3, Owner) ->
                serve_h3(Conn, Owner, erase(?SERVER_NAME_KEY), Config);
            (Conn, _Alpn, Owner) ->
                _ = erase(?SERVER_NAME_KEY),
                %% Every stream the client opens starts in manual flow
                %% control (see trevrpc_transport:hold_streams/1).
                ok = trevrpc_transport:hold_streams({quic, Conn}),
                {ok, Owner}
        end
    }.

%% Starts HTTP/3 on the QUIC connection `Conn', whose events go to `Owner'
%% from now on. With WebTransport, they go through the connection's stream
%% router, which keeps those of the sessions' streams and datagrams, and the
%% connection advertises WebTransport in its SETTINGS.
serve_h3(Conn, Owner, ServerName, #{webtransport := undefined}) ->
    %% No handler: the owner answers each request.
    accept_h3(Conn, Owner, ServerName, #{owner => Owner, handler => undefined});
serve_h3(Conn, Owner, ServerName, #{webtransport := #{h3 := H3Settings}}) ->
    #{connection_handler := Routed, settings := Settings} = H3Settings,
    #{owner := Router} = PerConnection = Routed(Conn),
    ok = webtransport_h3_router:set_passthrough(Router, Owner),
    accept_h3(Conn, Owner, ServerName, PerConnection#{
        handler => undefined,
        settings => Settings,
        %% The client's SETTINGS tell its WebTransport draft.
        defer_requests_until_settings => true,
        notify_streams_available => true
    }).

accept_h3(Conn, Owner, ServerName, Opts) ->
    maybe
        {ok, H3} ?= quic_h3:accept(Conn, Opts),
        ok ?= trevrpc_server_conn:serve_h3(Owner, H3, Conn, ServerName),
        {ok, H3}
    end.

%% HTTP/2 and HTTP/1.1 responses advertise HTTP/3 on the UDP socket, while
%% HTTP/2 is served: for the HTTP mapping or the fallback.
alt_svc(#{http := undefined, fallback := undefined} = Config, _UdpPort) ->
    Config;
alt_svc(#{http := Http} = Config, UdpPort) ->
    AltSvc = iolist_to_binary(io_lib:format("h3=\":~b\"; ma=86400", [UdpPort])),
    case Http of
        undefined -> Config#{alt_svc := AltSvc};
        _ -> Config#{alt_svc := AltSvc, http := Http#{alt_svc => AltSvc}}
    end.

%% The TCP listener for QMux over TLS, by default on the UDP socket's port
%% number, or any free port if that one is taken and none was asked for.
listen_tcp(#{tcp := false}, _Credentials, _State, _MaxConnections) ->
    {ok, undefined};
listen_tcp(
    Opts, Credentials, #state{name = Name, config = Config, counters = Counters}, MaxConnections
) ->
    %% Each serves a connection whose TLS handshake completed, which counts
    %% against the server's connections from then on.
    Serve = fun(Start) ->
        fun(Socket) ->
            case admit(fun() -> Start(Socket) end, Config, Counters, MaxConnections) of
                {ok, _Pid} -> ok;
                {error, _} = Error -> Error
            end
        end
    end,
    #{http := Mapping, webtransport := Browsers, fallback := Fallback} = Config,
    %% HTTP/2 carries the HTTP mapping and the fallback, HTTP/1.1 the
    %% WebSocket sessions and the fallback.
    Accept = maps:from_list(
        [{qmux, Serve(fun(Socket) -> trevrpc_server_conn:start_qmux(Socket, #{}, Config) end)}] ++
            [
                {h2, Serve(fun(Socket) -> trevrpc_server_conn:start_http(h2, Socket, Config) end)}
             || Mapping =/= undefined orelse Fallback =/= undefined
            ] ++
            [
                {http1,
                    Serve(fun(Socket) -> trevrpc_server_conn:start_http(h1, Socket, Config) end)}
             || Browsers =/= undefined orelse Fallback =/= undefined
            ]
    ),
    {ok, UdpPort} = quic:get_server_port(Name),
    Listen = tcp_listener_options(Opts, UdpPort),
    case trevrpc_tcp_listener:start_link(Listen, Credentials, Accept) of
        {error, eaddrinuse} when not is_map_key(tcp_port, Opts) ->
            trevrpc_tcp_listener:start_link(Listen#{port => 0}, Credentials, Accept);
        Result ->
            Result
    end.

config(Opts, Compression, Validators) ->
    #{
        validators => Validators,
        compression => Compression,
        stream_budget => trevrpc_zstd_stream:budget(
            maps:get(stream_compression_memory, Opts, ?STREAM_COMPRESSION_MEMORY)
        ),
        server => self(),
        routes => maps:map(
            fun(_Method, Route) -> datagram_route(Route) end, maps:get(routes, Opts)
        ),
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
        webtransport => webtransport_options(Opts),
        http_path =>
            case Opts of
                #{http := true} -> maps:get(http_path, Opts, ?HTTP_PATH);
                #{webtransport := true} -> maps:get(http_path, Opts, ?HTTP_PATH);
                #{} -> undefined
            end,
        fallback => maps:get(http_fallback, Opts, undefined),
        alt_svc => undefined,
        idle_timeout => maps:get(connection_idle_timeout, Opts, 30000),
        h1 => h1_options(Opts),
        %% An HTTP/1.1 client has the TLS handshake timeout to send a
        %% request's head, after the handshake or the request before, as
        %% from the Rust server.
        h1_idle_timeout => maps:get(tls_handshake_timeout, Opts, ?TLS_HANDSHAKE_TIMEOUT),
        misdirected_request_check => maps:get(misdirected_request_check, Opts, true)
    }.

http_options(#{http := true} = Opts) ->
    #{
        path => maps:get(http_path, Opts, ?HTTP_PATH),
        admission => maps:get(http_admission, Opts, undefined)
    };
http_options(#{}) ->
    undefined.

%% Request bodies are held to a stream window ahead of the fallback handler
%% that reads them; a body the handler does not read is drained for at
%% most a second after its response. erlang_h1's own timers are off: its
%% idle timer runs while a response goes out to a client that sends
%% nothing, and its request timer while a body pauses, so the connection's
%% process closes an idle connection itself (see h1_idle_timeout).
h1_options(Opts) ->
    MaxFrame = maps:get(max_frame_size, Opts, trevrpc:default_max_frame_size()),
    #{
        request_timeout => infinity,
        idle_timeout => infinity,
        flow_control => manual,
        recv_window => trevrpc_quic:stream_window(MaxFrame),
        max_body_size => infinity,
        early_response_drain => {65536, 1000}
    }.

%% WebTransport sessions grant their clients what QUIC connections do,
%% where their draft has flow control of its own (Safari's, and the latest
%% drafts'), and take no unidirectional streams, which TrevRPC does not use.
webtransport_options(#{webtransport := true} = Opts) ->
    MaxFrame = maps:get(max_frame_size, Opts, trevrpc:default_max_frame_size()),
    Streams = 4 * maps:get(max_streams_per_connection, Opts, 64),
    Session = #{
        max_data => trevrpc_quic:connection_window(MaxFrame, Streams),
        max_stream_data => trevrpc_quic:stream_window(MaxFrame),
        max_streams_bidi => Streams,
        max_streams_uni => 0
    },
    #{
        path => maps:get(http_path, Opts, ?HTTP_PATH),
        admission => maps:get(webtransport_admission, Opts, fun trevrpc_admission:no_origin/1),
        session => Session,
        h3 => webtransport:h3_settings(Session)
    };
webtransport_options(#{}) ->
    undefined.

%% The options of the TCP listener: its address, the TCP_USER_TIMEOUT of its
%% connections (the connection idle timeout unless `tcp_user_timeout' says
%% otherwise), and the bounds on TLS handshakes.
tcp_listener_options(Opts, UdpPort) ->
    #{
        ip => maps:get(ip, Opts, {0, 0, 0, 0}),
        port => maps:get(tcp_port, Opts, UdpPort),
        user_timeout => maps:get(
            tcp_user_timeout, Opts, maps:get(connection_idle_timeout, Opts, 30000)
        ),
        handshake_timeout => maps:get(tls_handshake_timeout, Opts, ?TLS_HANDSHAKE_TIMEOUT),
        max_handshakes => maps:get(max_tls_handshakes, Opts, 256)
    }.

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
            end,
        %% Calls return the credit for what they read.
        stream_flow_control => manual
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

-spec handle_call(term(), gen_server:from(), #state{}) ->
    {reply, term(), #state{}} | {stop, normal, ok, #state{}}.
handle_call(stop, _From, State) ->
    %% terminate/2 finds the shutdown done; the reply follows it.
    {stop, normal, ok, shutdown(State)};
handle_call(Request, _From, State) ->
    {reply, listening_port(Request, State), State}.

listening_port(port, #state{name = Name}) ->
    quic:get_server_port(Name);
listening_port(tcp_port, #state{tcp = undefined}) ->
    {error, disabled};
listening_port(tcp_port, #state{tcp = Tcp}) ->
    trevrpc_tcp_listener:port(Tcp).

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
handle_info({'EXIT', _From, normal}, State) ->
    {noreply, State};
handle_info({'EXIT', _From, Reason}, State) ->
    %% A server started with start/1 is its own parent, so gen_server leaves
    %% every exit signal to it: one that would end a process not trapping
    %% exits ends the server, gracefully, as its parent's would.
    {stop, Reason, State};
handle_info(_Message, State) ->
    {noreply, State}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, State) ->
    _ = shutdown(State),
    ok.

%% Stops new connections, has every connection cancel its calls and close,
%% and waits for them, then for the QUIC connections to drain, and stops
%% the listener.
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
            close_quic(Name, Deadline),
            _ = quic:stop_server(Name),
            State#state{connections = #{}, tcp = undefined}
    end.

%% Closes every QUIC connection of the listener and waits for them to end,
%% for up to ?QUIC_DRAIN_WAIT and not past `Deadline'. Stopping the listener
%% closes its socket, and on OTP's `socket' module the CONNECTION_CLOSE a
%% connection just wrote may still be queued in the listener's sender
%% process then, so the peer would only notice at its idle timeout. A
%% closed connection drains for three probe timeouts before it ends,
%% answering the peer's packets with its close, and its close has long gone
%% out by then. That takes a few round trips on a healthy path; one whose
%% probes went unanswered, as when its peer is gone, backed its timeout off
%% and drains far longer, and is not waited for. A connection that already
%% closed takes the close as a duplicate.
close_quic(Name, Deadline) ->
    case quic:get_server_connections(Name) of
        {ok, Conns} ->
            Monitors = maps:from_list([{monitor(process, Conn), Conn} || Conn <- Conns]),
            _ = [quic:close(Conn, 0) || Conn <- Conns],
            Wait = min(Deadline, erlang:monotonic_time(millisecond) + ?QUIC_DRAIN_WAIT),
            Left = await_connections(Monitors, Wait),
            _ = [demonitor(Monitor, [flush]) || Monitor <- maps:keys(Left)],
            ok;
        {error, _} ->
            ok
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
