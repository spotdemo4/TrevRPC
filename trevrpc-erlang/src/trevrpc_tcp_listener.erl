%% @private The server's TCP listener: TLS connections, dispatched on the
%% negotiated ALPN. `qmux-02.trevrpc-1' carries TrevRPC over QMux; the
%% HTTP/2 and WebSocket fallbacks are not served yet, so it is the only
%% protocol offered.
%%
%% Each connection's handshake runs in a process of its own, bounded by the
%% handshake timeout, so a slow client holds up no other. At most
%% `max_handshakes' run at once: past that, connections wait in the kernel's
%% backlog, so half-open handshakes cannot take every file descriptor.
-module(trevrpc_tcp_listener).

-export([start_link/3, port/1, stop/1]).

%% @doc Starts listening on `port' at `ip' with the TLS `Credentials', linked
%% to the caller, with at most `max_handshakes' handshakes of at most
%% `handshake_timeout' milliseconds at once. `Accept' takes over each QMux connection's socket, as its
%% controlling process, once its handshake completed; it runs in the
%% handshake's process.
-spec start_link(map(), map(), fun((ssl:sslsocket()) -> ok | {error, term()})) ->
    {ok, pid()} | {error, term()}.
start_link(Opts, Credentials, Accept) ->
    Self = self(),
    Pid = proc_lib:spawn_link(fun() -> init(Self, Opts, Credentials, Accept) end),
    receive
        {Pid, Result} -> Result
    end.

%% @doc The TCP port the listener is bound to.
-spec port(pid()) -> {ok, inet:port_number()} | {error, term()}.
port(Listener) ->
    Ref = make_ref(),
    Listener ! {port, self(), Ref},
    receive
        {Ref, Result} -> Result
    after 5000 -> {error, timeout}
    end.

%% @doc Stops accepting connections. Those already accepted stay open.
-spec stop(pid()) -> ok.
stop(Listener) ->
    unlink(Listener),
    exit(Listener, shutdown),
    ok.

init(Parent, Opts, #{cert := Cert, key := Key} = Credentials, Accept) ->
    KeyType = element(1, Key),
    Listen = [
        binary,
        {active, false},
        {reuseaddr, true},
        {nodelay, true},
        {backlog, 1024},
        {ip, maps:get(ip, Opts, {0, 0, 0, 0})},
        {versions, ['tlsv1.3', 'tlsv1.2']},
        {alpn_preferred_protocols, [trevrpc:qmux_alpn()]},
        {certs_keys, [
            #{
                cert => [Cert | maps:get(cert_chain, Credentials, [])],
                key => {KeyType, public_key:der_encode(KeyType, Key)}
            }
        ]}
    ],
    case ssl:listen(maps:get(port, Opts, 0), Listen) of
        {ok, Socket} ->
            process_flag(trap_exit, true),
            Limits = {maps:get(handshake_timeout, Opts), maps:get(max_handshakes, Opts)},
            Acceptor = proc_lib:spawn_link(fun() -> accept_loop(Socket, Accept, Limits, 0) end),
            Parent ! {self(), {ok, self()}},
            loop(Socket, Acceptor);
        {error, _} = Error ->
            Parent ! {self(), Error}
    end.

loop(Socket, Acceptor) ->
    receive
        {port, From, Ref} ->
            From ! {Ref, port_of(Socket)},
            loop(Socket, Acceptor);
        {'EXIT', _From, shutdown} ->
            _ = ssl:close(Socket),
            exit(shutdown);
        {'EXIT', Acceptor, Reason} ->
            _ = ssl:close(Socket),
            exit(Reason);
        {'EXIT', _Parent, Reason} ->
            _ = ssl:close(Socket),
            exit(Reason)
    end.

port_of(Socket) ->
    case ssl:sockname(Socket) of
        {ok, {_Ip, Port}} -> {ok, Port};
        Error -> Error
    end.

%% InFlight counts the handshake processes alive, each monitored.
accept_loop(Listen, Accept, {_Timeout, Max} = Limits, InFlight) when InFlight >= Max ->
    receive
        {'DOWN', _, process, _, _} -> accept_loop(Listen, Accept, Limits, InFlight - 1)
    end;
accept_loop(Listen, Accept, {Timeout, _Max} = Limits, InFlight) ->
    case ssl:transport_accept(Listen) of
        {ok, Socket} ->
            {Pid, _Monitor} = spawn_monitor(fun() -> handshake(Socket, Accept, Timeout) end),
            _ =
                case ssl:controlling_process(Socket, Pid) of
                    ok ->
                        Pid ! go;
                    {error, _} ->
                        exit(Pid, kill),
                        ssl:close(Socket)
                end,
            accept_loop(Listen, Accept, Limits, finished(InFlight + 1));
        {error, closed} ->
            ok;
        {error, _Reason} ->
            accept_loop(Listen, Accept, Limits, finished(InFlight))
    end.

%% Counts off the handshakes that ended.
finished(InFlight) ->
    receive
        {'DOWN', _, process, _, _} -> finished(InFlight - 1)
    after 0 -> InFlight
    end.

handshake(Socket, Accept, Timeout) ->
    receive
        go -> handshake_owned(Socket, Accept, Timeout)
    after Timeout ->
        %% The acceptor never handed the socket over.
        ok
    end.

handshake_owned(Socket, Accept, Timeout) ->
    Peer = peer(Socket),
    case ssl:handshake(Socket, Timeout) of
        {ok, Tls} ->
            Qmux = trevrpc:qmux_alpn(),
            case ssl:negotiated_protocol(Tls) of
                {ok, Qmux} ->
                    case Accept(Tls) of
                        ok ->
                            ok;
                        {error, Reason} ->
                            logger:warning("trevrpc: refused QMux connection from ~s: ~p", [
                                Peer, Reason
                            ]),
                            ssl:close(Tls)
                    end;
                Other ->
                    logger:warning(
                        "trevrpc: TLS connection from ~s negotiated ~p, which is not served", [
                            Peer, Other
                        ]
                    ),
                    ssl:close(Tls)
            end;
        {error, Reason} ->
            logger:warning("trevrpc: TLS handshake from ~s failed: ~p", [Peer, Reason]),
            _ = ssl:close(Socket),
            ok
    end.

peer(Socket) ->
    case ssl:peername(Socket) of
        {ok, {Ip, Port}} -> io_lib:format("~s:~b", [inet:ntoa(Ip), Port]);
        _ -> "an unknown peer"
    end.
