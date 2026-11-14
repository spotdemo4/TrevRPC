%% @private HTTP/1.1 on the server's TCP listener, through erlang_h1: the
%% connections that negotiated `http/1.1', or no ALPN at all.
%%
%% A WebSocket upgrade (a `GET' with `Upgrade: websocket') at the HTTP path
%% that offers subprotocol `qmux-02.trevrpc-1', and that the admission
%% accepts, carries a QMux session (QMux over WebSocket, for browsers). Every
%% other request gets an error and the connection closes after it, so a
%% connection holds its handshake slot for at most one request: `404' off
%% the HTTP path, `505' for a request at it that is not a WebSocket upgrade
%% (TrevRPC over HTTP needs HTTP/2 or HTTP/3), `400' for
%% an upgrade without the subprotocol or with invalid WebSocket headers,
%% `403' for one the admission refused, `500' when the admission failed,
%% and `503' when the server is full or shutting down.
-module(trevrpc_http1).

-export([serve/3]).

-export_type([options/0]).

%% `path': the HTTP path; `admission': who may open sessions; `timeout':
%% milliseconds a connection may wait for its next request, or take to send
%% one; `room': whether the server takes another session now.
-type options() :: #{
    path := binary(),
    admission := trevrpc_admission:admission(),
    timeout := pos_integer(),
    room := fun(() -> ok | {error, term()})
}.

%% Starts the QMux session of an upgraded socket and the bytes read past the
%% upgrade request; runs in the process that owns the socket.
-type start() :: fun((ssl:sslsocket(), binary()) -> ok | {error, term()}).

%% WebSocket close code 1013: try again later, for a session refused after
%% its upgrade, when the server filled up in between.
-define(TRY_AGAIN_LATER, 1013).

%% @doc Serves HTTP/1.1 on `Socket', a TLS connection whose handshake
%% completed, owned by the caller, and returns once the connection ended or
%% upgraded: erlang_h1 closes a connection whose caller exits. So in the TCP
%% listener, a connection counts as a handshake until it upgrades, which
%% bounds it to one request, each phase of it within `timeout'.
-spec serve(ssl:sslsocket(), options(), start()) -> ok | {error, term()}.
serve(Socket, #{timeout := Timeout} = Opts, Start) ->
    Peer =
        case ssl:peername(Socket) of
            {ok, Address} -> #{peer => Address};
            {error, _} -> #{}
        end,
    Handler = fun(Conn, Id, Method, Target, Headers) ->
        handle(Conn, Id, Method, Target, Headers, Peer, Opts, Start)
    end,
    case
        h1:serve_socket(Socket, #{
            handler => Handler,
            idle_timeout => Timeout,
            request_timeout => Timeout,
            max_keepalive_requests => 1,
            %% Nothing here reads a body: a client gets its error without
            %% holding the connection long.
            max_body_size => 65536,
            early_response_drain => {65536, 1000}
        })
    of
        {ok, Pid} ->
            Monitor = monitor(process, Pid),
            receive
                {'DOWN', Monitor, process, Pid, _Reason} -> ok
            end;
        {error, _} = Error ->
            Error
    end.

handle(Conn, Id, Method, Target, Headers, Peer, #{path := HttpPath} = Opts, Start) ->
    Path = trevrpc_http:path(Target),
    Upgrade =
        Method =:= <<"GET">> andalso
            lists:member(<<"websocket">>, tokens(trevrpc_http:header(<<"upgrade">>, Headers))),
    if
        Path =/= HttpPath ->
            respond(Conn, Id, 404, <<"not found">>);
        not Upgrade ->
            respond(Conn, Id, 505, <<"TrevRPC over HTTP requires HTTP/2 or HTTP/3">>);
        true ->
            Request = Peer#{
                method => Method,
                protocol => <<"HTTP/1.1">>,
                path => Path,
                authority => trevrpc_http:header(<<"host">>, Headers),
                secure => true,
                headers => Headers
            },
            upgrade(Conn, Id, Request, Opts, Start)
    end.

upgrade(Conn, Id, #{headers := Headers} = Request0, #{admission := Admission} = Opts, Start) ->
    Request =
        case trevrpc_http:header(<<"origin">>, Headers) of
            <<>> -> Request0;
            Origin -> Request0#{origin => Origin}
        end,
    Alpn = trevrpc:qmux_alpn(),
    case ws_h1_upgrade:validate_request(Headers, #{required_subprotocols => [Alpn]}) of
        {error, no_acceptable_subprotocol} ->
            refuse(
                Conn, Id, Request, 400, <<"WebSocket subprotocol ", Alpn/binary, " is required">>
            );
        {error, Reason} ->
            refuse(Conn, Id, Request, 400, iolist_to_binary(io_lib:format("~0p", [Reason])));
        {ok, Info} ->
            case trevrpc_admission:admit(Admission, Request) of
                true ->
                    case (maps:get(room, Opts))() of
                        ok ->
                            accept(Conn, Id, Request, Info, Start);
                        {error, Reason} ->
                            refuse(Conn, Id, Request, 503, atom_to_binary(Reason))
                    end;
                false ->
                    refuse(Conn, Id, Request, 403, <<"forbidden">>);
                {error, Reason} ->
                    logger:error("trevrpc: the WebTransport admission failed: ~p", [Reason]),
                    refuse(Conn, Id, Request, 500, <<"internal server error">>)
            end
    end.

accept(Conn, Id, Request, Info, Start) ->
    case h1:accept_upgrade(Conn, Id, ws_h1_upgrade:response_headers(Info)) of
        {ok, Socket, Received} ->
            case Start(Socket, Received) of
                ok ->
                    logger:info("trevrpc: WebSocket session from ~s", [describe(Request)]);
                {error, Reason} ->
                    logger:warning("trevrpc: refused WebSocket session from ~s: ~p", [
                        describe(Request), Reason
                    ]),
                    Close = ws_frame:encode({close, ?TRY_AGAIN_LATER, <<>>}, server),
                    _ = ssl:send(Socket, Close),
                    _ = ssl:close(Socket),
                    ok
            end;
        {error, Reason} ->
            logger:warning("trevrpc: WebSocket upgrade from ~s failed: ~p", [
                describe(Request), Reason
            ])
    end.

refuse(Conn, Id, Request, Status, Message) ->
    logger:warning("trevrpc: refused WebSocket upgrade from ~s with ~b: ~s", [
        describe(Request), Status, Message
    ]),
    respond(Conn, Id, Status, Message).

respond(Conn, Id, Status, Message) ->
    Headers = [
        {<<"content-type">>, <<"text/plain; charset=utf-8">>}, {<<"connection">>, <<"close">>}
    ],
    _ = h1:respond(Conn, Id, Status, Headers, [Message, $\n]),
    ok.

describe(Request) ->
    Peer =
        case Request of
            #{peer := {Ip, Port}} -> io_lib:format("~s:~b", [inet:ntoa(Ip), Port]);
            #{} -> "an unknown peer"
        end,
    io_lib:format("~s (origin ~s)", [Peer, maps:get(origin, Request, <<"none">>)]).

tokens(Value) ->
    [string:lowercase(string:trim(Token)) || Token <- binary:split(Value, <<",">>, [global])].
