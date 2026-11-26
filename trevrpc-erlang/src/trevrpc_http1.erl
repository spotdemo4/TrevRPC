%% @private The WebSocket upgrades of HTTP/1.1 on the server's TCP listener,
%% the connections that negotiated `http/1.1', or no ALPN at all, which
%% `trevrpc_server_conn' serves through erlang_h1.
%%
%% A WebSocket upgrade (a `GET' with `Upgrade: websocket') at the HTTP path
%% that offers subprotocol `qmux-02.trevrpc-1', and that the admission
%% accepts, carries a QMux session (QMux over WebSocket, for browsers). The
%% checks here answer the others, in the order the Go and Rust servers
%% check them: `505' for a request at the HTTP path that is not a WebSocket
%% upgrade (TrevRPC over HTTP needs HTTP/2 or HTTP/3), `400' for an upgrade
%% without the subprotocol or with invalid WebSocket headers, `403' for one
%% the admission refused and `500' when the admission failed. Requests off
%% the HTTP path go to the HTTP fallback, or get `404' without one.
-module(trevrpc_http1).

-export([check_upgrade/5, fields/1, refuse_session/1]).

%% WebSocket close code 1013: try again later, for a session refused after
%% its upgrade.
-define(TRY_AGAIN_LATER, 1013).

%% @doc Checks a request at the HTTP path for a WebSocket session, with the
%% WebTransport options (`path' and `admission'): `{ok, Headers, Request}'
%% with the headers of its `101', or `{refuse, Status, Message, Request}',
%% with the request the admission saw, or would have.
-spec check_upgrade(
    binary(),
    binary(),
    [{binary(), binary()}],
    {inet:ip_address(), inet:port_number()} | undefined,
    #{path := binary(), admission := trevrpc_admission:admission(), _ => _}
) -> {ok, [{binary(), binary()}], map()} | {refuse, 400..599, binary(), map()}.
check_upgrade(Method, Target, Headers, Peer, Opts) ->
    Request = request(Method, Target, Headers, Peer),
    case check(Method, Headers, Request, Opts) of
        {ok, Accept} -> {ok, Accept, Request};
        {refuse, Status, Message} -> {refuse, Status, Message, Request}
    end.

request(Method, Target, Headers, Peer) ->
    Request0 = #{
        method => Method,
        protocol => <<"HTTP/1.1">>,
        path => trevrpc_http:path(Target),
        authority => trevrpc_http:header(<<"host">>, Headers),
        secure => true,
        headers => Headers
    },
    Request1 =
        case trevrpc_http:header(<<"origin">>, Headers) of
            <<>> -> Request0;
            Origin -> Request0#{origin => Origin}
        end,
    case Peer of
        undefined -> Request1;
        _ -> Request1#{peer => Peer}
    end.

check(Method, Headers, Request, #{admission := Admission}) ->
    Upgrade =
        Method =:= <<"GET">> andalso
            lists:member(<<"websocket">>, tokens(trevrpc_http:header(<<"upgrade">>, Headers))),
    Alpn = trevrpc:qmux_alpn(),
    case Upgrade of
        false ->
            {refuse, 505, <<"TrevRPC over HTTP requires HTTP/2 or HTTP/3">>};
        true ->
            case ws_h1_upgrade:validate_request(Headers, #{required_subprotocols => [Alpn]}) of
                {error, no_acceptable_subprotocol} ->
                    {refuse, 400, <<"WebSocket subprotocol ", Alpn/binary, " is required">>};
                {error, Reason} ->
                    {refuse, 400, iolist_to_binary(io_lib:format("~0p", [Reason]))};
                {ok, Info} ->
                    case trevrpc_admission:admit(Admission, Request) of
                        true ->
                            {ok, ws_h1_upgrade:response_headers(Info)};
                        false ->
                            {refuse, 403, <<"forbidden">>};
                        {error, Reason} ->
                            trevrpc_log:log(error, server, admission_panic, Reason#{
                                admission => <<"WebTransport">>
                            }),
                            {refuse, 500, <<"internal server error">>}
                    end
            end
    end.

%% @doc The peer and origin of an upgrade's request, for its reports:
%% never its other headers, which may hold credentials.
-spec fields(map()) -> #{peer := term(), origin := binary() | undefined}.
fields(Request) ->
    #{peer => maps:get(peer, Request, undefined), origin => maps:get(origin, Request, undefined)}.

%% @doc Closes the upgraded socket of a session that could not start, with
%% WebSocket close code 1013.
-spec refuse_session(ssl:sslsocket()) -> ok.
refuse_session(Socket) ->
    Close = ws_frame:encode({close, ?TRY_AGAIN_LATER, <<>>}, server),
    _ = ssl:send(Socket, Close),
    _ = ssl:close(Socket),
    ok.

tokens(Value) ->
    [string:lowercase(string:trim(Token)) || Token <- binary:split(Value, <<",">>, [global])].
