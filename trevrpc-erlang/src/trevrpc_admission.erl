%% @doc Admission of browser sessions: which QMux over WebSocket upgrades
%% (and WebTransport sessions) a server accepts.
%%
%% An admission is a fun that gets the request and returns whether to
%% accept it. The `Origin' check is the defence against cross-site
%% WebSocket hijacking, so configure one before serving browsers:
%%
%% ```
%% trevrpc_server:start_link(#{
%%     ...,
%%     webtransport => true,
%%     webtransport_admission => trevrpc_admission:same_origin([<<"https://app.example.com">>])
%% }).
%% '''
%%
%% Without one, {@link no_origin/1} admits only requests without an
%% `Origin', which browsers always send.
-module(trevrpc_admission).

-export([no_origin/1, same_origin/1, admit/2, normalize_origin/1]).

-export_type([request/0, admission/0]).

-define(DEFAULT_PORTS, #{<<"https">> => 443, <<"wss">> => 443, <<"http">> => 80, <<"ws">> => 80}).

%% A request for a session:
%% <ul>
%%   <li>`method': `<<"GET">>' for a WebSocket upgrade.</li>
%%   <li>`protocol': the HTTP version, such as `<<"HTTP/1.1">>'.</li>
%%   <li>`path', `authority' (the `Host'), and `origin' when the request
%%       has one.</li>
%%   <li>`secure': whether the request came over TLS.</li>
%%   <li>`headers': every request header, names in lower case.</li>
%%   <li>`peer': the client's address and port, when known.</li>
%% </ul>
-type request() :: #{
    method := binary(),
    protocol := binary(),
    path := binary(),
    authority := binary(),
    origin => binary(),
    secure := boolean(),
    headers := [{binary(), binary()}],
    peer => {inet:ip_address(), inet:port_number()}
}.

-type admission() :: fun((request()) -> boolean()).

%% @doc Admits requests without an `Origin': clients other than browsers.
-spec no_origin(request()) -> boolean().
no_origin(Request) ->
    not is_map_key(origin, Request).

%% @doc Admits requests without an `Origin', pages from the server's own
%% origin (the request's scheme and authority), and pages from `Allowed'.
%% Origins compare as browsers serialize them: scheme and host in any case,
%% and a default port the same as none. `null' is never admitted. Raises
%% `badarg' for an entry of `Allowed' that is not an origin.
-spec same_origin([binary() | string()]) -> admission().
same_origin(Allowed) ->
    Origins = maps:from_keys(
        [
            case normalize_origin(iolist_to_binary(Origin)) of
                {ok, Normalized} -> Normalized;
                error -> error(badarg, [Allowed])
            end
         || Origin <- Allowed
        ],
        true
    ),
    fun(Request) -> same_origin(Request, Origins) end.

same_origin(#{origin := Origin} = Request, Allowed) ->
    case normalize_origin(Origin) of
        {ok, Normalized} ->
            Scheme =
                case maps:get(secure, Request) of
                    true -> <<"https">>;
                    false -> <<"http">>
                end,
            Own = normalize_origin(Scheme, maps:get(authority, Request)),
            Own =:= {ok, Normalized} orelse is_map_key(Normalized, Allowed);
        error ->
            false
    end;
same_origin(_Request, _Allowed) ->
    true.

%% @doc Runs `Admission' on `Request'. An admission that raises refuses
%% the request, and the error is returned to be reported.
-spec admit(admission(), request()) -> boolean() | {error, term()}.
admit(Admission, Request) ->
    try Admission(Request) of
        true -> true;
        false -> false;
        Other -> {error, {bad_return, Other}}
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

%% @doc An origin (`scheme://host[:port]') with its scheme and host in lower
%% case and its port omitted when it is the scheme's default, or `error' for
%% anything else, such as the opaque origin `null'.
-spec normalize_origin(binary()) -> {ok, binary()} | error.
normalize_origin(Origin) ->
    case binary:split(Origin, <<"://">>) of
        [Scheme, HostPort] -> normalize_origin(Scheme, HostPort);
        _ -> error
    end.

normalize_origin(Scheme0, HostPort) ->
    Scheme = string:lowercase(Scheme0),
    maybe
        true ?= valid_scheme(Scheme),
        true ?= HostPort =/= <<>>,
        nomatch ?= binary:match(HostPort, [<<"/">>, <<"?">>, <<"#">>, <<"@">>, <<"\\">>, <<" ">>]),
        {ok, Host, Port} ?= split_host(HostPort),
        true ?= Host =/= <<>>,
        {ok, Suffix} ?= port_suffix(Scheme, Port),
        {ok, <<Scheme/binary, "://", (string:lowercase(Host))/binary, Suffix/binary>>}
    else
        _ -> error
    end.

valid_scheme(<<First, Rest/binary>>) when First >= $a, First =< $z ->
    lists:all(
        fun(C) ->
            (C >= $a andalso C =< $z) orelse (C >= $0 andalso C =< $9) orelse C =:= $+ orelse
                C =:= $- orelse C =:= $.
        end,
        binary_to_list(Rest)
    );
valid_scheme(_) ->
    false.

%% An IPv6 address must be in brackets.
split_host(<<"[", Rest/binary>>) ->
    case binary:split(Rest, <<"]">>) of
        [Address, <<>>] -> ipv6(Address, <<>>);
        [Address, <<":", Port/binary>>] -> ipv6(Address, Port);
        _ -> error
    end;
split_host(HostPort) ->
    case binary:split(HostPort, <<":">>) of
        [Host] ->
            {ok, Host, <<>>};
        [Host, Port] ->
            case binary:match(Port, <<":">>) of
                nomatch -> {ok, Host, Port};
                _ -> error
            end
    end.

ipv6(Address, Port) ->
    case binary:match(Address, <<":">>) of
        nomatch -> error;
        _ -> {ok, <<"[", Address/binary, "]">>, Port}
    end.

port_suffix(_Scheme, <<>>) ->
    {ok, <<>>};
port_suffix(Scheme, Port) ->
    case lists:all(fun(C) -> C >= $0 andalso C =< $9 end, binary_to_list(Port)) of
        true ->
            case binary_to_integer(Port) of
                N when N > 65535 -> error;
                N when N =:= map_get(Scheme, ?DEFAULT_PORTS) -> {ok, <<>>};
                N -> {ok, <<":", (integer_to_binary(N))/binary>>}
            end;
        false ->
            error
    end.
