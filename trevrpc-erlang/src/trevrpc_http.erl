%% @private The HTTP mapping of TrevRPC, shared by HTTP/3 and HTTP/2: one
%% RPC per request stream, a `POST' to the HTTP path with media type
%% `application/trevrpc', whose bodies carry the usual frames.
%%
%% HTTP statuses are used only before a call is accepted, checked in the
%% order the Go and Rust servers check them: `404' off the HTTP path, `405'
%% for another method, `415' for another media type, `403' for a request
%% the admission refused and `500' when the admission failed. The
%% connection answers `503' itself when it is at its stream limit or
%% shutting down. After `200', the call ends with a TrevRPC STATUS like any
%% other.
-module(trevrpc_http).

-export([check/6, check_connect/4, accepted_headers/1, refusal_headers/2, path/1, header/2]).
-export([authority/1, server_name/1, misdirected/2]).

-export_type([options/0, refusal/0]).

-define(MEDIA_TYPE, <<"application/trevrpc">>).

%% `path': the HTTP path; `admission': which requests to accept, or
%% `undefined' for all of them; `alt_svc': the `Alt-Svc' value HTTP/2
%% responses advertise HTTP/3 with.
-type options() :: #{
    path := binary(),
    admission := trevrpc_admission:admission() | undefined,
    alt_svc => binary()
}.

%% A request turned away: its HTTP status, extra headers and message.
-type refusal() :: {refuse, 400..599, [{binary(), binary()}], binary()}.

%% @doc Checks a request for a call: `Version' is `<<"HTTP/3.0">>' or
%% `<<"HTTP/2.0">>', `Target' the request's path and query, `Headers' its
%% header list, pseudo-headers included, and `Peer' the client's address
%% when known.
-spec check(
    binary(),
    binary(),
    binary(),
    [{binary(), binary()}],
    {inet:ip_address(), inet:port_number()} | undefined,
    options()
) -> ok | refusal().
check(Version, Method, Target, Headers, Peer, #{path := HttpPath} = Opts) ->
    Path = path(Target),
    if
        Path =/= HttpPath ->
            {refuse, 404, [], <<"not found">>};
        Method =/= <<"POST">> ->
            {refuse, 405, [{<<"allow">>, <<"POST">>}], <<"method must be POST">>};
        true ->
            case media_type(Headers) of
                true -> admit(Version, Method, Path, Headers, Peer, Opts, <<"HTTP">>);
                false -> {refuse, 415, [], <<"unsupported media type">>}
            end
    end.

%% @doc Checks an HTTP/3 CONNECT request for a WebTransport session, with
%% the WebTransport options (`path' and `admission'): `404' off the path,
%% `403' for a request the admission refused and `500' when it failed.
%% erlang-webtransport checks the rest of the request.
-spec check_connect(
    binary(),
    [{binary(), binary()}],
    {inet:ip_address(), inet:port_number()} | undefined,
    #{path := binary(), admission := trevrpc_admission:admission() | undefined, _ => _}
) -> ok | refusal().
check_connect(Target, Headers, Peer, #{path := HttpPath} = Opts) ->
    case path(Target) of
        HttpPath ->
            admit(<<"HTTP/3.0">>, <<"CONNECT">>, HttpPath, Headers, Peer, Opts, <<"WebTransport">>);
        _ ->
            {refuse, 404, [], <<"not found">>}
    end.

%% Exactly one Content-Type, the TrevRPC media type in any case, without
%% parameters.
media_type(Headers) ->
    case [Value || {<<"content-type">>, Value} <- Headers] of
        [Value] -> string:lowercase(string:trim(Value)) =:= ?MEDIA_TYPE;
        _ -> false
    end.

admit(_Version, _Method, _Path, _Headers, _Peer, #{admission := undefined}, _What) ->
    ok;
admit(Version, Method, Path, Headers, Peer, #{admission := Admission}, What) ->
    Plain = [Header || {<<C, _/binary>>, _} = Header <- Headers, C =/= $:],
    Request0 = #{
        method => Method,
        protocol => Version,
        path => Path,
        authority => authority(Headers),
        secure => true,
        headers => Plain
    },
    Request1 =
        case header(<<"origin">>, Headers) of
            <<>> -> Request0;
            Origin -> Request0#{origin => Origin}
        end,
    Request =
        case Peer of
            undefined -> Request1;
            _ -> Request1#{peer => Peer}
        end,
    case trevrpc_admission:admit(Admission, Request) of
        true ->
            ok;
        false ->
            {refuse, 403, [], <<What/binary, " admission denied">>};
        {error, Reason} ->
            trevrpc_log:log(error, server, admission_panic, Reason#{admission => What}),
            {refuse, 500, [], <<"internal server error">>}
    end.

%% @doc The headers of the `200' that accepts a call.
-spec accepted_headers(options()) -> [{binary(), binary()}].
accepted_headers(Opts) ->
    [{<<"content-type">>, ?MEDIA_TYPE} | alt_svc(Opts)].

%% @doc The headers of a refusal with `Extra' headers of its own.
-spec refusal_headers([{binary(), binary()}], #{alt_svc => binary(), _ => _}) ->
    [{binary(), binary()}].
refusal_headers(Extra, Opts) ->
    [{<<"content-type">>, <<"text/plain; charset=utf-8">>} | Extra ++ alt_svc(Opts)].

alt_svc(#{alt_svc := AltSvc}) -> [{<<"alt-svc">>, AltSvc}];
alt_svc(#{}) -> [].

%% @doc The path of a request target, without its query.
-spec path(binary()) -> binary().
path(Target) ->
    hd(binary:split(Target, <<"?">>)).

%% @doc The authority a request is for: its `:authority', or its `host'
%% header without one, as over HTTP/1.1; empty without either.
-spec authority([{binary(), iodata()}]) -> binary().
authority(Headers) ->
    case header(<<":authority">>, Headers) of
        <<>> -> header(<<"host">>, Headers);
        Value -> Value
    end.

%% @doc The TLS server name (SNI) a client sent, in the form {@link
%% misdirected/2} compares: in lower case, without a trailing dot. No name,
%% an empty one and an IP address, which RFC 6066 forbids but some clients
%% send, are all `undefined'.
-spec server_name(binary() | string() | undefined) -> binary() | undefined.
server_name(undefined) ->
    undefined;
server_name(Name) when is_list(Name) ->
    server_name(unicode:characters_to_binary(Name));
server_name(Name) when is_binary(Name) ->
    case is_ip_address(Name) of
        true ->
            undefined;
        false ->
            case canonical_host(Name) of
                <<>> -> undefined;
                Canonical -> Canonical
            end
    end.

%% @doc Whether a request for `Authority' (see {@link authority/1}) came over
%% a connection for `ServerName' (see {@link server_name/1}), which makes it
%% misdirected (RFC 9110 Section 15.5.20): browsers send such requests over
%% a connection whose certificate also covers the name they want
%% (connection coalescing, RFC 9113 Section 9.1.1 and RFC 9114 Section 3.3),
%% and retry them on a connection of their own after a `421'. The port, a
%% trailing dot and case do not count. Browsers coalesce connections between
%% names only, so neither a connection without a server name, as clients
%% open to an IP address, nor a request without a host or for an IP
%% address, which a client sends when it reaches the server by address and
%% names it only in TLS, is ever misdirected.
-spec misdirected(binary(), binary() | undefined) -> boolean().
misdirected(_Authority, undefined) ->
    false;
misdirected(Authority, ServerName) ->
    Host = authority_host(Authority),
    Host =/= <<>> andalso not is_ip_address(Host) andalso canonical_host(Host) =/= ServerName.

%% The host of an authority, without its port and the brackets of an IPv6
%% address.
authority_host(<<"[", Rest/binary>>) ->
    hd(binary:split(Rest, <<"]">>));
authority_host(Authority) ->
    hd(binary:split(Authority, <<":">>)).

%% A host in ASCII lower case, as DNS compares names, without a trailing
%% dot. Clients may send any bytes, so it takes no Unicode.
canonical_host(Host) ->
    Size = byte_size(Host) - 1,
    Name =
        case Host of
            <<Trimmed:Size/binary, ".">> -> Trimmed;
            _ -> Host
        end,
    <<<<(ascii_lower(C))>> || <<C>> <= Name>>.

ascii_lower(C) when C >= $A, C =< $Z -> C + 32;
ascii_lower(C) -> C.

is_ip_address(Host) ->
    case inet:parse_strict_address(binary_to_list(Host)) of
        {ok, _} -> true;
        {error, _} -> false
    end.

%% @doc The first value of a header, or empty. Names are in lower case.
-spec header(binary(), [{binary(), iodata()}]) -> binary().
header(Name, Headers) ->
    case lists:keyfind(Name, 1, Headers) of
        {_, Value} -> iolist_to_binary(Value);
        false -> <<>>
    end.
