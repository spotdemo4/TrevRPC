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

-export([check/6, accepted_headers/1, refusal_headers/2, path/1, header/2]).

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
                true -> admit(Version, Method, Path, Headers, Peer, Opts);
                false -> {refuse, 415, [], <<"unsupported media type">>}
            end
    end.

%% Exactly one Content-Type, the TrevRPC media type in any case, without
%% parameters.
media_type(Headers) ->
    case [Value || {<<"content-type">>, Value} <- Headers] of
        [Value] -> string:lowercase(string:trim(Value)) =:= ?MEDIA_TYPE;
        _ -> false
    end.

admit(_Version, _Method, _Path, _Headers, _Peer, #{admission := undefined}) ->
    ok;
admit(Version, Method, Path, Headers, Peer, #{admission := Admission}) ->
    Plain = [Header || {<<C, _/binary>>, _} = Header <- Headers, C =/= $:],
    Authority =
        case header(<<":authority">>, Headers) of
            <<>> -> header(<<"host">>, Headers);
            Value -> Value
        end,
    Request0 = #{
        method => Method,
        protocol => Version,
        path => Path,
        authority => Authority,
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
            {refuse, 403, [], <<"HTTP admission denied">>};
        {error, Reason} ->
            logger:error("trevrpc: the HTTP admission failed: ~p", [Reason]),
            {refuse, 500, [], <<"internal server error">>}
    end.

%% @doc The headers of the `200' that accepts a call.
-spec accepted_headers(options()) -> [{binary(), binary()}].
accepted_headers(Opts) ->
    [{<<"content-type">>, ?MEDIA_TYPE} | alt_svc(Opts)].

%% @doc The headers of a refusal with `Extra' headers of its own.
-spec refusal_headers([{binary(), binary()}], options()) -> [{binary(), binary()}].
refusal_headers(Extra, Opts) ->
    [{<<"content-type">>, <<"text/plain; charset=utf-8">>} | Extra ++ alt_svc(Opts)].

alt_svc(#{alt_svc := AltSvc}) -> [{<<"alt-svc">>, AltSvc}];
alt_svc(#{}) -> [].

%% @doc The path of a request target, without its query.
-spec path(binary()) -> binary().
path(Target) ->
    hd(binary:split(Target, <<"?">>)).

%% @doc The first value of a header, or empty. Names are in lower case.
-spec header(binary(), [{binary(), iodata()}]) -> binary().
header(Name, Headers) ->
    case lists:keyfind(Name, 1, Headers) of
        {_, Value} -> iolist_to_binary(Value);
        false -> <<>>
    end.
