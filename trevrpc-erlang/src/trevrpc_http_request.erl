%% @doc A request of the HTTP fallback: an HTTP request on the server's
%% sockets that TrevRPC does not claim, such as one for the static content
%% of a web application served from the same port.
%%
%% With `http_fallback => Handler', the server serves HTTP/3 on its UDP
%% socket, and HTTP/2 and HTTP/1.1 on its TCP listener, even with `http' and
%% `webtransport' off. TrevRPC claims the HTTP path while either of them is
%% on; every other request goes to `Handler', a fun of one argument run in
%% a process of its own, which answers with the functions here:
%%
%% ```
%% Fallback = fun(Req) ->
%%     case trevrpc_http_request:path(Req) of
%%         <<"/">> ->
%%             trevrpc_http_request:reply(
%%                 Req, 200, [{<<"content-type">>, <<"text/html">>}], Html
%%             );
%%         _ ->
%%             trevrpc_http_request:reply(Req, 404, [], <<>>)
%%     end
%% end,
%% trevrpc_server:start_link(#{..., http_fallback => Fallback}).
%% '''
%%
%% A request takes a stream slot of its connection like a call, so a
%% connection at `max_streams_per_connection' answers it with `503' without
%% running the handler, as does a server that is shutting down. It does not
%% count toward `max_concurrent_requests', and the admissions do not apply
%% to it. On shutdown, a request in flight gets the graceful shutdown
%% timeout to finish before its process is killed and its stream reset.
%%
%% The handler answers with {@link reply/4}, or with {@link stream_reply/3}
%% and then {@link stream_body/3}. A handler that returns without answering
%% gets `204'; one that fails before it answered, `500'; and one that fails
%% while it streams a response body, or returns before it ended the body,
%% has the body ended for it, or the stream reset when it failed. A client
%% that resets the stream kills the handler's process. HTTP/1.1 and HTTP/2
%% responses advertise HTTP/3 through `Alt-Svc' unless the handler set the
%% header itself.
%%
%% The request is only for the process that runs the handler: the
%% functions fail with `badarg' in any other.
-module(trevrpc_http_request).

-export([
    method/1,
    path/1,
    qs/1,
    headers/1,
    header/2,
    header/3,
    authority/1,
    protocol/1,
    peer/1,
    read_body/1,
    read_body/2,
    reply/4,
    stream_reply/3,
    stream_body/3
]).

%% Called by trevrpc_server_conn.
-export([start/1]).

-export_type([req/0, handler/0, headers/0, read_opts/0]).

-opaque req() :: #{
    method := binary(),
    path := binary(),
    qs := binary(),
    headers := headers(),
    authority := binary(),
    protocol := binary(),
    peer := {inet:ip_address(), inet:port_number()} | undefined,
    %% What the functions use, and the process they may be used in.
    stream := trevrpc_stream:stream(),
    alt_svc := binary() | undefined,
    owner := pid()
}.

%% Header names are in lower case.
-type headers() :: [{binary(), binary()}].

-type handler() :: fun((req()) -> term()).

%% `length': the bytes read_body/2 collects before it returns, at least
%% one chunk; `timeout': the milliseconds it waits for them.
-type read_opts() :: #{length => pos_integer(), timeout => timeout()}.

%% The response so far, in the process dictionary of the request's process.
-define(RESPONSE, {?MODULE, response}).
%% Whether the client finished the request body.
-define(FIN, {?MODULE, fin}).
-define(READ_LENGTH, 8000000).
-define(READ_TIMEOUT, 15000).
-define(CANCELLED, 1).
%% Headers that only HTTP/1.1 uses, which HTTP/2 and HTTP/3 forbid (RFC
%% 9113 Section 8.2.2, RFC 9114 Section 4.2).
-define(CONNECTION_HEADERS, [
    <<"connection">>,
    <<"keep-alive">>,
    <<"proxy-connection">>,
    <<"te">>,
    <<"transfer-encoding">>,
    <<"upgrade">>
]).

%%% Requests

-spec method(req()) -> binary().
method(Req) -> get(method, Req).

%% @doc The path of the request target, without its query.
-spec path(req()) -> binary().
path(Req) -> get(path, Req).

%% @doc The query of the request target, without its `?': empty when
%% there is none.
-spec qs(req()) -> binary().
qs(Req) -> get(qs, Req).

%% @doc The request's header fields, pseudo-headers left out, with names
%% in lower case.
-spec headers(req()) -> headers().
headers(Req) -> get(headers, Req).

%% @doc The first value of header `Name' (in lower case), or `undefined'.
-spec header(binary(), req()) -> binary() | undefined.
header(Name, Req) -> header(Name, Req, undefined).

-spec header(binary(), req(), Default) -> binary() | Default.
header(Name, Req, Default) ->
    case lists:keyfind(Name, 1, get(headers, Req)) of
        {_, Value} -> Value;
        false -> Default
    end.

%% @doc The request's `:authority', or its `Host' header.
-spec authority(req()) -> binary().
authority(Req) -> get(authority, Req).

%% @doc `<<"HTTP/3.0">>', `<<"HTTP/2.0">>' or `<<"HTTP/1.1">>'.
-spec protocol(req()) -> binary().
protocol(Req) -> get(protocol, Req).

%% @doc The client's address, when known.
-spec peer(req()) -> {inet:ip_address(), inet:port_number()} | undefined.
peer(Req) -> get(peer, Req).

get(Key, #{owner := Owner} = Req) when Owner =:= self() -> maps:get(Key, Req);
get(_Key, Req) -> error(badarg, [Req]).

%%% Request bodies

%% @doc Reads the request body, with the default options.
-spec read_body(req()) -> {ok, binary(), req()} | {more, binary(), req()} | {error, term()}.
read_body(Req) ->
    read_body(Req, #{}).

%% @doc Reads the request body: `{ok, Data, Req}' with the rest of it,
%% `{more, Data, Req}' with at least `length' bytes (8 MB by default) of
%% it, or with what arrived within `timeout' (15 s by default), and
%% `{error, timeout}' if nothing did. The client may send a window past
%% what the handler read, so a handler that reads slowly holds it back.
-spec read_body(req(), read_opts()) ->
    {ok, binary(), req()} | {more, binary(), req()} | {error, term()}.
read_body(Req, Opts) ->
    Stream = get(stream, Req),
    case get(?FIN) of
        true ->
            {ok, <<>>, Req};
        _ ->
            Length = maps:get(length, Opts, ?READ_LENGTH),
            Deadline = deadline(maps:get(timeout, Opts, ?READ_TIMEOUT)),
            collect(Req, Stream, Length, Deadline, [], 0)
    end.

deadline(infinity) -> infinity;
deadline(Timeout) -> erlang:monotonic_time(millisecond) + Timeout.

collect(Req, Stream, Length, Deadline, Acc, Size) ->
    Ref = trevrpc_stream:ref(Stream),
    receive
        {trevrpc_stream, Ref, {data, Bin, Fin}} ->
            %% The bytes are the handler's now; the client may send more.
            _ = trevrpc_transport:consume(
                trevrpc_stream:transport(Stream), trevrpc_stream:id(Stream), byte_size(Bin)
            ),
            Data = [Acc, Bin],
            Total = Size + byte_size(Bin),
            if
                Fin ->
                    put(?FIN, true),
                    {ok, iolist_to_binary(Data), Req};
                Total >= Length ->
                    {more, iolist_to_binary(Data), Req};
                true ->
                    collect(Req, Stream, Length, Deadline, Data, Total)
            end
    after trevrpc_stream:timeout(Deadline) ->
        case Size of
            0 -> {error, timeout};
            _ -> {more, iolist_to_binary(Acc), Req}
        end
    end.

%%% Responses

%% @doc Answers with the whole response. The response to a `HEAD' request
%% leaves `Body' out, and keeps the headers that describe it, such as
%% `content-length'.
-spec reply(req(), 100..599, headers(), iodata()) -> ok | {error, term()}.
reply(Req, Status, Headers, Body) ->
    Stream = get(stream, Req),
    Transport = trevrpc_stream:transport(Stream),
    Id = trevrpc_stream:id(Stream),
    case get(?RESPONSE) of
        none ->
            put(?RESPONSE, done),
            Sent = response_headers(Headers, Req),
            case get(method, Req) of
                <<"HEAD">> -> head(Transport, Id, Status, Sent);
                _ -> trevrpc_transport:respond(Transport, Id, Status, Sent, Body)
            end;
        _ ->
            error(response_already_sent, [Req, Status, Headers, Body])
    end.

%% A response head, then the end of the stream, where a whole response
%% would say that it has no body.
head(Transport, Id, Status, Headers) ->
    maybe
        ok ?= trevrpc_transport:send_response(Transport, Id, Status, Headers),
        trevrpc_transport:send_data(Transport, Id, <<>>, true, infinity)
    end.

%% @doc Answers with the response head, whose body follows with
%% {@link stream_body/3}. Over HTTP/1.1, a response without
%% `content-length' is sent chunked.
-spec stream_reply(req(), 100..599, headers()) -> ok | {error, term()}.
stream_reply(Req, Status, Headers) ->
    Stream = get(stream, Req),
    case get(?RESPONSE) of
        none ->
            put(?RESPONSE, streaming),
            trevrpc_transport:send_response(
                trevrpc_stream:transport(Stream),
                trevrpc_stream:id(Stream),
                Status,
                response_headers(Headers, Req)
            );
        _ ->
            error(response_already_sent, [Req, Status, Headers])
    end.

%% @doc Sends part of the response body, the last part with `fin'. It
%% waits for a client that reads slowly, and returns an error once the
%% client is gone.
-spec stream_body(req(), iodata(), fin | nofin) -> ok | {error, term()}.
stream_body(Req, Data, IsFin) ->
    Stream = get(stream, Req),
    case get(?RESPONSE) of
        streaming ->
            Fin = IsFin =:= fin,
            _ =
                case Fin of
                    true -> put(?RESPONSE, done);
                    false -> ok
                end,
            Sent =
                case get(method, Req) of
                    <<"HEAD">> -> <<>>;
                    _ -> Data
                end,
            case iolist_size(Sent) of
                0 when not Fin ->
                    ok;
                _ ->
                    case trevrpc_stream:send(Stream, Sent, Fin, infinity) of
                        {ok, _} -> ok;
                        {error, Reason, _} -> {error, Reason}
                    end
            end;
        _ ->
            error(badarg, [Req, Data, IsFin])
    end.

%% Over HTTP/1.1 and HTTP/2, `Alt-Svc' advertises HTTP/3 unless the handler
%% set it; HTTP/2 and HTTP/3 drop the headers they forbid.
response_headers(Headers, #{protocol := Protocol, alt_svc := AltSvc}) ->
    Lower = [{string:lowercase(iolist_to_binary(Name)), Value} || {Name, Value} <- Headers],
    Allowed =
        case Protocol of
            <<"HTTP/1.1">> ->
                Lower;
            _ ->
                [Header || {Name, _} = Header <- Lower, not lists:member(Name, ?CONNECTION_HEADERS)]
        end,
    case
        Protocol =/= <<"HTTP/3.0">> andalso AltSvc =/= undefined andalso
            not lists:keymember(<<"alt-svc">>, 1, Allowed)
    of
        true -> Allowed ++ [{<<"alt-svc">>, AltSvc}];
        false -> Allowed
    end.

%%% The request's process

%% @doc Starts the process of a request, linked to the caller, the
%% connection's process, which routes the stream's events to it tagged with
%% `ref' and is `router'. `request' has what the accessors return.
-spec start(#{
    transport := trevrpc_transport:t(),
    id := non_neg_integer(),
    ref := reference(),
    router := pid(),
    handler := handler(),
    alt_svc := binary() | undefined,
    request := #{
        method := binary(),
        target := binary(),
        headers := [{binary(), iodata()}],
        protocol := binary(),
        peer := {inet:ip_address(), inet:port_number()} | undefined
    }
}) -> {pid(), reference()}.
start(Spec) ->
    {Pid, Monitor} = spawn_opt(fun() -> run(Spec) end, [link, monitor]),
    {Pid, Monitor}.

run(#{transport := Transport, id := Id, ref := Ref, router := Router} = Spec) ->
    #{method := Method, target := Target, headers := RawHeaders} =
        Request = maps:get(
            request, Spec
        ),
    {Path, Qs} =
        case binary:split(Target, <<"?">>) of
            [P, Q] -> {P, Q};
            [P] -> {P, <<>>}
        end,
    Headers = [
        {iolist_to_binary(Name), iolist_to_binary(Value)}
     || {Name, Value} <- RawHeaders, not pseudo(Name)
    ],
    Authority = trevrpc_http:authority(RawHeaders),
    %% Only writes go through the stream; its frame reader is unused.
    Stream = trevrpc_stream:new(Transport, Id, Ref, Router, trevrpc_frame:reader(1, #{})),
    Req = #{
        method => Method,
        path => Path,
        qs => Qs,
        headers => Headers,
        authority => Authority,
        protocol => maps:get(protocol, Request),
        peer => maps:get(peer, Request),
        stream => Stream,
        alt_svc => maps:get(alt_svc, Spec),
        owner => self()
    },
    put(?RESPONSE, none),
    Outcome =
        try (maps:get(handler, Spec))(Req) of
            _ -> ok
        catch
            Class:Reason:Stacktrace -> {failed, Class, Reason, Stacktrace}
        end,
    finish(Outcome, Req, Stream),
    %% The body the handler left unread no longer holds the client back.
    trevrpc_stream:release(Stream).

pseudo(<<$:, _/binary>>) -> true;
pseudo(_) -> false.

finish(Outcome, Req, Stream) ->
    case {get(?RESPONSE), Outcome} of
        {done, ok} ->
            ok;
        {done, {failed, Class, Reason, Stacktrace}} ->
            log_failure(Req, Class, Reason, Stacktrace);
        {none, ok} ->
            _ = reply(Req, 204, [], <<>>),
            ok;
        {none, {failed, Class, Reason, Stacktrace}} ->
            log_failure(Req, Class, Reason, Stacktrace),
            _ = reply(Req, 500, [], <<>>),
            ok;
        {streaming, ok} ->
            _ = stream_body(Req, <<>>, fin),
            ok;
        {streaming, {failed, Class, Reason, Stacktrace}} ->
            log_failure(Req, Class, Reason, Stacktrace),
            trevrpc_stream:abort(Stream, ?CANCELLED)
    end.

log_failure(Req, Class, Reason, Stacktrace) ->
    trevrpc_log:log(error, server, fallback_panic, (trevrpc_log:exception(
        Class, Reason, Stacktrace
    ))#{
        http_method => get(method, Req), path => get(path, Req)
    }).
