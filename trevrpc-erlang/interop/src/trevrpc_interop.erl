%% @doc The Erlang interop executable; see interop/README.md for the contract.
%%
%% ```
%% trevrpc-interop-erlang server --cert <pem> --key <pem> [--host 127.0.0.1] [--port 0]
%%                               [--web-root <dir>] [--dictionary <file>]
%% trevrpc-interop-erlang client --addr <host:port> --ca <pem> [--server-name localhost]
%%                               [--transport quic|tcp|auto] [--tcp-addr <host:port>]
%%                               [--expect-transport quic|qmux] [--dictionary <file>]
%%                               [--scenarios <name,...>]
%% '''
%%
%% The runtime serves and calls native QUIC only so far. The server reports
%% only its native port and the scenarios it supports; the client exits 77
%% for a transport it lacks, and reports the scenarios it does not implement
%% as skipped.
-module(trevrpc_interop).

-export([main/0, server_scenarios/0]).

-define(ECHO, "/trevrpc.interop.v1.Echo/").

%% @doc The scenarios this server passes; clients skip the others.
-spec server_scenarios() -> [binary()].
server_scenarios() ->
    [
        <<"unary">>,
        <<"unary-metadata">>,
        <<"unary-error">>,
        <<"unary-retry-delay">>,
        <<"unary-large">>,
        <<"client-stream">>,
        <<"server-stream">>,
        <<"server-stream-error">>,
        <<"bidi">>,
        <<"bidi-error">>,
        <<"unary-headers">>,
        <<"server-stream-headers">>,
        <<"compressed">>,
        <<"connection-metadata">>
    ].

%% @doc Runs the command line after `-extra'.
-spec main() -> no_return().
main() ->
    %% Diagnostics go to stderr; stdout carries only the contract's lines.
    _ = logger:remove_handler(default),
    ok = logger:add_handler(default, logger_std_h, #{config => #{type => standard_error}}),
    halt(
        try
            {ok, _} = application:ensure_all_started(trevrpc),
            run(init:get_plain_arguments())
        catch
            throw:{usage, Message} ->
                io:format(standard_error, "~ts~n", [Message]),
                2;
            Class:Reason:Stack ->
                io:format(standard_error, "error: ~p~n", [{Class, Reason, Stack}]),
                1
        end
    ).

run(["server" | Args]) ->
    server(flags(Args, ["cert", "key", "host", "port", "web-root", "dictionary"]));
run(["client" | Args]) ->
    trevrpc_interop_client:run(
        flags(Args, [
            "addr",
            "ca",
            "server-name",
            "transport",
            "tcp-addr",
            "expect-transport",
            "dictionary",
            "scenarios"
        ])
    );
run(_) ->
    throw({usage, "usage: trevrpc-interop-erlang (server|client) [--flag value]..."}).

flags(Args, Allowed) ->
    flags(Args, Allowed, #{}).

flags([], _Allowed, Flags) ->
    Flags;
flags(["--" ++ Name, Value | Rest], Allowed, Flags) ->
    case lists:member(Name, Allowed) of
        true -> flags(Rest, Allowed, Flags#{Name => Value});
        false -> throw({usage, "unknown flag --" ++ Name})
    end;
flags([Arg | _], _Allowed, _Flags) ->
    throw({usage, "unexpected argument " ++ Arg}).

%%% Server

server(#{"cert" := Cert, "key" := Key} = Flags) ->
    {ok, Host} = inet:parse_address(maps:get("host", Flags, "127.0.0.1")),
    %% The runtime does not compress yet, so --dictionary is accepted and
    %% unused; with no HTTP fallback, so is --web-root.
    {ok, Server} = trevrpc_server:start(#{
        ip => Host,
        port => list_to_integer(maps:get("port", Flags, "0")),
        certfile => Cert,
        keyfile => Key,
        routes => routes()
    }),
    {ok, Port} = trevrpc_server:port(Server),
    io:put_chars(json:encode(#{native => Port, scenarios => server_scenarios()})),
    io:nl(),
    %% Runs until stdin reaches EOF; SIGTERM stops the whole VM.
    wait_stdin(),
    ok = trevrpc_server:stop(Server),
    0;
server(_Flags) ->
    throw({usage, "server: --cert and --key are required"}).

wait_stdin() ->
    case io:get_line("") of
        eof -> ok;
        {error, _} -> ok;
        _ -> wait_stdin()
    end.

routes() ->
    #{
        <<?ECHO "Unary">> => #{kind => unary, handler => fun unary/2},
        <<?ECHO "ClientStream">> => #{kind => client_streaming, handler => fun client_stream/1},
        <<?ECHO "ServerStream">> => #{kind => server_streaming, handler => fun server_stream/2},
        <<?ECHO "Bidi">> => #{kind => bidirectional_streaming, handler => fun bidi/1}
    }.

unary(Bin, Call) ->
    with_request(Bin, fun(#{message := Message} = Request) ->
        start(Call),
        case failure(Request) of
            none ->
                Suffix = maps:get(<<"interop-suffix">>, trevrpc_call:metadata(Call), <<>>),
                ok(Call, response(<<Message/binary, Suffix/binary>>, 0));
            Status ->
                {error, Status}
        end
    end).

client_stream(Call) ->
    start(Call),
    client_stream(Call, [], 0).

client_stream(Call, Messages, Count) ->
    case trevrpc_call:recv(Call) of
        {ok, Bin} ->
            with_request(Bin, fun(#{message := Message} = Request) ->
                case failure(Request) of
                    none -> client_stream(Call, [Message | Messages], Count + 1);
                    Status -> {error, Status}
                end
            end);
        eof ->
            ok(Call, response(iolist_to_binary(lists:reverse(Messages)), Count));
        {error, Status} ->
            {error, Status}
    end.

server_stream(Bin, Call) ->
    with_request(Bin, fun(#{message := Message, count := Count} = Request) ->
        start(Call),
        Sent = send_all(Call, [response(Message, Index) || Index <- lists:seq(0, Count - 1)]),
        case {Sent, failure(Request)} of
            {ok, none} -> ok(Call);
            {ok, Status} -> {error, Status};
            {Error, _} -> Error
        end
    end).

bidi(Call) ->
    start(Call),
    bidi(Call, 0).

bidi(Call, Index) ->
    case trevrpc_call:recv(Call) of
        {ok, Bin} ->
            with_request(Bin, fun(#{message := Message} = Request) ->
                case failure(Request) of
                    none ->
                        case trevrpc_call:send(Call, response(Message, Index)) of
                            ok -> bidi(Call, Index + 1);
                            Error -> Error
                        end;
                    Status ->
                        {error, Status}
                end
            end);
        eof ->
            ok(Call);
        {error, Status} ->
            {error, Status}
    end.

send_all(_Call, []) ->
    ok;
send_all(Call, [Response | Rest]) ->
    case trevrpc_call:send(Call, Response) of
        ok -> send_all(Call, Rest);
        Error -> Error
    end.

with_request(Bin, Fun) ->
    case trevrpc_interop_proto:decode_request(Bin) of
        {ok, Request} -> Fun(Request);
        error -> {error, trevrpc_status:new(invalid_argument, <<"failed to decode request">>)}
    end.

%% Every method sends headers {interop-header: v} before its first response
%% when the request metadata has interop-header.
start(Call) ->
    case trevrpc_call:metadata(Call) of
        #{<<"interop-header">> := Value} ->
            ok = trevrpc_call:send_headers(Call, #{<<"interop-header">> => Value});
        #{} ->
            ok
    end.

%% An OK end carries trailing metadata {interop-trailer: v} under the same
%% condition.
ok(Call) ->
    trailers(Call),
    ok.

ok(Call, Response) ->
    trailers(Call),
    {ok, Response}.

trailers(Call) ->
    case trevrpc_call:metadata(Call) of
        #{<<"interop-header">> := Value} ->
            trevrpc_call:set_trailers(Call, #{<<"interop-trailer">> => Value});
        #{} ->
            ok
    end.

%% The status a request with a fail_code asks for. RESOURCE_EXHAUSTED
%% carries a RetryInfo of 1.5 s.
failure(#{fail_code := 0}) ->
    none;
failure(#{fail_code := 8, fail_message := Message}) ->
    #{
        code => resource_exhausted,
        message => Message,
        details => [trevrpc_interop_proto:retry_info(1, 500000000)]
    };
failure(#{fail_code := Code, fail_message := Message}) ->
    trevrpc_status:new(trevrpc_status:from_int(Code), Message).

response(Message, Index) ->
    trevrpc_interop_proto:encode_response(#{message => Message, index => Index}).
