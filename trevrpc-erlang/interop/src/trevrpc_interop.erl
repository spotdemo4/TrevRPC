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
%% The runtime serves native QUIC, QMux over TLS, QMux over WebSocket and the
%% HTTP mapping over HTTP/3 and HTTP/2, and calls native QUIC and QMux over
%% TLS; it has no WebTransport and no HTTP fallback yet. The server reports
%% only the ports of what it serves and the scenarios it supports; the client
%% reports the scenarios it does not implement as skipped. It leaves out the
%% `http3' and `http2' ports even so: the only clients of them in `run.py',
%% the browser app lanes, load their page through the HTTP fallback.
-module(trevrpc_interop).

-export([main/0, server_scenarios/0, compression/1]).

-define(STREAM_RECEIVED, <<"interop-stream-compressed-received">>).

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
        <<"compressed-stream">>,
        <<"refused-stream">>,
        <<"compressed-plain">>,
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
    %% With no HTTP fallback, --web-root is accepted and unused. The browser
    %% runner's page comes from an origin of its own, so every origin is
    %% admitted. HTTP is served, so that the WebSocket lanes run while the
    %% TCP listener offers h2 as well.
    {ok, Server} = trevrpc_server:start(#{
        ip => Host,
        port => list_to_integer(maps:get("port", Flags, "0")),
        certfile => Cert,
        keyfile => Key,
        routes => routes(),
        compression => compression(Flags),
        http => true,
        webtransport => true,
        webtransport_admission => fun(_Request) -> true end
    }),
    {ok, Port} = trevrpc_server:port(Server),
    {ok, TcpPort} = trevrpc_server:tcp_port(Server),
    Ready = #{
        native => Port, qmux => TcpPort, websocket => TcpPort, scenarios => server_scenarios()
    },
    io:put_chars(json:encode(Ready)),
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

%% @doc The compression settings of --dictionary: that dictionary, for the
%% Echo message types.
-spec compression(#{string() => string()}) -> trevrpc_zstd:settings().
compression(#{"dictionary" := Path}) ->
    {ok, Data} = file:read_file(Path),
    #{
        dictionaries => [
            #{
                data => Data,
                messages => [
                    <<"trevrpc.interop.v1.EchoRequest">>, <<"trevrpc.interop.v1.EchoResponse">>
                ]
            }
        ]
    };
compression(#{}) ->
    #{}.

%% The services of interop/proto, from the generated modules: their routes
%% carry each method's message types, compression and codec.
routes() ->
    Echo = #{
        unary => fun unary/2,
        client_stream => fun client_stream/1,
        server_stream => fun server_stream/2,
        bidi => fun bidi/1
    },
    Compressed = #{
        unary => fun unary/2,
        bidi => fun(Call) -> counted(Call, fun bidi/1) end,
        client_stream => fun(Call) -> counted(Call, fun client_stream/1) end,
        plain => fun plain/2
    },
    %% RefusingEcho drops its stream compression, as a server built from a
    %% schema without COMPRESSION_STREAM would, so it refuses every
    %% optimistic call.
    Refusing = maps:map(
        fun(_Path, Route) -> maps:remove(compression, Route) end,
        trevrpc_interop_v1_refusing_echo:routes(#{client_stream => fun client_stream/1})
    ),
    maps:merge(
        maps:merge(
            trevrpc_interop_v1_echo:routes(Echo),
            trevrpc_interop_v1_compressed_echo:routes(Compressed)
        ),
        Refusing
    ).

unary(#{message := Message} = Request, Call) ->
    start(Call),
    case failure(Request) of
        none ->
            Suffix = maps:get(<<"interop-suffix">>, trevrpc_call:metadata(Call), <<>>),
            ok(Call, response(<<Message/binary, Suffix/binary>>, 0));
        Status ->
            {error, Status}
    end.

%% Runs an Echo handler, adding to an OK end's trailing metadata how many
%% requests arrived stream-compressed.
counted(Call, Handler) ->
    case Handler(Call) of
        {error, _} = Error ->
            Error;
        Result ->
            #{stream := Stream} = trevrpc_call:compressed_received(Call),
            trevrpc_call:set_trailers(
                Call, (trevrpc_call:trailers(Call))#{?STREAM_RECEIVED => integer_to_binary(Stream)}
            ),
            Result
    end.

%% Echoes the text, with trailing metadata saying whether the request
%% arrived compressed without a dictionary.
plain(#{text := Text}, Call) ->
    #{plain := Plain} = trevrpc_call:compressed_received(Call),
    trevrpc_call:set_trailers(Call, #{
        <<"interop-plain-compressed-received">> => integer_to_binary(Plain)
    }),
    {ok, #{text => Text}}.

client_stream(Call) ->
    start(Call),
    client_stream(Call, [], 0).

client_stream(Call, Messages, Count) ->
    case trevrpc_call:recv(Call) of
        {ok, #{message := Message} = Request} ->
            case failure(Request) of
                none -> client_stream(Call, [Message | Messages], Count + 1);
                Status -> {error, Status}
            end;
        eof ->
            ok(Call, response(iolist_to_binary(lists:reverse(Messages)), Count));
        {error, Status} ->
            {error, Status}
    end.

server_stream(#{message := Message, count := Count} = Request, Call) ->
    start(Call),
    Sent = send_all(Call, [response(Message, Index) || Index <- lists:seq(0, Count - 1)]),
    case {Sent, failure(Request)} of
        {ok, none} -> ok(Call);
        {ok, Status} -> {error, Status};
        {Error, _} -> Error
    end.

bidi(Call) ->
    start(Call),
    bidi(Call, 0).

bidi(Call, Index) ->
    case trevrpc_call:recv(Call) of
        {ok, #{message := Message} = Request} ->
            case failure(Request) of
                none ->
                    case trevrpc_call:send(Call, response(Message, Index)) of
                        ok -> bidi(Call, Index + 1);
                        Error -> Error
                    end;
                Status ->
                    {error, Status}
            end;
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
        details => [trevrpc_interop_retry:retry_info(1, 500000000)]
    };
failure(#{fail_code := Code, fail_message := Message}) ->
    trevrpc_status:new(trevrpc_status:from_int(Code), Message).

response(Message, Index) ->
    #{message => Message, index => Index}.
