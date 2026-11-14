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
%% The runtime serves and calls native QUIC and QMux over TLS; it has no
%% HTTP/3, WebTransport, HTTP/2 or WebSocket yet. The server reports only the
%% ports of what it serves and the scenarios it supports; the client reports
%% the scenarios it does not implement as skipped.
-module(trevrpc_interop).

-export([main/0, server_scenarios/0, compression/1]).

-define(ECHO, "/trevrpc.interop.v1.Echo/").
-define(COMPRESSED_ECHO, "/trevrpc.interop.v1.CompressedEcho/").

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
    %% With no HTTP fallback, --web-root is accepted and unused.
    {ok, Server} = trevrpc_server:start(#{
        ip => Host,
        port => list_to_integer(maps:get("port", Flags, "0")),
        certfile => Cert,
        keyfile => Key,
        routes => routes(),
        compression => compression(Flags)
    }),
    {ok, Port} = trevrpc_server:port(Server),
    {ok, TcpPort} = trevrpc_server:tcp_port(Server),
    io:put_chars(json:encode(#{native => Port, qmux => TcpPort, scenarios => server_scenarios()})),
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
                    trevrpc_interop_proto:request_type(), trevrpc_interop_proto:response_type()
                ]
            }
        ]
    };
compression(#{}) ->
    #{}.

routes() ->
    Echo = #{
        request_type => trevrpc_interop_proto:request_type(),
        response_type => trevrpc_interop_proto:response_type()
    },
    Plain = trevrpc_interop_proto:plain_type(),
    #{
        <<?ECHO "Unary">> => Echo#{kind => unary, handler => fun unary/2},
        <<?ECHO "ClientStream">> => Echo#{kind => client_streaming, handler => fun client_stream/1},
        <<?ECHO "ServerStream">> => Echo#{kind => server_streaming, handler => fun server_stream/2},
        <<?ECHO "Bidi">> => Echo#{kind => bidirectional_streaming, handler => fun bidi/1},
        <<?COMPRESSED_ECHO "Unary">> => Echo#{
            kind => unary, handler => fun unary/2, compression => message
        },
        <<?COMPRESSED_ECHO "Plain">> => #{
            kind => unary,
            handler => fun plain/2,
            request_type => Plain,
            response_type => Plain,
            compression => message
        }
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

%% Echoes the text, with trailing metadata saying whether the request
%% arrived compressed without a dictionary.
plain(Bin, Call) ->
    case trevrpc_interop_proto:decode_plain(Bin) of
        {ok, Text} ->
            #{plain := Plain} = trevrpc_call:compressed_received(Call),
            trevrpc_call:set_trailers(Call, #{
                <<"interop-plain-compressed-received">> => integer_to_binary(Plain)
            }),
            {ok, trevrpc_interop_proto:encode_plain(Text)};
        error ->
            {error, trevrpc_status:new(invalid_argument, <<"failed to decode request">>)}
    end.

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
