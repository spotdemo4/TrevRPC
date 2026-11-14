%% @doc The `client' subcommand: runs the interop scenarios over native QUIC,
%% QMux over TLS, or QUIC with a QMux fallback, and prints `ok <name>', `fail <name>: <reason>' or
%% `skip <name>: <reason>' for each.
-module(trevrpc_interop_client).

-export([run/1]).

-define(ECHO, "/trevrpc.interop.v1.Echo/").
-define(COMPRESSED_ECHO, "/trevrpc.interop.v1.CompressedEcho/").
-define(DEADLINE, 10000).
%% The message of the plain.records compression golden vector: a JSON
%% record that compresses without a dictionary.
-define(PLAIN_RECORD, <<
    "{\"service\":\"search\",\"region\":\"us-east-1\",\"status\":200,\"latency_ms\":118,"
    "\"path\":\"/v1/search\",\"query\":\"wireless headphones\",\"results\":[{\"id\":\"p-1001\","
    "\"title\":\"Wireless headphones\",\"price\":59.99},{\"id\":\"p-1002\",\"title\":"
    "\"Wireless earbuds\",\"price\":39.99},{\"id\":\"p-1003\",\"title\":"
    "\"Wireless headphones pro\",\"price\":129.99}]}"
>>).

%% Every scenario of interop/README.md, in order; those this client cannot
%% run yet name why.
scenarios() ->
    [
        {<<"unary">>, fun unary/1},
        {<<"unary-metadata">>, fun unary_metadata/1},
        {<<"unary-error">>, fun unary_error/1},
        {<<"unary-retry-delay">>, fun unary_retry_delay/1},
        {<<"unary-large">>, fun unary_large/1},
        {<<"client-stream">>, fun client_stream/1},
        {<<"server-stream">>, fun server_stream/1},
        {<<"server-stream-error">>, fun server_stream_error/1},
        {<<"bidi">>, fun bidi/1},
        {<<"bidi-error">>, fun bidi_error/1},
        {<<"unary-headers">>, fun unary_headers/1},
        {<<"server-stream-headers">>, fun server_stream_headers/1},
        {<<"unreliable">>, {unsupported, <<"datagram messages are not implemented">>}},
        {<<"compressed">>, fun compressed/1},
        {<<"compressed-stream">>, {unsupported, <<"stream compression is not implemented">>}},
        {<<"refused-stream">>, {unsupported, <<"stream compression is not implemented">>}},
        {<<"compressed-plain">>, fun compressed_plain/1},
        {<<"connection-metadata">>, {unsupported, <<"connection metadata is not implemented">>}},
        {<<"validation">>, {unsupported, <<"buf.validate.Violations details are not decoded">>}}
    ].

%% @doc Runs the client and returns its exit status.
-spec run(#{string() => string()}) -> non_neg_integer().
run(Flags) ->
    Selected = selection(Flags),
    Transport = transport(Flags),
    Expected = expected_transport(Flags, Transport),
    case connect(Flags, Transport) of
        {ok, Channel} ->
            Connected = trevrpc_channel:transport(Channel),
            Passed =
                case Expected =:= any orelse Connected =:= Expected of
                    true ->
                        run_scenarios(Channel, Selected);
                    false ->
                        io:format(
                            standard_error, "client: connected over ~s, expected ~s~n", [
                                Connected, Expected
                            ]
                        ),
                        false
                end,
            trevrpc_channel:close(Channel),
            case Passed of
                true -> 0;
                false -> 1
            end;
        {error, Status} ->
            io:format(standard_error, "client: could not connect: ~p~n", [Status]),
            1
    end.

transport(Flags) ->
    case maps:get("transport", Flags, "quic") of
        "quic" -> quic;
        "tcp" -> tcp;
        "auto" -> auto;
        Other -> throw({usage, "client: unknown --transport " ++ Other})
    end.

%% The transport the channel must connect over: --expect-transport, or the
%% only one --transport allows.
expected_transport(Flags, Transport) ->
    case {maps:get("expect-transport", Flags, undefined), Transport} of
        {"quic", _} -> quic;
        {"qmux", _} -> qmux;
        {undefined, quic} -> quic;
        {undefined, tcp} -> qmux;
        {undefined, auto} -> any;
        {Other, _} -> throw({usage, "client: unknown --expect-transport " ++ Other})
    end.

selection(#{"scenarios" := List}) ->
    Known = [Name || {Name, _} <- scenarios()],
    Names = [string:trim(Name) || Name <- binary:split(list_to_binary(List), <<",">>, [global])],
    Selected = [Name || Name <- Names, Name =/= <<>>],
    case Selected -- Known of
        [] ->
            Selected;
        [Unknown | _] ->
            throw(
                {usage, "client: unknown scenario " ++ binary_to_list(Unknown) ++ " in --scenarios"}
            )
    end;
selection(#{}) ->
    all.

connect(#{"addr" := Addr, "ca" := Ca} = Flags, Transport) ->
    {Host, Port} = host_port(Addr),
    {TcpHost, TcpPort} = host_port(maps:get("tcp-addr", Flags, Addr)),
    trevrpc_channel:connect(
        #{
            host => Host,
            port => Port,
            tcp_host => TcpHost,
            tcp_port => TcpPort,
            transport => Transport,
            server_name => maps:get("server-name", Flags, "localhost"),
            cacertfile => Ca,
            compression => trevrpc_interop:compression(Flags)
        },
        ?DEADLINE
    );
connect(_Flags, _Transport) ->
    throw({usage, "client: --addr and --ca are required"}).

host_port(Addr) ->
    [Port | Rest] = lists:reverse(string:split(Addr, ":", all)),
    Host = string:trim(lists:join(":", lists:reverse(Rest)), both, "[]"),
    Parsed =
        case inet:parse_address(Host) of
            {ok, Ip} -> Ip;
            {error, _} -> Host
        end,
    {Parsed, list_to_integer(Port)}.

run_scenarios(Channel, Selected) ->
    lists:foldl(
        fun({Name, Scenario}, Passed) ->
            Line =
                case {selected(Name, Selected), Scenario} of
                    {false, _} -> {skip, <<"not in --scenarios">>};
                    {true, {unsupported, Why}} -> {skip, Why};
                    {true, Fun} -> run_scenario(Fun, Channel)
                end,
            report(Name, Line),
            Passed andalso element(1, Line) =/= fail
        end,
        true,
        scenarios()
    ).

selected(_Name, all) -> true;
selected(Name, Selected) -> lists:member(Name, Selected).

%% Each scenario runs in a process of its own, so a call it leaves behind is
%% cancelled when it ends.
run_scenario(Fun, Channel) ->
    {Pid, Monitor} = spawn_monitor(fun() -> exit({done, Fun(Channel)}) end),
    receive
        {'DOWN', Monitor, process, Pid, {done, ok}} ->
            {ok, <<>>};
        {'DOWN', Monitor, process, Pid, {done, {error, Reason}}} ->
            {fail, Reason};
        {'DOWN', Monitor, process, Pid, Other} ->
            {fail, iolist_to_binary(io_lib:format("crashed: ~0p", [Other]))}
    after ?DEADLINE + 5000 ->
        exit(Pid, kill),
        {fail, <<"timed out">>}
    end.

report(Name, {ok, _}) -> io:format("ok ~ts~n", [Name]);
report(Name, {skip, Why}) -> io:format("skip ~ts: ~ts~n", [Name, Why]);
report(Name, {fail, Why}) -> io:format("fail ~ts: ~ts~n", [Name, Why]).

%%% Scenarios

%% Echo calls name their message types, which pick the dictionary.
opts() ->
    #{
        timeout => ?DEADLINE,
        request_type => trevrpc_interop_proto:request_type(),
        response_type => trevrpc_interop_proto:response_type()
    }.

opts(Metadata) -> (opts())#{metadata => Metadata}.

request(Fields) -> trevrpc_interop_proto:encode_request(Fields).

unary(Channel) ->
    expect_unary(call_unary(Channel, #{message => <<"hello">>}, opts()), <<"hello">>, 0).

unary_metadata(Channel) ->
    expect_unary(
        call_unary(Channel, #{message => <<"hello">>}, opts(#{<<"interop-suffix">> => <<"!">>})),
        <<"hello!">>,
        0
    ).

unary_error(Channel) ->
    expect_status(
        call_unary(Channel, #{fail_code => 5, fail_message => <<"missing">>}, opts()),
        not_found,
        <<"missing">>
    ).

unary_retry_delay(Channel) ->
    maybe
        {error, Exhausted} ?=
            call_unary(Channel, #{fail_code => 8, fail_message => <<"slow down">>}, opts()),
        ok ?= expect_status({error, Exhausted}, resource_exhausted, <<"slow down">>),
        ok ?= expect(1500, trevrpc_interop_proto:retry_delay(Exhausted), "retry delay"),
        {error, Missing} ?=
            call_unary(Channel, #{fail_code => 5, fail_message => <<"missing">>}, opts()),
        ok ?= expect_status({error, Missing}, not_found, <<"missing">>),
        expect(none, trevrpc_interop_proto:retry_delay(Missing), "retry delay")
    else
        {ok, _, _} -> {error, <<"call succeeded">>};
        {error, _} = Error -> Error
    end.

unary_large(Channel) ->
    Large = binary:copy(<<"x">>, 1048576),
    expect_unary(call_unary(Channel, #{message => Large}, opts()), Large, 0).

client_stream(Channel) ->
    maybe
        {ok, Call} ?= trevrpc_client:client_stream(Channel, <<?ECHO "ClientStream">>, opts()),
        ok ?= send_all(Call, [<<"a">>, <<"b">>, <<"c">>]),
        {ok, Bin} ?= trevrpc_client:close_and_recv(Call),
        expect_response(Bin, <<"abc">>, 3)
    else
        Error -> describe(Error)
    end.

server_stream(Channel) ->
    expect_stream(
        trevrpc_client:server_stream(
            Channel, <<?ECHO "ServerStream">>, request(#{message => <<"s">>, count => 3}), opts()
        ),
        [{<<"s">>, 0}, {<<"s">>, 1}, {<<"s">>, 2}],
        ok
    ).

server_stream_error(Channel) ->
    Request = request(#{message => <<"s">>, count => 2, fail_code => 9, fail_message => <<"stop">>}),
    expect_stream(
        trevrpc_client:server_stream(Channel, <<?ECHO "ServerStream">>, Request, opts()),
        [{<<"s">>, 0}, {<<"s">>, 1}],
        {failed_precondition, <<"stop">>}
    ).

bidi(Channel) ->
    maybe
        {ok, Call} ?= trevrpc_client:bidi(Channel, <<?ECHO "Bidi">>, opts()),
        ok ?= ping_pong(Call, [<<"1">>, <<"2">>, <<"3">>], 0),
        ok ?= trevrpc_client:close_send(Call),
        {eof, _} ?= trevrpc_client:recv(Call),
        ok
    else
        Error -> describe(Error)
    end.

bidi_error(Channel) ->
    maybe
        {ok, Call} ?= trevrpc_client:bidi(Channel, <<?ECHO "Bidi">>, opts()),
        ok ?= ping_pong(Call, [<<"1">>], 0),
        %% The server can end the call before this send.
        _ = trevrpc_client:send(Call, request(#{fail_code => 10, fail_message => <<"abort">>})),
        expect_status(trevrpc_client:recv(Call), aborted, <<"abort">>)
    else
        Error -> describe(Error)
    end.

unary_headers(Channel) ->
    Header = #{<<"interop-header">> => <<"h">>},
    case call_unary(Channel, #{message => <<"hello">>}, opts(Header)) of
        {ok, Bin, #{headers := Headers, trailers := Trailers}} ->
            maybe
                ok ?= expect_response(Bin, <<"hello">>, 0),
                ok ?= expect(Header, Headers, "headers"),
                expect(#{<<"interop-trailer">> => <<"h">>}, Trailers, "trailers")
            end;
        Error ->
            describe(Error)
    end.

server_stream_headers(Channel) ->
    Header = #{<<"interop-header">> => <<"h">>},
    Request = request(#{message => <<"s">>, count => 2}),
    maybe
        {ok, Call} ?=
            trevrpc_client:server_stream(Channel, <<?ECHO "ServerStream">>, Request, opts(Header)),
        {ok, Headers} ?= trevrpc_client:headers(Call),
        ok ?= expect(Header, Headers, "headers"),
        ok ?= expect_messages(Call, [{<<"s">>, 0}, {<<"s">>, 1}]),
        {eof, Trailers} ?= trevrpc_client:recv(Call),
        expect(#{<<"interop-trailer">> => <<"h">>}, Trailers, "trailers")
    else
        Error -> describe(Error)
    end.

%% Without compression in this runtime the messages travel uncompressed; the
%% scenario checks the echoes.
compressed(Channel) ->
    Messages = [record(Latency) || Latency <- [100, 101, 102]],
    maybe
        ok ?= expect_unary(call_unary(Channel, #{message => record(100)}, opts()), record(100), 0),
        ok ?= expect_unary(call_unary(Channel, #{message => record(101)}, opts()), record(101), 0),
        {ok, Call} ?= trevrpc_client:bidi(Channel, <<?ECHO "Bidi">>, opts()),
        ok ?= send_all(Call, Messages),
        ok ?= trevrpc_client:close_send(Call),
        ok ?= expect_messages(Call, lists:zip(Messages, [0, 1, 2])),
        {eof, _} ?= trevrpc_client:recv(Call),
        ok
    else
        Error -> describe(Error)
    end.

%% CompressedEcho.Plain twice with a record that compresses on its own: the
%% first call's answer lets the second compress its request without a
%% dictionary.
compressed_plain(Channel) ->
    Text = binary:copy(?PLAIN_RECORD, 2),
    Opts = #{
        timeout => ?DEADLINE,
        request_type => trevrpc_interop_proto:plain_type(),
        response_type => trevrpc_interop_proto:plain_type(),
        compression => message
    },
    Plain = fun() ->
        maybe
            {ok, Bin, #{trailers := Trailers}} ?=
                trevrpc_client:unary(
                    Channel,
                    <<?COMPRESSED_ECHO "Plain">>,
                    trevrpc_interop_proto:encode_plain(Text),
                    Opts
                ),
            {ok, Text} ?= trevrpc_interop_proto:decode_plain(Bin),
            #{<<"interop-plain-compressed-received">> := Received} ?= Trailers,
            {ok, Received}
        else
            {ok, _Other} -> {error, <<"the echoed text differs">>};
            #{} -> {error, <<"no interop-plain-compressed-received trailer">>};
            Error -> describe(Error)
        end
    end,
    maybe
        {ok, _First} ?= Plain(),
        {ok, <<"1">>} ?= Plain(),
        ok
    else
        {ok, Second} ->
            {error,
                <<"the second request arrived with interop-plain-compressed-received ",
                    Second/binary, ", want 1">>};
        Failed ->
            Failed
    end.

record(Latency) ->
    iolist_to_binary([
        <<"{\"service\":\"search\",\"region\":\"us-east-1\",\"status\":404,\"latency_ms\":">>,
        integer_to_binary(Latency),
        <<"}">>
    ]).

%%% Helpers

call_unary(Channel, Fields, Opts) ->
    trevrpc_client:unary(Channel, <<?ECHO "Unary">>, request(Fields), Opts).

send_all(_Call, []) ->
    ok;
send_all(Call, [Message | Rest]) ->
    case trevrpc_client:send(Call, request(#{message => Message})) of
        ok -> send_all(Call, Rest);
        Error -> Error
    end.

ping_pong(_Call, [], _Index) ->
    ok;
ping_pong(Call, [Message | Rest], Index) ->
    maybe
        ok ?= trevrpc_client:send(Call, request(#{message => Message})),
        {ok, Bin} ?= trevrpc_client:recv(Call),
        ok ?= expect_response(Bin, Message, Index),
        ping_pong(Call, Rest, Index + 1)
    end.

expect_unary({ok, Bin, _Metadata}, Message, Index) -> expect_response(Bin, Message, Index);
expect_unary(Error, _Message, _Index) -> describe(Error).

expect_stream({ok, Call}, Expected, End) ->
    maybe
        ok ?= expect_messages(Call, Expected),
        case {trevrpc_client:recv(Call), End} of
            {{eof, _}, ok} -> ok;
            {Result, {Code, Message}} -> expect_status(Result, Code, Message);
            {Other, ok} -> describe(Other)
        end
    end;
expect_stream(Error, _Expected, _End) ->
    describe(Error).

expect_messages(_Call, []) ->
    ok;
expect_messages(Call, [{Message, Index} | Rest]) ->
    case trevrpc_client:recv(Call) of
        {ok, Bin} ->
            case expect_response(Bin, Message, Index) of
                ok -> expect_messages(Call, Rest);
                Error -> Error
            end;
        Other ->
            describe(Other)
    end.

expect_response(Bin, Message, Index) ->
    case trevrpc_interop_proto:decode_response(Bin) of
        {ok, #{message := Message, index := Index}} ->
            ok;
        {ok, #{message := Got, index := GotIndex}} ->
            {error,
                iolist_to_binary(
                    io_lib:format("response (~ts, ~b), want (~ts, ~b)", [
                        abbreviate(Got), GotIndex, abbreviate(Message), Index
                    ])
                )};
        error ->
            {error, <<"malformed EchoResponse">>}
    end.

expect_status({error, Status}, Code, Message) ->
    case {trevrpc_status:code(Status), trevrpc_status:message(Status)} of
        {Code, Message} ->
            ok;
        {GotCode, GotMessage} ->
            {error,
                iolist_to_binary(
                    io_lib:format("status ~p ~p, want ~p ~p", [GotCode, GotMessage, Code, Message])
                )}
    end;
expect_status(Other, Code, _Message) ->
    {error, iolist_to_binary(io_lib:format("~0p, want status ~p", [abbreviate(Other), Code]))}.

expect(Want, Want, _What) ->
    ok;
expect(Want, Got, What) ->
    {error, iolist_to_binary(io_lib:format("~s ~0p, want ~0p", [What, Got, Want]))}.

describe({error, #{code := _} = Status}) ->
    {error,
        iolist_to_binary(
            io_lib:format("status ~p ~p", [
                trevrpc_status:code(Status), trevrpc_status:message(Status)
            ])
        )};
describe({error, Reason}) when is_binary(Reason) ->
    {error, Reason};
describe(Other) ->
    {error, iolist_to_binary(io_lib:format("unexpected ~0p", [abbreviate(Other)]))}.

abbreviate(Bin) when is_binary(Bin), byte_size(Bin) > 64 ->
    <<
        (binary:part(Bin, 0, 32))/binary,
        "... (",
        (integer_to_binary(byte_size(Bin)))/binary,
        " bytes)"
    >>;
abbreviate(Term) ->
    Term.
