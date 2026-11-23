%% @doc The `client' subcommand: runs the interop scenarios over native QUIC,
%% QMux over TLS, or QUIC with a QMux fallback, and prints `ok <name>', `fail <name>: <reason>' or
%% `skip <name>: <reason>' for each.
-module(trevrpc_interop_client).

-export([run/1, connect/2, connect/3]).

-define(ECHO, trevrpc_interop_v1_echo).
-define(COMPRESSED_ECHO, trevrpc_interop_v1_compressed_echo).
-define(REFUSING_ECHO, trevrpc_interop_v1_refusing_echo).
-define(UNRELIABLE_ECHO, trevrpc_interop_v1_unreliable_echo).
%% The unreliable scenario's small requests, and the size of the one that
%% cannot be a datagram.
-define(UNRELIABLE_REQUESTS, 50).
-define(UNRELIABLE_LARGE, 4096).
-define(STREAM_RECEIVED, <<"interop-stream-compressed-received">>).
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

-define(SUFFIX, <<"interop-suffix">>).

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
        {<<"unreliable">>, fun unreliable/1},
        {<<"compressed">>, fun compressed/1},
        {<<"compressed-stream">>, fun compressed_stream/1},
        {<<"refused-stream">>, fun refused_stream/1},
        {<<"compressed-plain">>, fun compressed_plain/1},
        {<<"connection-metadata">>, {new_channel, fun connection_metadata/1}},
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
                        Connect = fun(Opts) -> connect(Flags, Transport, Opts) end,
                        run_scenarios(Channel, Connect, Selected);
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

%% @doc Connects a channel to --addr (and --tcp-addr) over `Transport',
%% trusting --ca, with the compression settings of --dictionary, and over
%% native QUIC a pool of --min-quic-connections connections (default 1)
%% that grows under load to --max-quic-connections (by default the
%% channel's).
-spec connect(#{string() => string()}, quic | tcp | auto) ->
    {ok, pid()} | {error, trevrpc_status:status()}.
connect(Flags, Transport) ->
    connect(Flags, Transport, #{}).

%% @doc Connects a channel as {@link connect/2} does, with the channel
%% options `Opts' on top.
-spec connect(#{string() => string()}, quic | tcp | auto, map()) ->
    {ok, pid()} | {error, trevrpc_status:status()}.
connect(#{"addr" := Addr, "ca" := Ca} = Flags, Transport, Opts) ->
    {Host, Port} = host_port(Addr),
    {TcpHost, TcpPort} = host_port(maps:get("tcp-addr", Flags, Addr)),
    Max =
        case Flags of
            #{"max-quic-connections" := Connections} ->
                #{max_quic_connections => list_to_integer(Connections)};
            #{} ->
                #{}
        end,
    trevrpc_channel:connect(
        (maps:merge(Max, Opts))#{
            host => Host,
            port => Port,
            tcp_host => TcpHost,
            tcp_port => TcpPort,
            transport => Transport,
            server_name => maps:get("server-name", Flags, "localhost"),
            cacertfile => Ca,
            compression => trevrpc_interop:compression(Flags),
            min_quic_connections => list_to_integer(maps:get("min-quic-connections", Flags, "1"))
        },
        ?DEADLINE
    );
connect(_Flags, _Transport, _Opts) ->
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

%% A scenario runs on the channel, or, for `{new_channel, Fun}', connects
%% channels of its own with `Connect'.
run_scenarios(Channel, Connect, Selected) ->
    lists:foldl(
        fun({Name, Scenario}, Passed) ->
            Line =
                case {selected(Name, Selected), Scenario} of
                    {false, _} -> {skip, <<"not in --scenarios">>};
                    {true, {unsupported, Why}} -> {skip, Why};
                    {true, {new_channel, Fun}} -> run_scenario(Fun, Connect);
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
run_scenario(Fun, Arg) ->
    {Pid, Monitor} = spawn_monitor(fun() -> exit({done, Fun(Arg)}) end),
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

%% The generated functions add each method's message types, which pick the
%% dictionary, its compression and its codec.
opts() ->
    #{timeout => ?DEADLINE}.

opts(Metadata) -> (opts())#{metadata => Metadata}.

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
        ok ?= expect(1500, trevrpc_interop_retry:retry_delay(Exhausted), "retry delay"),
        {error, Missing} ?=
            call_unary(Channel, #{fail_code => 5, fail_message => <<"missing">>}, opts()),
        ok ?= expect_status({error, Missing}, not_found, <<"missing">>),
        expect(none, trevrpc_interop_retry:retry_delay(Missing), "retry delay")
    else
        {ok, _, _} -> {error, <<"call succeeded">>};
        {error, _} = Error -> Error
    end.

unary_large(Channel) ->
    Large = binary:copy(<<"x">>, 1048576),
    expect_unary(call_unary(Channel, #{message => Large}, opts()), Large, 0).

client_stream(Channel) ->
    maybe
        {ok, Call} ?= ?ECHO:client_stream(Channel, opts()),
        ok ?= send_all(Call, [<<"a">>, <<"b">>, <<"c">>]),
        {ok, Response} ?= trevrpc_client:close_and_recv(Call),
        expect_response(Response, <<"abc">>, 3)
    else
        Error -> describe(Error)
    end.

server_stream(Channel) ->
    expect_stream(
        ?ECHO:server_stream(Channel, #{message => <<"s">>, count => 3}, opts()),
        [{<<"s">>, 0}, {<<"s">>, 1}, {<<"s">>, 2}],
        ok
    ).

server_stream_error(Channel) ->
    Request = #{message => <<"s">>, count => 2, fail_code => 9, fail_message => <<"stop">>},
    expect_stream(
        ?ECHO:server_stream(Channel, Request, opts()),
        [{<<"s">>, 0}, {<<"s">>, 1}],
        {failed_precondition, <<"stop">>}
    ).

bidi(Channel) ->
    maybe
        {ok, Call} ?= ?ECHO:bidi(Channel, opts()),
        ok ?= ping_pong(Call, [<<"1">>, <<"2">>, <<"3">>], 0),
        ok ?= trevrpc_client:close_send(Call),
        {eof, _} ?= trevrpc_client:recv(Call),
        ok
    else
        Error -> describe(Error)
    end.

bidi_error(Channel) ->
    maybe
        {ok, Call} ?= ?ECHO:bidi(Channel, opts()),
        ok ?= ping_pong(Call, [<<"1">>], 0),
        %% The server can end the call before this send.
        _ = trevrpc_client:send(Call, #{fail_code => 10, fail_message => <<"abort">>}),
        expect_status(trevrpc_client:recv(Call), aborted, <<"abort">>)
    else
        Error -> describe(Error)
    end.

%% Requests "0" to "49" without waiting, then, once a response came back, a
%% 4096-character one that no datagram fits. Every response must be one that
%% was sent, at most once, and the large one must come back. Over QMux all
%% of them come back on the stream; over QUIC some may be lost, but some
%% requests and responses travel as datagrams.
unreliable(Channel) ->
    Large = incompressible(?UNRELIABLE_LARGE),
    Small = [integer_to_binary(N) || N <- lists:seq(0, ?UNRELIABLE_REQUESTS - 1)],
    Total = ?UNRELIABLE_REQUESTS + 1,
    Before = datagrams_delivered(Channel),
    maybe
        {ok, Call} ?= ?UNRELIABLE_ECHO:bidi(Channel, opts()),
        ok ?= send_each(Call, Small),
        %% The stream may overtake the datagrams still queued, and the
        %% server drops those that arrive after the requests end: the rest
        %% go once one came back.
        {ok, Pending} ?= echoed(trevrpc_client:recv(Call), maps:from_keys([Large | Small], true)),
        ok ?= trevrpc_client:send(Call, #{message => Large, count => 1}),
        ok ?= trevrpc_client:close_send(Call),
        {ok, Left, Trailers} ?= echoes(Call, Pending),
        false ?= maps:is_key(Large, Left),
        {ok, Received} ?= count(Trailers, <<"interop-received">>),
        {ok, Datagrams} ?= count(Trailers, <<"interop-datagrams-received">>),
        Responses = Total - map_size(Left),
        case trevrpc_channel:transport(Channel) of
            quic when Received < 1; Received > Total; Datagrams < 1 ->
                {error,
                    iolist_to_binary(
                        io_lib:format(
                            "on QUIC: interop-received = ~b, interop-datagrams-received = ~b; "
                            "want 1..~b and at least 1",
                            [Received, Datagrams, Total]
                        )
                    )};
            quic ->
                case datagrams_delivered(Channel) > Before of
                    true ->
                        ok;
                    false ->
                        {error, <<
                            "the connection received no DATAGRAM frames, so the server sent no "
                            "response as a datagram"
                        >>}
                end;
            _ when Responses =/= Total; Received =/= Total; Datagrams =/= 0 ->
                {error,
                    iolist_to_binary(
                        io_lib:format(
                            "on QMux: ~b responses, interop-received = ~b, "
                            "interop-datagrams-received = ~b; want ~b, ~b, 0",
                            [Responses, Received, Datagrams, Total, Total]
                        )
                    )};
            _ ->
                ok
        end
    else
        true -> {error, <<"the 4096-byte echo, which cannot be a datagram, is missing">>};
        Error -> describe(Error)
    end.

send_each(_Call, []) ->
    ok;
send_each(Call, [Message | Rest]) ->
    case trevrpc_client:send(Call, #{message => Message, count => 1}) of
        ok -> send_each(Call, Rest);
        Error -> Error
    end.

%% Takes a response off the messages not echoed yet.
echoed({ok, #{message := Message, index := 0}}, Pending) when is_map_key(Message, Pending) ->
    {ok, maps:remove(Message, Pending)};
echoed({ok, #{message := Message, index := Index}}, _Pending) ->
    {error,
        iolist_to_binary(
            io_lib:format("unexpected or repeated response (~ts, ~b)", [abbreviate(Message), Index])
        )};
echoed({eof, _}, _Pending) ->
    {error, <<"the stream ended before the first response">>};
echoed(Other, _Pending) ->
    Other.

%% Reads the responses up to the end of the call.
echoes(Call, Pending) ->
    case trevrpc_client:recv(Call) of
        {eof, Trailers} ->
            {ok, Pending, Trailers};
        Result ->
            case echoed(Result, Pending) of
                {ok, Left} -> echoes(Call, Left);
                Error -> Error
            end
    end.

count(Metadata, Key) ->
    case Metadata of
        #{Key := Value} ->
            try binary_to_integer(Value) of
                Count when Count >= 0 -> {ok, Count};
                _ -> bad_count(Key, Value)
            catch
                error:badarg -> bad_count(Key, Value)
            end;
        #{} ->
            {error, <<"trailing metadata has no ", Key/binary>>}
    end.

bad_count(Key, Value) ->
    {error,
        iolist_to_binary(io_lib:format("trailing metadata ~ts = ~p, want a count", [Key, Value]))}.

%% How many datagrams the channel's connection delivered: 0 over QMux.
datagrams_delivered(Channel) ->
    case trevrpc_channel:datagram_stats(Channel) of
        #{delivered := Delivered} -> Delivered;
        none -> 0
    end.

%% `N' base64 digits from a fixed xorshift generator, as every client sends
%% them: they do not compress below what a datagram holds.
incompressible(N) ->
    Digits = <<"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/">>,
    incompressible(N, 16#9e3779b97f4a7c15, Digits, []).

incompressible(0, _State, _Digits, Acc) ->
    list_to_binary(lists:reverse(Acc));
incompressible(N, State0, Digits, Acc) ->
    State1 = State0 bxor ((State0 bsl 13) band 16#FFFFFFFFFFFFFFFF),
    State2 = State1 bxor (State1 bsr 7),
    State = State2 bxor ((State2 bsl 17) band 16#FFFFFFFFFFFFFFFF),
    incompressible(N - 1, State, Digits, [binary:at(Digits, State rem 64) | Acc]).

unary_headers(Channel) ->
    Header = #{<<"interop-header">> => <<"h">>},
    case call_unary(Channel, #{message => <<"hello">>}, opts(Header)) of
        {ok, Response, #{headers := Headers, trailers := Trailers}} ->
            maybe
                ok ?= expect_response(Response, <<"hello">>, 0),
                ok ?= expect(Header, Headers, "headers"),
                expect(#{<<"interop-trailer">> => <<"h">>}, Trailers, "trailers")
            end;
        Error ->
            describe(Error)
    end.

server_stream_headers(Channel) ->
    Header = #{<<"interop-header">> => <<"h">>},
    Request = #{message => <<"s">>, count => 2},
    maybe
        {ok, Call} ?= ?ECHO:server_stream(Channel, Request, opts(Header)),
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
        {ok, Call} ?= ?ECHO:bidi(Channel, opts()),
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
    Plain = fun() ->
        maybe
            {ok, #{text := Echoed}, #{trailers := Trailers}} ?=
                ?COMPRESSED_ECHO:plain(Channel, #{text => Text}, opts()),
            true ?= Echoed =:= Text,
            #{<<"interop-plain-compressed-received">> := Received} ?= Trailers,
            {ok, Received}
        else
            false -> {error, <<"the echoed text differs">>};
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

%% CompressedEcho.Bidi with eight messages, each echo awaited, then
%% CompressedEcho.ClientStream with the same eight at once. Both are
%% optimistic from the first call, so the server receives the third request
%% and every later one stream-compressed.
compressed_stream(Channel) ->
    maybe
        ok ?= stream_bidi(Channel, 6),
        stream_upload(Channel, ?COMPRESSED_ECHO, 6)
    end.

%% RefusingEcho.ClientStream, which the server refuses as optimistic and the
%% client replays; then compressed-stream's bidi call, which is not
%% optimistic and compresses once its answer arrives; then its client-stream
%% call, optimistic again after that answer.
refused_stream(Channel) ->
    maybe
        ok ?= stream_upload(Channel, ?REFUSING_ECHO, none),
        ok ?= stream_bidi(Channel, 1),
        stream_upload(Channel, ?COMPRESSED_ECHO, 6)
    end.

stream_messages() ->
    [record(Latency) || Latency <- lists:seq(100, 107)].

stream_bidi(Channel, AtLeast) ->
    Header = #{<<"interop-header">> => <<"h">>},
    Messages = stream_messages(),
    maybe
        {ok, Call} ?= ?COMPRESSED_ECHO:bidi(Channel, opts(Header)),
        ok ?= ping_pong(Call, Messages, 0),
        {ok, Headers} ?= trevrpc_client:headers(Call),
        ok ?= expect(Header, Headers, "headers"),
        ok ?= trevrpc_client:close_send(Call),
        {eof, Trailers} ?= trevrpc_client:recv(Call),
        ok ?=
            expect(
                <<"h">>, maps:get(<<"interop-trailer">>, Trailers, undefined), "interop-trailer"
            ),
        stream_received(Trailers, AtLeast)
    else
        Error -> describe(Error)
    end.

%% A ClientStream call of `Service', which is CompressedEcho or
%% RefusingEcho; both allow stream compression in the schema.
stream_upload(Channel, Service, AtLeast) ->
    Messages = stream_messages(),
    maybe
        {ok, Call} ?= Service:client_stream(Channel, opts()),
        ok ?= send_all(Call, Messages),
        ok ?= trevrpc_client:close_send(Call),
        {ok, Response} ?= trevrpc_client:recv(Call),
        ok ?= expect_response(Response, iolist_to_binary(Messages), 8),
        {eof, Trailers} ?= trevrpc_client:recv(Call),
        stream_received(Trailers, AtLeast)
    else
        Error -> describe(Error)
    end.

stream_received(_Trailers, none) ->
    ok;
stream_received(Trailers, AtLeast) ->
    case binary_to_integer(maps:get(?STREAM_RECEIVED, Trailers, <<"-1">>)) of
        Count when Count >= AtLeast ->
            ok;
        Count ->
            {error,
                iolist_to_binary(
                    io_lib:format("~s ~b, want at least ~b", [?STREAM_RECEIVED, Count, AtLeast])
                )}
    end.

record(Latency) ->
    iolist_to_binary([
        <<"{\"service\":\"search\",\"region\":\"us-east-1\",\"status\":404,\"latency_ms\":">>,
        integer_to_binary(Latency),
        <<"}">>
    ]).

%% On a channel of its own: the server merges the connection metadata into
%% every call, the later ones referring to the set it confirmed, and the
%% call's own entry wins.
connection_metadata(Connect) ->
    case Connect(#{connection_metadata => #{?SUFFIX => <<"!">>}}) of
        {ok, Channel} ->
            Result = connection_metadata_calls(Channel),
            trevrpc_channel:close(Channel),
            Result;
        Error ->
            describe(Error)
    end.

connection_metadata_calls(Channel) ->
    Call = fun(Message, Metadata) ->
        expect_unary(
            call_unary(Channel, #{message => Message}, opts(Metadata)),
            <<Message/binary, (maps:get(?SUFFIX, Metadata, <<"!">>))/binary>>,
            0
        )
    end,
    maybe
        ok ?= Call(<<"a">>, #{}),
        ok ?= Call(<<"b">>, #{}),
        ok ?= Call(<<"c">>, #{}),
        ok ?= Call(<<"d">>, #{?SUFFIX => <<"?">>}),
        ok ?= trevrpc_channel:set_connection_metadata(Channel, #{?SUFFIX => <<"#">>}),
        expect_unary(call_unary(Channel, #{message => <<"e">>}, opts()), <<"e#">>, 0)
    else
        Error -> describe(Error)
    end.

%%% Helpers

call_unary(Channel, Request, Opts) ->
    ?ECHO:unary(Channel, Request, Opts).

send_all(_Call, []) ->
    ok;
send_all(Call, [Message | Rest]) ->
    case trevrpc_client:send(Call, #{message => Message}) of
        ok -> send_all(Call, Rest);
        Error -> Error
    end.

ping_pong(_Call, [], _Index) ->
    ok;
ping_pong(Call, [Message | Rest], Index) ->
    maybe
        ok ?= trevrpc_client:send(Call, #{message => Message}),
        {ok, Response} ?= trevrpc_client:recv(Call),
        ok ?= expect_response(Response, Message, Index),
        ping_pong(Call, Rest, Index + 1)
    end.

expect_unary({ok, Response, _Metadata}, Message, Index) ->
    expect_response(Response, Message, Index);
expect_unary(Error, _Message, _Index) ->
    describe(Error).

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
        {ok, Response} ->
            case expect_response(Response, Message, Index) of
                ok -> expect_messages(Call, Rest);
                Error -> Error
            end;
        Other ->
            describe(Other)
    end.

expect_response(#{message := Message, index := Index}, Message, Index) ->
    ok;
expect_response(#{message := Got, index := GotIndex}, Message, Index) ->
    {error,
        iolist_to_binary(
            io_lib:format("response (~ts, ~b), want (~ts, ~b)", [
                abbreviate(Got), GotIndex, abbreviate(Message), Index
            ])
        )}.

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
