%% The bench client against the Erlang interop server, in one VM, over
%% native QUIC and QMux over TLS: every scenario measures something and
%% fails nothing, and the paced scenarios account for every message they
%% scheduled.
-module(trevrpc_interop_bench_tests).

-include_lib("eunit/include/eunit.hrl").

-define(SCENARIOS, [
    "unary",
    "server-stream",
    "client-stream",
    "bidi",
    "unreliable-server-stream",
    "unreliable-client-stream",
    "paced",
    "unreliable-paced"
]).
-define(CONCURRENCY, 2).
-define(RATE, 200).
-define(DURATION, 0.5).

bench_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Env) ->
        [
            {
                Scenario ++ " over " ++ Transport,
                {timeout, 60, fun() -> scenario(Env, Transport, Scenario) end}
            }
         || Transport <- ["quic", "tcp"], Scenario <- ?SCENARIOS
        ] ++
            [
                {"records, stream compression, metadata, two channels and two QUIC connections each",
                    {timeout, 60, fun() -> options(Env) end}},
                {"metadata as connection metadata",
                    {timeout, 60, fun() -> connection_metadata(Env) end}},
                {"what the runtime cannot do is a usage error", fun() -> refusals(Env) end},
                {"the slow count and the timeline are absent unless asked for",
                    {timeout, 60, fun() -> extras_off(Env) end}},
                {"slow counts the operations and messages over the limit",
                    {timeout, 60, fun() -> slow(Env) end}},
                {"the timeline has a bucket for every interval of the window",
                    {timeout, 60, fun() -> timeline_buckets(Env) end}},
                {"the timeline counts the messages of each interval",
                    {timeout, 60, fun() -> timeline_messages(Env) end}},
                {"a negative slow limit or bucket is a usage error", fun() ->
                    extras_refusals(Env)
                end}
            ]
    end}.

setup() ->
    {ok, _} = application:ensure_all_started(trevrpc),
    Dir = string:trim(os:cmd("mktemp -d")),
    San = {'Extension', {2, 5, 29, 17}, false, [{dNSName, "localhost"}]},
    #{cert := Cert, key := Key} = public_key:pkix_test_root_cert("localhost", [
        {key, {namedCurve, secp256r1}}, {digest, sha256}, {extensions, [San]}
    ]),
    CertFile = filename:join(Dir, "cert.pem"),
    KeyFile = filename:join(Dir, "key.pem"),
    ok = file:write_file(CertFile, public_key:pem_encode([{'Certificate', Cert, not_encrypted}])),
    ok = file:write_file(
        KeyFile, public_key:pem_encode([public_key:pem_entry_encode('ECPrivateKey', Key)])
    ),
    {ok, Server} = trevrpc_interop:start_server(#{"cert" => CertFile, "key" => KeyFile}),
    {ok, Port} = trevrpc_server:port(Server),
    {ok, TcpPort} = trevrpc_server:tcp_port(Server),
    #{
        server => Server,
        dir => Dir,
        flags => #{
            "addr" => "127.0.0.1:" ++ integer_to_list(Port),
            "tcp-addr" => "127.0.0.1:" ++ integer_to_list(TcpPort),
            "ca" => CertFile,
            "concurrency" => integer_to_list(?CONCURRENCY),
            "messages" => "5",
            "rate" => integer_to_list(?RATE),
            "duration" => float_to_list(?DURATION, [short]),
            "warmup" => "0.1"
        }
    }.

cleanup(#{server := Server, dir := Dir}) ->
    ok = trevrpc_server:stop(Server),
    ok = file:del_dir_r(Dir).

scenario(#{flags := Flags}, Transport, Scenario) ->
    {ok, Result} = trevrpc_interop_bench:bench(Flags#{
        "transport" => Transport, "scenario" => Scenario
    }),
    #{ops := Ops, msgs := Messages, errors := Errors, lost := Lost} = Result,
    ?assert(Ops > 0),
    ?assert(Messages > 0),
    ?assertEqual(0, Errors),
    ?assertEqual(
        case Transport of
            "quic" -> <<"quic">>;
            "tcp" -> <<"qmux">>
        end,
        maps:get(transport, Result)
    ),
    case Scenario of
        "paced" ++ _ -> paced(Result);
        "unreliable-paced" -> paced(Result);
        _ -> ?assertEqual(0, Lost)
    end.

%% Each worker schedules RATE messages a second, so the window holds
%% DURATION * RATE of each worker's, give or take the one at its edge, every
%% one either delivered or lost.
paced(#{msgs := Messages, lost := Lost, rate := Rate}) ->
    ?assertEqual(float(?RATE), Rate),
    PerWorker = round(?DURATION * ?RATE),
    Scheduled = Messages + Lost,
    ?assert(Scheduled >= ?CONCURRENCY * (PerWorker - 1)),
    ?assert(Scheduled =< ?CONCURRENCY * (PerWorker + 1)).

options(#{flags := Flags}) ->
    [
        begin
            {ok, Result} = trevrpc_interop_bench:bench(Flags#{
                "transport" => Transport,
                "scenario" => Scenario,
                "payload" => "records",
                "size" => "256",
                "compression" => "stream",
                "metadata-size" => "64",
                "connections" => "2",
                "min-quic-connections" => "2"
            }),
            ?assertMatch(
                #{
                    errors := 0,
                    payload := <<"records">>,
                    compression := <<"stream">>,
                    metadata_size := 64,
                    connections := 2
                },
                Result
            ),
            ?assert(maps:get(ops, Result) > 0)
        end
     || Transport <- ["quic", "tcp"], Scenario <- ["unary", "client-stream", "bidi"]
    ].

connection_metadata(#{flags := Flags}) ->
    [
        begin
            {ok, Result} = trevrpc_interop_bench:bench(Flags#{
                "transport" => Transport,
                "scenario" => "unary",
                "metadata-size" => "1024",
                "metadata-scope" => "connection"
            }),
            ?assertMatch(
                #{errors := 0, metadata_size := 1024, metadata_scope := <<"connection">>}, Result
            ),
            ?assert(maps:get(ops, Result) > 0)
        end
     || Transport <- ["quic", "tcp"]
    ].

refusals(#{flags := Flags}) ->
    Refused = fun(Extra) ->
        ?assertThrow({usage, _}, trevrpc_interop_bench:bench(maps:merge(Flags, Extra)))
    end,
    Refused(#{"scenario" => "validation-unary"}),
    Refused(#{"scenario" => "server-stream", "compression" => "stream"}),
    Refused(#{"scenario" => "unary", "compression" => "dictionary"}),
    Refused(#{"scenario" => "nope"}).

%% Without --slow-us and --timeline-ms the result has neither field.
extras_off(#{flags := Flags}) ->
    [
        begin
            {ok, Result} = trevrpc_interop_bench:bench(Flags#{
                "transport" => "quic",
                "scenario" => Scenario,
                "slow-us" => "0",
                "timeline-ms" => "0"
            }),
            ?assertNot(is_map_key(slow, Result)),
            ?assertNot(is_map_key(timeline, Result))
        end
     || Scenario <- ["unary", "paced"]
    ].

%% Every call and message takes longer than a microsecond, and none takes
%% longer than a thousand seconds.
slow(#{flags := Flags}) ->
    [
        begin
            {ok, All} = trevrpc_interop_bench:bench(Flags#{
                "transport" => "quic", "scenario" => Scenario, "slow-us" => "1"
            }),
            {ok, None} = trevrpc_interop_bench:bench(Flags#{
                "transport" => "quic", "scenario" => Scenario, "slow-us" => "1000000000"
            }),
            ?assertEqual(maps:get(Counted, All), maps:get(slow, All)),
            ?assertEqual(0, maps:get(slow, None)),
            ?assertNot(is_map_key(timeline, All))
        end
     || {Scenario, Counted} <- [{"unary", ops}, {"server-stream", ops}, {"paced", msgs}]
    ].

%% The window of DURATION seconds holds ceil(DURATION * 1000 / N) buckets of N
%% milliseconds, and starts about when the bench was called, by the wall
%% clock, after the warmup.
timeline_buckets(#{flags := Flags}) ->
    lists:foreach(
        fun({BucketMs, Expected}) ->
            Before = erlang:system_time(microsecond),
            {ok, Result} = trevrpc_interop_bench:bench(Flags#{
                "transport" => "quic",
                "scenario" => "unary",
                "timeline-ms" => integer_to_list(BucketMs)
            }),
            After = erlang:system_time(microsecond),
            #{timeline := #{start_unix_us := Start, bucket_ms := Bucket, msgs := Counts}} = Result,
            ?assertEqual(BucketMs, Bucket),
            ?assertEqual(Expected, length(Counts)),
            ?assert(lists:all(fun(Count) -> is_integer(Count) andalso Count >= 0 end, Counts)),
            %% The warmup is 0.1 s; the connection takes some time more.
            ?assert(Start >= Before + 100000),
            ?assert(Start =< After - round(?DURATION * 1000000)),
            ?assertNot(is_map_key(slow, Result))
        end,
        [{100, 5}, {300, 2}, {500, 1}, {1000, 1}, {7, 72}]
    ).

%% Every interval of a window of back-to-back operations, and of paced
%% messages, delivers some, and the intervals add up to the window's messages,
%% give or take the operations that started before the window and the paced
%% messages that arrived after it.
timeline_messages(#{flags := Flags}) ->
    [
        begin
            {ok, Result} = trevrpc_interop_bench:bench(Flags#{
                "transport" => Transport,
                "scenario" => Scenario,
                "timeline-ms" => "100",
                "slow-us" => "1"
            }),
            #{msgs := Messages, timeline := #{msgs := Counts}} = Result,
            ?assertEqual(5, length(Counts)),
            ?assert(lists:all(fun(Count) -> Count > 0 end, Counts)),
            Slack = ?CONCURRENCY * (maps:get(messages, Extra) + 1),
            ?assert(abs(lists:sum(Counts) - Messages) =< Slack)
        end
     || Transport <- ["quic", "tcp"],
        {Scenario, Extra} <- [
            {"unary", #{messages => 1}},
            {"server-stream", #{messages => 5}},
            {"paced", #{messages => 2}}
        ]
    ].

extras_refusals(#{flags := Flags}) ->
    ?assertThrow(
        {usage, _},
        trevrpc_interop_bench:bench(Flags#{"scenario" => "unary", "slow-us" => "-1"})
    ),
    ?assertThrow(
        {usage, _},
        trevrpc_interop_bench:bench(Flags#{"scenario" => "unary", "timeline-ms" => "-5"})
    ),
    ?assertThrow(
        {usage, _},
        trevrpc_interop_bench:bench(Flags#{"scenario" => "unary", "timeline-ms" => "x"})
    ).
