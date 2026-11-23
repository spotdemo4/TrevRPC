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
                {"what the runtime cannot do is a usage error", fun() -> refusals(Env) end}
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
