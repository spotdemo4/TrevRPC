%% Compressed messages, checked against testdata/zstd-golden-vectors.txt.
-module(trevrpc_zstd_tests).

-include_lib("eunit/include/eunit.hrl").

-export([vectors/0, config/0]).

-define(STRING_VALUE, <<"google.protobuf.StringValue">>).

%% The entries of zstd-golden-vectors.txt, hex-decoded, except the IDs.
vectors() ->
    Dir =
        case os:getenv("TREVRPC_TESTDATA") of
            false -> filename:join("..", "testdata");
            Path -> Path
        end,
    {ok, Bin} = file:read_file(filename:join(Dir, "zstd-golden-vectors.txt")),
    maps:from_list([
        {Name, binary:decode_hex(Value)}
     || Line <- binary:split(Bin, <<"\n">>, [global]),
        Line =/= <<>>,
        binary:first(Line) =/= $#,
        [Name, Value] <- [[string:trim(Part) || Part <- binary:split(Line, <<"=">>)]],
        binary:longest_common_suffix([Name, <<".id">>]) =/= 3
    ]).

dictionary() -> maps:get(<<"dict.d1.dictionary">>, vectors()).

config() ->
    {ok, Config} = trevrpc_zstd:config(#{
        dictionaries => [#{data => dictionary(), messages => [?STRING_VALUE]}]
    }),
    Config.

the_dictionary_vector_has_its_id_and_fingerprint_test() ->
    Vectors = vectors(),
    Dictionary = maps:get(<<"dict.d1.dictionary">>, Vectors),
    ?assertEqual({ok, 1}, trevrpc_zstd:dictionary_id(Dictionary)),
    ?assertEqual(
        maps:get(<<"dict.d1.fingerprint">>, Vectors), trevrpc_zstd:fingerprint(Dictionary)
    ),
    ?assertEqual(error, trevrpc_zstd:dictionary_id(<<"raw content dictionary">>)).

receivers_follow_the_golden_vectors_test_() ->
    Vectors = vectors(),
    Config = config(),
    Max = trevrpc:default_max_frame_size(),
    Payloads = [
        {binary:part(Key, 0, byte_size(Key) - 8), Payload}
     || Key := Payload <- Vectors, binary:longest_common_suffix([Key, <<".payload">>]) =:= 8
    ],
    [
        {Name, fun() -> check_vector(Name, Payload, Vectors, Config, Max) end}
     || {Name, Payload} <- lists:sort(Payloads), is_message(Name)
    ].

%% The ZSTD_MESSAGE payload entries; the rest are stream compression and
%% control messages.
is_message(Name) ->
    lists:member(hd(binary:split(Name, <<".">>)), [
        <<"ok">>, <<"plain">>, <<"bad">>, <<"too_large">>
    ]).

check_vector(<<"ok.", _/binary>> = Name, Payload, Vectors, Config, Max) ->
    Message = maps:get(<<Name/binary, ".message">>, Vectors),
    ?assertMatch({ok, Message, 1}, trevrpc_zstd:inflate(Config, all, Payload, Max)),
    ?assertMatch({ok, Message, 1}, trevrpc_zstd:inflate(Config, [1], Payload, Max));
check_vector(<<"plain.", _/binary>> = Name, Payload, Vectors, Config, Max) ->
    Message = maps:get(<<Name/binary, ".message">>, Vectors),
    ?assertMatch({ok, Message, 0}, trevrpc_zstd:inflate(Config, all, Payload, Max)),
    ?assertMatch({ok, Message, 0}, trevrpc_zstd:inflate(trevrpc_zstd:plain(), all, Payload, Max)),
    %% A receiver accepting only dict.d1 rejects it.
    ?assertMatch({error, {invalid, _}}, trevrpc_zstd:inflate(Config, [1], Payload, Max));
check_vector(<<"bad.", _/binary>>, Payload, _Vectors, Config, Max) ->
    ?assertMatch({error, {invalid, _}}, trevrpc_zstd:inflate(Config, all, Payload, Max));
check_vector(<<"too_large.", _/binary>>, Payload, _Vectors, Config, Max) ->
    ?assertMatch({error, {too_large, _}}, trevrpc_zstd:inflate(Config, all, Payload, Max)).

nothing_is_accepted_without_negotiation_test() ->
    Payload = maps:get(<<"ok.small.payload">>, vectors()),
    ?assertMatch({error, {invalid, _}}, trevrpc_zstd:inflate(config(), none, Payload, 1 bsl 20)),
    %% A client accepts only the dictionaries it listed.
    ?assertMatch({error, {invalid, _}}, trevrpc_zstd:inflate(config(), [0], Payload, 1 bsl 20)).

compression_round_trips_and_skips_incompressible_messages_test() ->
    Vectors = vectors(),
    Config = config(),
    Message = maps:get(<<"ok.small.message">>, Vectors),
    {ok, Payload} = trevrpc_zstd:compress(Config, 1, Message),
    ?assertEqual({ok, 1, 71}, trevrpc_zstd:parse_header(Payload)),
    %% libzstd writes what the vectors hold: no magic number, no checksum.
    ?assertEqual(maps:get(<<"ok.small.payload">>, Vectors), Payload),
    ?assertEqual(0, binary:first(Payload) band 16#04),
    ?assertEqual({ok, Message, 1}, trevrpc_zstd:inflate(Config, [1], Payload, 1 bsl 20)),
    ?assertEqual(
        none, trevrpc_zstd:compress(Config, 1, maps:get(<<"ok.raw_block.message">>, Vectors))
    ),
    ?assertEqual(none, trevrpc_zstd:compress(Config, 1, <<>>)),
    %% The same context serves the next message.
    {ok, Again} = trevrpc_zstd:compress(Config, 1, [Message]),
    ?assertEqual(Payload, Again).

compression_without_a_dictionary_starts_at_256_bytes_test() ->
    Vectors = vectors(),
    Config = config(),
    Record = maps:get(<<"plain.records.message">>, Vectors),
    {ok, Payload} = trevrpc_zstd:compress(Config, 0, Record),
    ?assertMatch({ok, 0, _}, trevrpc_zstd:parse_header(Payload)),
    ?assert(byte_size(Payload) < byte_size(Record)),
    ?assertEqual(
        {ok, Record, 0}, trevrpc_zstd:inflate(trevrpc_zstd:plain(), all, Payload, 1 bsl 20)
    ),
    Short = binary:copy(<<"a">>, 255),
    ?assertEqual(none, trevrpc_zstd:compress(Config, 0, Short)),
    ?assertMatch({ok, _}, trevrpc_zstd:compress(Config, 0, <<Short/binary, "a">>)).

large_messages_round_trip_test() ->
    Config = config(),
    Message = binary:copy(<<"0123456789abcdef">>, 65536),
    {ok, Payload} = trevrpc_zstd:compress(Config, 0, Message),
    ?assertEqual({ok, Message, 0}, trevrpc_zstd:inflate(Config, all, Payload, 1 bsl 20)),
    ?assertEqual({ok, Message, 0}, trevrpc_zstd:inflate(Config, all, Payload, 1 bsl 20)),
    ?assertEqual({error, {too_large, 1 bsl 20}}, trevrpc_zstd:inflate(Config, all, Payload, 1000)).

a_failed_message_leaves_the_next_one_alone_test() ->
    Vectors = vectors(),
    Config = config(),
    Good = maps:get(<<"ok.small.payload">>, Vectors),
    Message = maps:get(<<"ok.small.message">>, Vectors),
    Max = 1 bsl 20,
    [
        begin
            ?assertMatch({error, {invalid, _}}, trevrpc_zstd:inflate(Config, all, Bad, Max)),
            ?assertEqual({ok, Message, 1}, trevrpc_zstd:inflate(Config, all, Good, Max))
        end
     || Bad <- [
            maps:get(<<"bad.checksum.payload">>, Vectors),
            maps:get(<<"bad.content_size_mismatch.payload">>, Vectors),
            maps:get(<<"bad.no_dictionary_id.payload">>, Vectors)
        ]
    ].

invalid_configurations_are_rejected_test() ->
    Dictionary = dictionary(),
    <<Head:4/binary, _:4/binary, Tail/binary>> = Dictionary,
    Renumbered = <<Head/binary, 2:32/little, Tail/binary>>,
    [
        ?assertMatch({error, _}, trevrpc_zstd:config(Settings))
     || Settings <- [
            #{dictionaries => [#{data => <<"not a dictionary">>}]},
            #{dictionaries => [#{data => Dictionary}, #{data => Dictionary}]},
            #{dictionaries => [#{data => Dictionary, messages => [<<"not a name!">>]}]},
            #{dictionaries => [#{data => Dictionary, messages => [<<"a..B">>]}]},
            #{
                dictionaries => [
                    #{data => Dictionary, messages => [<<"a.B">>]},
                    #{data => Renumbered, messages => [<<"a.B">>]}
                ]
            },
            #{level => <<"1">>},
            not_a_map
        ]
    ],
    %% Without dictionaries, a configuration holds compression without one
    %% only, last in the query.
    {ok, Plain} = trevrpc_zstd:config(#{}),
    ?assertEqual([#{id => 0, fingerprint => <<>>}], trevrpc_zstd:query(Plain)),
    ?assertEqual(
        [
            #{id => 1, fingerprint => trevrpc_zstd:fingerprint(Dictionary)},
            #{id => 0, fingerprint => <<>>}
        ],
        trevrpc_zstd:query(config())
    ),
    ?assertEqual(1, trevrpc_zstd:level(Plain)),
    {ok, Default} = trevrpc_zstd:config(#{level => 0}),
    ?assertEqual(1, trevrpc_zstd:level(Default)).

negotiation_matches_ids_and_fingerprints_test() ->
    Config = config(),
    [D1, Plain] = trevrpc_zstd:query(Config),
    Wrong = D1#{fingerprint => <<"12345678">>},
    %% Each queried dictionary held with the same fingerprint, once; an
    %% explicit ID 0 counts as absent.
    ?assertEqual([D1, Plain], trevrpc_zstd:answer(Config, [D1, #{}, D1, #{id => 300}])),
    ?assertEqual([Plain], trevrpc_zstd:answer(Config, [Wrong, #{id => 0}])),
    ?assertEqual([Plain], trevrpc_zstd:answer(trevrpc_zstd:plain(), [D1, #{}])),
    ?assertEqual([0, 1], trevrpc_zstd:shared(Config, [D1, Plain, #{id => 300}])),
    ?assertEqual([], trevrpc_zstd:shared(Config, [Wrong])).

dictionaries_are_chosen_by_type_and_fall_back_to_none_test() ->
    Config = config(),
    Any = fun(_) -> true end,
    ?assertEqual(1, trevrpc_zstd:dict_for(Config, ?STRING_VALUE, false, Any)),
    ?assertEqual(none, trevrpc_zstd:dict_for(Config, <<"other.Type">>, false, Any)),
    ?assertEqual(0, trevrpc_zstd:dict_for(Config, <<"other.Type">>, true, Any)),
    ?assertEqual(none, trevrpc_zstd:dict_for(Config, undefined, false, Any)),
    %% A type whose dictionary the peer lacks still compresses without one.
    Plain = fun(Id) -> Id =:= 0 end,
    ?assertEqual(0, trevrpc_zstd:dict_for(Config, ?STRING_VALUE, true, Plain)),
    ?assertEqual(none, trevrpc_zstd:dict_for(Config, ?STRING_VALUE, false, Plain)),
    ?assertEqual({message, none}, trevrpc_zstd:directions(#{requests => message})),
    ?assertEqual({stream, stream}, trevrpc_zstd:directions(stream)).

clients_query_until_answered_and_accept_unconfirmed_dictionaries_test() ->
    Config = config(),
    [D1, Plain] = trevrpc_zstd:query(Config),
    %% The first call queries and accepts dictionary 1 before it is
    %% confirmed; its requests wait for the answer.
    ?assertEqual(
        #{query => [D1, Plain], accept => [1], requests => {pending, 1}},
        trevrpc_zstd:prepare(Config, undefined, ?STRING_VALUE, ?STRING_VALUE, {false, false})
    ),
    %% Once the set is known, nothing is queried and only shared
    %% dictionaries are used.
    ?assertEqual(
        #{query => [], accept => [1], requests => 1},
        trevrpc_zstd:prepare(Config, [0, 1], ?STRING_VALUE, ?STRING_VALUE, {false, false})
    ),
    ?assertEqual(
        #{query => [], accept => [0], requests => 0},
        trevrpc_zstd:prepare(Config, [0], ?STRING_VALUE, ?STRING_VALUE, {true, true})
    ),
    ?assertEqual(
        #{query => [], accept => [], requests => none},
        trevrpc_zstd:prepare(Config, [], ?STRING_VALUE, ?STRING_VALUE, {true, true})
    ),
    %% A client without dictionaries queries compression without one.
    ?assertEqual(
        #{query => [Plain], accept => [], requests => none},
        trevrpc_zstd:prepare(trevrpc_zstd:plain(), undefined, undefined, undefined, {false, false})
    ).

servers_answer_queries_and_compress_only_what_is_accepted_and_confirmed_test() ->
    Config = config(),
    [D1, Plain] = trevrpc_zstd:query(Config),
    Wrong = D1#{fingerprint => <<"12345678">>},
    Request = #{accept_dictionaries => [1], dictionary_query => [D1, Plain]},
    ?assertEqual({[D1, Plain], 1}, trevrpc_zstd:negotiate(Config, Request, ?STRING_VALUE, false)),
    %% No query, no answer.
    ?assertEqual(
        {undefined, 1},
        trevrpc_zstd:negotiate(Config, #{accept_dictionaries => [1]}, ?STRING_VALUE, false)
    ),
    ?assertEqual({undefined, none}, trevrpc_zstd:negotiate(Config, #{}, ?STRING_VALUE, true)),
    %% The answer must confirm what the client accepts.
    ?assertEqual(
        {[Plain], none},
        trevrpc_zstd:negotiate(
            Config, Request#{dictionary_query => [Wrong, Plain]}, ?STRING_VALUE, false
        )
    ),
    ?assertEqual(
        {[Plain], 0},
        trevrpc_zstd:negotiate(
            Config,
            #{accept_dictionaries => [1, 0], dictionary_query => [Wrong, Plain]},
            ?STRING_VALUE,
            true
        )
    ),
    %% A server without dictionaries still answers, with ID 0 only.
    ?assertEqual(
        {[Plain], none},
        trevrpc_zstd:negotiate(trevrpc_zstd:plain(), Request, ?STRING_VALUE, false)
    ).

a_process_keeps_one_context_per_dictionary_test() ->
    Message = maps:get(<<"ok.small.message">>, vectors()),
    Kept = fun() ->
        length(
            [Key || {{trevrpc_zstd, _, _} = Key, _} <- get()] ++
                [Key || {{trevrpc_zstd, _, _, _} = Key, _} <- get()]
        )
    end,
    Before = Kept(),
    [
        begin
            Config = config(),
            {ok, Payload} = trevrpc_zstd:compress(Config, 1, Message),
            {ok, Message, 1} = trevrpc_zstd:inflate(Config, all, Payload, 1 bsl 20)
        end
     || _ <- lists:seq(1, 5)
    ],
    ?assert(Kept() - Before =< 2).
