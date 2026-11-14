%% Stream compression, checked against testdata/zstd-golden-vectors.txt.
-module(trevrpc_zstd_stream_tests).

-include_lib("eunit/include/eunit.hrl").

-define(MAX, 4194304).

vectors() -> trevrpc_zstd_tests:vectors().

config() -> trevrpc_zstd_tests:config().

%% The payloads and messages of the golden sequence `Name', in order.
sequence(Name) ->
    Vectors = vectors(),
    sequence(Name, 0, Vectors).

sequence(Name, I, Vectors) ->
    Key = <<"stream.", Name/binary, ".", (integer_to_binary(I))/binary>>,
    case Vectors of
        #{<<Key/binary, ".payload">> := Payload, <<Key/binary, ".message">> := Message} ->
            [{Payload, Message} | sequence(Name, I + 1, Vectors)];
        #{} ->
            []
    end.

decode_all(Decoder, []) ->
    Decoder;
decode_all(Decoder, [{Payload, Message} | Rest]) ->
    {ok, Decoded, Next} = trevrpc_zstd_stream:decode(Decoder, Payload, ?MAX),
    ?assertEqual(Message, Decoded),
    decode_all(Next, Rest).

receivers_follow_the_golden_vectors_test_() ->
    [
        {binary_to_list(Name), fun() ->
            Sequence = sequence(Name),
            ?assert(length(Sequence) >= 2),
            decode_all(trevrpc_zstd_stream:decoder(config(), all), Sequence)
        end}
     || Name <- [<<"plain">>, <<"seeded">>, <<"reset">>, <<"empty">>]
    ].

receivers_reject_the_bad_vectors_test() ->
    Vectors = vectors(),
    Decoder = trevrpc_zstd_stream:decoder(config(), all),
    Bad = [P || K := P <- Vectors, binary:part(K, 0, min(11, byte_size(K))) =:= <<"stream_bad.">>],
    ?assert(length(Bad) >= 9),
    [?assertMatch({error, {invalid, _}}, trevrpc_zstd_stream:decode(Decoder, P, ?MAX)) || P <- Bad],
    ?assertMatch(
        {error, {too_large, _}},
        trevrpc_zstd_stream:decode(
            Decoder, maps:get(<<"stream_too_large.zeros.payload">>, Vectors), ?MAX
        )
    ).

a_seeded_frame_needs_an_accepted_dictionary_test() ->
    [{Payload, _} | _] = sequence(<<"seeded">>),
    ?assertMatch(
        {error, {invalid, _}},
        trevrpc_zstd_stream:decode(trevrpc_zstd_stream:decoder(config(), [0]), Payload, ?MAX)
    ).

a_chunk_must_decompress_to_its_size_test() ->
    [{<<_:2, _:14, Chunk/binary>>, Message} | _] = sequence(<<"plain">>),
    Decoder = trevrpc_zstd_stream:decoder(config(), all),
    Size = byte_size(Message),
    [
        ?assertMatch(
            {error, {invalid, _}},
            trevrpc_zstd_stream:decode(
                Decoder, <<(trevrpc_varint:encode(S))/binary, Chunk/binary>>, ?MAX
            )
        )
     || S <- [Size - 1, Size + 1]
    ],
    %% A chunk that continues a frame no one opened.
    [_, {Second, _} | _] = sequence(<<"plain">>),
    ?assertMatch({error, {invalid, _}}, trevrpc_zstd_stream:decode(Decoder, Second, ?MAX)).

senders_write_the_golden_payloads_test_() ->
    %% OTP compresses with libzstd 1.5.7 at level 1, as the vectors do.
    [
        {binary_to_list(Name), fun() ->
            {ok, Encoder} = trevrpc_zstd_stream:encoder(config(), Seed, 1),
            lists:foldl(
                fun({Payload, Message}, Enc) ->
                    {Out, Next} = trevrpc_zstd_stream:encode(Enc, Message),
                    ?assertEqual(Payload, iolist_to_binary(Out)),
                    Next
                end,
                Encoder,
                sequence(Name)
            )
        end}
     || {Name, Seed} <- [{<<"plain">>, 0}, {<<"seeded">>, 1}]
    ].

message(I) ->
    iolist_to_binary(
        io_lib:format(
            "{\"service\":\"search\",\"region\":\"us-east-1\",\"status\":404,\"latency_ms\":~b}",
            [I + 100]
        )
    ).

%% Sends `Messages' through `Sender', decoding what goes stream-compressed,
%% and returns how many did.
round_trip(Sender0, Messages, Reserve) ->
    Config = config(),
    {_, _, Streamed} = lists:foldl(
        fun(Message, {Sender, Decoder, Count}) ->
            case trevrpc_zstd_stream:next(Sender, Message, Config, 0, Reserve) of
                {stream, Payload, Next} ->
                    {ok, Message, Decoded} = trevrpc_zstd_stream:decode(
                        Decoder, iolist_to_binary(Payload), ?MAX
                    ),
                    {Next, Decoded, Count + 1};
                {message, Next} ->
                    {Next, Decoder, Count}
            end
        end,
        {Sender0, trevrpc_zstd_stream:decoder(Config, all), 0},
        Messages
    ),
    Streamed.

senders_start_at_the_third_message_once_enabled_test() ->
    Yes = fun() -> true end,
    Messages = [message(I) || I <- lists:seq(1, 10)],
    ?assertEqual(8, round_trip(trevrpc_zstd_stream:sender(true), Messages, Yes)),
    ?assertEqual(0, round_trip(trevrpc_zstd_stream:sender(false), Messages, Yes)),
    %% A spent budget keeps the direction per message.
    ?assertEqual(0, round_trip(trevrpc_zstd_stream:sender(true), Messages, fun() -> false end)),
    %% Enabled late, it starts with the next message.
    Config = config(),
    {message, S1} = trevrpc_zstd_stream:next(
        trevrpc_zstd_stream:sender(false), <<"a">>, Config, 0, Yes
    ),
    {message, S2} = trevrpc_zstd_stream:next(S1, <<"b">>, Config, 0, Yes),
    {message, S3} = trevrpc_zstd_stream:next(S2, <<"c">>, Config, 0, Yes),
    ?assertMatch(
        {stream, _, _},
        trevrpc_zstd_stream:next(trevrpc_zstd_stream:enable(S3), <<"d">>, Config, 0, Yes)
    ).

an_empty_message_is_an_empty_chunk_even_before_the_frame_header_test() ->
    {ok, Encoder} = trevrpc_zstd_stream:encoder(config(), 0, 1),
    {Empty, E1} = trevrpc_zstd_stream:encode(Encoder, <<>>),
    ?assertEqual(<<0>>, iolist_to_binary(Empty)),
    {First, _} = trevrpc_zstd_stream:encode(E1, message(1)),
    Decoder = trevrpc_zstd_stream:decoder(config(), all),
    {ok, <<>>, D1} = trevrpc_zstd_stream:decode(Decoder, <<0>>, ?MAX),
    ?assertMatch({ok, _, _}, trevrpc_zstd_stream:decode(D1, iolist_to_binary(First), ?MAX)).

senders_end_frames_before_the_window_wraps_test() ->
    {ok, Encoder} = trevrpc_zstd_stream:encoder(config(), 1, 1),
    Decoder = trevrpc_zstd_stream:decoder(config(), all),
    %% 20 KiB messages: the third brings the frame to 60 KiB and ends it, so
    %% the fourth starts a new frame, again from the dictionary.
    Messages = [crypto:strong_rand_bytes(20480) || _ <- lists:seq(1, 4)],
    {_, _, Starts} = lists:foldl(
        fun(Message, {Enc, Dec, Acc}) ->
            {Out, NextEnc} = trevrpc_zstd_stream:encode(Enc, Message),
            Payload = iolist_to_binary(Out),
            {ok, Message, NextDec} = trevrpc_zstd_stream:decode(Dec, Payload, ?MAX),
            {_, Chunk} = trevrpc_varint:decode(Payload),
            %% A frame header carrying Dictionary_ID 1: descriptor with a
            %% one-byte ID, a window descriptor, then the ID.
            Start =
                case Chunk of
                    <<1, _Window, 1, _/binary>> -> 1;
                    _ -> 0
                end,
            {NextEnc, NextDec, [Start | Acc]}
        end,
        {Encoder, Decoder, []},
        Messages
    ),
    ?assertEqual([1, 0, 0, 1], lists:reverse(Starts)).

budgets_reserve_and_release_test() ->
    ?assertEqual(undefined, trevrpc_zstd_stream:budget(0)),
    ?assertNot(trevrpc_zstd_stream:reserve(undefined, 1)),
    Budget = trevrpc_zstd_stream:budget(1000),
    ?assert(trevrpc_zstd_stream:reserve(Budget, 600)),
    ?assertNot(trevrpc_zstd_stream:reserve(Budget, 600)),
    ok = trevrpc_zstd_stream:release(Budget, 600),
    ?assert(trevrpc_zstd_stream:reserve(Budget, 1000)),
    ?assertNot(trevrpc_zstd_stream:reserve(Budget, 1)).
