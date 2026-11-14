%% Wire format behaviour the golden vectors do not pin: varint boundaries, frame
%% size limits, truncation, and protobuf decoding rules.
-module(trevrpc_wire_tests).

-include_lib("eunit/include/eunit.hrl").

%%% QUIC varints

varint_boundaries_test() ->
    Cases = [
        {0, 1},
        {63, 1},
        {64, 2},
        {16383, 2},
        {16384, 4},
        {1073741823, 4},
        {1073741824, 8},
        {trevrpc_varint:max(), 8}
    ],
    [
        begin
            Bin = trevrpc_varint:encode(Value),
            ?assertEqual(Size, byte_size(Bin)),
            ?assertEqual(Size, trevrpc_varint:size(Value)),
            ?assertEqual({Value, <<"x">>}, trevrpc_varint:decode(<<Bin/binary, "x">>))
        end
     || {Value, Size} <- Cases
    ].

varint_rejects_out_of_range_test() ->
    ?assertError(function_clause, trevrpc_varint:encode(trevrpc_varint:max() + 1)),
    ?assertError(function_clause, trevrpc_varint:encode(-1)).

varint_non_minimal_test() ->
    ?assertEqual({5, <<>>}, trevrpc_varint:decode(<<16#40, 5>>)),
    ?assertEqual({5, <<>>}, trevrpc_varint:decode(<<16#C0, 0, 0, 0, 0, 0, 0, 5>>)).

varint_truncated_test() ->
    ?assertEqual(more, trevrpc_varint:decode(<<>>)),
    ?assertEqual(more, trevrpc_varint:decode(<<16#40>>)),
    ?assertEqual(more, trevrpc_varint:decode(<<16#80, 0, 0>>)),
    ?assertEqual(more, trevrpc_varint:decode(<<16#C0, 0, 0, 0, 0, 0, 0>>)).

%%% Frames

grease_types_test() ->
    ?assertEqual([16#21, 16#40, 16#5f], [trevrpc_frame:grease_type(N) || N <- [0, 1, 2]]),
    ?assert(lists:all(fun trevrpc_frame:is_grease/1, [16#21, 16#40, 16#5f, 16#1f * 1000 + 16#21])),
    ?assertNot(lists:any(fun trevrpc_frame:is_grease/1, [0, 6, 16#20, 16#22, 16#41])).

datagram_count_frame_test() ->
    ?assertEqual(7, trevrpc_frame:type_code(datagram_count)),
    ?assertEqual(datagram_count, trevrpc_frame:type_name(7)),
    ?assertEqual("DATAGRAM_COUNT", trevrpc_frame:name(datagram_count)),
    %% Counts are written as minimal varints.
    ?assertEqual(<<7, 1, 0>>, iolist_to_binary(trevrpc_frame:encode_datagram_count(0))),
    ?assertEqual(<<7, 1, 63>>, iolist_to_binary(trevrpc_frame:encode_datagram_count(63))),
    ?assertEqual(<<7, 2, 16#40, 64>>, iolist_to_binary(trevrpc_frame:encode_datagram_count(64))),
    Reader = trevrpc_frame:reader(trevrpc:default_max_frame_size()),
    ?assertMatch(
        {ok, [{datagram_count, <<16#40, 64>>}, {message, <<"m">>}], _},
        trevrpc_frame:feed(<<7, 2, 16#40, 64, 0, 1, "m">>, Reader)
    ).

datagram_count_payload_test() ->
    ?assertEqual({ok, 0}, trevrpc_frame:decode_datagram_count(<<0>>)),
    ?assertEqual({ok, 300}, trevrpc_frame:decode_datagram_count(<<16#41, 16#2c>>)),
    %% Any valid encoding is accepted, down to the widest.
    ?assertEqual({ok, 3}, trevrpc_frame:decode_datagram_count(<<16#40, 3>>)),
    ?assertEqual({ok, 3}, trevrpc_frame:decode_datagram_count(<<16#80, 0, 0, 3>>)),
    ?assertEqual({ok, 3}, trevrpc_frame:decode_datagram_count(<<16#C0, 0, 0, 0, 0, 0, 0, 3>>)),
    %% Exactly one varint: not none, not a truncated one, not extra bytes.
    ?assertEqual(error, trevrpc_frame:decode_datagram_count(<<>>)),
    ?assertEqual(error, trevrpc_frame:decode_datagram_count(<<16#40>>)),
    ?assertEqual(error, trevrpc_frame:decode_datagram_count(<<1, 0>>)),
    ?assertEqual(error, trevrpc_frame:decode_datagram_count(<<16#40, 3, 0>>)).

frame_too_large_test() ->
    Reader = trevrpc_frame:reader(4),
    ?assertMatch({ok, [{message, <<"abcd">>}], _}, trevrpc_frame:feed(<<0, 4, "abcd">>, Reader)),
    ?assertEqual({error, {frame_too_large, 5}}, trevrpc_frame:feed(<<0, 5>>, Reader)),
    %% Unknown frames count against the limit too, and the limit is checked
    %% from the header alone, before any payload arrives.
    ?assertEqual({error, {frame_too_large, 5}}, trevrpc_frame:feed(<<16#21, 5>>, Reader)),
    ?assertEqual(
        {error, {frame_too_large, trevrpc_varint:max()}},
        trevrpc_frame:feed(<<0, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF>>, Reader)
    ).

frame_too_large_after_frames_test() ->
    Reader = trevrpc_frame:reader(4),
    ?assertEqual(
        {error, {frame_too_large, 9}}, trevrpc_frame:feed(<<0, 1, "a", 0, 9, "x">>, Reader)
    ).

frame_split_payload_test() ->
    Reader0 = trevrpc_frame:reader(100),
    {ok, [], Reader1} = trevrpc_frame:feed(<<0, 10, "abc">>, Reader0),
    ?assertNot(trevrpc_frame:at_boundary(Reader1)),
    {ok, [], Reader2} = trevrpc_frame:feed(<<"def">>, Reader1),
    {ok, Frames, Reader3} = trevrpc_frame:feed(<<"ghij", 3, 0>>, Reader2),
    ?assertEqual([{message, <<"abcdefghij">>}, {status, <<>>}], Frames),
    ?assert(trevrpc_frame:at_boundary(Reader3)).

frame_skip_across_chunks_test() ->
    Reader0 = trevrpc_frame:reader(100),
    {ok, [], Reader1} = trevrpc_frame:feed(<<16#40, 16#40, 6, "abc">>, Reader0),
    ?assertNot(trevrpc_frame:at_boundary(Reader1)),
    {ok, Frames, Reader2} = trevrpc_frame:feed(<<"def", 0, 2, "hi">>, Reader1),
    ?assertEqual([{message, <<"hi">>}], Frames),
    ?assert(trevrpc_frame:at_boundary(Reader2)).

frame_partial_header_test() ->
    Reader0 = trevrpc_frame:reader(100),
    {ok, [], Reader1} = trevrpc_frame:feed(<<16#40>>, Reader0),
    ?assertNot(trevrpc_frame:at_boundary(Reader1)),
    {ok, [], Reader2} = trevrpc_frame:feed(<<16#00, 16#40>>, Reader1),
    {ok, Frames, _} = trevrpc_frame:feed(<<16#02, "hi">>, Reader2),
    ?assertEqual([{message, <<"hi">>}], Frames).

frame_encode_iodata_test() ->
    ?assertEqual(
        <<0, 5, "hello">>,
        iolist_to_binary(trevrpc_frame:encode(message, [<<"he">>, [$l, <<"lo">>]]))
    ).

frame_large_payload_test() ->
    Payload = binary:copy(<<"x">>, 70000),
    Frame = iolist_to_binary(trevrpc_frame:encode(message, Payload)),
    ?assertEqual(<<0, 16#80, 16#01, 16#11, 16#70>>, binary:part(Frame, 0, 5)),
    Reader = trevrpc_frame:reader(trevrpc:default_max_frame_size()),
    ?assertMatch({ok, [{message, Payload}], _}, trevrpc_frame:feed(Frame, Reader)).

%% Skipped frames count towards the cumulative limit, headers included, as
%% Go's frameReader charges them.
frame_body_limit_skipped_test() ->
    %% A grease frame of type 0x40 with 3 payload bytes costs 2 + 1 + 3 = 6.
    Grease = <<16#40, 16#40, 3, 0, 1, 2>>,
    {ok, [], Reader} = trevrpc_frame:feed(Grease, trevrpc_frame:reader(100, #{max_bytes => 6})),
    ?assertEqual({error, {body_too_large, 6}}, trevrpc_frame:feed(<<16#21, 0>>, Reader)),
    %% Checked from the header alone, before any of the payload arrives.
    ?assertEqual(
        {error, {body_too_large, 5}},
        trevrpc_frame:feed(<<16#21, 9>>, trevrpc_frame:reader(100, #{max_bytes => 5}))
    ).

%% Known frames other than MESSAGE never count, and MESSAGE payloads count
%% only once messages are charged.
frame_body_limit_messages_test() ->
    Frames = <<1, 4, "abcd", 0, 4, "abcd", 2, 0, 3, 0>>,
    Reader = trevrpc_frame:reader(100, #{max_bytes => 3}),
    ?assertMatch({ok, [_, _, _, _], _}, trevrpc_frame:feed(Frames, Reader)),
    ?assertEqual(
        {error, {body_too_large, 3}},
        trevrpc_frame:feed(Frames, trevrpc_frame:charge_messages(true, Reader))
    ),
    %% Payload bytes only: four 1-byte messages fit a limit of 4, a fifth does not.
    Counting = trevrpc_frame:reader(100, #{max_bytes => 4, charge_messages => true}),
    {ok, Read, Next} = trevrpc_frame:feed(binary:copy(<<0, 1, "x">>, 4), Counting),
    ?assertEqual(4, length(Read)),
    ?assertEqual({error, {body_too_large, 4}}, trevrpc_frame:feed(<<0, 1, "x">>, Next)).

frame_body_limit_charge_test() ->
    Reader = trevrpc_frame:reader(100, #{max_bytes => 10}),
    {ok, Charged} = trevrpc_frame:charge(10, Reader),
    ?assertEqual({error, {body_too_large, 10}}, trevrpc_frame:charge(1, Charged)),
    %% Without a limit nothing is counted.
    ?assertMatch({ok, _}, trevrpc_frame:charge(1 bsl 40, trevrpc_frame:reader(100))).

%%% Datagrams

datagram_drops_malformed_test() ->
    %% No flow ID.
    ?assertEqual(error, trevrpc_datagram:decode(<<>>)),
    %% Flow 0, which no call has, as Go's parseDatagram drops it, in any
    %% encoding.
    ?assertEqual(error, trevrpc_datagram:decode(<<0, 0, 2, "hi">>)),
    ?assertEqual(error, trevrpc_datagram:decode(<<16#40, 0, 0, 2, "hi">>)),
    %% A flow ID and no frames carries no messages.
    ?assertEqual({ok, 1, []}, trevrpc_datagram:decode(<<1>>)),
    %% A truncated frame header, and a length past the end.
    ?assertEqual(error, trevrpc_datagram:decode(<<1, 0, 2, "hi", 16#40>>)),
    ?assertEqual(error, trevrpc_datagram:decode(<<1, 0, 3, "hi">>)),
    %% ZSTD_STREAM_MESSAGE drops the whole datagram.
    ?assertEqual(error, trevrpc_datagram:decode(<<1, 0, 2, "hi", 5, 1, 0>>)).

%% Like Go's walkDatagramFrames, datagram frames are not checked against the
%% frame size limit, so a message as large as a QUIC datagram can carry
%% arrives whole.
datagram_no_frame_size_limit_test() ->
    Message = binary:copy(<<"m">>, 65000),
    Datagram = iolist_to_binary(trevrpc_datagram:encode_messages(9, [Message])),
    ?assertEqual({ok, 9, [{message, Message}]}, trevrpc_datagram:decode(Datagram)).

%% Frames other than messages are skipped in a datagram, a DATAGRAM_COUNT
%% included.
datagram_skips_count_test() ->
    ?assertEqual(
        {ok, 1, [{message, <<"hi">>}]},
        trevrpc_datagram:decode(<<1, 7, 1, 2, 0, 2, "hi">>)
    ).

datagram_zstd_message_test() ->
    ?assertEqual(
        {ok, 3, [{zstd_message, <<"z">>}, {message, <<"m">>}]},
        trevrpc_datagram:decode(<<3, 4, 1, "z", 0, 1, "m">>)
    ).

%%% Protobuf primitives

pb_varint_test() ->
    Max = 16#FFFFFFFFFFFFFFFF,
    [
        ?assertEqual({V, <<>>}, trevrpc_pb:decode_varint(trevrpc_pb:encode_varint(V)))
     || V <- [0, 1, 127, 128, 300, 16#FFFFFFFF, Max]
    ],
    ?assertEqual(10, byte_size(trevrpc_pb:encode_varint(Max))),
    %% A tenth byte above 1 overflows 64 bits; an eleventh byte is too long.
    ?assertEqual(
        error,
        trevrpc_pb:decode_varint(
            <<16#FF:8, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 2>>
        )
    ),
    ?assertEqual(
        error,
        trevrpc_pb:decode_varint(
            <<16#80, 16#80, 16#80, 16#80, 16#80, 16#80, 16#80, 16#80, 16#80, 16#80, 0>>
        )
    ),
    ?assertEqual(error, trevrpc_pb:decode_varint(<<16#80>>)),
    %% Non-minimal encodings are valid.
    ?assertEqual({1, <<>>}, trevrpc_pb:decode_varint(<<16#81, 16#80, 0>>)).

pb_int32_test() ->
    ?assertEqual(-1, trevrpc_pb:to_int32(16#FFFFFFFFFFFFFFFF)),
    ?assertEqual(3, trevrpc_pb:to_int32(16#100000003)),
    ?assertEqual(16#FFFFFFFF, trevrpc_pb:to_uint32(16#1FFFFFFFF)).

pb_utf8_test() ->
    ?assert(trevrpc_pb:valid_utf8(<<"/svc/m">>)),
    ?assert(trevrpc_pb:valid_utf8(<<"héllo ☃"/utf8>>)),
    ?assertNot(trevrpc_pb:valid_utf8(<<16#FF>>)),
    %% A UTF-16 surrogate and an overlong encoding.
    ?assertNot(trevrpc_pb:valid_utf8(<<16#ED, 16#A0, 16#80>>)),
    ?assertNot(trevrpc_pb:valid_utf8(<<16#C0, 16#80>>)).

%%% Control messages

request(Fields) ->
    iolist_to_binary(Fields).

request_skips_unknown_fields_test() ->
    Bin = request([
        <<16#0a, 6, "/svc/m">>,
        %% field 15 of every wire type the decoder skips
        trevrpc_pb:varint_field(15, 7),
        <<(15 bsl 3 bor 1), 0:64>>,
        trevrpc_pb:bytes_field(15, <<"zz">>),
        <<(15 bsl 3 bor 5), 0:32>>
    ]),
    ?assertMatch({ok, #{method := <<"/svc/m">>}}, trevrpc_wire:decode_request(Bin)).

request_wrong_wire_type_is_unknown_test() ->
    %% method (field 1) as a varint is an unknown field, as protobuf-go treats it.
    Bin = request([trevrpc_pb:varint_field(1, 5), <<16#0a, 2, "/x">>]),
    ?assertMatch({ok, #{method := <<"/x">>}}, trevrpc_wire:decode_request(Bin)).

request_last_scalar_wins_test() ->
    Bin = request([<<16#0a, 2, "/a">>, <<16#0a, 2, "/b">>, <<16#20, 1>>, <<16#20, 2>>]),
    ?assertMatch(
        {ok, #{method := <<"/b">>, timeout_nanos := 2}}, trevrpc_wire:decode_request(Bin)
    ).

request_unpacked_and_packed_dictionaries_test() ->
    Bin = request([
        <<16#0a, 2, "/a">>,
        trevrpc_pb:varint_field(6, 7),
        trevrpc_pb:bytes_field(6, [trevrpc_pb:encode_varint(8), trevrpc_pb:encode_varint(300)]),
        trevrpc_pb:varint_field(6, 9)
    ]),
    ?assertMatch({ok, #{accept_dictionaries := [7, 8, 300, 9]}}, trevrpc_wire:decode_request(Bin)).

request_unknown_enums_test() ->
    Bin = request([<<16#0a, 2, "/a">>, <<16#18, 4>>, <<16#40, 3>>]),
    ?assertMatch(
        {ok, #{kind := {unknown, 4}, stream_compression := {unknown, 3}}},
        trevrpc_wire:decode_request(Bin)
    ),
    Negative = iolist_to_binary(
        trevrpc_wire:encode_request(#{method => <<"/a">>, kind => {unknown, -1}})
    ),
    ?assertEqual(
        <<16#0a, 2, "/a", 16#18, 16#FF:8, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF,
            1>>,
        Negative
    ),
    ?assertMatch({ok, #{kind := {unknown, -1}}}, trevrpc_wire:decode_request(Negative)).

request_merges_connection_metadata_test() ->
    Bin = request([
        <<16#0a, 2, "/a">>,
        trevrpc_pb:bytes_field(9, trevrpc_pb:varint_field(1, 4)),
        trevrpc_pb:bytes_field(
            9,
            trevrpc_pb:bytes_field(2, [
                trevrpc_pb:bytes_field(1, <<"k">>), trevrpc_pb:bytes_field(2, <<"v">>)
            ])
        )
    ]),
    ?assertMatch(
        {ok, #{connection_metadata := #{id := 4, metadata := #{<<"k">> := <<"v">>}}}},
        trevrpc_wire:decode_request(Bin)
    ).

request_empty_connection_metadata_is_present_test() ->
    Bin = iolist_to_binary(
        trevrpc_wire:encode_request(#{method => <<"/a">>, connection_metadata => #{}})
    ),
    ?assertEqual(<<16#0a, 2, "/a", 16#4a, 0>>, Bin),
    ?assertMatch(
        {ok, #{connection_metadata := #{id := 0, metadata := #{}}}},
        trevrpc_wire:decode_request(Bin)
    ).

request_rejects_invalid_utf8_test() ->
    ?assertEqual(error, trevrpc_wire:decode_request(<<16#0a, 1, 16#FF>>)),
    %% A metadata key must be UTF-8 too; values are bytes.
    BadKey = request([trevrpc_pb:bytes_field(2, [trevrpc_pb:bytes_field(1, <<16#FF>>)])]),
    ?assertEqual(error, trevrpc_wire:decode_request(BadKey)),
    BinaryValue = request([
        trevrpc_pb:bytes_field(2, [
            trevrpc_pb:bytes_field(1, <<"k">>), trevrpc_pb:bytes_field(2, <<16#FF>>)
        ])
    ]),
    ?assertMatch(
        {ok, #{metadata := #{<<"k">> := <<16#FF>>}}}, trevrpc_wire:decode_request(BinaryValue)
    ).

request_rejects_malformed_test() ->
    %% Truncated length-delimited field, field number 0, and a group.
    ?assertEqual(error, trevrpc_wire:decode_request(<<16#0a, 5, "/a">>)),
    ?assertEqual(error, trevrpc_wire:decode_request(<<16#00, 1>>)),
    ?assertEqual(error, trevrpc_wire:decode_request(<<16#0b>>)),
    %% A malformed embedded message.
    ?assertEqual(
        error, trevrpc_wire:decode_request(request([trevrpc_pb:bytes_field(9, <<16#0a, 9>>)]))
    ),
    %% A malformed packed list.
    ?assertEqual(
        error, trevrpc_wire:decode_request(request([trevrpc_pb:bytes_field(6, <<16#80>>)]))
    ).

map_entry_defaults_test() ->
    %% A map entry without a value has an empty one; without a key, an empty key.
    Bin = iolist_to_binary(trevrpc_pb:bytes_field(1, trevrpc_pb:bytes_field(1, <<"k">>))),
    ?assertEqual({ok, #{metadata => #{<<"k">> => <<>>}}}, trevrpc_wire:decode_headers(Bin)),
    Empty = iolist_to_binary(trevrpc_pb:bytes_field(1, <<>>)),
    ?assertEqual({ok, #{metadata => #{<<>> => <<>>}}}, trevrpc_wire:decode_headers(Empty)).

map_entry_writes_empty_value_test() ->
    Bin = iolist_to_binary(trevrpc_wire:encode_headers(#{metadata => #{<<"k">> => <<>>}})),
    ?assertEqual(<<16#0a, 5, 16#0a, 1, "k", 16#12, 0>>, Bin).

status_unknown_code_test() ->
    ?assertMatch({ok, #{code := unknown}}, trevrpc_wire:decode_status(<<16#08, 17>>)),
    ?assertMatch({ok, #{code := unknown}}, trevrpc_wire:decode_status(<<16#08, 2>>)),
    ?assertEqual(<<>>, iolist_to_binary(trevrpc_wire:encode_status(#{}))).

status_round_trip_test() ->
    Status = #{
        code => unauthenticated,
        message => <<"nope ☃"/utf8>>,
        metadata => #{<<"a">> => <<1, 2>>, <<"b">> => <<>>},
        refused_stream_compression => true,
        details => [#{type => <<"a.B">>, value => <<>>}, #{type => <<"c.D">>, value => <<9>>}]
    },
    ?assertEqual(
        {ok, Status},
        trevrpc_wire:decode_status(iolist_to_binary(trevrpc_wire:encode_status(Status)))
    ).

status_codes_round_trip_test() ->
    [
        ?assertEqual(Code, trevrpc_status:from_int(trevrpc_status:to_int(Code)))
     || Code <- trevrpc_status:codes()
    ],
    ?assertEqual(lists:seq(0, 16), [trevrpc_status:to_int(Code) || Code <- trevrpc_status:codes()]).

%%% Metadata

metadata_valid_test() ->
    ?assertEqual(ok, trevrpc_metadata:validate(#{})),
    ?assertEqual(ok, trevrpc_metadata:validate(#{<<"a.b_c-9">> => <<0, 255>>})).

metadata_limits_test() ->
    Entries = maps:from_list([{integer_to_binary(N), <<>>} || N <- lists:seq(1, 65)]),
    ?assertEqual(
        {error, <<"metadata has 65 entries, maximum is 64">>}, trevrpc_metadata:validate(Entries)
    ),
    ?assertEqual(
        ok, trevrpc_metadata:validate(maps:remove(<<"65">>, Entries))
    ),
    ?assertEqual(
        {error, <<"metadata value \"k\" is 8193 bytes, maximum is 8192">>},
        trevrpc_metadata:validate(#{<<"k">> => binary:copy(<<0>>, 8193)})
    ),
    %% Eight 8 KiB values with one-byte keys total 65544 bytes.
    Big = maps:from_list([{<<C>>, binary:copy(<<0>>, 8192)} || C <- lists:seq($a, $h)]),
    ?assertEqual(
        {error, <<"metadata is 65544 bytes, maximum is 65536">>}, trevrpc_metadata:validate(Big)
    ).

metadata_keys_test() ->
    ?assertEqual({error, <<"metadata key is empty">>}, trevrpc_metadata:validate_key(<<>>)),
    ?assertEqual(ok, trevrpc_metadata:validate_key(binary:copy(<<"a">>, 128))),
    ?assertMatch(
        {error, <<"metadata key \"aaa", _/binary>>},
        trevrpc_metadata:validate_key(binary:copy(<<"a">>, 129))
    ),
    ?assertEqual(
        {error, <<"metadata key \"trevrpc-x\" uses reserved prefix \"trevrpc-\"">>},
        trevrpc_metadata:validate_key(<<"trevrpc-x">>)
    ),
    ?assertEqual(
        {error,
            <<"metadata key \"A\\xff\" must use lowercase ASCII letters, digits, '.', '_' or '-'">>},
        trevrpc_metadata:validate_key(<<"A", 16#FF>>)
    ).

metadata_normalize_merge_test() ->
    ?assertEqual(<<"x-api-key">>, trevrpc_metadata:normalize_key(<<"X-API-Key">>)),
    ?assertEqual(
        #{<<"a">> => <<"own">>, <<"b">> => <<"base">>},
        trevrpc_metadata:merge(#{<<"a">> => <<"base">>, <<"b">> => <<"base">>}, #{
            <<"a">> => <<"own">>
        })
    ).
