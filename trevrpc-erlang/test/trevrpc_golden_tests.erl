%% The shared golden vectors in testdata/: frame and payload bytes, the
%% frames a receiver reads from shared byte streams, and datagram payloads.
-module(trevrpc_golden_tests).

-include_lib("eunit/include/eunit.hrl").

-define(METHOD, <<"/svc/m">>).
-define(D1, #{id => 1, fingerprint => binary:decode_hex(<<"084fd69b50b28542">>)}).

%% The logical value of every control message entry, from the comments at the
%% top of each vector file.
logical() ->
    #{
        %% wire-golden-vectors.txt
        <<"request.unary">> => {request, #{method => ?METHOD}},
        <<"request.bidi_timeout">> =>
            {request, #{
                method => ?METHOD, kind => bidirectional_streaming, timeout_nanos => 123456
            }},
        <<"request.metadata">> =>
            {request, #{method => ?METHOD, metadata => #{<<"authorization">> => <<"ok">>}}},
        <<"request.datagram_flow">> =>
            {request, #{method => ?METHOD, kind => bidirectional_streaming, datagram_flow => 1}},
        <<"request.connection_metadata_offer">> =>
            {request, #{
                method => ?METHOD,
                connection_metadata => #{id => 1, metadata => #{<<"authorization">> => <<"ok">>}}
            }},
        <<"request.connection_metadata_reference">> =>
            {request, #{
                method => ?METHOD,
                metadata => #{<<"x-a">> => <<"1">>},
                connection_metadata => #{id => 1}
            }},
        <<"headers.metadata">> => {headers, #{metadata => #{<<"x-a">> => <<"1">>}}},
        <<"headers.empty">> => {headers, #{}},
        <<"answer.connection_metadata">> => {answer, #{connection_metadata => 1}},
        <<"answer.empty">> => {answer, #{}},
        <<"status.ok">> => {status, #{code => ok}},
        <<"status.unavailable">> => {status, #{code => unavailable, message => <<"down">>}},
        <<"status.metadata">> =>
            {status, #{
                code => failed_precondition,
                message => <<"stop">>,
                metadata => #{<<"x-t">> => <<"2">>}
            }},
        <<"status.details">> =>
            {status, #{
                code => invalid_argument,
                message => <<"bad">>,
                details => [#{type => <<"x.Y">>, value => <<8, 1>>}]
            }},
        %% zstd-golden-vectors.txt
        <<"request.dictionaries">> =>
            {request, #{
                method => ?METHOD, accept_dictionaries => [1, 300], dictionary_query => [?D1]
            }},
        <<"answer.dictionaries">> => {answer, #{dictionaries => [?D1]}},
        <<"request.plain">> =>
            {request, #{method => ?METHOD, accept_dictionaries => [0], dictionary_query => [#{}]}},
        <<"answer.plain">> => {answer, #{dictionaries => [#{}]}},
        <<"request.stream_compression">> =>
            {request, #{method => ?METHOD, stream_compression => enabled}},
        <<"answer.stream_compression">> => {answer, #{stream_compression => true}},
        <<"request.optimistic_stream_compression">> =>
            {request, #{method => ?METHOD, stream_compression => optimistic}},
        <<"status.refused_stream_compression">> =>
            {status, #{code => resource_exhausted, refused_stream_compression => true}}
    }.

%%% Fixture

testdata(File) ->
    Dir =
        case os:getenv("TREVRPC_TESTDATA") of
            false -> filename:join("..", "testdata");
            Path -> Path
        end,
    filename:join(Dir, File).

%% Name => value, with hex values decoded except the comma-separated lists.
vectors(File) ->
    {ok, Bin} = file:read_file(testdata(File)),
    maps:from_list([
        entry(Line)
     || Line <- binary:split(Bin, <<"\n">>, [global]),
        Line =/= <<>>,
        binary:first(Line) =/= $#
    ]).

entry(Line) ->
    [Name, Value] = [string:trim(Part) || Part <- binary:split(Line, <<"=">>)],
    {Name, Value}.

hex(Value) -> binary:decode_hex(Value).

%% Entries named `<Base>.<Suffix>', as Base => Value.
with_suffix(Vectors, Suffix) ->
    maps:fold(
        fun(Name, Value, Acc) ->
            case string:split(Name, <<".">>, trailing) of
                [Base, Suffix] -> Acc#{Base => Value};
                _ -> Acc
            end
        end,
        #{},
        Vectors
    ).

all_vectors() ->
    maps:merge(vectors("wire-golden-vectors.txt"), vectors("zstd-golden-vectors.txt")).

%%% Control messages

payloads_encode_test_() ->
    Payloads = with_suffix(all_vectors(), <<"payload">>),
    [
        {Name, ?_assertEqual(hex(maps:get(Name, Payloads)), iolist_to_binary(encode(Kind, Value)))}
     || {Name, {Kind, Value}} <- maps:to_list(logical())
    ].

payloads_decode_test_() ->
    Payloads = with_suffix(all_vectors(), <<"payload">>),
    [
        {Name, ?_assertEqual({ok, full(Kind, Value)}, decode(Kind, hex(maps:get(Name, Payloads))))}
     || {Name, {Kind, Value}} <- maps:to_list(logical())
    ].

%% Every logical entry is covered: the fixture names no control message the
%% table above lacks.
logical_complete_test() ->
    Payloads = with_suffix(all_vectors(), <<"payload">>),
    Control = [
        Name
     || Name <- maps:keys(Payloads),
        lists:member(hd(string:split(Name, <<".">>)), [
            <<"request">>, <<"headers">>, <<"answer">>, <<"status">>
        ])
    ],
    ?assertEqual(lists:sort(Control), lists:sort(maps:keys(logical()))).

encode(request, Value) -> trevrpc_wire:encode_request(Value);
encode(headers, Value) -> trevrpc_wire:encode_headers(Value);
encode(answer, Value) -> trevrpc_wire:encode_answer(Value);
encode(status, Value) -> trevrpc_wire:encode_status(Value).

decode(request, Bin) -> trevrpc_wire:decode_request(Bin);
decode(headers, Bin) -> trevrpc_wire:decode_headers(Bin);
decode(answer, Bin) -> trevrpc_wire:decode_answer(Bin);
decode(status, Bin) -> trevrpc_wire:decode_status(Bin).

%% A logical value with every field a decoder fills in.
full(request, Value) ->
    Defaults = #{
        metadata => #{},
        kind => unary,
        timeout_nanos => 0,
        datagram_flow => 0,
        accept_dictionaries => [],
        dictionary_query => [],
        stream_compression => none,
        connection_metadata => undefined
    },
    Request = maps:merge(Defaults, Value),
    Request#{
        dictionary_query := [full_dictionary(Info) || Info <- maps:get(dictionary_query, Request)],
        connection_metadata :=
            case maps:get(connection_metadata, Request) of
                undefined -> undefined;
                Set -> maps:merge(#{id => 0, metadata => #{}}, Set)
            end
    };
full(headers, Value) ->
    maps:merge(#{metadata => #{}}, Value);
full(answer, Value) ->
    Answer = maps:merge(
        #{dictionaries => [], stream_compression => false, connection_metadata => 0}, Value
    ),
    Answer#{dictionaries := [full_dictionary(Info) || Info <- maps:get(dictionaries, Answer)]};
full(status, Value) ->
    maps:merge(
        #{
            code => ok,
            message => <<>>,
            metadata => #{},
            refused_stream_compression => false,
            details => []
        },
        Value
    ).

full_dictionary(Info) -> maps:merge(#{id => 0, fingerprint => <<>>}, Info).

%%% Frames

frame_type(<<"request">>) -> {known, request};
frame_type(<<"headers">>) -> {known, headers};
frame_type(<<"answer">>) -> {known, answer};
frame_type(<<"status">>) -> {known, status};
frame_type(<<"message">>) -> {known, message};
frame_type(<<"zstd_message">>) -> {known, zstd_message};
frame_type(<<"zstd_stream_message">>) -> {known, zstd_stream_message};
frame_type(<<"grease">>) -> grease.

%% Entries with both .payload and .frame: the frame a sender writes, and the
%% frame a reader yields from it.
frames_test_() ->
    Vectors = all_vectors(),
    Payloads = with_suffix(Vectors, <<"payload">>),
    Frames = with_suffix(Vectors, <<"frame">>),
    lists:append([
        frame_cases(Name, hex(Payload), hex(maps:get(Name, Frames)))
     || {Name, Payload} <- maps:to_list(Payloads),
        maps:is_key(Name, Frames)
    ]).

frame_cases(Name, Payload, Frame) ->
    [Prefix | _] = string:split(Name, <<".">>),
    case frame_type(Prefix) of
        {known, Type} ->
            [
                {
                    <<Name/binary, " encode">>,
                    ?_assertEqual(Frame, iolist_to_binary(trevrpc_frame:encode(Type, Payload)))
                },
                {<<Name/binary, " read">>, ?_assertEqual([{Type, Payload}], read_all(Frame))}
            ];
        grease ->
            {Code, _} = trevrpc_varint:decode(Frame),
            [
                {
                    <<Name/binary, " encode">>,
                    ?_assertEqual(Frame, iolist_to_binary(trevrpc_frame:encode_raw(Code, Payload)))
                },
                {<<Name/binary, " is grease">>, ?_assert(trevrpc_frame:is_grease(Code))},
                {<<Name/binary, " skipped">>, ?_assertEqual([], read_all(Frame))}
            ]
    end.

%% The .stream entries, read whole, a byte at a time, and in chunks of every
%% size up to the stream's.
streams_test_() ->
    Vectors = vectors("wire-golden-vectors.txt"),
    Streams = with_suffix(Vectors, <<"stream">>),
    Expected = with_suffix(Vectors, <<"frames">>),
    [
        {Name,
            ?_test(begin
                Bin = hex(Stream),
                Want = parse_frames(maps:get(Name, Expected)),
                [
                    ?assertEqual(Want, read_chunked(Bin, Size))
                 || Size <- lists:seq(1, byte_size(Bin))
                ]
            end)}
     || {Name, Stream} <- maps:to_list(Streams)
    ].

parse_frames(<<>>) ->
    [];
parse_frames(List) ->
    [
        begin
            [Code, Payload] = binary:split(Frame, <<":">>),
            {trevrpc_frame:type_name(binary_to_integer(Code)), hex(Payload)}
        end
     || Frame <- binary:split(List, <<",">>, [global])
    ].

read_all(Bin) ->
    read_chunked(Bin, max(byte_size(Bin), 1)).

read_chunked(Bin, Size) ->
    read_chunked(Bin, Size, trevrpc_frame:reader(trevrpc:default_max_frame_size()), []).

read_chunked(<<>>, _Size, Reader, Acc) ->
    ?assert(trevrpc_frame:at_boundary(Reader)),
    lists:append(lists:reverse(Acc));
read_chunked(Bin, Size, Reader, Acc) ->
    Take = min(Size, byte_size(Bin)),
    <<Chunk:Take/binary, Rest/binary>> = Bin,
    {ok, Frames, Next} = trevrpc_frame:feed(Chunk, Reader),
    read_chunked(Rest, Size, Next, [Frames | Acc]).

%%% Datagrams

datagrams_test_() ->
    Vectors = vectors("wire-golden-vectors.txt"),
    Flows = with_suffix(Vectors, <<"flow">>),
    [
        datagram_cases(Name, Vectors, binary_to_integer(Flow, 16))
     || {Name, Flow} <- maps:to_list(Flows)
    ].

datagram_cases(Name, Vectors, Flow) ->
    Messages = split_messages(maps:get(<<Name/binary, ".messages">>, Vectors)),
    case Vectors of
        #{<<Name/binary, ".datagram">> := Datagram} ->
            Bin = hex(Datagram),
            {Name, [
                ?_assertEqual(
                    Bin, iolist_to_binary(trevrpc_datagram:encode_messages(Flow, Messages))
                ),
                ?_assertEqual(
                    {ok, Flow, [{message, M} || M <- Messages]}, trevrpc_datagram:decode(Bin)
                )
            ]};
        #{<<Name/binary, ".received">> := Received} ->
            Got =
                case trevrpc_datagram:decode(hex(Received)) of
                    {ok, Flow, Decoded} -> [M || {message, M} <- Decoded];
                    error -> []
                end,
            {Name, ?_assertEqual(Messages, Got)}
    end.

split_messages(<<>>) -> [];
split_messages(List) -> [hex(M) || M <- binary:split(List, <<",">>, [global])].
