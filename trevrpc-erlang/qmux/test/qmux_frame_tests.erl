%% The shared QMux golden vectors in testdata/qmux-golden-vectors.txt, which
%% every TrevRPC QMux implementation is checked against, and the frames and
%% parameters only this one has.
-module(qmux_frame_tests).

-include_lib("eunit/include/eunit.hrl").

vectors() ->
    Dir =
        case os:getenv("QMUX_TESTDATA") of
            false -> filename:join(["..", "..", "testdata"]);
            Path -> Path
        end,
    {ok, Bin} = file:read_file(filename:join(Dir, "qmux-golden-vectors.txt")),
    maps:from_list([
        begin
            [Name, Value] = [string:trim(Part) || Part <- binary:split(Line, <<"=">>)],
            {Name, binary:decode_hex(Value)}
        end
     || Line <- binary:split(Bin, <<"\n">>, [global]),
        Line =/= <<>>,
        binary:first(Line) =/= $#
    ]).

%% Each frame entry and the frame it holds.
frames() ->
    #{
        <<"frame.stream.fin">> => {stream, 0, 0, <<"hi">>, true},
        <<"frame.stream.offset">> => {stream, 4, 16, <<"hi">>, false},
        <<"frame.reset_stream">> => {reset_stream, 0, 1, 5},
        <<"frame.stop_sending">> => {stop_sending, 0, 1},
        <<"frame.max_data">> => {max_data, 65},
        <<"frame.max_stream_data">> => {max_stream_data, 4, 1028},
        <<"frame.max_streams_bidi">> => {max_streams, bidi, 65},
        <<"frame.application_close">> => {connection_close, application, 0, <<"bye">>},
        <<"frame.transport_close">> => {connection_close, transport, 16#0a, 16#08, <<>>},
        <<"frame.ping">> => {ping, 1},
        <<"frame.ping_response">> => {ping_response, 1}
    }.

encode({stream, Id, Offset, Data, Fin}) ->
    qmux_frame:stream(Id, Offset, Data, Fin);
encode({reset_stream, Id, Code, Final}) ->
    qmux_frame:reset_stream(Id, Code, Final);
encode({stop_sending, Id, Code}) ->
    qmux_frame:stop_sending(Id, Code);
encode({max_data, Max}) ->
    qmux_frame:max_data(Max);
encode({max_stream_data, Id, Max}) ->
    qmux_frame:max_stream_data(Id, Max);
encode({max_streams, Direction, Max}) ->
    qmux_frame:max_streams(Direction, Max);
encode({connection_close, application, Code, Reason}) ->
    qmux_frame:application_close(Code, Reason);
encode({connection_close, transport, Code, Type, Reason}) ->
    qmux_frame:transport_close(Code, Type, Reason);
encode({ping, Seq}) ->
    qmux_frame:ping(Seq);
encode({ping_response, Seq}) ->
    qmux_frame:ping_response(Seq).

varints_test_() ->
    [
        {Name, ?_assertEqual({Value, <<>>}, qmux_varint:decode(Bin))}
     || {Name, Bin, Value} <- [
            {<<"varint.1byte">>, vector(<<"varint.1byte">>), 37},
            {<<"varint.2byte">>, vector(<<"varint.2byte">>), 15293},
            {<<"varint.4byte">>, vector(<<"varint.4byte">>), 494878333},
            {<<"varint.8byte">>, vector(<<"varint.8byte">>), 151288809941952652}
        ]
    ].

vector(Name) -> maps:get(Name, vectors()).

frames_test_() ->
    Vectors = vectors(),
    lists:append([
        [
            {
                <<Name/binary, " encode">>,
                ?_assertEqual(maps:get(Name, Vectors), iolist_to_binary(encode(Frame)))
            },
            {
                <<Name/binary, " decode">>,
                ?_assertEqual(
                    {ok, [Frame]}, qmux_frame:decode_frames(maps:get(Name, Vectors))
                )
            }
        ]
     || {Name, Frame} <- maps:to_list(frames())
    ]).

parameters_test_() ->
    Server = #{
        max_idle_timeout => 30000,
        initial_max_data => 16777216,
        initial_max_stream_data_bidi_remote => 4194308,
        initial_max_streams_bidi => 65,
        max_record_size => 65536
    },
    Client = #{
        initial_max_stream_data_bidi_local => 4194308,
        initial_max_data => 4194308,
        max_record_size => 65536
    },
    Vectors = vectors(),
    lists:append([
        [
            {
                <<Name/binary, " encode">>,
                ?_assertEqual(
                    maps:get(Name, Vectors), qmux_frame:transport_parameters(Params)
                )
            },
            {
                <<Name/binary, " decode">>,
                ?_test(begin
                    {ok, [{transport_parameters, Body}]} = qmux_frame:decode_frames(
                        maps:get(Name, Vectors)
                    ),
                    ?assertEqual({ok, Params}, qmux_frame:decode_parameters(Body))
                end)
            }
        ]
     || {Name, Params} <- [{<<"params.server">>, Server}, {<<"params.client">>, Client}]
    ]).

records_test() ->
    Vectors = vectors(),
    Frames = [qmux_frame:stream(0, 0, <<"hi">>, true), qmux_frame:max_data(65)],
    Record = maps:get(<<"record.stream_and_max_data">>, Vectors),
    Message = maps:get(<<"message.stream_and_max_data">>, Vectors),
    ?assertEqual(Record, iolist_to_binary(qmux_frame:record(Frames))),
    ?assertEqual(Message, iolist_to_binary(Frames)),
    ?assertEqual(
        {ok, Message, <<"x">>}, qmux_frame:decode_record(<<Record/binary, "x">>, 65536)
    ),
    ?assertEqual(
        {ok, [{stream, 0, 0, <<"hi">>, true}, {max_data, 65}]},
        qmux_frame:decode_frames(Message)
    ).

record_limits_test() ->
    Record = iolist_to_binary(qmux_frame:record([qmux_frame:max_data(65)])),
    ?assertEqual(more, qmux_frame:decode_record(binary:part(Record, 0, 2), 65536)),
    ?assertEqual(more, qmux_frame:decode_record(<<>>, 65536)),
    ?assertMatch(
        {error, {frame_encoding, 0, _}}, qmux_frame:decode_record(<<16#40, 16#10>>, 15)
    ).

invalid_test_() ->
    [
        {Name, ?_test(check_invalid(Name, Bin))}
     || {Name, Bin} <- maps:to_list(vectors()),
        binary:match(Name, <<"invalid.">>) =:= {0, 8}
    ].

check_invalid(<<"invalid.frame_encoding.", _/binary>>, Bin) ->
    ?assertMatch({error, {frame_encoding, _, _}}, qmux_frame:decode_frames(Bin));
check_invalid(<<"invalid.protocol_violation.", _/binary>>, Bin) ->
    ?assertMatch({error, {protocol_violation, _, _}}, qmux_frame:decode_frames(Bin));
check_invalid(<<"invalid.transport_parameter.", _/binary>>, Bin) ->
    {ok, [{transport_parameters, Body}]} = qmux_frame:decode_frames(Bin),
    ?assertMatch({error, {transport_parameter, _, _}}, qmux_frame:decode_parameters(Body)).

padding_and_unknown_test() ->
    ?assertEqual({ok, [{max_data, 1}]}, qmux_frame:decode_frames(<<0, 0, 16#10, 1, 0>>)),
    %% A STREAM frame without LEN runs to the end of the record.
    ?assertEqual(
        {ok, [{stream, 4, 0, <<"abc">>, false}]},
        qmux_frame:decode_frames(<<16#08, 4, "abc">>)
    ),
    %% Unknown parameters are accepted and ignored.
    Body = <<16#42, 16#ab, 2, 0, 0>>,
    ?assertEqual({ok, #{max_record_size => 16382}}, qmux_frame:decode_parameters(Body)).

%% The parameters of unidirectional streams and datagrams, which the shared
%% vectors leave out, as not every implementation has them.
uni_and_datagram_parameters_test() ->
    Params = #{
        initial_max_stream_data_uni => 1000,
        initial_max_streams_uni => 3,
        max_datagram_frame_size => 1200,
        max_record_size => 16382
    },
    Frame = qmux_frame:transport_parameters(Params),
    ?assertEqual(
        <<16#ff, 16#51, 16#53, 16#30, 16#0d, 16#0a, 16#0d, 16#0a, 22, 16#07, 2, 16#43, 16#e8, 16#09,
            1, 3, 16#20, 2, 16#44, 16#b0, 16#c5, 16#71, 16#c5, 16#94, 16#29, 16#cd, 16#08, 16#45, 2,
            16#7f, 16#fe>>,
        Frame
    ),
    {ok, [{transport_parameters, Body}]} = qmux_frame:decode_frames(Frame),
    ?assertEqual({ok, Params}, qmux_frame:decode_parameters(Body)),
    TooMany = iolist_to_binary(
        qmux_frame:transport_parameters(#{initial_max_streams_uni => 1 bsl 61})
    ),
    {ok, [{transport_parameters, TooManyBody}]} = qmux_frame:decode_frames(TooMany),
    ?assertMatch({error, {transport_parameter, _, _}}, qmux_frame:decode_parameters(TooManyBody)).

max_streams_uni_test() ->
    Frame = qmux_frame:max_streams(uni, 65),
    ?assertEqual(<<16#13, 16#40, 16#41>>, Frame),
    ?assertEqual({ok, [{max_streams, uni, 65}]}, qmux_frame:decode_frames(Frame)).

datagram_test() ->
    Frame = iolist_to_binary(qmux_frame:datagram(<<"hi">>)),
    ?assertEqual(<<16#31, 2, "hi">>, Frame),
    %% Refused by an endpoint that accepts no datagrams, or none that large.
    ?assertMatch({error, {protocol_violation, 16#31, _}}, qmux_frame:decode_frames(Frame)),
    ?assertMatch({error, {protocol_violation, 16#31, _}}, qmux_frame:decode_frames(Frame, 3)),
    ?assertEqual({ok, [{datagram, <<"hi">>}]}, qmux_frame:decode_frames(Frame, 4)),
    %% Another frame may follow one with a length; one without runs to the
    %% end of the record.
    ?assertEqual(
        {ok, [{datagram, <<"hi">>}, {max_data, 1}]},
        qmux_frame:decode_frames(<<Frame/binary, 16#10, 1>>, 4)
    ),
    ?assertEqual({ok, [{datagram, <<"abc">>}]}, qmux_frame:decode_frames(<<16#30, "abc">>, 4)),
    ?assertMatch(
        {error, {frame_encoding, 16#31, _}}, qmux_frame:decode_frames(<<16#31, 5, "hi">>, 100)
    ).

max_datagram_payload_test() ->
    ?assertEqual(0, qmux_frame:max_datagram_payload(1)),
    ?assertEqual(0, qmux_frame:max_datagram_payload(2)),
    ?assertEqual(61, qmux_frame:max_datagram_payload(63)),
    ?assertEqual(62, qmux_frame:max_datagram_payload(64)),
    ?assertEqual(63, qmux_frame:max_datagram_payload(65)),
    %% 64 bytes take a 2-byte length, so 66 bytes hold 63 at most.
    ?assertEqual(63, qmux_frame:max_datagram_payload(66)),
    ?assertEqual(64, qmux_frame:max_datagram_payload(67)),
    ?assertEqual(1197, qmux_frame:max_datagram_payload(1200)),
    _ = [
        begin
            Payload = qmux_frame:max_datagram_payload(Size),
            ?assert(iolist_size(qmux_frame:datagram(binary:copy(<<0>>, Payload))) =< Size),
            ?assert(iolist_size(qmux_frame:datagram(binary:copy(<<0>>, Payload + 1))) > Size)
        end
     || Size <- lists:seq(2, 20000)
    ],
    ok.
