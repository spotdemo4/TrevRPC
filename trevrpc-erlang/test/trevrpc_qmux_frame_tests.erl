%% The shared QMux golden vectors in testdata/qmux-golden-vectors.txt.
-module(trevrpc_qmux_frame_tests).

-include_lib("eunit/include/eunit.hrl").

vectors() ->
    Dir =
        case os:getenv("TREVRPC_TESTDATA") of
            false -> filename:join("..", "testdata");
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
    trevrpc_qmux_frame:stream(Id, Offset, Data, Fin);
encode({reset_stream, Id, Code, Final}) ->
    trevrpc_qmux_frame:reset_stream(Id, Code, Final);
encode({stop_sending, Id, Code}) ->
    trevrpc_qmux_frame:stop_sending(Id, Code);
encode({max_data, Max}) ->
    trevrpc_qmux_frame:max_data(Max);
encode({max_stream_data, Id, Max}) ->
    trevrpc_qmux_frame:max_stream_data(Id, Max);
encode({max_streams, bidi, Max}) ->
    trevrpc_qmux_frame:max_streams_bidi(Max);
encode({connection_close, application, Code, Reason}) ->
    trevrpc_qmux_frame:application_close(Code, Reason);
encode({connection_close, transport, Code, Type, Reason}) ->
    trevrpc_qmux_frame:transport_close(Code, Type, Reason);
encode({ping, Seq}) ->
    trevrpc_qmux_frame:ping(Seq);
encode({ping_response, Seq}) ->
    trevrpc_qmux_frame:ping_response(Seq).

varints_test_() ->
    [
        {Name, ?_assertEqual({Value, <<>>}, trevrpc_varint:decode(Bin))}
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
                    {ok, [Frame]}, trevrpc_qmux_frame:decode_frames(maps:get(Name, Vectors))
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
                    maps:get(Name, Vectors), trevrpc_qmux_frame:transport_parameters(Params)
                )
            },
            {
                <<Name/binary, " decode">>,
                ?_test(begin
                    {ok, [{transport_parameters, Body}]} = trevrpc_qmux_frame:decode_frames(
                        maps:get(Name, Vectors)
                    ),
                    ?assertEqual({ok, Params}, trevrpc_qmux_frame:decode_parameters(Body))
                end)
            }
        ]
     || {Name, Params} <- [{<<"params.server">>, Server}, {<<"params.client">>, Client}]
    ]).

records_test() ->
    Vectors = vectors(),
    Frames = [trevrpc_qmux_frame:stream(0, 0, <<"hi">>, true), trevrpc_qmux_frame:max_data(65)],
    Record = maps:get(<<"record.stream_and_max_data">>, Vectors),
    Message = maps:get(<<"message.stream_and_max_data">>, Vectors),
    ?assertEqual(Record, iolist_to_binary(trevrpc_qmux_frame:record(Frames))),
    ?assertEqual(Message, iolist_to_binary(Frames)),
    ?assertEqual(
        {ok, Message, <<"x">>}, trevrpc_qmux_frame:decode_record(<<Record/binary, "x">>, 65536)
    ),
    ?assertEqual(
        {ok, [{stream, 0, 0, <<"hi">>, true}, {max_data, 65}]},
        trevrpc_qmux_frame:decode_frames(Message)
    ).

record_limits_test() ->
    Record = iolist_to_binary(trevrpc_qmux_frame:record([trevrpc_qmux_frame:max_data(65)])),
    ?assertEqual(more, trevrpc_qmux_frame:decode_record(binary:part(Record, 0, 2), 65536)),
    ?assertEqual(more, trevrpc_qmux_frame:decode_record(<<>>, 65536)),
    ?assertMatch(
        {error, {frame_encoding, 0, _}}, trevrpc_qmux_frame:decode_record(<<16#40, 16#10>>, 15)
    ).

invalid_test_() ->
    [
        {Name, ?_test(check_invalid(Name, Bin))}
     || {Name, Bin} <- maps:to_list(vectors()),
        binary:match(Name, <<"invalid.">>) =:= {0, 8}
    ].

check_invalid(<<"invalid.frame_encoding.", _/binary>>, Bin) ->
    ?assertMatch({error, {frame_encoding, _, _}}, trevrpc_qmux_frame:decode_frames(Bin));
check_invalid(<<"invalid.transport_parameter.", _/binary>>, Bin) ->
    {ok, [{transport_parameters, Body}]} = trevrpc_qmux_frame:decode_frames(Bin),
    ?assertMatch({error, {transport_parameter, _, _}}, trevrpc_qmux_frame:decode_parameters(Body)).

padding_and_unknown_test() ->
    ?assertEqual({ok, [{max_data, 1}]}, trevrpc_qmux_frame:decode_frames(<<0, 0, 16#10, 1, 0>>)),
    %% A STREAM frame without LEN runs to the end of the record.
    ?assertEqual(
        {ok, [{stream, 4, 0, <<"abc">>, false}]},
        trevrpc_qmux_frame:decode_frames(<<16#08, 4, "abc">>)
    ),
    %% Unknown and uni-stream parameters are accepted and ignored.
    Body = <<16#07, 1, 5, 16#09, 1, 5, 16#20, 2, 0, 0>>,
    ?assertEqual({ok, #{max_record_size => 16382}}, trevrpc_qmux_frame:decode_parameters(Body)).
