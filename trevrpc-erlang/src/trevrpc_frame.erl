%% @doc TrevRPC frames: a QUIC varint type, a QUIC varint payload length,
%% and the payload.
%%
%% {@link reader/2} and {@link feed/2} turn the bytes of a stream, in chunks
%% of any size, into the frames they hold. The reader checks each frame's
%% declared length against the frame size limit before it buffers anything,
%% and skips frames of unknown type (grease included) without buffering
%% them.
%%
%% A reader can also bound the cumulative bytes a stream may carry, as
%% servers bound request streams: skipped frames always count (headers
%% included), and MESSAGE payloads count once {@link charge_messages/2} turns
%% them on. Like the frame size limit, this is checked from a frame's header,
%% before its payload is read. Compressed messages count at their
%% decompressed size, which the reader does not know, so whoever decompresses
%% them charges it with {@link charge/2}.
-module(trevrpc_frame).

-export([
    encode/2,
    encode_raw/2,
    encode_datagram_count/1,
    decode_datagram_count/1,
    type_code/1,
    type_name/1,
    name/1,
    is_grease/1,
    grease_type/1,
    reader/1,
    reader/2,
    charge_messages/2,
    charge/2,
    charge_message/2,
    feed/2,
    at_boundary/1
]).

-export_type([type/0, frame/0, reader/0, error/0]).

-type type() ::
    message
    | request
    | headers
    | status
    | zstd_message
    | zstd_stream_message
    | answer
    | datagram_count.

-type frame() :: {type(), binary()}.

%% A frame longer than the frame size limit, or a stream past its cumulative
%% byte limit. Either ends the stream, and the reader must not be used after
%% one.
-type error() ::
    {frame_too_large, non_neg_integer()}
    | {body_too_large, non_neg_integer()}.

-record(reader, {
    max :: non_neg_integer(),
    %% The cumulative byte limit, and the bytes charged against it.
    max_bytes = infinity :: non_neg_integer() | infinity,
    charged = 0 :: non_neg_integer(),
    %% Whether MESSAGE payloads count towards `max_bytes'.
    charge_messages = false :: boolean(),
    %% The bytes of a frame header that is not complete yet (at most 15).
    buf = <<>> :: binary(),
    %% `header' between frames; otherwise the frame being read, with the
    %% number of payload bytes still missing.
    state = header ::
        header
        | {payload, type(), pos_integer(), [binary()]}
        | {skip, pos_integer()}
}).

-opaque reader() :: #reader{}.

%% @doc Encodes a frame of a known type.
-spec encode(type(), iodata()) -> iodata().
encode(Type, Payload) ->
    Length = iolist_size(Payload),
    [<<(type_code(Type)), (trevrpc_varint:encode(Length))/binary>> | Payload].

%% @doc Encodes a frame of any type code, such as an extension or a grease
%% type.
-spec encode_raw(non_neg_integer(), iodata()) -> iodata().
encode_raw(Code, Payload) ->
    Length = iolist_size(Payload),
    [<<(trevrpc_varint:encode(Code))/binary, (trevrpc_varint:encode(Length))/binary>> | Payload].

%% @doc Encodes a DATAGRAM_COUNT frame: the number of datagrams a direction
%% sent, as a minimal varint.
-spec encode_datagram_count(non_neg_integer()) -> iodata().
encode_datagram_count(Count) ->
    encode(datagram_count, trevrpc_varint:encode(Count)).

%% @doc The count a DATAGRAM_COUNT payload holds: exactly one varint, in
%% any valid encoding.
-spec decode_datagram_count(binary()) -> {ok, non_neg_integer()} | error.
decode_datagram_count(Payload) ->
    case trevrpc_varint:decode(Payload) of
        {Count, <<>>} -> {ok, Count};
        _ -> error
    end.

%% @doc The type code of a known frame type.
-spec type_code(type()) -> 0..7.
type_code(message) -> 0;
type_code(request) -> 1;
type_code(headers) -> 2;
type_code(status) -> 3;
type_code(zstd_message) -> 4;
type_code(zstd_stream_message) -> 5;
type_code(answer) -> 6;
type_code(datagram_count) -> 7.

%% @doc The frame type of a type code, or `unknown' for a code the protocol
%% does not assign (receivers skip those frames).
-spec type_name(non_neg_integer()) -> type() | unknown.
type_name(0) -> message;
type_name(1) -> request;
type_name(2) -> headers;
type_name(3) -> status;
type_name(4) -> zstd_message;
type_name(5) -> zstd_stream_message;
type_name(6) -> answer;
type_name(7) -> datagram_count;
type_name(Code) when is_integer(Code), Code >= 0 -> unknown.

%% @doc The name of a frame type as the protocol writes it, such as
%% `"ZSTD_MESSAGE"', for error messages.
-spec name(type()) -> unicode:chardata().
name(Type) ->
    string:uppercase(atom_to_list(Type)).

%% @doc Whether `Code' is a grease type, `0x1f * N + 0x21', which is never
%% assigned.
-spec is_grease(non_neg_integer()) -> boolean().
is_grease(Code) when is_integer(Code), Code >= 16#21 -> (Code - 16#21) rem 16#1f =:= 0;
is_grease(Code) when is_integer(Code), Code >= 0 -> false.

%% @doc The `N'th grease type, `0x1f * N + 0x21'.
-spec grease_type(non_neg_integer()) -> pos_integer().
grease_type(N) when is_integer(N), N >= 0 -> 16#1f * N + 16#21.

%% @doc A reader for one stream, rejecting frames whose payload is longer
%% than `MaxFrameSize' bytes, with no cumulative limit.
-spec reader(non_neg_integer()) -> reader().
reader(MaxFrameSize) ->
    reader(MaxFrameSize, #{}).

%% @doc A reader for one stream, rejecting frames whose payload is longer
%% than `MaxFrameSize' bytes. Options:
%%
%% <ul>
%%   <li>`max_bytes': the cumulative bytes the stream may carry, or
%%       `infinity' (the default).</li>
%%   <li>`charge_messages': whether MESSAGE payloads count towards
%%       `max_bytes' from the start (default `false'). Servers turn it on for
%%       request streams of many messages, since the frame size limit alone
%%       bounds a single message.</li>
%% </ul>
-spec reader(non_neg_integer(), #{
    max_bytes => non_neg_integer() | infinity, charge_messages => boolean()
}) -> reader().
reader(MaxFrameSize, Options) when is_integer(MaxFrameSize), MaxFrameSize >= 0 ->
    #reader{
        max = MaxFrameSize,
        max_bytes = maps:get(max_bytes, Options, infinity),
        charge_messages = maps:get(charge_messages, Options, false)
    }.

%% @doc Sets whether MESSAGE payloads read from now on count towards the
%% cumulative limit.
-spec charge_messages(boolean(), reader()) -> reader().
charge_messages(Enabled, Reader) when is_boolean(Enabled) ->
    Reader#reader{charge_messages = Enabled}.

%% @doc Charges `Bytes' against the cumulative limit, such as the
%% decompressed size of a compressed message.
-spec charge(non_neg_integer(), reader()) ->
    {ok, reader()} | {error, {body_too_large, non_neg_integer()}}.
charge(_Bytes, #reader{max_bytes = infinity} = Reader) ->
    {ok, Reader};
charge(Bytes, #reader{max_bytes = Max, charged = Charged} = Reader) ->
    case Charged + Bytes of
        Total when Total > Max -> {error, {body_too_large, Max}};
        Total -> {ok, Reader#reader{charged = Total}}
    end.

%% @doc Charges `Bytes', the decompressed size of a compressed message,
%% against the cumulative limit if messages count towards it.
-spec charge_message(non_neg_integer(), reader()) ->
    {ok, reader()} | {error, {body_too_large, non_neg_integer()}}.
charge_message(Bytes, #reader{charge_messages = true} = Reader) ->
    charge(Bytes, Reader);
charge_message(_Bytes, Reader) ->
    {ok, Reader}.

%% @doc Reads the frames `Data' completes, in order. Frames of unknown type
%% are skipped. A frame longer than the frame size limit, or one that takes
%% the stream past its cumulative limit, is an error, and the reader must not
%% be used after one.
-spec feed(binary(), reader()) -> {ok, [frame()], reader()} | {error, error()}.
feed(Data, #reader{state = header, buf = <<>>} = Reader) ->
    parse(Data, Reader, []);
feed(Data, #reader{state = header, buf = Buf} = Reader) ->
    parse(<<Buf/binary, Data/binary>>, Reader#reader{buf = <<>>}, []);
feed(Data, #reader{state = {payload, Type, Need, Acc}} = Reader) ->
    case Data of
        <<Last:Need/binary, Rest/binary>> ->
            Payload = iolist_to_binary(lists:reverse(Acc, [Last])),
            parse(Rest, Reader#reader{state = header}, [{Type, Payload}]);
        _ ->
            State = {payload, Type, Need - byte_size(Data), [Data | Acc]},
            {ok, [], Reader#reader{state = State}}
    end;
feed(Data, #reader{state = {skip, Need}} = Reader) ->
    case Data of
        <<_:Need/binary, Rest/binary>> ->
            parse(Rest, Reader#reader{state = header}, []);
        _ ->
            {ok, [], Reader#reader{state = {skip, Need - byte_size(Data)}}}
    end.

%% @doc Whether the reader is between frames, so the stream may end here.
%% A stream that ends anywhere else ends inside a frame.
-spec at_boundary(reader()) -> boolean().
at_boundary(#reader{state = header, buf = <<>>}) -> true;
at_boundary(#reader{}) -> false.

parse(Bin, #reader{max = Max} = Reader, Acc) ->
    case header(Bin) of
        more ->
            {ok, lists:reverse(Acc), Reader#reader{buf = Bin}};
        {_Code, Length, _Rest} when Length > Max ->
            {error, {frame_too_large, Length}};
        {Code, Length, Rest} ->
            Type = type_name(Code),
            case
                charge(frame_cost(Type, Length, byte_size(Bin) - byte_size(Rest), Reader), Reader)
            of
                {ok, Charged} -> take(Type, Length, Rest, Charged, Acc);
                Error -> Error
            end
    end.

%% What a frame counts towards the cumulative limit: all of a skipped frame,
%% header included, and the payload of a MESSAGE while messages count.
frame_cost(unknown, Length, HeaderLength, _Reader) -> HeaderLength + Length;
frame_cost(message, Length, _HeaderLength, #reader{charge_messages = true}) -> Length;
frame_cost(_Type, _Length, _HeaderLength, _Reader) -> 0.

take(unknown, Length, Rest, Reader, Acc) ->
    case Rest of
        <<_:Length/binary, Next/binary>> ->
            parse(Next, Reader, Acc);
        _ ->
            State = {skip, Length - byte_size(Rest)},
            {ok, lists:reverse(Acc), Reader#reader{state = State}}
    end;
take(Type, Length, Rest, Reader, Acc) ->
    case Rest of
        <<Payload:Length/binary, Next/binary>> ->
            parse(Next, Reader, [{Type, Payload} | Acc]);
        _ ->
            State = {payload, Type, Length - byte_size(Rest), [Rest]},
            {ok, lists:reverse(Acc), Reader#reader{state = State}}
    end.

header(Bin) ->
    case trevrpc_varint:decode(Bin) of
        more ->
            more;
        {Code, Rest} ->
            case trevrpc_varint:decode(Rest) of
                more -> more;
                {Length, Payload} -> {Code, Length, Payload}
            end
    end.
