%% @doc The QMux wire format (draft-ietf-quic-qmux-02): the QUIC v1 frames
%% QMux carries, its QX_TRANSPORT_PARAMETERS and QX_PING frames, and the
%% Size-prefixed records of QMux over TLS.
%%
%% Only what TrevRPC uses is supported: bidirectional streams with QUIC flow
%% control and stream limits. Frames QMux forbids (ACK, PING, CRYPTO,
%% NEW_TOKEN, DATAGRAM, ...) and extensions TrevRPC never negotiates are
%% FRAME_ENCODING_ERRORs, and transport parameters that only make sense over
%% QUIC packets are TRANSPORT_PARAMETER_ERRORs.
-module(trevrpc_qmux_frame).

-export([
    stream/4,
    reset_stream/3,
    stop_sending/2,
    max_data/1,
    max_stream_data/2,
    max_streams_bidi/1,
    application_close/2,
    transport_close/3,
    ping/1,
    ping_response/1,
    transport_parameters/1,
    record/1,
    decode_frames/1,
    decode_parameters/1,
    decode_record/2,
    stream_overhead/3,
    error_code/1,
    default_max_record_size/0,
    max_streams_limit/0
]).

-export_type([frame/0, parameters/0, error/0]).

-define(PADDING, 16#00).
-define(RESET_STREAM, 16#04).
-define(STOP_SENDING, 16#05).
-define(STREAM, 16#08).
-define(MAX_DATA, 16#10).
-define(MAX_STREAM_DATA, 16#11).
-define(MAX_STREAMS_BIDI, 16#12).
-define(MAX_STREAMS_UNI, 16#13).
-define(DATA_BLOCKED, 16#14).
-define(STREAM_DATA_BLOCKED, 16#15).
-define(STREAMS_BLOCKED_BIDI, 16#16).
-define(STREAMS_BLOCKED_UNI, 16#17).
-define(CONNECTION_CLOSE, 16#1c).
-define(APPLICATION_CLOSE, 16#1d).
%% draft-ietf-quic-qmux-02 codepoints.
-define(TRANSPORT_PARAMETERS, 16#3f5153300d0a0d0a).
-define(PING, 16#348c67529ef8c7bd).
-define(PING_RESPONSE, 16#348c67529ef8c7be).
-define(MAX_RECORD_SIZE, 16#0571c59429cd0845).

-define(FIN, 16#01).
-define(LEN, 16#02).
-define(OFF, 16#04).

-define(DEFAULT_MAX_RECORD_SIZE, 16382).
-define(MAX_STREAMS, 1 bsl 60).

-type frame() ::
    {stream, non_neg_integer(), non_neg_integer(), binary(), boolean()}
    | {reset_stream, non_neg_integer(), non_neg_integer(), non_neg_integer()}
    | {stop_sending, non_neg_integer(), non_neg_integer()}
    | {max_data, non_neg_integer()}
    | {max_stream_data, non_neg_integer(), non_neg_integer()}
    | {max_streams, bidi | uni, non_neg_integer()}
    | {data_blocked, non_neg_integer()}
    | {stream_data_blocked, non_neg_integer(), non_neg_integer()}
    | {streams_blocked, bidi | uni, non_neg_integer()}
    | {connection_close, transport, non_neg_integer(), non_neg_integer(), binary()}
    | {connection_close, application, non_neg_integer(), binary()}
    | {transport_parameters, binary()}
    | {ping, non_neg_integer()}
    | {ping_response, non_neg_integer()}.

%% Transport parameters, all optional: max_idle_timeout (ms),
%% initial_max_data, initial_max_stream_data_bidi_local and _remote,
%% initial_max_streams_bidi, and max_record_size.
-type parameters() :: #{
    max_idle_timeout => non_neg_integer(),
    initial_max_data => non_neg_integer(),
    initial_max_stream_data_bidi_local => non_neg_integer(),
    initial_max_stream_data_bidi_remote => non_neg_integer(),
    initial_max_streams_bidi => non_neg_integer(),
    max_record_size => non_neg_integer()
}.

%% A protocol error: the QUIC transport error code, the frame type that
%% caused it (0 when none), and a reason.
-type error() :: {atom(), non_neg_integer(), binary()}.

%%% Encoding

%% @doc A STREAM frame. It always carries its length, so more frames may
%% follow it in the record.
-spec stream(non_neg_integer(), non_neg_integer(), iodata(), boolean()) -> iodata().
stream(Id, Offset, Data, Fin) ->
    Type = ?STREAM bor ?LEN bor off_flag(Offset) bor fin_flag(Fin),
    [
        varint(Type),
        varint(Id),
        case Offset of
            0 -> <<>>;
            _ -> varint(Offset)
        end,
        varint(iolist_size(Data))
        | Data
    ].

off_flag(0) -> 0;
off_flag(_) -> ?OFF.

fin_flag(true) -> ?FIN;
fin_flag(false) -> 0.

%% @doc The bytes a STREAM frame header takes for Length bytes at Offset.
-spec stream_overhead(non_neg_integer(), non_neg_integer(), non_neg_integer()) -> pos_integer().
stream_overhead(Id, Offset, Length) ->
    1 + trevrpc_varint:size(Id) + trevrpc_varint:size(Length) +
        case Offset of
            0 -> 0;
            _ -> trevrpc_varint:size(Offset)
        end.

-spec reset_stream(non_neg_integer(), non_neg_integer(), non_neg_integer()) -> binary().
reset_stream(Id, Code, FinalSize) ->
    <<
        (varint(?RESET_STREAM))/binary,
        (varint(Id))/binary,
        (varint(Code))/binary,
        (varint(FinalSize))/binary
    >>.

-spec stop_sending(non_neg_integer(), non_neg_integer()) -> binary().
stop_sending(Id, Code) ->
    <<(varint(?STOP_SENDING))/binary, (varint(Id))/binary, (varint(Code))/binary>>.

-spec max_data(non_neg_integer()) -> binary().
max_data(Max) ->
    <<(varint(?MAX_DATA))/binary, (varint(Max))/binary>>.

-spec max_stream_data(non_neg_integer(), non_neg_integer()) -> binary().
max_stream_data(Id, Max) ->
    <<(varint(?MAX_STREAM_DATA))/binary, (varint(Id))/binary, (varint(Max))/binary>>.

-spec max_streams_bidi(non_neg_integer()) -> binary().
max_streams_bidi(Max) ->
    <<(varint(?MAX_STREAMS_BIDI))/binary, (varint(Max))/binary>>.

%% @doc An application CONNECTION_CLOSE (type 0x1d).
-spec application_close(non_neg_integer(), binary()) -> binary().
application_close(Code, Reason) ->
    <<
        (varint(?APPLICATION_CLOSE))/binary,
        (varint(Code))/binary,
        (varint(byte_size(Reason)))/binary,
        Reason/binary
    >>.

%% @doc A transport CONNECTION_CLOSE (type 0x1c).
-spec transport_close(non_neg_integer(), non_neg_integer(), binary()) -> binary().
transport_close(Code, FrameType, Reason) ->
    <<
        (varint(?CONNECTION_CLOSE))/binary,
        (varint(Code))/binary,
        (varint(FrameType))/binary,
        (varint(byte_size(Reason)))/binary,
        Reason/binary
    >>.

-spec ping(non_neg_integer()) -> binary().
ping(Sequence) ->
    <<(varint(?PING))/binary, (varint(Sequence))/binary>>.

-spec ping_response(non_neg_integer()) -> binary().
ping_response(Sequence) ->
    <<(varint(?PING_RESPONSE))/binary, (varint(Sequence))/binary>>.

%% @doc The QX_TRANSPORT_PARAMETERS frame advertising `Params'. Zero values
%% are left out; max_record_size is always sent, defaulting to 16382.
-spec transport_parameters(parameters()) -> binary().
transport_parameters(Params) ->
    Body = <<
        (parameter(16#01, maps:get(max_idle_timeout, Params, 0)))/binary,
        (parameter(16#04, maps:get(initial_max_data, Params, 0)))/binary,
        (parameter(16#05, maps:get(initial_max_stream_data_bidi_local, Params, 0)))/binary,
        (parameter(16#06, maps:get(initial_max_stream_data_bidi_remote, Params, 0)))/binary,
        (parameter(16#08, maps:get(initial_max_streams_bidi, Params, 0)))/binary,
        (parameter(?MAX_RECORD_SIZE, maps:get(max_record_size, Params, ?DEFAULT_MAX_RECORD_SIZE)))/binary
    >>,
    <<(varint(?TRANSPORT_PARAMETERS))/binary, (varint(byte_size(Body)))/binary, Body/binary>>.

parameter(_Id, 0) ->
    <<>>;
parameter(Id, Value) ->
    Encoded = varint(Value),
    <<(varint(Id))/binary, (varint(byte_size(Encoded)))/binary, Encoded/binary>>.

%% @doc A record of QMux over TLS: its Size, then its frames.
-spec record(iodata()) -> iodata().
record(Frames) ->
    [varint(iolist_size(Frames)) | Frames].

varint(Value) -> trevrpc_varint:encode(Value).

%%% Decoding

%% @doc Takes the next record from a byte stream. Returns `more' when `Bin'
%% ends inside it, and an error for a record larger than `MaxRecordSize'.
-spec decode_record(binary(), non_neg_integer()) ->
    {ok, binary(), binary()} | more | {error, error()}.
decode_record(Bin, MaxRecordSize) ->
    case trevrpc_varint:decode(Bin) of
        more ->
            more;
        {Size, _Rest} when Size > MaxRecordSize ->
            {error,
                {frame_encoding, 0,
                    iolist_to_binary(
                        io_lib:format("record of ~b bytes exceeds max_record_size ~b", [
                            Size, MaxRecordSize
                        ])
                    )}};
        {Size, Rest} ->
            case Rest of
                <<Record:Size/binary, Next/binary>> -> {ok, Record, Next};
                _ -> more
            end
    end.

%% @doc Decodes the frames of one record, in order. PADDING is dropped.
-spec decode_frames(binary()) -> {ok, [frame()]} | {error, error()}.
decode_frames(Record) ->
    decode_frames(Record, []).

decode_frames(<<>>, Acc) ->
    {ok, lists:reverse(Acc)};
decode_frames(Bin, Acc) ->
    case trevrpc_varint:decode(Bin) of
        more ->
            truncated(0);
        {?PADDING, Rest} ->
            decode_frames(Rest, Acc);
        {Type, Rest} ->
            case frame(Type, Rest) of
                {ok, Frame, Next} -> decode_frames(Next, [Frame | Acc]);
                {error, _} = Error -> Error
            end
    end.

frame(Type, Bin) when Type >= ?STREAM, Type =< ?STREAM bor 7 ->
    stream_frame(Type, Bin);
frame(?RESET_STREAM, Bin) ->
    fields(?RESET_STREAM, 3, Bin, fun([Id, Code, Final]) -> {reset_stream, Id, Code, Final} end);
frame(?STOP_SENDING, Bin) ->
    fields(?STOP_SENDING, 2, Bin, fun([Id, Code]) -> {stop_sending, Id, Code} end);
frame(?MAX_DATA, Bin) ->
    fields(?MAX_DATA, 1, Bin, fun([Max]) -> {max_data, Max} end);
frame(?MAX_STREAM_DATA, Bin) ->
    fields(?MAX_STREAM_DATA, 2, Bin, fun([Id, Max]) -> {max_stream_data, Id, Max} end);
frame(?MAX_STREAMS_BIDI, Bin) ->
    fields(?MAX_STREAMS_BIDI, 1, Bin, fun([Max]) -> {max_streams, bidi, Max} end);
frame(?MAX_STREAMS_UNI, Bin) ->
    fields(?MAX_STREAMS_UNI, 1, Bin, fun([Max]) -> {max_streams, uni, Max} end);
frame(?DATA_BLOCKED, Bin) ->
    fields(?DATA_BLOCKED, 1, Bin, fun([Max]) -> {data_blocked, Max} end);
frame(?STREAM_DATA_BLOCKED, Bin) ->
    fields(?STREAM_DATA_BLOCKED, 2, Bin, fun([Id, Max]) -> {stream_data_blocked, Id, Max} end);
frame(?STREAMS_BLOCKED_BIDI, Bin) ->
    fields(?STREAMS_BLOCKED_BIDI, 1, Bin, fun([Max]) -> {streams_blocked, bidi, Max} end);
frame(?STREAMS_BLOCKED_UNI, Bin) ->
    fields(?STREAMS_BLOCKED_UNI, 1, Bin, fun([Max]) -> {streams_blocked, uni, Max} end);
frame(?CONNECTION_CLOSE, Bin) ->
    case varints(?CONNECTION_CLOSE, 3, Bin) of
        {ok, [Code, FrameType, Length], Rest} ->
            with_bytes(?CONNECTION_CLOSE, Length, Rest, fun(Reason) ->
                {connection_close, transport, Code, FrameType, Reason}
            end);
        Error ->
            Error
    end;
frame(?APPLICATION_CLOSE, Bin) ->
    case varints(?APPLICATION_CLOSE, 2, Bin) of
        {ok, [Code, Length], Rest} ->
            with_bytes(?APPLICATION_CLOSE, Length, Rest, fun(Reason) ->
                {connection_close, application, Code, Reason}
            end);
        Error ->
            Error
    end;
frame(?TRANSPORT_PARAMETERS, Bin) ->
    case varints(?TRANSPORT_PARAMETERS, 1, Bin) of
        {ok, [Length], Rest} ->
            with_bytes(?TRANSPORT_PARAMETERS, Length, Rest, fun(Body) ->
                {transport_parameters, Body}
            end);
        Error ->
            Error
    end;
frame(?PING, Bin) ->
    fields(?PING, 1, Bin, fun([Sequence]) -> {ping, Sequence} end);
frame(?PING_RESPONSE, Bin) ->
    fields(?PING_RESPONSE, 1, Bin, fun([Sequence]) -> {ping_response, Sequence} end);
frame(Type, _Bin) ->
    {error,
        {frame_encoding, Type,
            iolist_to_binary(io_lib:format("frame type 0x~.16b is not allowed in QMux", [Type]))}}.

stream_frame(Type, Bin) ->
    maybe
        {ok, Id, Rest1} ?= varint_field(Type, Bin),
        {ok, Offset, Rest2} ?=
            case Type band ?OFF of
                0 -> {ok, 0, Rest1};
                _ -> varint_field(Type, Rest1)
            end,
        {ok, Length, Rest3} ?=
            case Type band ?LEN of
                0 -> {ok, byte_size(Rest2), Rest2};
                _ -> varint_field(Type, Rest2)
            end,
        true ?= Length =< byte_size(Rest3) orelse truncated(Type),
        true ?=
            Offset + Length =< trevrpc_varint:max() orelse
            {error, {frame_encoding, Type, <<"stream data beyond the maximum offset">>}},
        <<Data:Length/binary, Next/binary>> = Rest3,
        {ok, {stream, Id, Offset, Data, Type band ?FIN =/= 0}, Next}
    end.

fields(Type, Count, Bin, Build) ->
    case varints(Type, Count, Bin) of
        {ok, Values, Rest} -> {ok, Build(Values), Rest};
        Error -> Error
    end.

varints(Type, Count, Bin) ->
    varints(Type, Count, Bin, []).

varints(_Type, 0, Bin, Acc) ->
    {ok, lists:reverse(Acc), Bin};
varints(Type, Count, Bin, Acc) ->
    case varint_field(Type, Bin) of
        {ok, Value, Rest} -> varints(Type, Count - 1, Rest, [Value | Acc]);
        Error -> Error
    end.

varint_field(Type, Bin) ->
    case trevrpc_varint:decode(Bin) of
        more -> truncated(Type);
        {Value, Rest} -> {ok, Value, Rest}
    end.

with_bytes(Type, Length, Bin, Build) ->
    case Bin of
        <<Bytes:Length/binary, Rest/binary>> -> {ok, Build(Bytes), Rest};
        _ -> truncated(Type)
    end.

truncated(Type) ->
    {error, {frame_encoding, Type, <<"truncated frame">>}}.

%% @doc Decodes the body of a QX_TRANSPORT_PARAMETERS frame. Parameters that
%% only make sense over QUIC packets are refused, unknown ones ignored.
-spec decode_parameters(binary()) -> {ok, parameters()} | {error, error()}.
decode_parameters(Body) ->
    case decode_parameters(Body, #{}, #{max_record_size => ?DEFAULT_MAX_RECORD_SIZE}) of
        {ok, #{max_record_size := Size}} when Size < ?DEFAULT_MAX_RECORD_SIZE ->
            parameter_error(
                io_lib:format("max_record_size ~b is below ~b", [Size, ?DEFAULT_MAX_RECORD_SIZE])
            );
        {ok, #{initial_max_streams_bidi := Streams}} when Streams > ?MAX_STREAMS ->
            parameter_error(io_lib:format("initial_max_streams_bidi ~b exceeds 2^60", [Streams]));
        Result ->
            Result
    end.

decode_parameters(<<>>, _Seen, Params) ->
    {ok, Params};
decode_parameters(Bin, Seen, Params) ->
    maybe
        {ok, Id, Rest1} ?= param_varint(Bin, <<"truncated transport parameter ID">>),
        {ok, Length, Rest2} ?= param_varint(Rest1, <<"truncated transport parameter">>),
        true ?= Length =< byte_size(Rest2) orelse
            parameter_error(<<"truncated transport parameter">>),
        <<Value:Length/binary, Next/binary>> = Rest2,
        false ?=
            is_map_key(Id, Seen) andalso
            parameter_error(io_lib:format("duplicate transport parameter 0x~.16b", [Id])),
        {ok, Updated} ?= parameter_value(Id, Value, Params),
        decode_parameters(Next, Seen#{Id => true}, Updated)
    end.

param_varint(Bin, Message) ->
    case trevrpc_varint:decode(Bin) of
        more -> parameter_error(Message);
        {Value, Rest} -> {ok, Value, Rest}
    end.

parameter_value(Id, _Value, _Params) when
    Id =:= 16#00;
    Id =:= 16#02;
    Id =:= 16#03;
    Id =:= 16#0a;
    Id =:= 16#0b;
    Id =:= 16#0c;
    Id =:= 16#0d;
    Id =:= 16#0e;
    Id =:= 16#0f;
    Id =:= 16#10
->
    parameter_error(io_lib:format("transport parameter 0x~.16b is not allowed in QMux", [Id]));
parameter_value(Id, Value, Params) ->
    Key =
        case Id of
            16#01 -> max_idle_timeout;
            16#04 -> initial_max_data;
            16#05 -> initial_max_stream_data_bidi_local;
            16#06 -> initial_max_stream_data_bidi_remote;
            16#07 -> ignored;
            16#08 -> initial_max_streams_bidi;
            16#09 -> ignored;
            ?MAX_RECORD_SIZE -> max_record_size;
            _ -> unknown
        end,
    case {Key, trevrpc_varint:decode(Value)} of
        {unknown, _} ->
            {ok, Params};
        {_, {Integer, <<>>}} when Key =:= ignored ->
            _ = Integer,
            {ok, Params};
        {_, {Integer, <<>>}} ->
            {ok, Params#{Key => Integer}};
        _ ->
            parameter_error(io_lib:format("malformed transport parameter 0x~.16b", [Id]))
    end.

parameter_error(Message) ->
    {error, {transport_parameter, ?TRANSPORT_PARAMETERS, iolist_to_binary(Message)}}.

%% @doc The QUIC transport error code of a protocol error.
-spec error_code(atom()) -> non_neg_integer().
error_code(no_error) -> 16#00;
error_code(internal) -> 16#01;
error_code(flow_control) -> 16#03;
error_code(stream_limit) -> 16#04;
error_code(stream_state) -> 16#05;
error_code(final_size) -> 16#06;
error_code(frame_encoding) -> 16#07;
error_code(transport_parameter) -> 16#08;
error_code(protocol_violation) -> 16#0a.

-spec default_max_record_size() -> pos_integer().
default_max_record_size() -> ?DEFAULT_MAX_RECORD_SIZE.

-spec max_streams_limit() -> pos_integer().
max_streams_limit() -> ?MAX_STREAMS.
