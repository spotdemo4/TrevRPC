%% @doc The protobuf wire format primitives the control messages need.
%%
%% Only what {@link trevrpc_wire} uses: base-128 varints, tags, and
%% length-delimited fields, plus reading any field so unknown ones can be
%% skipped. Application messages never pass through here; they are opaque
%% bytes to the runtime.
-module(trevrpc_pb).

-export([
    encode_varint/1,
    decode_varint/1,
    varint_field/2,
    bytes_field/2,
    decode_field/1,
    valid_utf8/1,
    to_uint32/1,
    to_int32/1
]).

-export_type([field_value/0]).

-define(MAX_FIELD_NUMBER, 16#1FFFFFFF).
-define(UINT64_MASK, 16#FFFFFFFFFFFFFFFF).

%% A decoded field value by wire type: a varint, a length-delimited payload,
%% or the raw bytes of a fixed-width value.
-type field_value() ::
    {varint, non_neg_integer()}
    | {bytes, binary()}
    | {fixed64, binary()}
    | {fixed32, binary()}.

%% @doc Encodes a base-128 varint. Values are unsigned 64-bit.
-spec encode_varint(non_neg_integer()) -> binary().
encode_varint(Value) when is_integer(Value), Value >= 0, Value < 16#80 ->
    <<Value>>;
encode_varint(Value) when is_integer(Value), Value >= 0, Value =< ?UINT64_MASK ->
    <<1:1, (Value band 16#7F):7, (encode_varint(Value bsr 7))/binary>>.

%% @doc Decodes a base-128 varint of at most 10 bytes. Like protobuf-go, it
%% rejects one whose value does not fit 64 bits.
-spec decode_varint(binary()) -> {non_neg_integer(), binary()} | error.
decode_varint(<<0:1, B0:7, Rest/binary>>) ->
    {B0, Rest};
decode_varint(Bin) ->
    decode_varint(Bin, 0, 0).

%% The tenth byte holds bit 63 only.
decode_varint(<<0:1, B:7, Rest/binary>>, Shift, Acc) when Shift < 63; B =< 1 ->
    {Acc bor (B bsl Shift), Rest};
decode_varint(<<1:1, B:7, Rest/binary>>, Shift, Acc) when Shift < 63 ->
    decode_varint(Rest, Shift + 7, Acc bor (B bsl Shift));
decode_varint(_, _, _) ->
    error.

%% @doc A varint field (wire type 0).
-spec varint_field(pos_integer(), non_neg_integer()) -> binary().
varint_field(Number, Value) ->
    <<(encode_varint(Number bsl 3))/binary, (encode_varint(Value))/binary>>.

%% @doc A length-delimited field (wire type 2).
-spec bytes_field(pos_integer(), iodata()) -> iodata().
bytes_field(Number, Payload) ->
    [
        <<
            (encode_varint((Number bsl 3) bor 2))/binary,
            (encode_varint(iolist_size(Payload)))/binary
        >>
        | Payload
    ].

%% @doc Decodes the field at the start of `Bin'. Groups (wire types 3 and
%% 4), field number 0 and truncated fields are malformed.
-spec decode_field(binary()) ->
    {pos_integer(), field_value(), binary()} | error.
decode_field(Bin) ->
    case decode_varint(Bin) of
        {Tag, Rest} when Tag bsr 3 >= 1, Tag bsr 3 =< ?MAX_FIELD_NUMBER ->
            decode_value(Tag bsr 3, Tag band 7, Rest);
        _ ->
            error
    end.

decode_value(Number, 0, Bin) ->
    case decode_varint(Bin) of
        {Value, Rest} -> {Number, {varint, Value}, Rest};
        error -> error
    end;
decode_value(Number, 1, <<Value:8/binary, Rest/binary>>) ->
    {Number, {fixed64, Value}, Rest};
decode_value(Number, 2, Bin) ->
    case decode_varint(Bin) of
        {Length, Rest} when byte_size(Rest) >= Length ->
            <<Value:Length/binary, Next/binary>> = Rest,
            {Number, {bytes, Value}, Next};
        _ ->
            error
    end;
decode_value(Number, 5, <<Value:4/binary, Rest/binary>>) ->
    {Number, {fixed32, Value}, Rest};
decode_value(_, _, _) ->
    error.

%% @doc Whether `Bin' is valid UTF-8, as protobuf strings must be.
-spec valid_utf8(binary()) -> boolean().
valid_utf8(<<C, Rest/binary>>) when C < 16#80 -> valid_utf8(Rest);
valid_utf8(<<_/utf8, Rest/binary>>) -> valid_utf8(Rest);
valid_utf8(<<>>) -> true;
valid_utf8(<<_/binary>>) -> false.

%% @doc The low 32 bits of a decoded varint, as a `uint32' field holds.
-spec to_uint32(non_neg_integer()) -> non_neg_integer().
to_uint32(Value) -> Value band 16#FFFFFFFF.

%% @doc A decoded varint as an `int32' (or enum) field holds it: its low 32
%% bits, signed.
-spec to_int32(non_neg_integer()) -> integer().
to_int32(Value) ->
    <<Signed:32/signed>> = <<(Value band 16#FFFFFFFF):32>>,
    Signed.
