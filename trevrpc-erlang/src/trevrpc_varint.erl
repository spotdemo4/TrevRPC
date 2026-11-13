%% @doc QUIC variable-length integers (RFC 9000, section 16).
%%
%% TrevRPC frame headers and datagram flow IDs use them. Encoding is always
%% minimal; decoding accepts any valid encoding.
-module(trevrpc_varint).

-export([encode/1, decode/1, size/1, max/0]).

-define(MAX, 16#3FFFFFFFFFFFFFFF).

%% @doc The largest value a varint holds, 2^62 - 1.
-spec max() -> non_neg_integer().
max() -> ?MAX.

%% @doc Encodes `Value' in the fewest bytes.
-spec encode(non_neg_integer()) -> binary().
encode(Value) when is_integer(Value), Value >= 0, Value < 16#40 ->
    <<0:2, Value:6>>;
encode(Value) when is_integer(Value), Value >= 0, Value < 16#4000 ->
    <<1:2, Value:14>>;
encode(Value) when is_integer(Value), Value >= 0, Value < 16#40000000 ->
    <<2:2, Value:30>>;
encode(Value) when is_integer(Value), Value >= 0, Value =< ?MAX ->
    <<3:2, Value:62>>.

%% @doc Decodes the varint at the start of `Bin'. Returns `more' when `Bin'
%% ends inside it.
-spec decode(binary()) -> {non_neg_integer(), binary()} | more.
decode(<<0:2, Value:6, Rest/binary>>) -> {Value, Rest};
decode(<<1:2, Value:14, Rest/binary>>) -> {Value, Rest};
decode(<<2:2, Value:30, Rest/binary>>) -> {Value, Rest};
decode(<<3:2, Value:62, Rest/binary>>) -> {Value, Rest};
decode(<<_/binary>>) -> more.

%% @doc The number of bytes the minimal encoding of `Value' takes.
-spec size(non_neg_integer()) -> 1 | 2 | 4 | 8.
size(Value) when is_integer(Value), Value >= 0, Value < 16#40 -> 1;
size(Value) when is_integer(Value), Value >= 0, Value < 16#4000 -> 2;
size(Value) when is_integer(Value), Value >= 0, Value < 16#40000000 -> 4;
size(Value) when is_integer(Value), Value >= 0, Value =< ?MAX -> 8.
