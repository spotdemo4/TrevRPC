%% @doc The control messages: the payloads of REQUEST (`RpcRequest'),
%% HEADERS (`RpcHeaders'), ANSWER (`RpcAnswer') and STATUS (`RpcStatus')
%% frames.
%%
%% They are encoded and decoded by hand, so the runtime depends on no
%% protobuf library. Encoders accept maps that leave out any field but the
%% request's `method' and take proto3 defaults for the rest. Decoders return
%% every field, follow protobuf's rules (unknown fields are skipped, the last
%% value of a scalar wins, repeated fields accept packed and unpacked
%% encodings, a repeated embedded message merges) and reject strings that are
%% not valid UTF-8.
-module(trevrpc_wire).

-export([
    encode_request/1,
    decode_request/1,
    encode_headers/1,
    decode_headers/1,
    encode_answer/1,
    decode_answer/1,
    encode_status/1,
    decode_status/1,
    kind_to_int/1,
    kind_from_int/1
]).

-export_type([
    kind/0,
    stream_compression/0,
    dictionary_info/0,
    connection_metadata/0,
    detail/0,
    request/0,
    headers/0,
    answer/0,
    status/0
]).

-type metadata() :: trevrpc_metadata:metadata().

%% The RPC kind. Values the protocol does not define decode as `{unknown, N}';
%% servers reject them.
-type kind() ::
    unary
    | client_streaming
    | server_streaming
    | bidirectional_streaming
    | {unknown, integer()}.

%% Whether a client takes part in stream compression. Values the protocol does
%% not define decode as `{unknown, N}'; servers reject them.
-type stream_compression() :: none | enabled | optimistic | {unknown, integer()}.

-type dictionary_info() :: #{id => non_neg_integer(), fingerprint => binary()}.

-type connection_metadata() :: #{id => non_neg_integer(), metadata => metadata()}.

-type detail() :: #{type => binary(), value => binary()}.

-type request() :: #{
    method := binary(),
    metadata => metadata(),
    kind => kind(),
    timeout_nanos => non_neg_integer(),
    datagram_flow => non_neg_integer(),
    accept_dictionaries => [non_neg_integer()],
    dictionary_query => [dictionary_info()],
    stream_compression => stream_compression(),
    connection_metadata => connection_metadata() | undefined
}.

-type headers() :: #{metadata => metadata()}.

-type answer() :: #{
    dictionaries => [dictionary_info()],
    stream_compression => boolean(),
    connection_metadata => non_neg_integer()
}.

-type status() :: #{
    code => trevrpc_status:code(),
    message => binary(),
    metadata => metadata(),
    refused_stream_compression => boolean(),
    details => [detail()]
}.

-define(UINT64_MASK, 16#FFFFFFFFFFFFFFFF).

%%% Request

%% @doc Encodes an `RpcRequest'.
-spec encode_request(request()) -> iodata().
encode_request(#{method := Method} = Request) ->
    [
        string_field(1, Method),
        metadata_field(2, maps:get(metadata, Request, #{})),
        enum_field(3, kind_to_int(maps:get(kind, Request, unary))),
        uint_field(4, maps:get(timeout_nanos, Request, 0)),
        uint_field(5, maps:get(datagram_flow, Request, 0)),
        packed_field(6, maps:get(accept_dictionaries, Request, [])),
        [
            trevrpc_pb:bytes_field(7, encode_dictionary_info(Info))
         || Info <- maps:get(dictionary_query, Request, [])
        ],
        enum_field(8, stream_compression_to_int(maps:get(stream_compression, Request, none))),
        case maps:get(connection_metadata, Request, undefined) of
            undefined -> [];
            Set -> trevrpc_pb:bytes_field(9, encode_connection_metadata(Set))
        end
    ].

%% @doc Decodes an `RpcRequest'.
-spec decode_request(binary()) ->
    {ok, #{
        method := binary(),
        metadata := metadata(),
        kind := kind(),
        timeout_nanos := non_neg_integer(),
        datagram_flow := non_neg_integer(),
        accept_dictionaries := [non_neg_integer()],
        dictionary_query := [dictionary_info()],
        stream_compression := stream_compression(),
        connection_metadata := connection_metadata() | undefined
    }}
    | error.
decode_request(Bin) ->
    Empty = #{
        method => <<>>,
        metadata => #{},
        kind => unary,
        timeout_nanos => 0,
        datagram_flow => 0,
        accept_dictionaries => [],
        dictionary_query => [],
        stream_compression => none,
        connection_metadata => undefined
    },
    case decode_message(Bin, fun request_field/3, Empty) of
        {ok, #{accept_dictionaries := Accept, dictionary_query := Query} = Request} ->
            {ok, Request#{
                accept_dictionaries := lists:reverse(Accept),
                dictionary_query := lists:reverse(Query)
            }};
        error ->
            error
    end.

request_field(1, {bytes, Method}, Request) ->
    string_value(Method, fun(Value) -> Request#{method := Value} end);
request_field(2, {bytes, Entry}, #{metadata := Metadata} = Request) ->
    case map_entry(Entry) of
        {Key, Value} -> Request#{metadata := Metadata#{Key => Value}};
        error -> error
    end;
request_field(3, {varint, Value}, Request) ->
    Request#{kind := kind_from_int(trevrpc_pb:to_int32(Value))};
request_field(4, {varint, Value}, Request) ->
    Request#{timeout_nanos := Value};
request_field(5, {varint, Value}, Request) ->
    Request#{datagram_flow := Value};
request_field(6, {varint, Value}, #{accept_dictionaries := Accept} = Request) ->
    Request#{accept_dictionaries := [trevrpc_pb:to_uint32(Value) | Accept]};
request_field(6, {bytes, Packed}, #{accept_dictionaries := Accept} = Request) ->
    case packed_uint32s(Packed, Accept) of
        error -> error;
        Values -> Request#{accept_dictionaries := Values}
    end;
request_field(7, {bytes, Bin}, #{dictionary_query := Query} = Request) ->
    case decode_dictionary_info(Bin) of
        {ok, Info} -> Request#{dictionary_query := [Info | Query]};
        error -> error
    end;
request_field(8, {varint, Value}, Request) ->
    Request#{stream_compression := stream_compression_from_int(trevrpc_pb:to_int32(Value))};
request_field(9, {bytes, Bin}, #{connection_metadata := Previous} = Request) ->
    Base =
        case Previous of
            undefined -> #{id => 0, metadata => #{}};
            _ -> Previous
        end,
    case decode_message(Bin, fun connection_metadata_field/3, Base) of
        {ok, Set} -> Request#{connection_metadata := Set};
        error -> error
    end;
request_field(_, _, Request) ->
    Request.

connection_metadata_field(1, {varint, Value}, Set) ->
    Set#{id := trevrpc_pb:to_uint32(Value)};
connection_metadata_field(2, {bytes, Entry}, #{metadata := Metadata} = Set) ->
    case map_entry(Entry) of
        {Key, Value} -> Set#{metadata := Metadata#{Key => Value}};
        error -> error
    end;
connection_metadata_field(_, _, Set) ->
    Set.

encode_connection_metadata(Set) ->
    [
        uint_field(1, maps:get(id, Set, 0)),
        metadata_field(2, maps:get(metadata, Set, #{}))
    ].

%% @doc The wire value of an RPC kind.
-spec kind_to_int(kind()) -> integer().
kind_to_int(unary) -> 0;
kind_to_int(client_streaming) -> 1;
kind_to_int(server_streaming) -> 2;
kind_to_int(bidirectional_streaming) -> 3;
kind_to_int({unknown, Value}) when is_integer(Value) -> Value.

%% @doc The RPC kind of a wire value.
-spec kind_from_int(integer()) -> kind().
kind_from_int(0) -> unary;
kind_from_int(1) -> client_streaming;
kind_from_int(2) -> server_streaming;
kind_from_int(3) -> bidirectional_streaming;
kind_from_int(Value) when is_integer(Value) -> {unknown, Value}.

stream_compression_to_int(none) -> 0;
stream_compression_to_int(enabled) -> 1;
stream_compression_to_int(optimistic) -> 2;
stream_compression_to_int({unknown, Value}) when is_integer(Value) -> Value.

stream_compression_from_int(0) -> none;
stream_compression_from_int(1) -> enabled;
stream_compression_from_int(2) -> optimistic;
stream_compression_from_int(Value) -> {unknown, Value}.

%%% Headers

%% @doc Encodes an `RpcHeaders'.
-spec encode_headers(headers()) -> iodata().
encode_headers(Headers) ->
    metadata_field(1, maps:get(metadata, Headers, #{})).

%% @doc Decodes an `RpcHeaders'.
-spec decode_headers(binary()) -> {ok, #{metadata := metadata()}} | error.
decode_headers(Bin) ->
    decode_message(Bin, fun headers_field/3, #{metadata => #{}}).

headers_field(1, {bytes, Entry}, #{metadata := Metadata} = Headers) ->
    case map_entry(Entry) of
        {Key, Value} -> Headers#{metadata := Metadata#{Key => Value}};
        error -> error
    end;
headers_field(_, _, Headers) ->
    Headers.

%%% Answer

%% @doc Encodes an `RpcAnswer'.
-spec encode_answer(answer()) -> iodata().
encode_answer(Answer) ->
    [
        [
            trevrpc_pb:bytes_field(1, encode_dictionary_info(Info))
         || Info <- maps:get(dictionaries, Answer, [])
        ],
        bool_field(2, maps:get(stream_compression, Answer, false)),
        uint_field(3, maps:get(connection_metadata, Answer, 0))
    ].

%% @doc Decodes an `RpcAnswer'.
-spec decode_answer(binary()) ->
    {ok, #{
        dictionaries := [dictionary_info()],
        stream_compression := boolean(),
        connection_metadata := non_neg_integer()
    }}
    | error.
decode_answer(Bin) ->
    Empty = #{dictionaries => [], stream_compression => false, connection_metadata => 0},
    case decode_message(Bin, fun answer_field/3, Empty) of
        {ok, #{dictionaries := Dictionaries} = Answer} ->
            {ok, Answer#{dictionaries := lists:reverse(Dictionaries)}};
        error ->
            error
    end.

answer_field(1, {bytes, Bin}, #{dictionaries := Dictionaries} = Answer) ->
    case decode_dictionary_info(Bin) of
        {ok, Info} -> Answer#{dictionaries := [Info | Dictionaries]};
        error -> error
    end;
answer_field(2, {varint, Value}, Answer) ->
    Answer#{stream_compression := Value =/= 0};
answer_field(3, {varint, Value}, Answer) ->
    Answer#{connection_metadata := trevrpc_pb:to_uint32(Value)};
answer_field(_, _, Answer) ->
    Answer.

encode_dictionary_info(Info) ->
    [
        uint_field(1, maps:get(id, Info, 0)),
        bytes_field(2, maps:get(fingerprint, Info, <<>>))
    ].

decode_dictionary_info(Bin) ->
    decode_message(Bin, fun dictionary_info_field/3, #{id => 0, fingerprint => <<>>}).

dictionary_info_field(1, {varint, Value}, Info) ->
    Info#{id := trevrpc_pb:to_uint32(Value)};
dictionary_info_field(2, {bytes, Value}, Info) ->
    Info#{fingerprint := Value};
dictionary_info_field(_, _, Info) ->
    Info.

%%% Status

%% @doc Encodes an `RpcStatus'. An OK status with nothing else is empty.
-spec encode_status(status()) -> iodata().
encode_status(Status) ->
    [
        uint_field(1, trevrpc_status:to_int(maps:get(code, Status, ok))),
        string_field(2, maps:get(message, Status, <<>>)),
        metadata_field(3, maps:get(metadata, Status, #{})),
        bool_field(4, maps:get(refused_stream_compression, Status, false)),
        [
            trevrpc_pb:bytes_field(5, encode_detail(Detail))
         || Detail <- maps:get(details, Status, [])
        ]
    ].

%% @doc Decodes an `RpcStatus'. Codes the protocol does not define decode as
%% `unknown'.
-spec decode_status(binary()) ->
    {ok, #{
        code := trevrpc_status:code(),
        message := binary(),
        metadata := metadata(),
        refused_stream_compression := boolean(),
        details := [detail()]
    }}
    | error.
decode_status(Bin) ->
    Empty = #{
        code => 0,
        message => <<>>,
        metadata => #{},
        refused_stream_compression => false,
        details => []
    },
    case decode_message(Bin, fun status_field/3, Empty) of
        {ok, #{code := Code, details := Details} = Status} ->
            {ok, Status#{
                code := trevrpc_status:from_int(Code),
                details := lists:reverse(Details)
            }};
        error ->
            error
    end.

status_field(1, {varint, Value}, Status) ->
    Status#{code := trevrpc_pb:to_uint32(Value)};
status_field(2, {bytes, Message}, Status) ->
    string_value(Message, fun(Value) -> Status#{message := Value} end);
status_field(3, {bytes, Entry}, #{metadata := Metadata} = Status) ->
    case map_entry(Entry) of
        {Key, Value} -> Status#{metadata := Metadata#{Key => Value}};
        error -> error
    end;
status_field(4, {varint, Value}, Status) ->
    Status#{refused_stream_compression := Value =/= 0};
status_field(5, {bytes, Bin}, #{details := Details} = Status) ->
    case decode_message(Bin, fun detail_field/3, #{type => <<>>, value => <<>>}) of
        {ok, Detail} -> Status#{details := [Detail | Details]};
        error -> error
    end;
status_field(_, _, Status) ->
    Status.

encode_detail(Detail) ->
    [
        string_field(1, maps:get(type, Detail, <<>>)),
        bytes_field(2, maps:get(value, Detail, <<>>))
    ].

detail_field(1, {bytes, Type}, Detail) ->
    string_value(Type, fun(Value) -> Detail#{type := Value} end);
detail_field(2, {bytes, Value}, Detail) ->
    Detail#{value := Value};
detail_field(_, _, Detail) ->
    Detail.

%%% Shared

decode_message(<<>>, _Field, Message) ->
    {ok, Message};
decode_message(Bin, Field, Message) ->
    case trevrpc_pb:decode_field(Bin) of
        {Number, Value, Rest} ->
            case Field(Number, Value, Message) of
                error -> error;
                Next -> decode_message(Rest, Field, Next)
            end;
        error ->
            error
    end.

string_value(Bin, Set) ->
    case trevrpc_pb:valid_utf8(Bin) of
        true -> Set(Bin);
        false -> error
    end.

%% A map<string, bytes> entry. A missing key or value is empty.
map_entry(Bin) ->
    case decode_message(Bin, fun map_entry_field/3, {<<>>, <<>>}) of
        {ok, Entry} -> Entry;
        error -> error
    end.

map_entry_field(1, {bytes, Key}, {_, Value}) ->
    string_value(Key, fun(Valid) -> {Valid, Value} end);
map_entry_field(2, {bytes, Value}, {Key, _}) ->
    {Key, Value};
map_entry_field(_, _, Entry) ->
    Entry.

packed_uint32s(<<>>, Acc) ->
    Acc;
packed_uint32s(Bin, Acc) ->
    case trevrpc_pb:decode_varint(Bin) of
        {Value, Rest} -> packed_uint32s(Rest, [trevrpc_pb:to_uint32(Value) | Acc]);
        error -> error
    end.

%% Both fields of a map entry are always written, even when empty, as
%% protobuf-go does.
metadata_field(Number, Metadata) ->
    maps:fold(
        fun(Key, Value, Acc) ->
            [
                trevrpc_pb:bytes_field(Number, [
                    trevrpc_pb:bytes_field(1, Key), trevrpc_pb:bytes_field(2, Value)
                ])
                | Acc
            ]
        end,
        [],
        Metadata
    ).

string_field(_Number, <<>>) -> [];
string_field(Number, Value) when is_binary(Value) -> trevrpc_pb:bytes_field(Number, Value).

bytes_field(_Number, <<>>) -> [];
bytes_field(Number, Value) when is_binary(Value) -> trevrpc_pb:bytes_field(Number, Value).

uint_field(_Number, 0) -> [];
uint_field(Number, Value) -> trevrpc_pb:varint_field(Number, Value).

%% Negative enum values take ten bytes, as int32 fields do.
enum_field(_Number, 0) -> [];
enum_field(Number, Value) -> trevrpc_pb:varint_field(Number, Value band ?UINT64_MASK).

bool_field(_Number, false) -> [];
bool_field(Number, true) -> trevrpc_pb:varint_field(Number, 1).

packed_field(_Number, []) ->
    [];
packed_field(Number, Values) ->
    trevrpc_pb:bytes_field(Number, [trevrpc_pb:encode_varint(Value) || Value <- Values]).
