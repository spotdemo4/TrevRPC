%% @doc `buf.validate.Violations', the status detail of a request that broke
%% its validation rules (see the `validator' option of {@link
%% trevrpc_server}).
%%
%% It is encoded and decoded by hand, as the control messages are, so that
%% the runtime depends on no protobuf library and its users need no
%% bindings of `buf/validate/validate.proto'. A violation is the map that
%% gpb decodes a `buf.validate.Violation' into, with strings as binaries and
%% unset fields left out: a gpb module generated from `validate.proto' reads
%% and writes the same terms.
%%
%% A client reads the violations of a failed call with {@link
%% from_status/1}:
%%
%% ```
%% {error, Status} = example_v1_greeter:say_hello(Channel, #{name => <<>>}),
%% invalid_argument = trevrpc_status:code(Status),
%% {ok, [#{rule_id := <<"string.min_len">>}]} = trevrpc_violations:from_status(Status).
%% '''
%%
%% A validator reports them with {@link status/1}, which builds the status
%% the Go and Rust servers fail such a call with.
-module(trevrpc_violations).

-export([type/0, status/1, from_status/1, encode/1, decode/1, format/1]).

-export_type([violation/0, field_path/0, element/0, field_type/0, subscript/0]).

-define(TYPE, <<"buf.validate.Violations">>).
%% How many violations the detail of status/1 lists at most, as Go's
%% protovalidatetrevrpc.MaxDetailViolations, so that a request breaking a
%% rule on each of many items still fails with a status that fits a frame.
-define(MAX_DETAIL_VIOLATIONS, 100).
-define(UINT64_MASK, 16#FFFFFFFFFFFFFFFF).

%% A broken rule: the field it applies to (none for a rule on the message
%% itself), the path of the rule within `buf.validate''s rules, its ID, a
%% message saying what broke it, and, for a rule on a map field, whether it
%% applies to the key rather than the value.
-type violation() :: #{
    field => field_path(),
    rule => field_path(),
    rule_id => binary(),
    message => binary(),
    for_key => boolean()
}.

-type field_path() :: #{elements => [element()]}.

%% One step of a field path: a field, with the index or key of a repeated
%% or map field's item.
-type element() :: #{
    field_number => integer(),
    field_name => binary(),
    field_type => field_type(),
    key_type => field_type(),
    value_type => field_type(),
    subscript => subscript()
}.

%% A `google.protobuf.FieldDescriptorProto.Type'. Values it does not define
%% decode as their number, as gpb decodes them.
-type field_type() ::
    'TYPE_DOUBLE'
    | 'TYPE_FLOAT'
    | 'TYPE_INT64'
    | 'TYPE_UINT64'
    | 'TYPE_INT32'
    | 'TYPE_FIXED64'
    | 'TYPE_FIXED32'
    | 'TYPE_BOOL'
    | 'TYPE_STRING'
    | 'TYPE_GROUP'
    | 'TYPE_MESSAGE'
    | 'TYPE_BYTES'
    | 'TYPE_UINT32'
    | 'TYPE_ENUM'
    | 'TYPE_SFIXED32'
    | 'TYPE_SFIXED64'
    | 'TYPE_SINT32'
    | 'TYPE_SINT64'
    | integer().

-type subscript() ::
    {index, non_neg_integer()}
    | {bool_key, boolean()}
    | {int_key, integer()}
    | {uint_key, non_neg_integer()}
    | {string_key, binary()}.

%% @doc The detail's type, `buf.validate.Violations'.
-spec type() -> binary().
type() -> ?TYPE.

%% @doc The status a call whose request broke its rules fails with:
%% `invalid_argument', with a message naming the first violation and
%% counting the others, and a `buf.validate.Violations' detail listing the
%% first 100. Without violations, it has no detail.
-spec status([violation()]) -> trevrpc_status:status().
status([]) ->
    trevrpc_status:new(invalid_argument, <<"validation error">>);
status([First | Rest] = Violations) ->
    Message =
        case length(Rest) of
            0 ->
                [<<"validation error: ">>, format(First)];
            More ->
                [<<"validation error: ">>, format(First), io_lib:format(" (and ~b more)", [More])]
        end,
    Detail = #{
        type => ?TYPE,
        value => iolist_to_binary(encode(lists:sublist(Violations, ?MAX_DETAIL_VIOLATIONS)))
    },
    (trevrpc_status:new(invalid_argument, utf8_message(Message)))#{details => [Detail]}.

%% A status message must be UTF-8, which a violation's strings need not be.
utf8_message(Message) ->
    Bin = iolist_to_binary(Message),
    case trevrpc_pb:valid_utf8(Bin) of
        true -> Bin;
        false -> <<"validation error">>
    end.

%% @doc The violations of the first `buf.validate.Violations' detail of
%% `Status', or `error' when it has none or it does not decode.
-spec from_status(trevrpc_status:status()) -> {ok, [violation()]} | error.
from_status(Status) ->
    case trevrpc_status:find_details(Status, ?TYPE) of
        [Value | _] -> decode(Value);
        [] -> error
    end.

%% @doc A violation as protovalidate prints it: its field path, then its
%% message, or its rule ID in brackets when it has no message, such as
%% `items[0].name: value length must be at least 1 characters'.
-spec format(violation()) -> iodata().
format(Violation) ->
    Path =
        case path_string(maps:get(elements, maps:get(field, Violation, #{}), [])) of
            [] -> [];
            String -> [String, <<": ">>]
        end,
    case Violation of
        #{message := Message} when Message =/= <<>> -> [Path, Message];
        #{rule_id := RuleId} when RuleId =/= <<>> -> [Path, $[, RuleId, $]];
        #{} -> [Path, <<"[unknown]">>]
    end.

path_string(Elements) ->
    lists:join($., [
        [maps:get(field_name, Element, <<>>), subscript_string(Element)]
     || Element <- Elements
    ]).

subscript_string(#{subscript := {index, Index}}) -> [$[, integer_to_binary(Index), $]];
subscript_string(#{subscript := {bool_key, Key}}) -> [$[, atom_to_binary(Key), $]];
subscript_string(#{subscript := {int_key, Key}}) -> [$[, integer_to_binary(Key), $]];
subscript_string(#{subscript := {uint_key, Key}}) -> [$[, integer_to_binary(Key), $]];
subscript_string(#{subscript := {string_key, Key}}) -> [$[, quote(Key), $]];
subscript_string(#{}) -> [].

%% A string key as Go's strconv.Quote prints it, which protovalidate uses,
%% except that every code point from U+0080 up counts as printable.
quote(Key) -> [$", escape(Key), $"].

escape(<<C/utf8, Rest/binary>>) -> [escape_char(C) | escape(Rest)];
escape(<<Byte, Rest/binary>>) -> [hex(Byte) | escape(Rest)];
escape(<<>>) -> [].

escape_char($") -> <<"\\\"">>;
escape_char($\\) -> <<"\\\\">>;
escape_char(7) -> <<"\\a">>;
escape_char($\b) -> <<"\\b">>;
escape_char($\f) -> <<"\\f">>;
escape_char($\n) -> <<"\\n">>;
escape_char($\r) -> <<"\\r">>;
escape_char($\t) -> <<"\\t">>;
escape_char($\v) -> <<"\\v">>;
escape_char(C) when C < 16#20; C =:= 16#7F -> hex(C);
escape_char(C) -> <<C/utf8>>.

hex(Byte) -> io_lib:format("\\x~2.16.0b", [Byte]).

%%% Encoding

%% @doc Encodes a `buf.validate.Violations' holding `Violations'.
-spec encode([violation()]) -> iodata().
encode(Violations) ->
    [trevrpc_pb:bytes_field(1, encode_violation(Violation)) || Violation <- Violations].

encode_violation(Violation) ->
    [
        string(2, rule_id, Violation),
        string(3, message, Violation),
        case Violation of
            #{for_key := ForKey} -> bool(4, ForKey);
            #{} -> []
        end,
        path(5, field, Violation),
        path(6, rule, Violation)
    ].

path(Number, Key, Map) ->
    case Map of
        #{Key := #{} = Path} ->
            trevrpc_pb:bytes_field(Number, [
                trevrpc_pb:bytes_field(1, encode_element(Element))
             || Element <- maps:get(elements, Path, [])
            ]);
        #{} ->
            []
    end.

encode_element(Element) ->
    [
        case Element of
            #{field_number := FieldNumber} -> signed(1, FieldNumber);
            #{} -> []
        end,
        string(2, field_name, Element),
        [
            signed(Number, type_to_int(maps:get(Key, Element)))
         || {Number, Key} <- [{3, field_type}, {4, key_type}, {5, value_type}],
            is_map_key(Key, Element)
        ],
        case Element of
            #{subscript := {index, Index}} -> trevrpc_pb:varint_field(6, Index);
            #{subscript := {bool_key, Key}} -> bool(7, Key);
            #{subscript := {int_key, Key}} -> signed(8, Key);
            #{subscript := {uint_key, Key}} -> trevrpc_pb:varint_field(9, Key);
            #{subscript := {string_key, Key}} -> trevrpc_pb:bytes_field(10, Key);
            #{} -> []
        end
    ].

%% proto2 optional fields: a field that is set goes out even when it holds
%% its default.
string(Number, Key, Map) ->
    case Map of
        #{Key := Value} -> trevrpc_pb:bytes_field(Number, Value);
        #{} -> []
    end.

bool(Number, true) -> trevrpc_pb:varint_field(Number, 1);
bool(Number, false) -> trevrpc_pb:varint_field(Number, 0).

%% An int32, int64 or enum: a negative value is its 64-bit two's complement.
signed(Number, Value) -> trevrpc_pb:varint_field(Number, Value band ?UINT64_MASK).

%%% Decoding

%% @doc Decodes a `buf.validate.Violations' into its violations. As every
%% protobuf decoder, it skips unknown fields, keeps the last value of a
%% scalar field, and merges the occurrences of an embedded message field.
-spec decode(binary()) -> {ok, [violation()]} | error.
decode(Bin) ->
    case trevrpc_pb:decode_message(Bin, fun violations_field/3, []) of
        {ok, Violations} -> {ok, lists:reverse(Violations)};
        error -> error
    end.

violations_field(1, {bytes, Bin}, Violations) ->
    case decode_violation(Bin) of
        {ok, Violation} -> [Violation | Violations];
        error -> error
    end;
violations_field(_, _, Violations) ->
    Violations.

%% The occurrences of an embedded message merge, as their encodings
%% concatenated decode, so the paths decode once the violation is read.
decode_violation(Bin) ->
    case trevrpc_pb:decode_message(Bin, fun violation_field/3, {#{}, #{}}) of
        {ok, {Violation, Paths}} ->
            maps:fold(
                fun
                    (_Key, _Parts, error) ->
                        error;
                    (Key, Parts, {ok, Acc}) ->
                        case decode_path(iolist_to_binary(Parts)) of
                            {ok, Path} -> {ok, Acc#{Key => Path}};
                            error -> error
                        end
                end,
                {ok, Violation},
                Paths
            );
        error ->
            error
    end.

violation_field(2, {bytes, RuleId}, {Violation, Paths}) ->
    {Violation#{rule_id => RuleId}, Paths};
violation_field(3, {bytes, Message}, {Violation, Paths}) ->
    {Violation#{message => Message}, Paths};
violation_field(4, {varint, ForKey}, {Violation, Paths}) ->
    {Violation#{for_key => ForKey =/= 0}, Paths};
violation_field(5, {bytes, Path}, {Violation, Paths}) ->
    {Violation, append(field, Path, Paths)};
violation_field(6, {bytes, Path}, {Violation, Paths}) ->
    {Violation, append(rule, Path, Paths)};
violation_field(_, _, Acc) ->
    Acc.

append(Key, Bin, Paths) ->
    Paths#{Key => [maps:get(Key, Paths, []), Bin]}.

decode_path(Bin) ->
    case trevrpc_pb:decode_message(Bin, fun path_field/3, []) of
        {ok, Elements} -> {ok, #{elements => lists:reverse(Elements)}};
        error -> error
    end.

path_field(1, {bytes, Bin}, Elements) ->
    case trevrpc_pb:decode_message(Bin, fun element_field/3, #{}) of
        {ok, Element} -> [Element | Elements];
        error -> error
    end;
path_field(_, _, Elements) ->
    Elements.

element_field(1, {varint, Value}, Element) ->
    Element#{field_number => trevrpc_pb:to_int32(Value)};
element_field(2, {bytes, Name}, Element) ->
    Element#{field_name => Name};
element_field(3, {varint, Value}, Element) ->
    Element#{field_type => type_from_int(trevrpc_pb:to_int32(Value))};
element_field(4, {varint, Value}, Element) ->
    Element#{key_type => type_from_int(trevrpc_pb:to_int32(Value))};
element_field(5, {varint, Value}, Element) ->
    Element#{value_type => type_from_int(trevrpc_pb:to_int32(Value))};
element_field(6, {varint, Index}, Element) ->
    Element#{subscript => {index, Index}};
element_field(7, {varint, Key}, Element) ->
    Element#{subscript => {bool_key, Key =/= 0}};
element_field(8, {varint, Key}, Element) ->
    Element#{subscript => {int_key, trevrpc_pb:to_int64(Key)}};
element_field(9, {varint, Key}, Element) ->
    Element#{subscript => {uint_key, Key}};
element_field(10, {bytes, Key}, Element) ->
    Element#{subscript => {string_key, Key}};
element_field(_, _, Element) ->
    Element.

%%% google.protobuf.FieldDescriptorProto.Type

types() ->
    [
        {'TYPE_DOUBLE', 1},
        {'TYPE_FLOAT', 2},
        {'TYPE_INT64', 3},
        {'TYPE_UINT64', 4},
        {'TYPE_INT32', 5},
        {'TYPE_FIXED64', 6},
        {'TYPE_FIXED32', 7},
        {'TYPE_BOOL', 8},
        {'TYPE_STRING', 9},
        {'TYPE_GROUP', 10},
        {'TYPE_MESSAGE', 11},
        {'TYPE_BYTES', 12},
        {'TYPE_UINT32', 13},
        {'TYPE_ENUM', 14},
        {'TYPE_SFIXED32', 15},
        {'TYPE_SFIXED64', 16},
        {'TYPE_SINT32', 17},
        {'TYPE_SINT64', 18}
    ].

type_to_int(Type) when is_integer(Type) ->
    Type;
type_to_int(Type) when is_atom(Type) ->
    {Type, Value} = lists:keyfind(Type, 1, types()),
    Value.

type_from_int(Value) ->
    case lists:keyfind(Value, 2, types()) of
        {Type, Value} -> Type;
        false -> Value
    end.
