%% @doc Reads the protoc plugin request and writes its response.
%%
%% Decodes the parts of `CodeGeneratorRequest' the generator needs, by hand:
%% gpb decodes descriptors only without the extensions on their options,
%% and the `trevrpc.method' method option is one. What it reads:
%%
%% - the files to generate and the plugin parameter;
%% - per file, its name, package and services;
%% - per message, the types of its fields and whether they are marked
%%   `debug_redact', for the compression of the methods that carry it;
%% - per method, its types, whether each side streams, and its
%%   `trevrpc.method' option.
%%
%% The proto files also come back whole, as a `FileDescriptorSet', which
%% gpb compiles the messages from.
-module(trevrpc_gen_descriptor).

-export([decode_request/1, encode_response/1]).

-export_type([request/0, file/0, message/0, service/0, method/0, response/0]).

-type request() :: #{
    files_to_generate := [binary()],
    parameter := binary(),
    files := [file()],
    file_set := binary()
}.

%% Messages are flattened, nested ones included, and named by their full
%% names, without the leading dot.
-type file() :: #{
    name := binary(),
    package := binary(),
    messages := [message()],
    services := [service()]
}.

-type message() :: #{name := binary(), fields := [field()]}.

%% A field's type name is the full name of its message or enum type.
-type field() :: #{type_name := binary() | undefined, debug_redact := boolean()}.

-type service() :: #{name := binary(), methods := [method()]}.

-type method() :: #{
    name := binary(),
    input := binary(),
    output := binary(),
    client_streaming := boolean(),
    server_streaming := boolean(),
    unreliable_requests := boolean(),
    unreliable_responses := boolean(),
    compression := non_neg_integer()
}.

-type response() :: #{
    error => binary(),
    supported_features => non_neg_integer(),
    files => [{binary(), iodata()}]
}.

%% The extension field of `trevrpc.method' on google.protobuf.MethodOptions.
-define(TREVRPC_METHOD, 58247).

%% @doc Decodes a `CodeGeneratorRequest'.
-spec decode_request(binary()) -> {ok, request()} | {error, binary()}.
decode_request(Bin) ->
    try fields(Bin) of
        Fields ->
            Raw = [File || {15, {bytes, File}} <- Fields],
            {ok, #{
                files_to_generate => [Name || {1, {bytes, Name}} <- Fields],
                parameter => last([Parameter || {2, {bytes, Parameter}} <- Fields], <<>>),
                files => [file(File) || File <- Raw],
                file_set => iolist_to_binary([bytes_field(1, File) || File <- Raw])
            }}
    catch
        throw:malformed -> {error, <<"malformed CodeGeneratorRequest">>}
    end.

%% FileDescriptorProto: name 1, package 2, message_type 4, service 6.
file(Bin) ->
    Fields = fields(Bin),
    Package = last([Package || {2, {bytes, Package}} <- Fields], <<>>),
    #{
        name => last([Name || {1, {bytes, Name}} <- Fields], <<>>),
        package => Package,
        messages => lists:append([message(Message, Package) || {4, {bytes, Message}} <- Fields]),
        services => [service(Service) || {6, {bytes, Service}} <- Fields]
    }.

%% DescriptorProto: name 1, field 2, nested_type 3. Returns the message
%% and the messages nested in it.
message(Bin, Scope) ->
    Fields = fields(Bin),
    Name = qualified(Scope, last([Name || {1, {bytes, Name}} <- Fields], <<>>)),
    [
        #{name => Name, fields => [field(Field) || {2, {bytes, Field}} <- Fields]}
        | lists:append([message(Nested, Name) || {3, {bytes, Nested}} <- Fields])
    ].

qualified(<<>>, Name) -> Name;
qualified(Scope, Name) -> <<Scope/binary, ".", Name/binary>>.

%% FieldDescriptorProto: type_name 6, options 8; FieldOptions:
%% debug_redact 16.
field(Bin) ->
    Fields = fields(Bin),
    Redact = lists:append([
        [Value =/= 0 || {16, {varint, Value}} <- fields(Options)]
     || {8, {bytes, Options}} <- Fields
    ]),
    #{
        type_name =>
            case last([TypeName || {6, {bytes, TypeName}} <- Fields], undefined) of
                <<".", Full/binary>> -> Full;
                Other -> Other
            end,
        debug_redact => last(Redact, false)
    }.

%% ServiceDescriptorProto: name 1, method 2.
service(Bin) ->
    Fields = fields(Bin),
    #{
        name => last([Name || {1, {bytes, Name}} <- Fields], <<>>),
        methods => [method(Method) || {2, {bytes, Method}} <- Fields]
    }.

%% MethodDescriptorProto: name 1, input_type 2, output_type 3, options 4,
%% client_streaming 5, server_streaming 6.
method(Bin) ->
    Fields = fields(Bin),
    Options = lists:append([
        [Trevrpc || {?TREVRPC_METHOD, {bytes, Trevrpc}} <- fields(MethodOptions)]
     || {4, {bytes, MethodOptions}} <- Fields
    ]),
    maps:merge(
        #{
            name => last([Name || {1, {bytes, Name}} <- Fields], <<>>),
            input => type_name(last([Input || {2, {bytes, Input}} <- Fields], <<>>)),
            output => type_name(last([Output || {3, {bytes, Output}} <- Fields], <<>>)),
            client_streaming => flag(5, Fields),
            server_streaming => flag(6, Fields)
        },
        method_options(iolist_to_binary(Options))
    ).

type_name(<<".", Full/binary>>) -> Full;
type_name(Name) -> Name.

%% trevrpc.MethodOptions: unreliable_requests 1, unreliable_responses 2,
%% compression 5. Occurrences of a message merge, so its fields are read
%% from all of them as one.
method_options(Bin) ->
    Fields = fields(Bin),
    #{
        unreliable_requests => flag(1, Fields),
        unreliable_responses => flag(2, Fields),
        compression => last([Value || {5, {varint, Value}} <- Fields], 0)
    }.

flag(Number, Fields) ->
    last([Value =/= 0 || {N, {varint, Value}} <- Fields, N =:= Number], false).

last([], Default) -> Default;
last(Values, _Default) -> lists:last(Values).

%%% Wire format

%% The fields of a message, in order. Groups (wire types 3 and 4) are
%% malformed: no descriptor field is one.
fields(<<>>) ->
    [];
fields(Bin) ->
    {Tag, Rest} = varint(Bin),
    Number = Tag bsr 3,
    Number > 0 orelse throw(malformed),
    {Value, Next} =
        case Tag band 7 of
            0 ->
                {V, R} = varint(Rest),
                {{varint, V}, R};
            1 when byte_size(Rest) >= 8 ->
                <<V:8/binary, R/binary>> = Rest,
                {{fixed64, V}, R};
            2 ->
                {Length, R0} = varint(Rest),
                byte_size(R0) >= Length orelse throw(malformed),
                <<V:Length/binary, R/binary>> = R0,
                {{bytes, V}, R};
            5 when byte_size(Rest) >= 4 ->
                <<V:4/binary, R/binary>> = Rest,
                {{fixed32, V}, R};
            _ ->
                throw(malformed)
        end,
    [{Number, Value} | fields(Next)].

varint(Bin) -> varint(Bin, 0, 0).

varint(<<0:1, B:7, Rest/binary>>, Shift, Acc) when Shift =< 63 ->
    {Acc bor (B bsl Shift), Rest};
varint(<<1:1, B:7, Rest/binary>>, Shift, Acc) when Shift < 63 ->
    varint(Rest, Shift + 7, Acc bor (B bsl Shift));
varint(_, _, _) ->
    throw(malformed).

encode_varint(Value) when Value < 16#80 ->
    <<Value>>;
encode_varint(Value) ->
    <<1:1, (Value band 16#7F):7, (encode_varint(Value bsr 7))/binary>>.

bytes_field(Number, Payload) ->
    [encode_varint((Number bsl 3) bor 2), encode_varint(iolist_size(Payload)), Payload].

%% @doc Encodes a `CodeGeneratorResponse': error 1, supported_features 2,
%% and file 15 (File: name 1, content 15).
-spec encode_response(response()) -> iodata().
encode_response(Response) ->
    [
        case Response of
            #{error := Error} -> bytes_field(1, Error);
            #{} -> []
        end,
        case Response of
            #{supported_features := Features} -> [encode_varint(2 bsl 3), encode_varint(Features)];
            #{} -> []
        end,
        [
            bytes_field(15, [bytes_field(1, Name), bytes_field(15, Content)])
         || {Name, Content} <- maps:get(files, Response, [])
        ]
    ].
