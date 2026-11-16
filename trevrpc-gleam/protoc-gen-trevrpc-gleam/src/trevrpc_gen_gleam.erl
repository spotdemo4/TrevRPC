%% @doc protoc-gen-trevrpc-gleam's files: Gleam types for the messages of
%% each proto file, encoded and decoded by gpb.
%%
%% Gleam's custom types compile to tagged tuples: `Point(x: 1, y: 2)' is
%% `{point, 1, 2}', the constructor's name in Gleam's snake case and then
%% its fields. That is what gpb's records are, so gpb encodes and decodes
%% Gleam values, and Gleam needs no protobuf runtime of its own. For
%% `example/v1/shapes.proto', the plugin writes:
%%
%% - `example/v1/shapes.gleam': a type per message, enum and oneof, a
%%   `new_*' function per message with its default value, and `encode_*'
%%   and `decode_*' functions per message;
%% - `example/v1/example@v1@shapes_pb.erl' and `.hrl': gpb's module, whose
%%   records take the names of the Gleam constructors;
%% - `example/v1/example@v1@shapes_shim.erl': the conversions between the
%%   two where they differ. A message whose fields all match needs none.
%%
%% Where they differ:
%%
%% - presence: gpb leaves an unset field `undefined', Gleam has `Option'.
%%   proto3 `optional' fields, message fields and oneofs are `Option's;
%% - enums: gpb's atoms are the values' names (`'COLOR_RED''), Gleam's the
%%   constructors' (`color_red'), and a value the schema does not know is
%%   an `Unrecognized(Int)' constructor of its enum;
%% - oneofs: gpb tags the value with its field's name, Gleam with the
%%   constructor's, named after the field (`Radius(Float)'), or after the
%%   oneof and the field (`ShapeKindRadius(Float)') where the short name is
%%   taken;
%% - floats: a Gleam `Float' cannot be infinite or NaN, which gpb decodes
%%   as atoms, so a message holding one fails to decode;
%% - strings: a Gleam `String' is UTF-8, which gpb does not check, so a
%%   message holding one that is not, a map key too, fails to decode;
%% - messages without fields: Gleam makes their constructor an atom, gpb a
%%   one-element tuple.
%%
%% Unknown fields are dropped, as gpb drops them. Each service gets a module
%% of its own, over trevrpc-gleam (see {@link trevrpc_gen_gleam_service}).
-module(trevrpc_gen_gleam).

-export([files/1, gleam_snake/1, upper_camel/1]).
%% Used by trevrpc_gen_gleam_service.
-export([label/1, alias/2, type_name_of/2]).

-define(KEYWORDS, [
    <<"as">>,
    <<"assert">>,
    <<"auto">>,
    <<"case">>,
    <<"const">>,
    <<"delegate">>,
    <<"derive">>,
    <<"echo">>,
    <<"else">>,
    <<"fn">>,
    <<"if">>,
    <<"implement">>,
    <<"import">>,
    <<"let">>,
    <<"macro">>,
    <<"opaque">>,
    <<"panic">>,
    <<"pub">>,
    <<"test">>,
    <<"todo">>,
    <<"type">>,
    <<"use">>
]).
%% Types and constructors of the prelude and of the modules the generated
%% code uses, which generated names must not hide.
-define(TAKEN_TYPES, [
    <<"Int">>,
    <<"Float">>,
    <<"Bool">>,
    <<"String">>,
    <<"BitArray">>,
    <<"List">>,
    <<"Result">>,
    <<"Nil">>,
    <<"UtfCodepoint">>,
    <<"Option">>,
    <<"Dict">>
]).
-define(TAKEN_CONSTRUCTORS, [<<"True">>, <<"False">>, <<"Nil">>, <<"Ok">>, <<"Error">>]).

%% Modules the generated code imports, whose names a proto file's module
%% cannot take as its alias.
-define(TAKEN_ALIASES, [
    <<"dict">>,
    <<"option">>,
    <<"channel">>,
    <<"client">>,
    <<"handler">>,
    <<"schema">>,
    <<"server">>,
    <<"status">>
]).

-define(INTS, [
    int32, int64, uint32, uint64, sint32, sint64, fixed32, fixed64, sfixed32, sfixed64
]).

%%% Names

%% @doc Gleam's snake case, with which it names the atoms of constructors:
%% a capital starts a word, so `ShapeStyle' is `shape_style' and
%% `HTTPStatus' `h_t_t_p_status'. As the Gleam compiler's `to_snake_case'.
-spec gleam_snake(binary()) -> binary().
gleam_snake(Name) ->
    list_to_binary(gleam_snake(binary_to_list(Name), true, [])).

gleam_snake([], _Boundary, Acc) ->
    lists:reverse(Acc);
gleam_snake([C | Rest], _Boundary, Acc) when C =:= $_; C =:= $\s ->
    gleam_snake(Rest, true, Acc);
gleam_snake([C | Rest], Boundary, Acc) ->
    Starts = Boundary orelse (C >= $A andalso C =< $Z),
    Lower = lower(C),
    case Starts andalso Acc =/= [] of
        true -> gleam_snake(Rest, false, [Lower, $_ | Acc]);
        false -> gleam_snake(Rest, false, [Lower | Acc])
    end.

%% @doc Gleam's upper camel case: underscores go, and the letter after each
%% is a capital. As the Gleam compiler's `to_upper_camel_case'.
-spec upper_camel(binary()) -> binary().
upper_camel(Name) ->
    list_to_binary(upper_camel(binary_to_list(Name), true)).

upper_camel([], _Boundary) -> [];
upper_camel([$_ | Rest], _Boundary) -> upper_camel(Rest, true);
upper_camel([C | Rest], true) -> [upper(C) | upper_camel(Rest, false)];
upper_camel([C | Rest], false) -> [C | upper_camel(Rest, false)].

lower(C) when C >= $A, C =< $Z -> C - $A + $a;
lower(C) -> C.

upper(C) when C >= $a, C =< $z -> C - $a + $A;
upper(C) -> C.

%%% Files

%% @doc The files for the proto files a `CodeGeneratorRequest' asks for.
-spec files(map()) -> {ok, [{binary(), iodata()}]} | {error, binary()}.
files(#{files_to_generate := Names, files := Files, file_set := FileSet}) ->
    maybe
        ok ?=
            trevrpc_gen_service:check_files([
                File
             || #{name := Name} = File <- Files, lists:member(Name, Names)
            ]),
        {ok, Defs} ?= trevrpc_gen_messages:defs(FileSet),
        {ok, World} ?= world(Defs, [Name || #{name := Name} <- Files]),
        Descriptors = #{
            descriptors => maps:from_list([{Name, File} || #{name := Name} = File <- Files]),
            %% Every message of the request, for the fields marked
            %% debug_redact, as trevrpc_gen_service:modes/2 takes them.
            descriptor_messages => maps:from_list([
                {Name, Message}
             || #{messages := Ms} <- Files, #{name := Name} = Message <- Ms
            ])
        },
        generate(Names, Defs, maps:merge(World, Descriptors), [])
    end.

generate([], _Defs, _World, Acc) ->
    {ok, lists:append(lists:reverse(Acc))};
generate([Name | Rest], Defs, World, Acc) ->
    case file(Name, Defs, World) of
        {ok, Files} -> generate(Rest, Defs, World, [Files | Acc]);
        {error, Message} -> {error, <<Name/binary, ": ", Message/binary>>}
    end.

%% Everything about every message and enum of the request, by full name,
%% and the names each file gives them in Gleam. gpb names a file by its base
%% name, which `ProtoPaths', the files' paths, give back.
world(Defs, ProtoPaths) ->
    Paths = maps:from_list([{trevrpc_gen_messages:gpb_name(P), P} || P <- ProtoPaths]),
    Files = maps:from_list([
        {Key, maps:get(unicode:characters_to_binary(Name), Paths)}
     || {file, {Key, Name}} <- Defs
    ]),
    Packages = maps:from_list([
        {Key, atom_to_binary(Package)}
     || {{pkg_containment, Key}, Package} <- Defs
    ]),
    Owners = maps:from_list(
        [{Name, Key} || {{Kind, Key}, Names} <- Defs, contains(Kind), Name <- Names]
    ),
    Messages = maps:from_list([{Name, Fields} || {{msg, Name}, Fields} <- Defs]),
    Enums = maps:from_list([{Name, Values} || {{enum, Name}, Values} <- Defs]),
    Base = #{
        files => Files,
        packages => Packages,
        owners => Owners,
        messages => Messages,
        enums => Enums
    },
    maybe
        ok ?= no_groups(Defs),
        {ok, Modules} ?= modules(Files),
        {ok, Names} ?= names(Base),
        World = Base#{modules => Modules, names => Names},
        {ok, World#{needs => needs(World)}}
    end.

contains(msg_containment) -> true;
contains(enum_containment) -> true;
contains(_) -> false.

no_groups(Defs) ->
    case [Name || {{group, Name}, _} <- Defs] of
        [] -> ok;
        [Name | _] -> trevrpc_gen_errors:new("~ts: groups are not supported", [Name])
    end.

%% The Gleam module of each proto file: its path without `.proto', whose
%% parts Gleam module names must be, and the Erlang name it shares with the
%% gpb and shim modules beside it.
modules(Files) ->
    Pairs = [module(Key, Name) || Key := Name <- Files],
    case [Error || {error, _} = Error <- Pairs] of
        [] -> {ok, maps:from_list(Pairs)};
        [Error | _] -> Error
    end.

module(Key, Name) ->
    Path = filename:rootname(Name, <<".proto">>),
    Parts = binary:split(Path, <<"/">>, [global]),
    case lists:all(fun module_part/1, Parts) of
        true ->
            {Key, #{
                path => Path,
                erlang => iolist_to_binary(lists:join(<<"@">>, Parts)),
                last => lists:last(Parts)
            }};
        false ->
            trevrpc_gen_errors:new(
                "~ts: a Gleam module needs a path of lower-case letters, digits and underscores, "
                "each part starting with a letter",
                [Name]
            )
    end.

module_part(Part) ->
    re:run(Part, <<"^[a-z][a-z0-9_]*$">>, [{capture, none}]) =:= match andalso
        not lists:member(Part, ?KEYWORDS).

%% The names of each file's types and constructors, checked to be unique in
%% the file's Gleam module.
names(#{files := Files} = World) ->
    Results = [file_names(Key, World) || Key := _ <- Files],
    case [Error || {error, _} = Error <- Results] of
        [] -> {ok, maps:from_list([Names || {ok, Names} <- Results])};
        [Error | _] -> Error
    end.

file_names(Key, #{owners := Owners, messages := Messages, enums := Enums} = World) ->
    Own = lists:sort([Name || Name := K <- Owners, K =:= Key]),
    MessageNames = [{Name, type_name(Name, World)} || Name <- Own, is_map_key(Name, Messages)],
    EnumNames = [{Name, type_name(Name, World)} || Name <- Own, is_map_key(Name, Enums)],
    Oneofs = [
        {{Message, Oneof}, <<Type/binary, (upper_camel(atom_to_binary(Oneof)))/binary>>}
     || {Message, Type} <- MessageNames,
        {gpb_oneof, Oneof, _, _, _} <- maps:get(Message, Messages)
    ],
    Types = [Type || {_, Type} <- MessageNames ++ EnumNames ++ Oneofs],
    EnumConstructors = [
        {Enum, Value, enum_constructor(Type, Value)}
     || {Enum, Type} <- EnumNames, {Value, _Number, _} <- values(maps:get(Enum, Enums))
    ],
    Unrecognized = [{Enum, <<Type/binary, "Unrecognized">>} || {Enum, Type} <- EnumNames],
    Fixed =
        [Type || {_, Type} <- MessageNames] ++
            [C || {_, _, C} <- EnumConstructors] ++
            [C || {_, C} <- Unrecognized],
    OneofConstructors = oneof_constructors(Oneofs, Messages, Fixed),
    Constructors = Fixed ++ [C || {_, _, C} <- OneofConstructors],
    maybe
        ok ?= unique(<<"type">>, Types, ?TAKEN_TYPES),
        ok ?= unique(<<"constructor">>, Constructors, ?TAKEN_CONSTRUCTORS),
        ok ?=
            unique(
                <<"function">>,
                [F || {_, Type} <- MessageNames, F <- message_functions(Type)],
                []
            ),
        ok ?= trevrpc_gen_errors:first([labels(Message, World) || {Message, _} <- MessageNames]),
        {ok,
            {Key, #{
                messages => maps:from_list(MessageNames),
                enums => maps:from_list(EnumNames),
                oneofs => maps:from_list(Oneofs),
                enum_constructors => maps:from_list(
                    [{{Enum, Value}, C} || {Enum, Value, C} <- EnumConstructors]
                ),
                unrecognized => maps:from_list(Unrecognized),
                oneof_constructors => maps:from_list(
                    [{{Oneof, Field}, C} || {Oneof, Field, C} <- OneofConstructors]
                )
            }}}
    end.

%% A message's or enum's name in Gleam: its name within its package, nested
%% names joined, in upper camel case: `example.v1.Shape.Style' is
%% `ShapeStyle'.
type_name(Name, #{owners := Owners, packages := Packages}) ->
    Full = atom_to_binary(Name),
    Local =
        case maps:get(maps:get(Name, Owners), Packages, <<>>) of
            <<>> ->
                Full;
            Package ->
                binary:part(Full, byte_size(Package) + 1, byte_size(Full) - byte_size(Package) - 1)
        end,
    upper_camel(binary:replace(Local, <<".">>, <<"_">>, [global])).

%% gpb lists an enum's values as `{Name, Number}' or `{Name, Number, Opts}'.
values(Values) ->
    [
        case Value of
            {Name, Number} -> {Name, Number, []};
            {Name, Number, Opts} -> {Name, Number, Opts}
        end
     || Value <- Values, is_tuple(Value), element(1, Value) =/= option
    ].

%% An enum value's constructor: the value's name in upper camel case,
%% `COLOR_RED' `ColorRed', with the enum's name before it unless it starts
%% with it already, as proto style has it.
enum_constructor(Type, Value) ->
    Lower = string:lowercase(atom_to_binary(Value)),
    Prefix = <<(trevrpc_gen_names:snake(Type))/binary, "_">>,
    Name = upper_camel(Lower),
    case binary:longest_common_prefix([Lower, Prefix]) =:= byte_size(Prefix) of
        true -> Name;
        false -> <<Type/binary, Name/binary>>
    end.

%% A oneof's constructors: the field's name in upper camel case, or the
%% oneof type's name and that where the short one is taken.
oneof_constructors(Oneofs, Messages, Fixed) ->
    Candidates = [
        {Oneof, Field, upper_camel(atom_to_binary(Field)),
            <<Type/binary, (upper_camel(atom_to_binary(Field)))/binary>>}
     || {{Message, OneofName} = Oneof, Type} <- Oneofs,
        {gpb_oneof, O, _, Fields, _} <- maps:get(Message, Messages),
        O =:= OneofName,
        {field, Field, _, _, _, _, _} <- Fields
    ],
    Shorts = [Short || {_, _, Short, _} <- Candidates],
    [
        {Oneof, Field,
            case
                lists:member(Short, Fixed ++ ?TAKEN_CONSTRUCTORS) orelse
                    length([S || S <- Shorts, S =:= Short]) > 1
            of
                true -> Long;
                false -> Short
            end}
     || {Oneof, Field, Short, Long} <- Candidates
    ].

message_functions(Type) ->
    Snake = gleam_snake(Type),
    [<<"new_", Snake/binary>>, <<"encode_", Snake/binary>>, <<"decode_", Snake/binary>>].

unique(What, Names, Taken) ->
    case [N || N <- Names, lists:member(N, Taken)] of
        [Name | _] ->
            trevrpc_gen_errors:new("Gleam ~ts ~ts is taken by Gleam itself", [What, Name]);
        [] ->
            case Names -- lists:usort(Names) of
                [] -> ok;
                [Name | _] -> trevrpc_gen_errors:new("two Gleam ~tss are named ~ts", [What, Name])
            end
    end.

%% Which messages need converting between Gleam and gpb, each way: to gpb
%% (`to') and from it (`from'). Found as a fixed point, as messages can
%% refer to themselves.
needs(#{messages := Messages} = World) ->
    Start = maps:map(fun(_, _) -> {false, false} end, Messages),
    needs(Start, World).

needs(Needs, #{messages := Messages} = World) ->
    Next = maps:map(
        fun(Name, _) ->
            Slots = slots(Name, World#{needs => Needs}),
            case Slots of
                [] ->
                    {true, true};
                _ ->
                    {
                        lists:any(fun({_, _, T}) -> needs_to(T, Needs) end, Slots),
                        lists:any(fun({_, _, T}) -> needs_from(T, Needs) end, Slots)
                    }
            end
        end,
        Messages
    ),
    case Next =:= Needs of
        true -> Needs;
        false -> needs(Next, World)
    end.

needs_to({scalar, _}, _Needs) -> false;
needs_to({enum, _}, _Needs) -> true;
needs_to({msg, Name}, Needs) -> element(1, maps:get(Name, Needs));
needs_to({list, T}, Needs) -> needs_to(T, Needs);
needs_to({dict, _, T}, Needs) -> needs_to(T, Needs);
needs_to({option, _}, _Needs) -> true;
needs_to({oneof, _, _}, _Needs) -> true.

needs_from({scalar, float}, _Needs) -> true;
needs_from({scalar, string}, _Needs) -> true;
needs_from({scalar, _}, _Needs) -> false;
needs_from({enum, _}, _Needs) -> true;
needs_from({msg, Name}, Needs) -> element(2, maps:get(Name, Needs));
needs_from({list, T}, Needs) -> needs_from(T, Needs);
needs_from({dict, K, T}, Needs) -> needs_from(K, Needs) orelse needs_from(T, Needs);
needs_from({option, _}, _Needs) -> true;
needs_from({oneof, _, _}, _Needs) -> true.

%% A message's fields as gpb's record lays them out, in order: their label,
%% their record position, and their type.
slots(Message, #{messages := Messages} = World) ->
    Slots = [slot(Field, Message, World) || Field <- maps:get(Message, Messages)],
    lists:keysort(2, Slots).

slot({field, Name, _Number, Position, Type, Occurrence, _Opts}, _Message, _World) ->
    {atom_to_binary(Name), Position, occurrence(Occurrence, base(Type))};
slot({gpb_oneof, Name, Position, Fields, _Opts}, Message, _World) ->
    Members = [{Field, base(Type)} || {field, Field, _, _, Type, _, _} <- Fields],
    {atom_to_binary(Name), Position, {oneof, {Message, Name}, Members}}.

base(Type) when Type =:= double; Type =:= float -> {scalar, float};
base(bool) ->
    {scalar, bool};
base(string) ->
    {scalar, string};
base(bytes) ->
    {scalar, bytes};
base({enum, Name}) ->
    {enum, Name};
base({msg, Name}) ->
    {msg, Name};
base({map, Key, Value}) ->
    {map, base(Key), base(Value)};
base(Int) ->
    true = lists:member(Int, ?INTS),
    {scalar, int}.

occurrence(_, {map, Key, Value}) -> {dict, Key, Value};
occurrence(repeated, Type) -> {list, Type};
occurrence(optional, Type) -> {option, Type};
occurrence(defaulty, {msg, _} = Type) -> {option, Type};
occurrence(_, Type) -> Type.

%%% One file

file(Name, Defs, #{files := Files, modules := Modules, owners := Owners} = World) ->
    [Key] = [K || K := N <- Files, N =:= Name],
    #{path := Path, erlang := Erlang} = maps:get(Key, Modules),
    Pb = <<Erlang/binary, "_pb">>,
    Shim = <<Erlang/binary, "_shim">>,
    Own = lists:sort([N || N := K <- Owners, K =:= Key]),
    Records = maps:from_list([
        {Message, binary_to_atom(record(Message, World))}
     || Message := _ <- maps:get(messages, World)
    ]),
    Dir =
        case filename:dirname(Path) of
            <<".">> -> <<>>;
            D -> <<D/binary, "/">>
        end,
    maybe
        {ok, PbFiles} ?=
            trevrpc_gen_messages:generate(Name, Pb, Defs, {records, Records}),
        ok ?= distinct_records(PbFiles, Pb),
        Context = World#{key => Key, pb => Pb, shim => Shim, own => Own},
        {ok, ServiceFiles} ?= services(Name, Dir, Context),
        {ok,
            [{<<Dir/binary, File/binary>>, Data} || {File, Data} <- PbFiles] ++
                [
                    {<<Path/binary, ".gleam">>, gleam_module(Name, Context)},
                    {<<Dir/binary, Shim/binary, ".erl">>, shim_module(Name, Context)}
                ] ++ ServiceFiles}
    else
        {error, Message} -> {error, Message}
    end.

%% A module per service of the file, whose path no file's module has.
services(Name, Dir, #{descriptors := Descriptors, modules := Modules} = Context) ->
    #{package := Package, services := Services} = maps:get(Name, Descriptors),
    Paths = [Path || _ := #{path := Path} <- Modules],
    Results = [
        begin
            Path = trevrpc_gen_gleam_service:module_path(Dir, Service),
            case lists:member(Path, Paths) of
                true ->
                    trevrpc_gen_errors:new(
                        "service ~ts's module ~ts is a proto file's module too", [Service, Path]
                    );
                false ->
                    case
                        trevrpc_gen_gleam_service:generate(Name, S, Context#{package => Package})
                    of
                        {ok, Source} -> {ok, {<<Path/binary, ".gleam">>, Source}};
                        {error, _} = Error -> Error
                    end
            end
        end
     || #{name := Service} = S <- Services
    ],
    case trevrpc_gen_errors:first(Results) of
        ok -> {ok, [File || {ok, File} <- Results]};
        Error -> Error
    end.

%% The atom of a message's record: its Gleam constructor's.
record(Message, #{owners := Owners, names := Names}) ->
    #{messages := Types} = maps:get(maps:get(Message, Owners), Names),
    gleam_snake(maps:get(Message, Types)).

%% gpb's module holds the messages its file uses from others too: their
%% records must not share a name.
distinct_records(Files, Pb) ->
    Header =
        case lists:keyfind(<<Pb/binary, ".hrl">>, 1, Files) of
            {_, Data} -> Data;
            false -> <<>>
        end,
    Names =
        case re:run(Header, <<"-record\\(([^,]+),">>, [global, {capture, all_but_first, binary}]) of
            {match, Records} -> [R || [R] <- Records];
            nomatch -> []
        end,
    case Names -- lists:usort(Names) of
        [] ->
            ok;
        [Record | _] ->
            trevrpc_gen_errors:new(
                "two messages this file uses would both be Gleam's ~ts, "
                "which a gpb module cannot tell apart",
                [Record]
            )
    end.

%%% The Gleam module

gleam_module(Name, #{own := Own, messages := Messages, enums := Enums} = Context) ->
    OwnMessages = [M || M <- Own, is_map_key(M, Messages)],
    OwnEnums = [E || E <- Own, is_map_key(E, Enums)],
    Types = lists:append([message_types(M, Context) || M <- OwnMessages]),
    Body = [
        [enum_type(E, Context) || E <- OwnEnums],
        [Type || {_, Type} <- Types],
        [message_functions(M, Context) || M <- OwnMessages]
    ],
    [
        "//// Generated by protoc-gen-trevrpc-gleam from ",
        Name,
        ". Do not edit.\n\n",
        imports(OwnMessages, Context),
        Body
    ].

%% The imports the module's messages use: the modules of other proto files
%% whose types their fields have, by the alias `alias' gives them, and
%% `gleam/dict' and `gleam/option' for maps and presence.
imports(Messages, #{modules := Modules, key := Key, owners := Owners} = Context) ->
    Types = [T || M <- Messages, {_, _, Slot} <- slots(M, Context), T <- types(Slot)],
    Others = lists:usort([
        maps:get(Name, Owners)
     || {Kind, Name} <- Types,
        Kind =:= enum orelse Kind =:= msg,
        maps:get(Name, Owners) =/= Key
    ]),
    Std =
        [<<"import gleam/dict.{type Dict}\n">> || lists:keymember(dict, 1, Types)] ++
            [
                <<"import gleam/option.{type Option}\n">>
             || lists:keymember(option, 1, Types) orelse lists:keymember(oneof, 1, Types)
            ],
    Lines =
        Std ++
            [
                begin
                    #{path := Path, last := Last} = maps:get(K, Modules),
                    case alias(K, Modules) of
                        Last -> <<"import ", Path/binary, "\n">>;
                        Alias -> <<"import ", Path/binary, " as ", Alias/binary, "\n">>
                    end
                end
             || K <- Others
            ],
    case Lines of
        [] -> [];
        _ -> [lists:sort(Lines), "\n"]
    end.

%% A field's type and every type within it.
types({list, T} = Type) -> [Type | types(T)];
types({dict, K, V} = Type) -> [Type | types(K) ++ types(V)];
types({option, T} = Type) -> [Type | types(T)];
types({oneof, _, Members} = Type) -> [Type | lists:append([types(T) || {_, T} <- Members])];
types(Type) -> [Type].

%% How a module refers to another file's module: by the last part of its
%% path, or its whole path with underscores where that part is ambiguous or
%% taken.
-spec alias(term(), #{term() => #{path := binary(), last := binary(), atom() => term()}}) ->
    binary().
alias(Key, Modules) ->
    #{last := Last, path := Path} = maps:get(Key, Modules),
    Ambiguous =
        lists:member(Last, ?TAKEN_ALIASES) orelse
            length([K || K := #{last := L} <- Modules, L =:= Last]) > 1,
    case Ambiguous of
        true -> binary:replace(Path, <<"/">>, <<"_">>, [global]);
        false -> Last
    end.

enum_type(Enum, #{enums := Enums} = Context) ->
    Type = type_ref(Enum, Context),
    Constructors = [
        ["  ", enum_ref(Enum, Value, Context), "\n"]
     || {Value, _, _} <- values(maps:get(Enum, Enums))
    ],
    [
        "pub type ",
        Type,
        " {\n",
        Constructors,
        "  /// A value this schema does not know.\n  ",
        unrecognized_ref(Enum, Context),
        "(Int)\n}\n\n"
    ].

%% A message's type, and its oneofs' types.
message_types(Message, Context) ->
    Type = type_ref(Message, Context),
    Slots = slots(Message, Context),
    Labels = [label(Label) || {Label, _, _} <- Slots],
    Fields = [
        ["    ", L, ": ", gleam_type(T, Context), ",\n"]
     || {L, {_, _, T}} <- lists:zip(Labels, Slots)
    ],
    MessageType =
        case Slots of
            [] -> ["pub type ", Type, " {\n  ", Type, "\n}\n\n"];
            _ -> ["pub type ", Type, " {\n  ", Type, "(\n", Fields, "  )\n}\n\n"]
        end,
    Oneofs = [
        {Oneof, [
            "pub type ",
            oneof_type(Oneof, Context),
            " {\n",
            [
                ["  ", oneof_constructor(Oneof, Field, Context), "(", gleam_type(T, Context), ")\n"]
             || {Field, T} <- Members
            ],
            "}\n\n"
        ]}
     || {_, _, {oneof, Oneof, Members}} <- Slots
    ],
    [{Message, MessageType} | Oneofs].

%% @doc A field's label, or a function's name: its name in snake case, with
%% an underscore after a Gleam keyword.
-spec label(binary()) -> binary().
label(Name) ->
    Snake = trevrpc_gen_names:snake(Name),
    case lists:member(Snake, ?KEYWORDS) of
        true -> <<Snake/binary, "_">>;
        false -> Snake
    end.

%% A message's fields need labels Gleam takes, one each.
labels(Message, Context) ->
    Labels = [label(Label) || {Label, _, _} <- slots(Message, Context)],
    Valid = [L || L <- Labels, re:run(L, <<"^[a-z][a-z0-9_]*$">>, [{capture, none}]) =:= match],
    case {Labels -- Valid, Labels -- lists:usort(Labels)} of
        {[], []} ->
            ok;
        {[Label | _], _} ->
            trevrpc_gen_errors:new("~ts: field ~ts cannot be a Gleam label", [Message, Label]);
        {[], [Label | _]} ->
            trevrpc_gen_errors:new("~ts: two fields are labelled ~ts in Gleam", [Message, Label])
    end.

gleam_type({scalar, int}, _Context) ->
    <<"Int">>;
gleam_type({scalar, float}, _Context) ->
    <<"Float">>;
gleam_type({scalar, bool}, _Context) ->
    <<"Bool">>;
gleam_type({scalar, string}, _Context) ->
    <<"String">>;
gleam_type({scalar, bytes}, _Context) ->
    <<"BitArray">>;
gleam_type({enum, Name}, Context) ->
    type_ref(Name, Context);
gleam_type({msg, Name}, Context) ->
    type_ref(Name, Context);
gleam_type({list, T}, Context) ->
    ["List(", gleam_type(T, Context), ")"];
gleam_type({dict, K, V}, Context) ->
    ["Dict(", gleam_type(K, Context), ", ", gleam_type(V, Context), ")"];
gleam_type({option, T}, Context) ->
    ["Option(", gleam_type(T, Context), ")"];
gleam_type({oneof, Oneof, _}, Context) ->
    ["Option(", oneof_type(Oneof, Context), ")"].

%% A message's `new_*', `encode_*' and `decode_*'.
message_functions(Message, #{shim := Shim} = Context) ->
    Type = type_name_of(Message, Context),
    Snake = gleam_snake(Type),
    Slots = slots(Message, Context),
    New =
        case Slots of
            [] ->
                Type;
            _ ->
                [
                    Type,
                    "(\n",
                    [
                        ["    ", label(L), ": ", default(T, Context), ",\n"]
                     || {L, _, T} <- Slots
                    ],
                    "  )"
                ]
        end,
    [
        "/// A `",
        Type,
        "` with every field at its default.\npub fn new_",
        Snake,
        "() -> ",
        Type,
        " {\n  ",
        New,
        "\n}\n\n",
        "/// Encodes a `",
        Type,
        "` in the protobuf wire format. A `BitArray` field must hold whole bytes:\n"
        "/// one that does not makes this panic.\n@external(erlang, \"",
        Shim,
        "\", \"encode_",
        Snake,
        "\")\npub fn encode_",
        Snake,
        "(message: ",
        Type,
        ") -> BitArray\n\n",
        "/// Decodes a `",
        Type,
        "` from the protobuf wire format. Malformed data, a string that is not\n"
        "/// UTF-8, or a float that is infinite or NaN, which Gleam cannot hold, is\n"
        "/// an error.\n@external(erlang, \"",
        Shim,
        "\", \"decode_",
        Snake,
        "\")\npub fn decode_",
        Snake,
        "(data: BitArray) -> Result(",
        Type,
        ", Nil)\n\n"
    ].

default({scalar, int}, _Context) ->
    <<"0">>;
default({scalar, float}, _Context) ->
    <<"0.0">>;
default({scalar, bool}, _Context) ->
    <<"False">>;
default({scalar, string}, _Context) ->
    <<"\"\"">>;
default({scalar, bytes}, _Context) ->
    <<"<<>>">>;
default({list, _}, _Context) ->
    <<"[]">>;
default({dict, _, _}, _Context) ->
    <<"dict.new()">>;
default({option, _}, _Context) ->
    <<"option.None">>;
default({oneof, _, _}, _Context) ->
    <<"option.None">>;
default({enum, Enum}, #{enums := Enums} = Context) ->
    Values = values(maps:get(Enum, Enums)),
    Value =
        case [V || {V, 0, _} <- Values] of
            [Zero | _] -> Zero;
            [] -> element(1, hd(Values))
        end,
    qualified(Enum, enum_ref(Enum, Value, Context), Context);
default({msg, Message}, Context) ->
    %% A proto2 required message field.
    [qualified_prefix(Message, Context), "new_", gleam_snake(type_name_of(Message, Context)), "()"].

%%% References

names_of(Name, #{owners := Owners, names := Names}) ->
    maps:get(maps:get(Name, Owners), Names).

%% @doc A message's or enum's type name in Gleam.
-spec type_name_of(atom(), map()) -> binary().
type_name_of(Name, Context) ->
    #{messages := Messages, enums := Enums} = names_of(Name, Context),
    case Messages of
        #{Name := Type} -> Type;
        #{} -> maps:get(Name, Enums)
    end.

%% A message's or enum's type as the current module writes it.
type_ref(Name, Context) ->
    [qualified_prefix(Name, Context), type_name_of(Name, Context)].

qualified_prefix(Name, #{owners := Owners, key := Key, modules := Modules}) ->
    case maps:get(Name, Owners) of
        Key -> [];
        Other -> [alias(Other, Modules), "."]
    end.

qualified(Name, Text, Context) ->
    [qualified_prefix(Name, Context), Text].

enum_ref(Enum, Value, Context) ->
    #{enum_constructors := Constructors} = names_of(Enum, Context),
    maps:get({Enum, Value}, Constructors).

unrecognized_ref(Enum, Context) ->
    #{unrecognized := Unrecognized} = names_of(Enum, Context),
    maps:get(Enum, Unrecognized).

oneof_type({Message, _} = Oneof, Context) ->
    #{oneofs := Oneofs} = names_of(Message, Context),
    maps:get(Oneof, Oneofs).

oneof_constructor({Message, _} = Oneof, Field, Context) ->
    #{oneof_constructors := Constructors} = names_of(Message, Context),
    maps:get({Oneof, Field}, Constructors).

%%% The shim

shim_module(
    Name, #{own := Own, messages := Messages, needs := Needs, shim := Shim, pb := Pb} = Context
) ->
    OwnMessages = [M || M <- Own, is_map_key(M, Messages)],
    Functions = [message_shim(M, Context) || M <- OwnMessages],
    Exports = lists:append([
        [
            [<<"encode_">>, gleam_snake(type_name_of(M, Context)), <<"/1">>],
            [<<"decode_">>, gleam_snake(type_name_of(M, Context)), <<"/1">>]
        ] ++
            [[to_name(M, Context), <<"/1">>] || element(1, maps:get(M, Needs))] ++
            [[from_name(M, Context), <<"/1">>] || element(2, maps:get(M, Needs))]
     || M <- OwnMessages
    ]),
    Body = iolist_to_binary(Functions),
    Enums = used_enums(Body, Context),
    Finite =
        case binary:match(Body, <<"finite(">>) of
            nomatch ->
                [];
            _ ->
                <<
                    "%% A Gleam Float cannot be infinite or NaN, which gpb decodes as atoms.\n"
                    "finite(F) when is_float(F) -> F;\n"
                    "finite(F) when is_integer(F) -> float(F);\n"
                    "finite(F) -> erlang:error({unrepresentable_float, F}).\n\n"
                >>
        end,
    Utf8 =
        case binary:match(Body, <<"utf8(">>) of
            nomatch ->
                [];
            _ ->
                <<
                    "%% A Gleam String is UTF-8, as proto3 requires strings to be.\n"
                    "utf8(S) ->\n"
                    "    case unicode:characters_to_binary(S) of\n"
                    "        S -> S;\n"
                    "        _ -> erlang:error({invalid_utf8, S})\n"
                    "    end.\n\n"
                >>
        end,
    [
        "%% Generated by protoc-gen-trevrpc-gleam from ",
        Name,
        ". Do not edit.\n%%\n"
        "%% Converts between the Gleam types of the module beside it and the records\n"
        "%% of ",
        Pb,
        ", where they differ, and encodes and decodes them.\n-module(",
        trevrpc_gen_names:atom(Shim),
        ").\n\n",
        case Exports of
            [] ->
                [];
            _ ->
                [
                    "-export([\n    ",
                    lists:join(",\n    ", [quote_export(E) || E <- Exports]),
                    "\n]).\n\n"
                ]
        end,
        Body,
        Enums,
        Finite,
        Utf8
    ].

quote_export([Prefix, Snake, Arity]) ->
    [trevrpc_gen_names:atom(iolist_to_binary([Prefix, Snake])), Arity];
quote_export([Name, Arity]) ->
    [trevrpc_gen_names:atom(iolist_to_binary(Name)), Arity].

to_name(Message, Context) -> iolist_to_binary(["to_", record(Message, Context)]).
from_name(Message, Context) -> iolist_to_binary(["from_", record(Message, Context)]).

message_shim(Message, #{needs := Needs, pb := Pb} = Context) ->
    {To, From} = maps:get(Message, Needs),
    Snake = gleam_snake(type_name_of(Message, Context)),
    Record = trevrpc_gen_names:atom(record(Message, Context)),
    PbAtom = trevrpc_gen_names:atom(Pb),
    Encoded =
        case To of
            true -> [call(to_name(Message, Context)), "(Message)"];
            false -> "Message"
        end,
    Decoded =
        case From of
            true ->
                [
                    call(from_name(Message, Context)),
                    "(",
                    PbAtom,
                    ":decode_msg(Data, ",
                    Record,
                    "))"
                ];
            false ->
                [PbAtom, ":decode_msg(Data, ", Record, ")"]
        end,
    Slots = slots(Message, Context),
    [
        call(<<"encode_", Snake/binary>>),
        "(Message) ->\n    ",
        PbAtom,
        ":encode_msg(",
        Encoded,
        ").\n\n",
        call(<<"decode_", Snake/binary>>),
        "(Data) ->\n    try\n        {ok, ",
        Decoded,
        "}\n    catch\n        error:_ -> {error, nil}\n    end.\n\n",
        [conversion(to, Message, Record, Slots, Context) || To],
        [conversion(from, Message, Record, Slots, Context) || From]
    ].

%% `to_*' or `from_*': a record of one side to the other's, field by field.
conversion(Way, Message, Record, [], Context) ->
    {Pattern, Result} =
        case Way of
            to -> {Record, ["{", Record, "}"]};
            from -> {["{", Record, "}"], Record}
        end,
    [call(name(Way, Message, Context)), "(", Pattern, ") ->\n    ", Result, ".\n\n"];
conversion(Way, Message, Record, Slots, Context) ->
    Vars = [["F", integer_to_list(P)] || {_, P, _} <- Slots],
    Exprs = [
        convert(Way, T, ["F", integer_to_list(P)], P, 1, Context)
     || {_, P, T} <- Slots
    ],
    [
        call(name(Way, Message, Context)),
        "({",
        Record,
        ", ",
        lists:join(", ", Vars),
        "}) ->\n    {",
        Record,
        ",\n        ",
        lists:join(",\n        ", Exprs),
        "}.\n\n"
    ].

name(to, Message, Context) -> to_name(Message, Context);
name(from, Message, Context) -> from_name(Message, Context).

call(Name) -> trevrpc_gen_names:atom(iolist_to_binary(Name)).

%% The expression converting `Var', of type `T', one way; `P' and `Depth'
%% name the variables it binds, unique in the function.
convert(Way, T, Var, P, Depth, #{needs := Needs} = Context) ->
    Needed =
        case Way of
            to -> needs_to(T, Needs);
            from -> needs_from(T, Needs)
        end,
    case Needed of
        false -> Var;
        true -> converted(Way, T, Var, P, Depth, Context)
    end.

converted(from, {scalar, float}, Var, _P, _Depth, _Context) ->
    ["finite(", Var, ")"];
converted(from, {scalar, string}, Var, _P, _Depth, _Context) ->
    ["utf8(", Var, ")"];
converted(Way, {enum, Enum}, Var, _P, _Depth, _Context) ->
    [call(enum_function(Way, Enum)), "(", Var, ")"];
converted(
    Way,
    {msg, Message},
    Var,
    _P,
    _Depth,
    #{owners := Owners, key := Key, modules := Modules} = Context
) ->
    Module =
        case maps:get(Message, Owners) of
            Key ->
                [];
            Other ->
                [
                    trevrpc_gen_names:atom(<<
                        (maps:get(erlang, maps:get(Other, Modules)))/binary, "_shim"
                    >>),
                    ":"
                ]
        end,
    [Module, call(name(Way, Message, Context)), "(", Var, ")"];
converted(Way, {list, T}, Var, P, Depth, Context) ->
    V = var(P, Depth),
    ["[", convert(Way, T, V, P, Depth + 1, Context), " || ", V, " <- ", Var, "]"];
converted(from, {dict, {scalar, string} = K, T}, Var, P, Depth, Context) ->
    %% String keys are checked too.
    Key = ["K", integer_to_list(P), "_", integer_to_list(Depth)],
    V = var(P, Depth),
    [
        "maps:fold(fun(",
        Key,
        ", ",
        V,
        ", Acc) -> Acc#{",
        convert(from, K, Key, P, Depth + 1, Context),
        " => ",
        convert(from, T, V, P, Depth + 1, Context),
        "} end, #{}, ",
        Var,
        ")"
    ];
converted(Way, {dict, _K, T}, Var, P, Depth, Context) ->
    V = var(P, Depth),
    ["maps:map(fun(_, ", V, ") -> ", convert(Way, T, V, P, Depth + 1, Context), " end, ", Var, ")"];
converted(to, {option, T}, Var, P, Depth, Context) ->
    V = var(P, Depth),
    [
        "case ",
        Var,
        " of none -> undefined; {some, ",
        V,
        "} -> ",
        convert(to, T, V, P, Depth + 1, Context),
        " end"
    ];
converted(from, {option, T}, Var, P, Depth, Context) ->
    V = var(P, Depth),
    [
        "case ",
        Var,
        " of undefined -> none; ",
        V,
        " -> {some, ",
        convert(from, T, V, P, Depth + 1, Context),
        "} end"
    ];
converted(to, {oneof, Oneof, Members}, Var, P, Depth, Context) ->
    V = var(P, Depth),
    [
        "case ",
        Var,
        " of none -> undefined",
        [
            [
                "; {some, {",
                tag(Oneof, Field, Context),
                ", ",
                V,
                "}} -> {",
                trevrpc_gen_names:atom(atom_to_binary(Field)),
                ", ",
                convert(to, T, V, P, Depth + 1, Context),
                "}"
            ]
         || {Field, T} <- Members
        ],
        " end"
    ];
converted(from, {oneof, Oneof, Members}, Var, P, Depth, Context) ->
    V = var(P, Depth),
    [
        "case ",
        Var,
        " of undefined -> none",
        [
            [
                "; {",
                trevrpc_gen_names:atom(atom_to_binary(Field)),
                ", ",
                V,
                "} -> {some, {",
                tag(Oneof, Field, Context),
                ", ",
                convert(from, T, V, P, Depth + 1, Context),
                "}}"
            ]
         || {Field, T} <- Members
        ],
        " end"
    ].

var(P, Depth) -> ["V", integer_to_list(P), "_", integer_to_list(Depth)].

tag(Oneof, Field, Context) ->
    trevrpc_gen_names:atom(gleam_snake(oneof_constructor(Oneof, Field, Context))).

enum_function(to, Enum) -> <<"enum_to_", (atom_to_binary(Enum))/binary>>;
enum_function(from, Enum) -> <<"enum_from_", (atom_to_binary(Enum))/binary>>.

%% The conversions of the enums the shim's functions use, each way.
used_enums(Body, #{enums := Enums} = Context) ->
    [
        enum_conversions(Way, Enum, Values, Context)
     || Enum := Values <- maps:iterator(Enums, ordered),
        Way <- [to, from],
        binary:match(Body, iolist_to_binary(call(enum_function(Way, Enum)))) =/= nomatch
    ].

enum_conversions(Way, Enum, Values, Context) ->
    Name = call(enum_function(Way, Enum)),
    Unrecognized = trevrpc_gen_names:atom(gleam_snake(unrecognized_ref(Enum, Context))),
    Clauses = [
        begin
            Gleam = trevrpc_gen_names:atom(gleam_snake(enum_ref(Enum, Value, Context))),
            Gpb = trevrpc_gen_names:atom(atom_to_binary(Value)),
            case Way of
                to -> [Name, "(", Gleam, ") -> ", Gpb];
                from -> [Name, "(", Gpb, ") -> ", Gleam]
            end
        end
     || {Value, _, _} <- values(Values)
    ],
    Last =
        case Way of
            to -> [Name, "({", Unrecognized, ", N}) -> N"];
            from -> [Name, "(N) when is_integer(N) -> {", Unrecognized, ", N}"]
        end,
    [lists:join(";\n", Clauses ++ [Last]), ".\n\n"].
