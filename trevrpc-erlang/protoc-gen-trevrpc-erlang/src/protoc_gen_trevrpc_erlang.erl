%% @doc protoc-gen-trevrpc-erlang: the protoc plugin that generates Erlang
%% code for TrevRPC services.
%%
%% For every proto file it generates a gpb module that encodes and decodes
%% its messages (see {@link trevrpc_gen_messages}), and for every service a
%% module with its behaviour, routes and client functions (see
%% {@link trevrpc_gen_service}). Every file goes to the root of the output
%% directory, as Erlang modules share one namespace.
%%
%% Parameters, as `--trevrpc-erlang_opt=name=value,...':
%%
%% - `msg_format': `maps' (the default) or `records';
%% - `module_prefix': put before every module name (none by default);
%% - `pb_suffix': put after the names of message modules (`_pb');
%% - `service_suffix': put after the names of service modules (none).
-module(protoc_gen_trevrpc_erlang).

-export([main/1, generate/1]).

%% CodeGeneratorResponse.Feature: FEATURE_PROTO3_OPTIONAL. Editions are
%% not supported, so protoc refuses editions files.
-define(FEATURES, 1).

%% @doc Runs the plugin: reads a `CodeGeneratorRequest' from standard input
%% and writes the `CodeGeneratorResponse'.
-spec main([string()]) -> no_return().
main(_Args) ->
    ok = io:setopts(standard_io, [binary, {encoding, latin1}]),
    Request = read(<<>>),
    ok = file:write(standard_io, generate(Request)),
    halt(0).

read(Acc) ->
    case file:read(standard_io, 65536) of
        {ok, Data} -> read(<<Acc/binary, Data/binary>>);
        eof -> Acc
    end.

%% @doc The `CodeGeneratorResponse' to a `CodeGeneratorRequest'. Failures
%% go in its `error' field, as protoc expects.
-spec generate(binary()) -> iodata().
generate(Bin) ->
    Response =
        maybe
            {ok, Request} ?= trevrpc_gen_descriptor:decode_request(Bin),
            {ok, Options} ?= options(maps:get(parameter, Request)),
            {ok, Files} ?= files(Request, Options),
            #{supported_features => ?FEATURES, files => Files}
        else
            {error, Message} -> #{error => Message}
        end,
    trevrpc_gen_descriptor:encode_response(Response).

options(Parameter) ->
    Defaults = #{
        msg_format => maps, prefix => <<>>, pb_suffix => <<"_pb">>, service_suffix => <<>>
    },
    Pairs = [P || P <- binary:split(Parameter, <<",">>, [global]), P =/= <<>>],
    lists:foldl(
        fun
            (_Pair, {error, _} = Error) -> Error;
            (Pair, {ok, Options}) -> option(binary:split(Pair, <<"=">>), Options)
        end,
        {ok, Defaults},
        Pairs
    ).

option([<<"msg_format">>, <<"maps">>], Options) ->
    {ok, Options#{msg_format := maps}};
option([<<"msg_format">>, <<"records">>], Options) ->
    {ok, Options#{msg_format := records}};
option([<<"msg_format">>, Value], _Options) ->
    {error, <<"invalid msg_format \"", Value/binary, "\": expected maps or records">>};
option([<<"module_prefix">> = Name, Value], Options) ->
    maybe
        ok ?= name_part(Name, Value),
        {ok, Options#{prefix := Value}}
    end;
option([<<"pb_suffix">> = Name, Value], Options) ->
    maybe
        ok ?= name_part(Name, Value),
        {ok, Options#{pb_suffix := Value}}
    end;
option([<<"service_suffix">> = Name, Value], Options) ->
    maybe
        ok ?= name_part(Name, Value),
        {ok, Options#{service_suffix := Value}}
    end;
option([Name | _], _Options) ->
    {error, <<"unknown trevrpc-erlang option \"", Name/binary, "\"">>}.

%% A part of module names must keep them atoms that need no quotes.
name_part(Name, Value) ->
    case re:run(Value, <<"^[a-z0-9_]*$">>, [{capture, none}]) of
        match ->
            ok;
        nomatch ->
            {error, <<
                "invalid ",
                Name/binary,
                " \"",
                Value/binary,
                "\": expected lower-case letters, digits and underscores"
            >>}
    end.

files(#{files_to_generate := Names, files := Files, file_set := FileSet}, Options) ->
    #{msg_format := Format} = Options,
    ByName = maps:from_list([{Name, File} || #{name := Name} = File <- Files]),
    Messages = maps:from_list([
        {Name, Message}
     || #{messages := Ms} <- Files, #{name := Name} = Message <- Ms
    ]),
    Generate = [maps:get(Name, ByName) || Name <- Names],
    maybe
        ok ?= check_services(Generate),
        ok ?= check_modules(Generate, Options),
        {ok, Defs} ?= trevrpc_gen_messages:defs(FileSet),
        generated(Generate, Defs, Messages, Format, Options, [])
    end.

check_services(Files) ->
    first_error([
        trevrpc_gen_service:check(Package, Service)
     || #{package := Package, services := Services} <- Files, Service <- Services
    ]).

%% Every generated module needs a name no other one has.
check_modules(Files, Options) ->
    Modules = lists:append([
        [
            {trevrpc_gen_names:message_module(Name, Options), <<"the messages of ", Name/binary>>}
            | [
                {
                    trevrpc_gen_names:service_module(Package, Service, Options),
                    <<"service ", (qualified(Package, Service))/binary>>
                }
             || #{name := Service} <- Services
            ]
        ]
     || #{name := Name, package := Package, services := Services} <- Files
    ]),
    first_error([
        {error, <<First/binary, " and ", Second/binary, " both require module ", Module/binary>>}
     || {Module, First} <- Modules,
        {M, Second} <- Modules,
        M =:= Module,
        First < Second
    ]).

qualified(<<>>, Name) -> Name;
qualified(Package, Name) -> <<Package/binary, ".", Name/binary>>.

first_error(Results) ->
    case [Error || {error, _} = Error <- Results] of
        [] -> ok;
        [Error | _] -> Error
    end.

generated([], _Defs, _Messages, _Format, _Options, Acc) ->
    {ok, lists:append(lists:reverse(Acc))};
generated([File | Rest], Defs, Messages, Format, Options, Acc) ->
    #{name := Name, package := Package, services := Services} = File,
    MessagesModule = trevrpc_gen_names:message_module(Name, Options),
    case trevrpc_gen_messages:generate(Name, MessagesModule, Defs, Format) of
        {ok, MessageFiles} ->
            ServiceFiles = [
                begin
                    Module = trevrpc_gen_names:service_module(Package, ServiceName, Options),
                    Context = #{
                        file => File,
                        service => Service,
                        module => Module,
                        messages_module => MessagesModule,
                        format => Format,
                        messages => Messages
                    },
                    {<<Module/binary, ".erl">>, trevrpc_gen_service:generate(Name, Context)}
                end
             || #{name := ServiceName} = Service <- Services
            ],
            generated(Rest, Defs, Messages, Format, Options, [MessageFiles ++ ServiceFiles | Acc]);
        {error, Message} ->
            {error, <<Name/binary, ": ", Message/binary>>}
    end.
