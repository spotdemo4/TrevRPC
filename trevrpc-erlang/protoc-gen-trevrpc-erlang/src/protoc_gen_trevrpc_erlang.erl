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
%%
%% The plugin protocol and the modules protoc-gen-trevrpc-gleam and
%% protoc-gen-trevrpc-elixir build on are shared with them: see
%% {@link trevrpc_gen_plugin}.
-module(protoc_gen_trevrpc_erlang).

-export([main/1, generate/1]).

%% @doc Runs the plugin: reads a `CodeGeneratorRequest' from standard input
%% and writes the `CodeGeneratorResponse'.
-spec main([string()]) -> no_return().
main(_Args) ->
    trevrpc_gen_plugin:main(fun files/1).

%% @doc The `CodeGeneratorResponse' to a `CodeGeneratorRequest'. Failures
%% go in its `error' field, as protoc expects.
-spec generate(binary()) -> iodata().
generate(Bin) ->
    trevrpc_gen_plugin:generate(fun files/1, Bin).

files(Request) ->
    maybe
        {ok, Options} ?= options(maps:get(parameter, Request)),
        files(Request, Options)
    end.

options(Parameter) ->
    Defaults = #{
        msg_format => maps, prefix => <<>>, pb_suffix => <<"_pb">>, service_suffix => <<>>
    },
    trevrpc_gen_plugin:options(Parameter, Defaults, fun option/2).

option([<<"msg_format">>, <<"maps">>], Options) ->
    {ok, Options#{msg_format := maps}};
option([<<"msg_format">>, <<"records">>], Options) ->
    {ok, Options#{msg_format := records}};
option([<<"msg_format">>, Value], _Options) ->
    {error, <<"invalid msg_format \"", Value/binary, "\": expected maps or records">>};
option([<<"module_prefix">> = Name, Value], Options) ->
    maybe
        ok ?= trevrpc_gen_names:check_part(Name, Value),
        {ok, Options#{prefix := Value}}
    end;
option([<<"pb_suffix">> = Name, Value], Options) ->
    maybe
        ok ?= trevrpc_gen_names:check_part(Name, Value),
        {ok, Options#{pb_suffix := Value}}
    end;
option([<<"service_suffix">> = Name, Value], Options) ->
    maybe
        ok ?= trevrpc_gen_names:check_part(Name, Value),
        {ok, Options#{service_suffix := Value}}
    end;
option([Name | _], _Options) ->
    {error, <<"unknown trevrpc-erlang option \"", Name/binary, "\"">>}.

files(#{file_set := FileSet} = Request, Options) ->
    #{msg_format := Format} = Options,
    {Generate, Messages} = trevrpc_gen_descriptor:to_generate(Request),
    maybe
        ok ?= trevrpc_gen_service:check_files(Generate),
        ok ?= check_modules(Generate, Options),
        {ok, Defs} ?= trevrpc_gen_messages:defs(FileSet),
        {ok, MessageFiles} ?=
            trevrpc_gen_messages:generate_files(
                Generate,
                fun(Name) -> trevrpc_gen_names:message_module(Name, Options) end,
                Defs,
                Format
            ),
        {ok, MessageFiles ++ service_files(Generate, Messages, Options)}
    end.

%% Every generated module needs a name no other one has.
check_modules(Files, Options) ->
    Modules = lists:append([
        [
            {trevrpc_gen_names:message_module(Name, Options), <<"the messages of ", Name/binary>>}
            | [
                {
                    trevrpc_gen_names:service_module(Package, Service, Options),
                    <<"service ", (trevrpc_gen_names:qualified(Package, Service))/binary>>
                }
             || #{name := Service} <- Services
            ]
        ]
     || #{name := Name, package := Package, services := Services} <- Files
    ]),
    trevrpc_gen_errors:distinct(Modules, <<"module">>).

service_files(Files, Messages, #{msg_format := Format} = Options) ->
    [
        begin
            Module = trevrpc_gen_names:service_module(Package, ServiceName, Options),
            Context = #{
                file => File,
                service => Service,
                module => Module,
                messages_module => trevrpc_gen_names:message_module(Name, Options),
                format => Format,
                messages => Messages
            },
            {<<Module/binary, ".erl">>, trevrpc_gen_service:generate(Name, Context)}
        end
     || #{name := Name, package := Package, services := Services} = File <- Files,
        #{name := ServiceName} = Service <- Services
    ].
