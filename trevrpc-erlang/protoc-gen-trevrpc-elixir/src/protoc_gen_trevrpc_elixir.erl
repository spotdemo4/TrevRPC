%% @doc protoc-gen-trevrpc-elixir: the protoc plugin that generates Elixir
%% code for TrevRPC services, over the Erlang runtime.
%%
%% For every proto file it generates a gpb module that encodes and decodes
%% its messages as maps, as protoc-gen-trevrpc-erlang does, and for every
%% service an Elixir module with its behaviour, routes and client functions
%% (see {@link trevrpc_gen_elixir}). It builds on protoc-gen-trevrpc-erlang,
%% which it carries: the plugin protocol, gpb's modules, and the checks and
%% options of methods.
%%
%% Parameters, as `--trevrpc-elixir_opt=name=value,...':
%%
%% - `module_prefix': an Elixir alias put before every service module's
%%   name, such as `MyApp.Rpc' (none by default);
%% - `pb_prefix': put before the names of message modules (none);
%% - `pb_suffix': put after the names of message modules (`_pb').
-module(protoc_gen_trevrpc_elixir).

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

files(#{parameter := Parameter} = Request) ->
    Defaults = #{module_prefix => <<>>, prefix => <<>>, pb_suffix => <<"_pb">>},
    maybe
        {ok, Options} ?= trevrpc_gen_plugin:options(Parameter, Defaults, fun option/2),
        trevrpc_gen_elixir:files(Request, Options)
    end.

option([<<"module_prefix">>, Value], Options) ->
    maybe
        ok ?= trevrpc_gen_elixir:check_prefix(Value),
        {ok, Options#{module_prefix := Value}}
    end;
option([<<"pb_prefix">> = Name, Value], Options) ->
    maybe
        ok ?= trevrpc_gen_names:check_part(Name, Value),
        {ok, Options#{prefix := Value}}
    end;
option([<<"pb_suffix">> = Name, Value], Options) ->
    maybe
        ok ?= trevrpc_gen_names:check_part(Name, Value),
        {ok, Options#{pb_suffix := Value}}
    end;
option([Name | _], _Options) ->
    {error, <<"unknown trevrpc-elixir option \"", Name/binary, "\"">>}.
