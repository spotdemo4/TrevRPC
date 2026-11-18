%% @doc protoc-gen-trevrpc-gleam: the protoc plugin that generates Gleam
%% code for TrevRPC services, over the Gleam binding in trevrpc.
%%
%% For every proto file it generates Gleam types for its messages, which a
%% gpb module encodes and decodes, and a module per service (see
%% {@link trevrpc_gen_gleam}). It builds on protoc-gen-trevrpc-erlang, which
%% it carries: the plugin protocol, gpb's definitions and modules, and the
%% checks and options of methods.
%%
%% It takes no parameters.
-module(protoc_gen_trevrpc_gleam).

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

files(#{parameter := <<>>} = Request) ->
    trevrpc_gen_gleam:files(Request);
files(#{parameter := Parameter}) ->
    {error, <<"protoc-gen-trevrpc-gleam takes no parameters, got \"", Parameter/binary, "\"">>}.
