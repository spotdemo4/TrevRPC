%% @doc The protoc plugin protocol, which protoc-gen-trevrpc-erlang,
%% protoc-gen-trevrpc-gleam and protoc-gen-trevrpc-elixir share: a
%% `CodeGeneratorRequest' on standard input, and the
%% `CodeGeneratorResponse' with the files a generator makes of it, or its
%% error, on standard output.
-module(trevrpc_gen_plugin).

-export([main/1, generate/2, options/3]).

-export_type([generator/0]).

%% Makes the files of a request, or fails with a message for protoc.
-type generator() :: fun(
    (trevrpc_gen_descriptor:request()) -> {ok, [{binary(), iodata()}]} | {error, binary()}
).

%% CodeGeneratorResponse.Feature: FEATURE_PROTO3_OPTIONAL. Editions are
%% not supported, so protoc refuses editions files.
-define(FEATURES, 1).

%% @doc Runs a plugin with `Generator': reads the request from standard
%% input, writes the response, and halts.
-spec main(generator()) -> no_return().
main(Generator) ->
    ok = io:setopts(standard_io, [binary, {encoding, latin1}]),
    ok = file:write(standard_io, generate(Generator, read(<<>>))),
    halt(0).

read(Acc) ->
    case file:read(standard_io, 65536) of
        {ok, Data} -> read(<<Acc/binary, Data/binary>>);
        eof -> Acc
    end.

%% @doc The `CodeGeneratorResponse' to a `CodeGeneratorRequest'. Failures go
%% in its `error' field, as protoc expects.
-spec generate(generator(), binary()) -> iodata().
generate(Generator, Bin) ->
    Response =
        maybe
            {ok, Request} ?= trevrpc_gen_descriptor:decode_request(Bin),
            {ok, Files} ?= Generator(Request),
            #{supported_features => ?FEATURES, files => Files}
        else
            {error, Message} -> #{error => Message}
        end,
    trevrpc_gen_descriptor:encode_response(Response).

%% @doc The options of plugin parameter `Parameter', `name=value' pairs
%% separated by commas (`--NAME_opt=...'): `Defaults' with each pair given
%% to `Option', in order, which returns the options with the pair's value
%% or why the pair is invalid. `Option' gets `[Name, Value]', or `[Name]'
%% for a pair without `='.
-spec options(binary(), Options, fun(([binary()], Options) -> {ok, Options} | {error, binary()})) ->
    {ok, Options} | {error, binary()}
when
    Options :: map().
options(Parameter, Defaults, Option) ->
    Pairs = [P || P <- binary:split(Parameter, <<",">>, [global]), P =/= <<>>],
    lists:foldl(
        fun
            (_Pair, {error, _} = Error) -> Error;
            (Pair, {ok, Options}) -> Option(binary:split(Pair, <<"=">>), Options)
        end,
        {ok, Defaults},
        Pairs
    ).
