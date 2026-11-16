%% @doc Errors of the generators: messages as binaries, for the error field
%% of a `CodeGeneratorResponse'. protoc-gen-trevrpc-gleam uses these too.
-module(trevrpc_gen_errors).

-export([first/1, new/2, format/2]).

%% @doc `ok', or the first error of `Results', whose other elements are
%% `ok' or `{ok, Value}'.
-spec first([ok | {ok, term()} | {error, binary()}]) -> ok | {error, binary()}.
first(Results) ->
    case [Error || {error, _} = Error <- Results] of
        [] -> ok;
        [Error | _] -> Error
    end.

%% @doc An error whose message is `io_lib:format(Format, Args)'.
-spec new(io:format(), [term()]) -> {error, binary()}.
new(Format, Args) ->
    {error, format(Format, Args)}.

%% @doc `io_lib:format(Format, Args)' as a binary.
-spec format(io:format(), [term()]) -> binary().
format(Format, Args) ->
    <<_/binary>> = Message = unicode:characters_to_binary(io_lib:format(Format, Args)),
    Message.
