%% @doc Errors of the generators: messages as binaries, for the error field
%% of a `CodeGeneratorResponse'. The Gleam and Elixir plugins use these
%% too.
-module(trevrpc_gen_errors).

-export([first/1, new/2, format/2, distinct/2]).

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

%% @doc `ok', or an error naming the first two things that need the same
%% generated name, a module or a file, as `What' says. `Names' are
%% `{Name, Thing}', each thing described as the message names it.
-spec distinct([{binary(), binary()}], binary()) -> ok | {error, binary()}.
distinct(Names, What) ->
    first([
        {error,
            <<First/binary, " and ", Second/binary, " both require ", What/binary, " ",
                Name/binary>>}
     || {Name, First} <- Names,
        {N, Second} <- Names,
        N =:= Name,
        First < Second
    ]).
