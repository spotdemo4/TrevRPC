%% @doc The runtime's diagnostics: structured logger reports.
%%
%% Every event the runtime logs is a report map with a `phase', named as the
%% Go server's diagnostic phases are where one matches (`handler_panic',
%% `internal_error', `tls_handshake_failed', ...), and the fields that
%% describe it, such as the call's `method'. Reports carry the logger domain
%% `[trevrpc, server]', `[trevrpc, client]', `[trevrpc, codec]' or
%% `[trevrpc, validator]' (the last two serve clients and servers), so that an
%% application routes or filters them with ordinary logger handlers and
%% filters, which have overload protection of their own:
%%
%% ```
%% logger:add_handler(trevrpc_diagnostics, my_handler, #{
%%     filters => [{trevrpc, {fun logger_filters:domain/2, {log, sub, [trevrpc]}}}]
%% }).
%% '''
%%
%% Levels follow the Go server's: `error' for crashes of handlers and hooks,
%% internal errors, invalid responses and incomplete shutdowns, `warning'
%% for refusals an operator should see, and `debug' for what clients cause,
%% such as failed TLS handshakes, browser session events and misdirected
%% requests.
%%
%% Reports never hold request or response messages, metadata or HTTP
%% headers, which may carry credentials: an exception is reported by its
%% class, its reason's tag and the stack's functions without their
%% arguments ({@link exception/3}), and a value a callback returned by its
%% shape ({@link shape/1}).
-module(trevrpc_log).

-export([log/4, format/1, exception/3, shape/1]).

-export_type([side/0, exception/0]).

-type side() :: server | client | codec | validator.

%% What an exception was, without what it was raised on.
-type exception() :: #{
    class := error | exit | throw,
    reason := term(),
    location := location(),
    stack := [location()]
}.

-type location() ::
    {module(), atom(), arity(), [{file, string()} | {line, pos_integer()}]} | unknown.

%% @doc Logs the report of `Phase' with `Fields' at `Level', in the domain
%% `[trevrpc, Side]'.
-spec log(logger:level(), side(), atom(), map()) -> ok.
log(Level, Side, Phase, Fields) ->
    logger:log(Level, Fields#{phase => Phase}, #{
        domain => [trevrpc, Side], report_cb => fun ?MODULE:format/1
    }).

%% @private The report callback: `trevrpc <phase>: key=value ...'. Some
%% values are the client's, such as an HTTP authority, so text is printed
%% with its control characters escaped, and a binary that is not UTF-8 as a
%% binary: a value cannot add lines to a text log.
-spec format(map()) -> {io:format(), [term()]}.
format(#{phase := Phase} = Report) ->
    Fields = lists:sort(maps:to_list(maps:remove(phase, Report))),
    {"trevrpc ~s:" ++ lists:append([" ~s=~ts" || _ <- Fields]), [
        Phase | lists:append([[Key, value(Value)] || {Key, Value} <- Fields])
    ]}.

value(Value) when is_binary(Value) ->
    case unicode:characters_to_list(Value) of
        Chars when is_list(Chars) -> [escape(C) || C <- Chars];
        _ -> io_lib:format("~0p", [Value])
    end;
value(Value) ->
    io_lib:format("~0p", [Value]).

escape($\n) -> "\\n";
escape($\r) -> "\\r";
escape($\t) -> "\\t";
escape($\\) -> "\\\\";
escape(C) when C < 16#20; C =:= 16#7F -> io_lib:format("\\x~2.16.0B", [C]);
escape(C) -> C.

%% @doc What an exception was, and where, for a report that must not hold
%% what it was raised on: its class, its reason's tag, and the functions of
%% its stack, without their arguments, with their files and lines.
-spec exception(error | exit | throw, term(), [tuple()]) -> exception().
exception(Class, Reason, Stack) ->
    Frames = [frame(Frame) || Frame <- Stack],
    #{
        class => Class,
        reason => cause(Reason),
        location =>
            case Frames of
                [Top | _] -> Top;
                [] -> unknown
            end,
        stack => Frames
    }.

%% What a failure was, without what it failed on: gpb's failures carry the
%% bytes or the message, and others may too, so only tags and names stay.
cause({gpb_error, {decoding_failure, {_Bin, Type, {Class, Reason, _Stack}}}}) when
    is_atom(Type)
->
    {decoding_failure, Type, Class, tag(Reason)};
cause({gpb_type_error, {Error, Info}}) when is_list(Info) ->
    {type_error, tag(Error), path(proplists:get_value(path, Info))};
cause(Reason) ->
    tag(Reason).

tag(Reason) when is_atom(Reason) -> Reason;
tag(Reason) when tuple_size(Reason) > 0, is_atom(element(1, Reason)) -> element(1, Reason);
tag(_Reason) -> other.

%% gpb's path names fields, as an atom or a string.
path(Path) when is_atom(Path) -> Path;
path(Path) when is_list(Path) -> unicode:characters_to_binary(Path);
path(_Path) -> unknown.

%% A function of the stack, without its arguments, and its file and line.
frame({Module, Function, Arity, Info}) when is_integer(Arity) ->
    {Module, Function, Arity, place(Info)};
frame({Module, Function, Args, Info}) when is_list(Args) ->
    {Module, Function, length(Args), place(Info)};
frame(_Frame) ->
    unknown.

place(Info) when is_list(Info) ->
    [Entry || {Key, _} = Entry <- Info, Key =:= file orelse Key =:= line];
place(_Info) ->
    [].

%% @doc What a term is, without what it holds: an atom, the tag and size of
%% a tagged tuple, or its type.
-spec shape(term()) -> term().
shape(Term) when is_atom(Term) -> Term;
shape(Term) when tuple_size(Term) > 0, is_atom(element(1, Term)) ->
    {element(1, Term), tuple_size(Term)};
shape(Term) when is_tuple(Term) -> {tuple, tuple_size(Term)};
shape(Term) when is_map(Term) -> map;
shape(Term) when is_list(Term) -> list;
shape(Term) when is_binary(Term) -> binary;
shape(Term) when is_function(Term) -> function;
shape(Term) when is_number(Term) -> number;
shape(_Term) ->
    other.
