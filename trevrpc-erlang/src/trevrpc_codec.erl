%% @doc How a method's messages turn into bytes and back.
%%
%% The runtime moves messages as binaries. A route or a call that has a
%% codec moves terms instead: handlers and callers get decoded messages
%% from `recv' and give terms to `send', which the codec encodes. Code that
%% protoc-gen-trevrpc-erlang generates sets one on every route and call;
%% without one, messages stay binaries.
%%
%% `encode' turns a message into its bytes, and `decode' turns bytes into
%% a message. A server encodes responses and decodes requests, and a client
%% the other way round. A codec that raises cannot encode or decode the
%% message, and the call gets a status saying so. A failed encode is a bug
%% of this side, logged as an error; a failed decode is more likely a bad
%% peer, logged at debug level. Either way the log has the exception's
%% class, its reason's tag and where it was raised, never the message or
%% its bytes.
-module(trevrpc_codec).

-export([check/1, encode/2, decode/2]).

-export_type([codec/0]).

-type codec() :: #{
    encode := fun((term()) -> iodata()),
    decode := fun((binary()) -> term())
}.

%% @doc Checks that `Codec' is a codec, or `undefined' (none).
-spec check(term()) -> ok | error.
check(undefined) ->
    ok;
check(#{encode := Encode, decode := Decode}) when
    is_function(Encode, 1), is_function(Decode, 1)
->
    ok;
check(_Codec) ->
    error.

%% @doc Encodes `Message', which is already bytes without a codec.
-spec encode(codec() | undefined, term()) -> {ok, iodata()} | error.
encode(undefined, Message) ->
    {ok, Message};
encode(#{encode := Encode}, Message) ->
    try Encode(Message) of
        Bytes -> {ok, Bytes}
    catch
        Class:Reason:Stack -> failed(error, encode, Class, Reason, Stack)
    end.

%% @doc Decodes `Bin', which stays a binary without a codec.
-spec decode(codec() | undefined, binary()) -> {ok, term()} | error.
decode(undefined, Bin) ->
    {ok, Bin};
decode(#{decode := Decode}, Bin) ->
    try Decode(Bin) of
        Message -> {ok, Message}
    catch
        Class:Reason:Stack -> failed(debug, decode, Class, Reason, Stack)
    end.

failed(Level, Op, Class, Reason, Stack) ->
    logger:log(Level, "trevrpc codec failed to ~s a message: ~p:~p at ~p", [
        Op, Class, cause(Reason), location(Stack)
    ]),
    error.

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

%% The function that raised, without its arguments.
location([{Module, Function, Arity, _} | _]) when is_integer(Arity) ->
    {Module, Function, Arity};
location([{Module, Function, Args, _} | _]) when is_list(Args) ->
    {Module, Function, length(Args)};
location(_Stack) ->
    unknown.
