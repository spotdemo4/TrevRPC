%% @doc Ready-made authorizers for a server's `authorizer' option (see
%% {@link trevrpc_server:authorizer()}), as the Go and Rust runtimes have:
%% one that accepts a fixed metadata value or bearer token, and a chain of
%% authorizers.
%%
%% ```
%% trevrpc_server:start_link(#{authorizer => trevrpc_authorizer:bearer(Token), ...})
%% '''
%%
%% To check tokens that vary, such as per-user session tokens, write an
%% authorizer that parses them with {@link trevrpc_bearer:token/1} and
%% passes the user it finds on to the handler:
%%
%% ```
%% fun(#{metadata := Metadata}) ->
%%     maybe
%%         {ok, Token} ?= trevrpc_bearer:token(Metadata),
%%         {ok, User} ?= sessions:lookup(Token),
%%         {ok, #{user => User}}
%%     else
%%         {error, #{code := _} = Status} -> {error, Status};
%%         {error, not_found} -> {error, trevrpc_status:new(unauthenticated, <<"session expired">>)}
%%     end
%% end
%% '''
-module(trevrpc_authorizer).

-export([metadata_value/2, bearer/1, chain/1]).

%% @doc An authorizer that accepts calls whose metadata holds exactly
%% `Value' under `Key', compared in constant time, and rejects the others as
%% `unauthenticated'. The key is lowercased, as received keys are.
-spec metadata_value(binary(), binary()) -> trevrpc_server:authorizer().
metadata_value(Key, Value) when is_binary(Key), is_binary(Value) ->
    Normalized = trevrpc_metadata:normalize_key(Key),
    fun(#{metadata := Metadata}) ->
        case Metadata of
            #{Normalized := Got} when byte_size(Got) =:= byte_size(Value) ->
                case crypto:hash_equals(Got, Value) of
                    true -> ok;
                    false -> unauthenticated()
                end;
            #{} ->
                unauthenticated()
        end
    end.

%% @doc An authorizer that accepts calls whose `authorization' metadata is
%% `Bearer <Token>', and rejects the others as `unauthenticated'. The token
%% should be a `b64token', so that a client can send it with {@link
%% trevrpc_bearer:metadata/1}.
-spec bearer(binary()) -> trevrpc_server:authorizer().
bearer(Token) when is_binary(Token) ->
    metadata_value(<<"authorization">>, <<"Bearer ", Token/binary>>).

%% @doc An authorizer that runs `Authorizers' in order and accepts a call
%% only when every one of them does, rejecting it with the first error. Each
%% gets the call as the server gives it; the contexts they pass on are
%% merged, a later one winning for a key both have.
-spec chain([trevrpc_server:authorizer()]) -> trevrpc_server:authorizer().
chain(Authorizers) when is_list(Authorizers) ->
    true = lists:all(fun(Authorizer) -> is_function(Authorizer, 1) end, Authorizers),
    fun(Info) -> chain(Authorizers, Info, #{}) end.

chain([], _Info, Context) ->
    {ok, Context};
chain([Authorizer | Rest], Info, Context) ->
    case Authorizer(Info) of
        ok -> chain(Rest, Info, Context);
        {ok, Added} when is_map(Added) -> chain(Rest, Info, maps:merge(Context, Added));
        Other -> Other
    end.

unauthenticated() ->
    {error, trevrpc_status:new(unauthenticated, <<"request is not authenticated">>)}.
