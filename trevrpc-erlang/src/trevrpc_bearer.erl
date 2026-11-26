%% @doc Bearer credentials in the `authorization' metadata, as RFC 6750
%% defines them: a server's parser ({@link token/1}) and a client's builder
%% ({@link metadata/1}) share the token syntax, as Go's `BearerToken' and
%% `BearerMetadata' and Rust's `bearer_token' and `bearer_metadata' do.
%%
%% ```
%% {ok, Authorization} = trevrpc_bearer:metadata(Token),
%% {ok, Channel} = trevrpc_channel:connect(#{connection_metadata => Authorization, ...}, 5000),
%%
%% say_hello(Request, Call) ->
%%     case trevrpc_bearer:token(trevrpc_call:metadata(Call)) of
%%         {ok, Token} -> ...;
%%         {error, Status} -> {error, Status}
%%     end.
%% '''
-module(trevrpc_bearer).

-export([token/1, metadata/1]).

-define(KEY, <<"authorization">>).

%% @doc The token of the `authorization' metadata, which must hold bearer
%% credentials as RFC 6750 defines them: the case-insensitive scheme
%% `Bearer', one or more spaces, and the token. An `unauthenticated' status
%% when the metadata is missing or holds anything else, with a message that
%% tells the two apart.
-spec token(trevrpc_metadata:metadata()) -> {ok, binary()} | {error, trevrpc_status:status()}.
token(#{?KEY := <<Scheme:6/binary, $\s, Credentials/binary>>}) ->
    Token = skip_spaces(Credentials),
    case lowercase(Scheme) =:= <<"bearer">> andalso b64token(Token) of
        true -> {ok, Token};
        false -> malformed()
    end;
token(#{?KEY := _}) ->
    malformed();
token(Metadata) when is_map(Metadata) ->
    {error, trevrpc_status:new(unauthenticated, <<"missing bearer token">>)}.

%% @doc The request metadata that carries `Token' as bearer credentials:
%% the key `authorization' and the value `Bearer <Token>', for a call's
%% `metadata' option or a channel's `connection_metadata'
%% ({@link trevrpc_channel:set_connection_metadata/2}). It accepts the
%% tokens {@link token/1} parses, RFC 6750's `b64token' syntax: one or more
%% ASCII letters, digits and the characters `-._~+/', followed by any
%% number of `='. An `invalid_argument' status for any other token, such as
%% an empty one or one with whitespace, which a server would reject as
%% unauthenticated, and which a client could not tell from an expired
%% session.
-spec metadata(binary()) -> {ok, trevrpc_metadata:metadata()} | {error, trevrpc_status:status()}.
metadata(Token) when is_binary(Token) ->
    case b64token(Token) of
        true -> {ok, #{?KEY => <<"Bearer ", Token/binary>>}};
        false -> {error, trevrpc_status:new(invalid_argument, <<"bearer token is not a b64token">>)}
    end.

malformed() ->
    {error, trevrpc_status:new(unauthenticated, <<"malformed bearer token">>)}.

skip_spaces(<<$\s, Rest/binary>>) -> skip_spaces(Rest);
skip_spaces(Bin) -> Bin.

lowercase(Bin) ->
    <<
        <<
            (if
                C >= $A, C =< $Z -> C + 32;
                true -> C
            end)
        >>
     || <<C>> <= Bin
    >>.

%% Whether `Token' matches RFC 6750's `b64token':
%% `1*( ALPHA / DIGIT / "-" / "." / "_" / "~" / "+" / "/" ) *"="'.
b64token(<<C, _/binary>> = Token) when C =/= $= -> b64token_chars(Token);
b64token(_) -> false.

b64token_chars(<<C, Rest/binary>>) when
    (C >= $a andalso C =< $z);
    (C >= $A andalso C =< $Z);
    (C >= $0 andalso C =< $9);
    C =:= $-;
    C =:= $.;
    C =:= $_;
    C =:= $~;
    C =:= $+;
    C =:= $/
->
    b64token_chars(Rest);
b64token_chars(Padding) ->
    padding(Padding).

padding(<<$=, Rest/binary>>) -> padding(Rest);
padding(<<>>) -> true;
padding(_) -> false.
