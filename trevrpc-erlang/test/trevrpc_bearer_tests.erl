%% Bearer credentials, with the cases of the Go and Rust runtimes' tests.
-module(trevrpc_bearer_tests).

-include_lib("eunit/include/eunit.hrl").

token(Value) ->
    trevrpc_bearer:token(#{<<"authorization">> => Value}).

token_parses_rfc_6750_credentials_test_() ->
    [
        ?_assertEqual({ok, Want}, token(Value))
     || {Value, Want} <- [
            {<<"Bearer abc.DEF-123_~+/==">>, <<"abc.DEF-123_~+/==">>},
            {<<"bEaReR token">>, <<"token">>},
            {<<"Bearer   token">>, <<"token">>}
        ]
    ].

token_tells_missing_from_malformed_test_() ->
    Missing = trevrpc_status:new(unauthenticated, <<"missing bearer token">>),
    Malformed = trevrpc_status:new(unauthenticated, <<"malformed bearer token">>),
    [
        ?_assertEqual({error, Missing}, trevrpc_bearer:token(#{})),
        ?_assertEqual({error, Missing}, trevrpc_bearer:token(#{<<"other">> => <<"Bearer token">>}))
        | [
            ?_assertEqual({error, Malformed}, token(Value))
         || Value <- [
                <<"">>,
                <<"token">>,
                <<"Bearer">>,
                <<"Bearer ">>,
                <<"Basic dXNlcjpwYXNz">>,
                <<"Bearertoken">>,
                <<"Bearer\ttoken">>,
                <<"Bearer token ">>,
                <<"Bearer to ken">>,
                <<"Bearer ==">>,
                <<"Bearer a=b">>,
                <<"Bearer a,b">>,
                <<"Bearer t", 16#c3, 16#b6, "k">>
            ]
        ]
    ].

metadata_builds_what_token_parses_test_() ->
    [
        ?_test(begin
            {ok, Metadata} = trevrpc_bearer:metadata(Token),
            ?assertEqual(#{<<"authorization">> => <<"Bearer ", Token/binary>>}, Metadata),
            ?assertEqual(ok, trevrpc_metadata:validate(Metadata)),
            ?assertEqual({ok, Token}, trevrpc_bearer:token(Metadata))
        end)
     || Token <- [<<"abc">>, <<"abc.DEF-123_~+/">>, <<"abc.DEF-123_~+/==">>, <<"a">>, <<"a=">>]
    ].

metadata_rejects_tokens_that_are_not_b64tokens_test_() ->
    [
        ?_assertMatch({error, #{code := invalid_argument}}, trevrpc_bearer:metadata(Token))
     || Token <- [
            <<"">>,
            <<"==">>,
            <<" token">>,
            <<"token ">>,
            <<"to ken">>,
            <<"token\n">>,
            <<"t", 16#c3, 16#b6, "k", 16#c3, 16#a9, "n">>,
            <<"a=b">>,
            <<"a,b">>
        ]
    ].
