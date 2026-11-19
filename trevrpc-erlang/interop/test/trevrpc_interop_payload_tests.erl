-module(trevrpc_interop_payload_tests).

-include_lib("eunit/include/eunit.hrl").

%% bench/payload.py's GOLDEN_SHA256: the SHA-256 of the messages of sizes 64,
%% 256, 1024 and 4096, in that order, from worker 0's seed.
-define(GOLDEN, <<"1414f2e3835a891eecc8579d6736157e456eee7f4f3547b5a326b48f625a8a98">>).

records_match_the_reference_test() ->
    Payload = trevrpc_interop_payload:new(records, 0, 0),
    {Messages, _} = lists:mapfoldl(
        fun(Size, P) ->
            {Message, Next} = trevrpc_interop_payload:next(P, Size),
            ?assertEqual(Size, byte_size(Message)),
            {Message, Next}
        end,
        Payload,
        [64, 256, 1024, 4096]
    ),
    ?assertEqual(?GOLDEN, binary:encode_hex(crypto:hash(sha256, Messages), lowercase)).

payloads_differ_per_worker_and_per_message_test() ->
    {A, First} = trevrpc_interop_payload:next(trevrpc_interop_payload:new(records, 256, 0)),
    {B, _} = trevrpc_interop_payload:next(First),
    {C, _} = trevrpc_interop_payload:next(trevrpc_interop_payload:new(records, 256, 1)),
    ?assertNotEqual(A, B),
    ?assertNotEqual(A, C),
    Repeat = trevrpc_interop_payload:new(repeat, 4, 0),
    ?assertMatch({<<"xxxx">>, _}, trevrpc_interop_payload:next(Repeat)).
