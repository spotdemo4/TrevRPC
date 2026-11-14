%% @doc The `google.rpc.RetryInfo' status detail, encoded by hand: the
%% interop protos do not carry google/rpc/error_details.proto.
-module(trevrpc_interop_retry).

-export([retry_info/2, retry_delay/1]).

-define(RETRY_INFO, <<"google.rpc.RetryInfo">>).

%% @doc A `RetryInfo' status detail whose `retry_delay' (a
%% `google.protobuf.Duration') is `Seconds' and `Nanos'.
-spec retry_info(non_neg_integer(), non_neg_integer()) -> trevrpc_wire:detail().
retry_info(Seconds, Nanos) ->
    Duration = [uint(1, Seconds), uint(2, Nanos)],
    #{type => ?RETRY_INFO, value => iolist_to_binary(trevrpc_pb:bytes_field(1, Duration))}.

%% @doc The retry delay of a status's `RetryInfo' detail in milliseconds,
%% or `none'.
-spec retry_delay(trevrpc_status:status()) -> non_neg_integer() | none.
retry_delay(Status) ->
    case [Value || #{type := ?RETRY_INFO, value := Value} <- maps:get(details, Status, [])] of
        [Value | _] ->
            maybe
                {ok, #{delay := Duration}} ?=
                    decode(Value, #{delay => <<>>}, fun
                        (1, {bytes, V}, M) -> M#{delay := V};
                        (_, _, M) -> M
                    end),
                {ok, #{seconds := Seconds, nanos := Nanos}} ?=
                    decode(Duration, #{seconds => 0, nanos => 0}, fun
                        (1, {varint, V}, M) -> M#{seconds := V};
                        (2, {varint, V}, M) -> M#{nanos := V};
                        (_, _, M) -> M
                    end),
                Seconds * 1000 + Nanos div 1000000
            else
                _ -> none
            end;
        [] ->
            none
    end.

decode(<<>>, Message, _Field) ->
    {ok, Message};
decode(Bin, Message, Field) ->
    case trevrpc_pb:decode_field(Bin) of
        {Number, Value, Rest} -> decode(Rest, Field(Number, Value, Message), Field);
        error -> error
    end.

uint(_Number, 0) -> [];
uint(Number, Value) -> trevrpc_pb:varint_field(Number, Value).
