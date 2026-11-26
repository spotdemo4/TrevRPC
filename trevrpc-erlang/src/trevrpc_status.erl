%% @doc TrevRPC status codes, whose numeric values match gRPC's.
-module(trevrpc_status).

-export([to_int/1, from_int/1, codes/0, new/2, ok/0, code/1, message/1, find_details/2]).
-export([with_retry_delay/2, with_retry_delay/3, retry_delay/1, retry_delay/2]).

-export_type([code/0, status/0]).

%% A status: a code, and optionally a message, trailing metadata and
%% details, as an `RpcStatus' carries them.
-type status() :: trevrpc_wire:status().

-define(RETRY_INFO, <<"google.rpc.RetryInfo">>).
-define(NANOS_PER_SECOND, 1000000000).
-define(INT64_MAX, 16#7FFFFFFFFFFFFFFF).

-type code() ::
    ok
    | cancelled
    | unknown
    | invalid_argument
    | deadline_exceeded
    | not_found
    | already_exists
    | permission_denied
    | resource_exhausted
    | failed_precondition
    | aborted
    | out_of_range
    | unimplemented
    | internal
    | unavailable
    | data_loss
    | unauthenticated.

%% @doc A status with a code and a message.
-spec new(code(), unicode:unicode_binary()) -> status().
new(Code, Message) when is_atom(Code), is_binary(Message) ->
    #{code => Code, message => Message}.

%% @doc The OK status.
-spec ok() -> status().
ok() -> #{code => ok}.

%% @doc The code of a status.
-spec code(status()) -> code().
code(Status) -> maps:get(code, Status, ok).

%% @doc The message of a status, empty when it has none.
-spec message(status()) -> binary().
message(Status) -> maps:get(message, Status, <<>>).

%% @doc The details of a status whose type is `Type', a protobuf message's
%% full name such as `buf.validate.Violations', in order: each the encoding
%% of a message, for the caller to decode (see {@link trevrpc_violations}
%% for that one).
-spec find_details(status(), binary()) -> [binary()].
find_details(Status, Type) when is_binary(Type) ->
    [
        maps:get(value, Detail, <<>>)
     || Detail <- maps:get(details, Status, []),
        maps:get(type, Detail, <<>>) =:= Type
    ].

%% @doc Tells the client to wait `Milliseconds' before it retries, with a
%% `google.rpc.RetryInfo' detail; see {@link with_retry_delay/3}.
-spec with_retry_delay(status(), integer()) -> status().
with_retry_delay(Status, Milliseconds) ->
    with_retry_delay(Status, Milliseconds, millisecond).

%% @doc Tells the client to wait `Delay', in `Unit', before it retries,
%% with a `google.rpc.RetryInfo' detail, the standard detail a rate limit
%% answers with. The detail replaces any `RetryInfo' the status had and
%% leaves its other details alone. A negative delay is zero, and one too
%% long for a `google.protobuf.Duration' is the longest one. Clients read
%% the delay with {@link retry_delay/1}, or the equivalent in their
%% language.
%%
%% TrevRPC encodes the message itself, so that it does not depend on
%% googleapis; the encoding is the one every protobuf library produces.
-spec with_retry_delay(status(), integer(), erlang:time_unit()) -> status().
with_retry_delay(Status, Delay, Unit) when is_map(Status), is_integer(Delay) ->
    Nanos = erlang:convert_time_unit(max(Delay, 0), Unit, nanosecond),
    Details = [
        Detail
     || Detail <- maps:get(details, Status, []),
        maps:get(type, Detail, <<>>) =/= ?RETRY_INFO
    ],
    Status#{details => Details ++ [#{type => ?RETRY_INFO, value => encode_retry_info(Nanos)}]}.

%% @doc How many milliseconds the status tells the client to wait before
%% it retries; see {@link retry_delay/2}.
-spec retry_delay(status()) -> {ok, non_neg_integer()} | error.
retry_delay(Status) ->
    retry_delay(Status, millisecond).

%% @doc How long, in `Unit', the status tells the client to wait before it
%% retries, from its first well-formed `google.rpc.RetryInfo' detail, such
%% as that of a call a server's rate limit rejected; `error' when it has
%% none. A detail that does not decode, or whose delay is negative or not a
%% valid `google.protobuf.Duration' (its nanos must be below a second), is
%% ignored. Fields the message does not know are skipped, and a repeated
%% field takes its last value, as protobuf decoders do. The delay is
%% rounded down to a whole `Unit'.
-spec retry_delay(status(), erlang:time_unit()) -> {ok, non_neg_integer()} | error.
retry_delay(Status, Unit) when is_map(Status) ->
    first_retry_delay(maps:get(details, Status, []), Unit).

first_retry_delay([#{type := ?RETRY_INFO} = Detail | Rest], Unit) ->
    case decode_retry_info(maps:get(value, Detail, <<>>)) of
        {ok, Nanos} -> {ok, erlang:convert_time_unit(Nanos, nanosecond, Unit)};
        error -> first_retry_delay(Rest, Unit)
    end;
first_retry_delay([_ | Rest], Unit) ->
    first_retry_delay(Rest, Unit);
first_retry_delay([], _Unit) ->
    error.

%% A `RetryInfo' whose `retry_delay' is `Nanos'. Like any protobuf encoder,
%% it leaves out a zero `seconds' or `nanos'.
encode_retry_info(Nanos) ->
    {Seconds, Subsecond} =
        case Nanos div ?NANOS_PER_SECOND of
            Whole when Whole > ?INT64_MAX -> {?INT64_MAX, ?NANOS_PER_SECOND - 1};
            Whole -> {Whole, Nanos rem ?NANOS_PER_SECOND}
        end,
    Duration = [uint_field(1, Seconds), uint_field(2, Subsecond)],
    iolist_to_binary(trevrpc_pb:bytes_field(1, Duration)).

uint_field(_Number, 0) -> <<>>;
uint_field(Number, Value) -> trevrpc_pb:varint_field(Number, Value).

%% The delay of a `RetryInfo' in nanoseconds: `retry_delay' (field 1) is a
%% `google.protobuf.Duration' of `seconds' (1, int64) and `nanos' (2,
%% int32).
decode_retry_info(Value) ->
    maybe
        {ok, {delay, Duration}} ?= trevrpc_pb:decode_message(Value, fun retry_info_field/3, none),
        {ok, {Seconds, Nanos}} ?= trevrpc_pb:decode_message(Duration, fun duration_field/3, {0, 0}),
        true ?= Seconds >= 0 andalso Nanos >= 0 andalso Nanos < ?NANOS_PER_SECOND,
        {ok, Seconds * ?NANOS_PER_SECOND + Nanos}
    else
        _ -> error
    end.

retry_info_field(1, {bytes, Duration}, _) -> {delay, Duration};
retry_info_field(1, _, _) -> error;
retry_info_field(_, _, Message) -> Message.

duration_field(1, {varint, Value}, {_, Nanos}) -> {trevrpc_pb:to_int64(Value), Nanos};
duration_field(2, {varint, Value}, {Seconds, _}) -> {Seconds, trevrpc_pb:to_int32(Value)};
duration_field(Number, _, _) when Number =:= 1; Number =:= 2 -> error;
duration_field(_, _, Duration) -> Duration.

%% @doc Every code, in numeric order.
-spec codes() -> [code(), ...].
codes() ->
    [
        ok,
        cancelled,
        unknown,
        invalid_argument,
        deadline_exceeded,
        not_found,
        already_exists,
        permission_denied,
        resource_exhausted,
        failed_precondition,
        aborted,
        out_of_range,
        unimplemented,
        internal,
        unavailable,
        data_loss,
        unauthenticated
    ].

%% @doc The numeric value of a code.
-spec to_int(code()) -> 0..16.
to_int(ok) -> 0;
to_int(cancelled) -> 1;
to_int(unknown) -> 2;
to_int(invalid_argument) -> 3;
to_int(deadline_exceeded) -> 4;
to_int(not_found) -> 5;
to_int(already_exists) -> 6;
to_int(permission_denied) -> 7;
to_int(resource_exhausted) -> 8;
to_int(failed_precondition) -> 9;
to_int(aborted) -> 10;
to_int(out_of_range) -> 11;
to_int(unimplemented) -> 12;
to_int(internal) -> 13;
to_int(unavailable) -> 14;
to_int(data_loss) -> 15;
to_int(unauthenticated) -> 16.

%% @doc The code of a numeric value. Values the protocol does not define
%% map to `unknown'.
-spec from_int(integer()) -> code().
from_int(0) -> ok;
from_int(1) -> cancelled;
from_int(2) -> unknown;
from_int(3) -> invalid_argument;
from_int(4) -> deadline_exceeded;
from_int(5) -> not_found;
from_int(6) -> already_exists;
from_int(7) -> permission_denied;
from_int(8) -> resource_exhausted;
from_int(9) -> failed_precondition;
from_int(10) -> aborted;
from_int(11) -> out_of_range;
from_int(12) -> unimplemented;
from_int(13) -> internal;
from_int(14) -> unavailable;
from_int(15) -> data_loss;
from_int(16) -> unauthenticated;
from_int(Value) when is_integer(Value) -> unknown.
