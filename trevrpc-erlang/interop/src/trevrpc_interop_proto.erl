%% @doc The interop messages, encoded by hand: `trevrpc.interop.v1.EchoRequest'
%% and `EchoResponse' (interop/proto/interop.proto), `PlainMessage'
%% (interop/proto/compressed.proto), and the `google.rpc.RetryInfo' status
%% detail.
-module(trevrpc_interop_proto).

-export([
    encode_request/1,
    decode_request/1,
    encode_response/1,
    decode_response/1,
    encode_plain/1,
    decode_plain/1,
    request_type/0,
    response_type/0,
    plain_type/0,
    retry_info/2,
    retry_delay/1
]).

-export_type([request/0, response/0]).

-type request() :: #{
    message => binary(),
    count => non_neg_integer(),
    fail_code => non_neg_integer(),
    fail_message => binary()
}.
-type response() :: #{message => binary(), index => non_neg_integer()}.

-define(RETRY_INFO, <<"google.rpc.RetryInfo">>).

%% @doc Encodes an `EchoRequest'.
-spec encode_request(request()) -> iodata().
encode_request(Request) ->
    [
        string(1, maps:get(message, Request, <<>>)),
        uint(2, maps:get(count, Request, 0)),
        uint(3, maps:get(fail_code, Request, 0)),
        string(4, maps:get(fail_message, Request, <<>>))
    ].

%% @doc Decodes an `EchoRequest'.
-spec decode_request(binary()) ->
    {ok, #{
        message := binary(),
        count := non_neg_integer(),
        fail_code := non_neg_integer(),
        fail_message := binary()
    }}
    | error.
decode_request(Bin) ->
    decode(Bin, #{message => <<>>, count => 0, fail_code => 0, fail_message => <<>>}, fun
        (1, {bytes, V}, M) -> M#{message := V};
        (2, {varint, V}, M) -> M#{count := trevrpc_pb:to_uint32(V)};
        (3, {varint, V}, M) -> M#{fail_code := trevrpc_pb:to_uint32(V)};
        (4, {bytes, V}, M) -> M#{fail_message := V};
        (_, _, M) -> M
    end).

%% @doc Encodes an `EchoResponse'.
-spec encode_response(response()) -> iodata().
encode_response(Response) ->
    [string(1, maps:get(message, Response, <<>>)), uint(2, maps:get(index, Response, 0))].

%% @doc Decodes an `EchoResponse'.
-spec decode_response(binary()) -> {ok, #{message := binary(), index := non_neg_integer()}} | error.
decode_response(Bin) ->
    decode(Bin, #{message => <<>>, index => 0}, fun
        (1, {bytes, V}, M) -> M#{message := V};
        (2, {varint, V}, M) -> M#{index := trevrpc_pb:to_uint32(V)};
        (_, _, M) -> M
    end).

%% @doc Encodes a `PlainMessage' carrying `Text'.
-spec encode_plain(binary()) -> iodata().
encode_plain(Text) ->
    string(1, Text).

%% @doc Decodes a `PlainMessage' into its text.
-spec decode_plain(binary()) -> {ok, binary()} | error.
decode_plain(Bin) ->
    case
        decode(Bin, #{text => <<>>}, fun
            (1, {bytes, V}, M) -> M#{text := V};
            (_, _, M) -> M
        end)
    of
        {ok, #{text := Text}} -> {ok, Text};
        error -> error
    end.

%% @doc The full names of the message types, which pick their compression
%% dictionaries.
-spec request_type() -> binary().
request_type() -> <<"trevrpc.interop.v1.EchoRequest">>.

-spec response_type() -> binary().
response_type() -> <<"trevrpc.interop.v1.EchoResponse">>.

-spec plain_type() -> binary().
plain_type() -> <<"trevrpc.interop.v1.PlainMessage">>.

%% @doc A `google.rpc.RetryInfo' status detail whose `retry_delay' (a
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

string(_Number, <<>>) -> [];
string(Number, Value) -> trevrpc_pb:bytes_field(Number, Value).

uint(_Number, 0) -> [];
uint(Number, Value) -> trevrpc_pb:varint_field(Number, Value).
