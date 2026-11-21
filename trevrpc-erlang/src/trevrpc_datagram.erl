%% @doc Datagram messages: their payload (a call's flow ID as a QUIC varint,
%% then one or more TrevRPC frames to the end of the datagram), the
%% `unreliable' option that lets a streaming direction's messages travel in
%% them, and the receiving that client and server calls share; {@link
%% trevrpc_datagram_sender} sends them.
%%
%% Native QUIC carries the payload in a DATAGRAM frame; WebTransport carries
%% it as a session datagram after the session's quarter stream ID.
-module(trevrpc_datagram).

-export([encode/2, encode_messages/2, decode/1, parse/1]).
-export([check_unreliable/1, effective/2, compression/2]).
-export([messages/4, count_wait/1]).

-export_type([message/0, unreliable/0]).

%% A message of the call: a MESSAGE payload, or a ZSTD_MESSAGE payload the
%% receiver still has to decompress.
-type message() :: {message | zstd_message, binary()}.

%% Which directions of a streaming method may travel as datagrams, as the
%% method's `trevrpc.method' schema option says. `requests' applies to
%% client-streaming and bidirectional methods and `responses' to
%% server-streaming and bidirectional ones; a flag for a direction that does
%% not stream has no effect.
-type unreliable() :: #{requests => boolean(), responses => boolean()}.

%% How long the end of a direction waits for the datagrams its
%% DATAGRAM_COUNT announced that have not arrived.
-define(COUNT_WAIT, 10).

%% @doc Encodes a datagram payload from frames already encoded.
-spec encode(non_neg_integer(), [iodata()]) -> iodata().
encode(Flow, Frames) ->
    [trevrpc_varint:encode(Flow) | Frames].

%% @doc Encodes a datagram payload carrying each message in a MESSAGE frame.
-spec encode_messages(non_neg_integer(), [iodata()]) -> iodata().
encode_messages(Flow, Messages) ->
    encode(Flow, [trevrpc_frame:encode(message, Message) || Message <- Messages]).

%% @doc Decodes a datagram payload into its flow ID and messages, in order.
%%
%% Every other frame type, known or not, is skipped, except
%% ZSTD_STREAM_MESSAGE, which needs a stream's ordered delivery. The whole
%% datagram is dropped (`error') when it has no flow ID or flow 0, which no
%% call has, when its frames do not exactly fill it, or when it holds a
%% ZSTD_STREAM_MESSAGE.
%%
%% Frames are not checked against the frame size limit, as the Go runtime
%% does not check them: a datagram is bounded by the path MTU, far below any
%% limit. A ZSTD_MESSAGE is still checked at its decompressed size by
%% whoever decompresses it.
-spec decode(binary()) -> {ok, pos_integer(), [message()]} | error.
decode(Payload) ->
    case parse(Payload) of
        {ok, Flow, Messages} -> {ok, Flow, Messages};
        _ -> error
    end.

%% @doc Decodes a datagram payload as {@link decode/1} does, telling a
%% datagram of a flow whose frames are malformed (`{malformed, Flow}'),
%% which still counts towards the call's DATAGRAM_COUNT, from one without a
%% flow (`error').
-spec parse(binary()) -> {ok, pos_integer(), [message()]} | {malformed, pos_integer()} | error.
parse(Payload) ->
    case trevrpc_varint:decode(Payload) of
        {Flow, Frames} when Flow > 0 ->
            case frames(Frames, []) of
                error -> {malformed, Flow};
                Messages -> {ok, Flow, Messages}
            end;
        _ ->
            error
    end.

frames(<<>>, Acc) ->
    lists:reverse(Acc);
frames(Bin, Acc) ->
    case trevrpc_varint:decode(Bin) of
        more ->
            error;
        {Code, Rest} ->
            case trevrpc_varint:decode(Rest) of
                {Length, Data} when Length =< byte_size(Data) ->
                    <<Payload:Length/binary, Next/binary>> = Data,
                    case trevrpc_frame:type_name(Code) of
                        message -> frames(Next, [{message, Payload} | Acc]);
                        zstd_message -> frames(Next, [{zstd_message, Payload} | Acc]);
                        zstd_stream_message -> error;
                        _ -> frames(Next, Acc)
                    end;
                _ ->
                    error
            end
    end.

%%% The unreliable option

%% @doc Checks an `unreliable' option.
-spec check_unreliable(term()) -> ok | error.
check_unreliable(#{} = Unreliable) ->
    case
        maps:fold(
            fun
                (Key, Value, Ok) when
                    (Key =:= requests orelse Key =:= responses) andalso is_boolean(Value)
                ->
                    Ok;
                (_Key, _Value, _Ok) ->
                    false
            end,
            true,
            Unreliable
        )
    of
        true -> ok;
        false -> error
    end;
check_unreliable(_) ->
    error.

%% @doc The directions of a call of `Kind' that `Unreliable' marks, as
%% `{Requests, Responses}': only directions that stream.
-spec effective(unreliable(), trevrpc_wire:kind()) -> {boolean(), boolean()}.
effective(Unreliable, Kind) ->
    {
        maps:get(requests, Unreliable, false) andalso trevrpc_wire:streams_requests(Kind),
        maps:get(responses, Unreliable, false) andalso trevrpc_wire:streams_responses(Kind)
    }.

%% @doc The compression of a method's directions without a dictionary, with
%% stream compression, which needs a stream's ordered delivery, turned into
%% per-message compression for the directions that travel as datagrams.
-spec compression({trevrpc_zstd:mode(), trevrpc_zstd:mode()}, {boolean(), boolean()}) ->
    {trevrpc_zstd:mode(), trevrpc_zstd:mode()}.
compression({Requests, Responses}, {UnreliableRequests, UnreliableResponses}) ->
    {per_message(Requests, UnreliableRequests), per_message(Responses, UnreliableResponses)}.

per_message(stream, true) -> message;
per_message(Mode, _Unreliable) -> Mode.

%%% Receiving

%% @doc Until when the end of a direction waits for the datagrams its
%% DATAGRAM_COUNT announced: 10 ms, within the call's `Deadline'.
-spec count_wait(trevrpc_stream:deadline()) -> integer().
count_wait(infinity) ->
    erlang:monotonic_time(millisecond) + ?COUNT_WAIT;
count_wait(Deadline) ->
    min(Deadline, erlang:monotonic_time(millisecond) + ?COUNT_WAIT).

%% @doc The messages of a datagram, ZSTD_MESSAGE payloads decompressed with
%% the dictionaries `Accept' allows, each with the dictionary it used (0 for
%% none) or `raw'. A message that would be larger than `Max', the frame size
%% limit, is dropped; a ZSTD_MESSAGE that cannot be decompressed drops the
%% whole datagram (`drop').
-spec messages([message()], trevrpc_zstd:config(), trevrpc_zstd:accept(), pos_integer()) ->
    {ok, [{binary(), non_neg_integer() | raw}]} | drop.
messages(Messages, Compression, Accept, Max) ->
    messages(Messages, Compression, Accept, Max, []).

messages([], _Compression, _Accept, _Max, Acc) ->
    {ok, lists:reverse(Acc)};
messages([{message, Message} | Rest], Compression, Accept, Max, Acc) when
    byte_size(Message) > Max
->
    messages(Rest, Compression, Accept, Max, Acc);
messages([{message, Message} | Rest], Compression, Accept, Max, Acc) ->
    messages(Rest, Compression, Accept, Max, [{Message, raw} | Acc]);
messages([{zstd_message, Payload} | Rest], Compression, Accept, Max, Acc) ->
    case trevrpc_zstd:inflate(Compression, Accept, Payload, Max) of
        {ok, Message, Id} -> messages(Rest, Compression, Accept, Max, [{Message, Id} | Acc]);
        {error, _} -> drop
    end.
