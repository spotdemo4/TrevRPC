%% @doc Datagram payloads: a call's flow ID as a QUIC varint, then one or
%% more TrevRPC frames to the end of the datagram.
%%
%% Native QUIC carries the payload in a DATAGRAM frame; WebTransport carries
%% it as a session datagram after the session's quarter stream ID.
-module(trevrpc_datagram).

-export([encode/2, encode_messages/2, decode/1]).

-export_type([message/0]).

%% A message of the call: a MESSAGE payload, or a ZSTD_MESSAGE payload the
%% receiver still has to decompress.
-type message() :: {message | zstd_message, binary()}.

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
    case trevrpc_varint:decode(Payload) of
        {Flow, Frames} when Flow > 0 ->
            case frames(Frames, []) of
                error -> error;
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
