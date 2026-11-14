%% @doc Stream compression: ZSTD_STREAM_MESSAGE frames, which carry a message
%% compressed with the zstd context of its direction of a call, so each
%% message compresses against the direction's earlier ones. See the wiki's
%% Stream Compression section of Protocol and Wire Format.
%%
%% A direction's messages, in order, are chunks of a sequence of magicless
%% zstd frames with a 64 KiB window. A sender flushes after each message
%% (`zstd:flush/1', OTP 29), so every chunk is whole blocks, and ends a frame
%% with the message that brings it to 60 KiB of messages. A receiver checks a
%% chunk's declared size and its frame header and block headers before
%% decompressing it, and decompresses no more than the declared size.
%%
%% Contexts cost memory for as long as the call lasts, so each server and
%% channel has a budget ({@link budget/1}); the process that owns the
%% connection reserves from it for its streams and gives the reservations
%% back when they end.
-module(trevrpc_zstd_stream).

-export([
    budget/1,
    reserve/2,
    has_room/2,
    release/2,
    encoder_cost/0,
    decoder_cost/0,
    encoder/3,
    encode/2,
    decoder/2,
    decode/3,
    sender/1,
    enable/1,
    next/5
]).

-export_type([budget/0, encoder/0, decoder/0, sender/0]).

%% The bytes a server's or channel's stream compression contexts may still
%% take.
-type budget() :: atomics:atomics_ref().

-record(encoder, {
    context :: zstd:context(),
    %% The bytes of the messages in the current frame.
    in_frame = 0 :: non_neg_integer(),
    %% No frame is open: the next non-empty chunk starts one.
    at_frame_start = true :: boolean()
}).

-record(decoder, {
    config :: trevrpc_zstd:config(),
    accept :: trevrpc_zstd:accept(),
    %% The context of the open frame, created when a frame starts.
    context :: zstd:context() | undefined,
    at_frame_start = true :: boolean()
}).

-record(sender, {
    %% The receiver enabled the direction.
    enabled :: boolean(),
    %% The messages the direction has sent so far, in any frame.
    sent = 0 :: non_neg_integer(),
    %% No context yet, the running context, or off for good: the budget
    %% was spent, or compressing failed.
    state = waiting :: waiting | {active, #encoder{}} | off
}).

-opaque encoder() :: #encoder{}.
-opaque decoder() :: #decoder{}.
-opaque sender() :: #sender{}.

%% The window, as a base-2 logarithm, that the protocol fixes at 64 KiB.
-define(WINDOW_LOG, 16).
%% A frame ends with the message that brings it to this many bytes:
%% libzstd's streaming buffer wraps once a 64 KiB window has filled, and
%% matches in two pieces after, which costs a quarter more CPU per message.
-define(FRAME_LIMIT, 61440).
%% A direction's context starts at this message; earlier ones go per
%% message, so short streams cost nothing.
-define(FIRST_STREAMED_MESSAGE, 3).
%% libzstd's own estimates for a 64 KiB window, which OTP cannot measure.
-define(ENCODER_COST, 614400).
-define(DECODER_COST, 368640).
-define(FRAME_MAGIC, <<16#28, 16#B5, 16#2F, 16#FD>>).

%%% Memory

%% @doc A budget of `Bytes', or `undefined' for zero, which turns stream
%% compression off.
-spec budget(non_neg_integer()) -> budget() | undefined.
budget(0) ->
    undefined;
budget(Bytes) when is_integer(Bytes), Bytes > 0 ->
    Budget = atomics:new(1, [{signed, true}]),
    atomics:put(Budget, 1, Bytes),
    Budget.

%% @doc Takes `Cost' bytes from `Budget', if it has that many left.
-spec reserve(budget() | undefined, non_neg_integer()) -> boolean().
reserve(undefined, _Cost) ->
    false;
reserve(Budget, Cost) ->
    Available = atomics:get(Budget, 1),
    Available >= Cost andalso
        (atomics:compare_exchange(Budget, 1, Available, Available - Cost) =:= ok orelse
            reserve(Budget, Cost)).

%% @doc Whether `Budget' has `Cost' bytes left now, without taking them.
-spec has_room(budget() | undefined, non_neg_integer()) -> boolean().
has_room(undefined, _Cost) -> false;
has_room(Budget, Cost) -> atomics:get(Budget, 1) >= Cost.

%% @doc Gives `Cost' bytes back to `Budget'.
-spec release(budget() | undefined, non_neg_integer()) -> ok.
release(undefined, _Cost) ->
    ok;
release(_Budget, 0) ->
    ok;
release(Budget, Cost) ->
    atomics:add(Budget, 1, Cost).

%% @doc What one direction's compressing context costs.
-spec encoder_cost() -> pos_integer().
encoder_cost() -> ?ENCODER_COST.

%% @doc What one direction's decompressing context costs.
-spec decoder_cost() -> pos_integer().
decoder_cost() -> ?DECODER_COST.

%%% Compressing

%% @doc A context that writes the chunks of magicless frames with the
%% stream compression window, without checksum or content size, starting
%% every frame from dictionary `Seed' (none for 0) of `Config'.
-spec encoder(trevrpc_zstd:config(), non_neg_integer(), integer()) -> {ok, encoder()} | error.
encoder(Config, Seed, Level) ->
    case trevrpc_zstd:dictionaries(Config, Seed) of
        {CDict, _DDict} ->
            Options = #{
                compressionLevel => Level,
                windowLog => ?WINDOW_LOG,
                checksumFlag => false,
                contentSizeFlag => false,
                dictIDFlag => true
            },
            try
                {ok, Context} = zstd:context(compress, Options),
                ok =
                    case CDict of
                        undefined -> ok;
                        _ -> zstd:set_parameter(Context, dictionary, CDict)
                    end,
                {ok, #encoder{context = Context}}
            catch
                error:_ -> error
            end;
        error ->
            error
    end.

%% @doc The ZSTD_STREAM_MESSAGE payload of the next message. A failed
%% message may have changed the context without reaching the receiver, so
%% the encoder must not be used after `error'.
-spec encode(encoder(), iodata()) -> {iodata(), encoder()} | error.
encode(Encoder, Message) ->
    case iolist_size(Message) of
        0 ->
            %% libzstd writes nothing for an empty message, not even a frame
            %% header, so it never touches the context.
            {<<0>>, Encoder};
        Size ->
            #encoder{context = Context, in_frame = InFrame, at_frame_start = Start} = Encoder,
            End = InFrame + Size >= ?FRAME_LIMIT,
            try compress_chunk(Context, Message, End) of
                Chunk ->
                    Body =
                        case Start of
                            true ->
                                <<Magic:4/binary, Rest/binary>> = Chunk,
                                Magic = ?FRAME_MAGIC,
                                Rest;
                            false ->
                                Chunk
                        end,
                    Next =
                        case End of
                            true ->
                                Encoder#encoder{in_frame = 0, at_frame_start = true};
                            false ->
                                Encoder#encoder{in_frame = InFrame + Size, at_frame_start = false}
                        end,
                    {[trevrpc_varint:encode(Size), Body], Next}
            catch
                error:_ -> error
            end
    end.

%% Compresses `Message' and flushes it, or with `End' ends the frame.
compress_chunk(Context, Message, true) ->
    {done, Out} = zstd:finish(Context, Message),
    iolist_to_binary(Out);
compress_chunk(Context, Message, false) ->
    In = feed(Context, Message, []),
    {continue, Flushed} = zstd:flush(Context),
    iolist_to_binary([In, Flushed]).

feed(Context, Data, Acc) ->
    case zstd:stream(Context, Data) of
        {continue, Out} -> lists:reverse(Acc, [Out]);
        {continue, Rest, Out} -> feed(Context, Rest, [Out | Acc])
    end.

%%% One direction's sender

%% @doc A direction's sender, whose receiver enabled it already or not yet.
-spec sender(boolean()) -> sender().
sender(Enabled) ->
    #sender{enabled = Enabled}.

%% @doc The receiver enabled the direction.
-spec enable(sender()) -> sender().
enable(Sender) ->
    Sender#sender{enabled = true}.

%% @doc Encodes the direction's next message: `{stream, Payload, Sender}'
%% once its context runs, `{message, Sender}' when it goes per message. The
%% context starts at the third message once the receiver enabled the
%% direction, from dictionary `Seed', if `Reserve' (called once) finds
%% room for it in the budget.
-spec next(sender(), iodata(), trevrpc_zstd:config(), non_neg_integer(), fun(() -> boolean())) ->
    {stream, iodata(), sender()} | {message, sender()}.
next(#sender{sent = Sent} = Sender0, Message, Config, Seed, Reserve) ->
    Sender = start(Sender0#sender{sent = Sent + 1}, Config, Seed, Reserve),
    case Sender of
        #sender{state = {active, Encoder}} ->
            case encode(Encoder, Message) of
                {Payload, Next} -> {stream, Payload, Sender#sender{state = {active, Next}}};
                error -> {message, Sender#sender{state = off}}
            end;
        _ ->
            {message, Sender}
    end.

start(#sender{state = waiting, enabled = true, sent = Sent} = Sender, Config, Seed, Reserve) when
    Sent >= ?FIRST_STREAMED_MESSAGE
->
    case Reserve() of
        true ->
            case encoder(Config, Seed, trevrpc_zstd:level(Config)) of
                {ok, Encoder} -> Sender#sender{state = {active, Encoder}};
                error -> Sender#sender{state = off}
            end;
        false ->
            Sender#sender{state = off}
    end;
start(Sender, _Config, _Seed, _Reserve) ->
    Sender.

%%% Decompressing

%% @doc A direction's receiver, whose frames may start from the
%% dictionaries `Accept' allows.
-spec decoder(trevrpc_zstd:config(), trevrpc_zstd:accept()) -> decoder().
decoder(Config, Accept) ->
    #decoder{config = Config, accept = Accept}.

%% @doc Decompresses the payload of a ZSTD_STREAM_MESSAGE. A message larger
%% than `Max' is rejected before it is decompressed. The direction cannot
%% continue after a rejection.
-spec decode(decoder(), binary(), non_neg_integer()) ->
    {ok, binary(), decoder()}
    | {error, {too_large, non_neg_integer()} | {invalid, binary()}}.
decode(Decoder, Payload, Max) ->
    case trevrpc_varint:decode(Payload) of
        more ->
            invalid();
        {Size, _Chunk} when Size > Max ->
            {error, {too_large, Size}};
        {Size, Chunk} ->
            case parse_chunk(Chunk, Decoder#decoder.at_frame_start) of
                error -> invalid();
                {empty} when Size =:= 0 -> {ok, <<>>, Decoder};
                {empty} -> invalid();
                {Starts, DictId, Ends} -> inflate(Decoder, Chunk, Size, Starts, DictId, Ends)
            end
    end.

invalid() ->
    {error, {invalid, <<"invalid ZSTD_STREAM_MESSAGE payload">>}}.

inflate(Decoder, Chunk, Size, true, DictId, Ends) ->
    case frame_context(Decoder, DictId) of
        {ok, Context} ->
            inflate(Decoder#decoder{context = Context}, [?FRAME_MAGIC, Chunk], Size, Ends);
        {error, _} = Error ->
            Error
    end;
inflate(#decoder{context = undefined}, _Chunk, _Size, false, _DictId, _Ends) ->
    invalid();
inflate(Decoder, Chunk, Size, false, _DictId, Ends) ->
    inflate(Decoder, Chunk, Size, Ends).

inflate(#decoder{context = Context} = Decoder, Chunk, Size, Ends) ->
    try decompress(Context, iolist_to_binary(Chunk), Size, 0, []) of
        {ok, Message} when byte_size(Message) =:= Size ->
            {ok, Message, Decoder#decoder{at_frame_start = Ends}};
        _ ->
            {error, {invalid, <<"ZSTD_STREAM_MESSAGE decompressed to the wrong size">>}}
    catch
        error:_ ->
            {error, {invalid, <<"ZSTD_STREAM_MESSAGE chunk is corrupt">>}}
    end.

%% Each call returns at most one output buffer, so a chunk that would
%% decompress to more than declared is stopped one buffer past it. Once the
%% input is taken, what libzstd still holds back comes out with empty
%% input.
decompress(_Context, _In, Size, Total, _Acc) when Total > Size ->
    too_long;
decompress(Context, <<>>, Size, Total, Acc) ->
    case zstd:stream(Context, <<>>) of
        {continue, <<>>} -> {ok, iolist_to_binary(lists:reverse(Acc))};
        {continue, Out} -> decompress(Context, <<>>, Size, Total + byte_size(Out), [Out | Acc])
    end;
decompress(Context, In, Size, Total, Acc) ->
    case zstd:stream(Context, In) of
        {continue, Rest, Out} ->
            decompress(Context, Rest, Size, Total + byte_size(Out), [Out | Acc]);
        {continue, Out} ->
            decompress(Context, <<>>, Size, Total + byte_size(Out), [Out | Acc])
    end.

%% A context for a frame that starts from dictionary `DictId', or from
%% nothing for 0.
frame_context(#decoder{config = Config, accept = Accept}, DictId) ->
    Dictionary =
        case DictId of
            0 ->
                {ok, undefined};
            _ ->
                case trevrpc_zstd:accepted(Config, Accept, DictId) of
                    true ->
                        {_CDict, DDict} = trevrpc_zstd:dictionaries(Config, DictId),
                        {ok, DDict};
                    false ->
                        {error,
                            {invalid,
                                iolist_to_binary(
                                    io_lib:format(
                                        "stream-compressed frame uses dictionary ~b, which is not accepted",
                                        [DictId]
                                    )
                                )}}
                end
        end,
    case Dictionary of
        {ok, DDict0} ->
            Options =
                case DDict0 of
                    undefined -> #{windowLogMax => ?WINDOW_LOG};
                    _ -> #{windowLogMax => ?WINDOW_LOG, dictionary => DDict0}
                end,
            {ok, Context} = zstd:context(decompress, Options),
            {ok, Context};
        {error, _} = Error ->
            Error
    end.

%% Validates a chunk: with no frame open, a non-empty chunk starts with a
%% frame header without Single_Segment_flag, Frame_Content_Size_flag,
%% Content_Checksum_flag or the reserved bit, whose window is at most 64 KiB.
%% The chunk must then be whole blocks, and a Last_Block ends it. Returns
%% whether it starts a frame, the frame's Dictionary_ID, and whether it ends
%% the frame.
parse_chunk(<<>>, _AtFrameStart) ->
    {empty};
parse_chunk(Chunk, true) ->
    case frame_header(Chunk) of
        {ok, DictId, Blocks} ->
            case walk(Blocks, false) of
                {ok, Ends} -> {true, DictId, Ends};
                error -> error
            end;
        error ->
            error
    end;
parse_chunk(Chunk, false) ->
    case walk(Chunk, false) of
        {ok, Ends} -> {false, 0, Ends};
        error -> error
    end.

frame_header(<<0:2, 0:1, _Unused:1, 0:1, 0:1, DictFlag:2, Exponent:5, Mantissa:3, Rest/binary>>) ->
    Base = 1 bsl (Exponent + 10),
    Window = Base + (Base div 8) * Mantissa,
    DictLength = element(DictFlag + 1, {0, 1, 2, 4}),
    case Rest of
        <<DictId:DictLength/little-unit:8, Blocks/binary>> when Window =< 1 bsl ?WINDOW_LOG ->
            {ok, DictId, Blocks};
        _ ->
            error
    end;
frame_header(_) ->
    error.

%% Walks whole blocks to the end of the chunk; a Last_Block must end it.
walk(<<>>, Last) ->
    {ok, Last};
walk(<<Header:24/little, Rest/binary>>, false) ->
    Size = Header bsr 3,
    Length =
        case (Header bsr 1) band 3 of
            0 -> Size;
            2 -> Size;
            1 -> 1;
            3 -> reserved
        end,
    case Rest of
        _ when Length =:= reserved -> error;
        <<_:Length/binary, Next/binary>> -> walk(Next, Header band 1 =:= 1);
        _ -> error
    end;
walk(_, _) ->
    error.
