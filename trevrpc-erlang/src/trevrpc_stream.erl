%% @doc One end of an RPC's bidirectional stream (a QUIC or QMux stream, or
%% an HTTP request stream), as the process that reads and writes it sees
%% it: a server's call process or a client's calling process.
%%
%% erlang_quic delivers every event of a connection to the one process that
%% owns it, and so do quic_h3 and erlang_h2. That process (`trevrpc_server_conn' or `trevrpc_channel') routes
%% a stream's events to the process using the stream as
%% `{trevrpc_stream, Ref, Event}' messages:
%%
%% <ul>
%%   <li>`{data, Bin, Fin}': stream data, and whether the peer finished.</li>
%%   <li>`{reset, Code}': the peer reset its sending side.</li>
%%   <li>`{stop_sending, Code}': the peer stopped reading.</li>
%%   <li>`closed': the connection closed.</li>
%%   <li>`send_ready': the connection's send queue drained, after a
%%       {@link send/4} found it full.</li>
%%   <li>`{datagram, Messages, Bytes}': the messages of a datagram of the
%%       call's flow (see {@link trevrpc_datagram_flows}), read only once
%%       {@link datagrams/2} enabled them.</li>
%%   <li>`datagrams_arrived': the datagrams {@link end_datagrams/4} waits
%%       for arrived.</li>
%% </ul>
%%
%% {@link next_frame/2} returns the datagrams read, in the order they
%% arrived, after the frames read before them, and before the end of the
%% stream. They never block the stream: the connection drops datagrams for
%% a reader that falls behind.
%%
%% The stream is in manual flow control: the peer may send one window past
%% the bytes returned with `trevrpc_transport:consume/3'. Bytes are returned as they are
%% read into the frame reader, which is only fed when the decoded frames
%% have all been taken, so a slow reader holds the peer back. They are
%% returned in batches while more data is already waiting, and at once
%% before the reader waits.
-module(trevrpc_stream).

-export([
    new/5,
    transport/1,
    id/1,
    ref/1,
    finished/1,
    peer_finished/1,
    consumed/1,
    stopped/1,
    charge_messages/2,
    charge_message/2,
    next_frame/2,
    peek/1,
    peek/2,
    datagrams/2,
    end_datagrams/4,
    take_datagrams/1,
    send/4,
    abort/2,
    release/1,
    flush_events/1
]).

-export_type([stream/0, deadline/0, read_error/0]).

%% An absolute deadline in `erlang:monotonic_time(millisecond)' units.
-type deadline() :: integer() | infinity.

-type read_error() ::
    trevrpc_frame:error()
    | truncated
    | {reset, non_neg_integer()}
    | closed
    | timeout.

-record(stream, {
    transport :: trevrpc_transport:t(),
    id :: non_neg_integer(),
    ref :: reference(),
    %% The process that routes this stream's events, asked for send_ready.
    router :: pid(),
    reader :: trevrpc_frame:reader(),
    %% Frames read and not taken yet, in order.
    frames = [] :: [trevrpc_frame:frame()],
    %% Bytes read and not returned to the peer as credit yet.
    unconsumed = 0 :: non_neg_integer(),
    %% Bytes returned to the peer as credit so far.
    consumed = 0 :: non_neg_integer(),
    %% The peer finished sending (FIN).
    fin = false :: boolean(),
    %% The peer stopped reading (STOP_SENDING); sends are pointless.
    stopped = false :: boolean(),
    %% Why reading ended, once it failed.
    error :: read_error() | undefined,
    %% The credit of the call's flow while its datagrams are read, and the
    %% datagrams read and not taken yet, as `{Messages, Bytes}'.
    credit :: trevrpc_datagram_flows:credit() | undefined,
    datagrams = queue:new() :: queue:queue({[trevrpc_datagram:message()], non_neg_integer()})
}).

-opaque stream() :: #stream{}.

%% Returned credit is batched up to this many bytes while more data is
%% already waiting, and returned at once before the reader blocks.
-define(CONSUME_BATCH, 65536).

%% @doc A stream of connection `Transport', whose events `Router' sends tagged
%% with `Ref', read with `Reader'.
-spec new(trevrpc_transport:t(), non_neg_integer(), reference(), pid(), trevrpc_frame:reader()) ->
    stream().
new(Transport, Id, Ref, Router, Reader) ->
    #stream{transport = Transport, id = Id, ref = Ref, router = Router, reader = Reader}.

-spec transport(stream()) -> trevrpc_transport:t().
transport(#stream{transport = Transport}) -> Transport.

-spec id(stream()) -> non_neg_integer().
id(#stream{id = Id}) -> Id.

-spec ref(stream()) -> reference().
ref(#stream{ref = Ref}) -> Ref.

%% @doc Whether the peer finished sending and every frame it sent was taken.
-spec finished(stream()) -> boolean().
finished(#stream{fin = Fin, frames = Frames}) -> Fin andalso Frames =:= [].

%% @doc Whether the peer finished sending, whether or not every frame it
%% sent was taken.
-spec peer_finished(stream()) -> boolean().
peer_finished(#stream{fin = Fin}) -> Fin.

%% @doc The bytes returned to the peer as credit so far.
-spec consumed(stream()) -> non_neg_integer().
consumed(#stream{consumed = Consumed}) -> Consumed.

%% @doc Whether the peer stopped reading.
-spec stopped(stream()) -> boolean().
stopped(#stream{stopped = Stopped}) -> Stopped.

%% @doc Sets whether MESSAGE payloads read from now on count towards the
%% reader's cumulative limit.
-spec charge_messages(boolean(), stream()) -> stream().
charge_messages(Enabled, #stream{reader = Reader} = Stream) ->
    Stream#stream{reader = trevrpc_frame:charge_messages(Enabled, Reader)}.

%% @doc Charges `Bytes', the decompressed size of a compressed message,
%% against the reader's cumulative limit, if messages count towards it. A
%% stream past its limit fails, as if the reader had found it.
-spec charge_message(non_neg_integer(), stream()) ->
    {ok, stream()} | {error, read_error(), stream()}.
charge_message(Bytes, #stream{reader = Reader} = Stream) ->
    case trevrpc_frame:charge_message(Bytes, Reader) of
        {ok, Next} -> {ok, Stream#stream{reader = Next}};
        {error, Reason} -> fail(Reason, Stream)
    end.

%% @doc The next frame the peer sent, if it has arrived, without taking it
%% and without waiting.
-spec peek(stream()) -> {trevrpc_frame:frame() | none, stream()}.
peek(#stream{frames = [Frame | _]} = Stream) ->
    {Frame, Stream};
peek(#stream{error = undefined, fin = false, ref = Ref} = Stream) ->
    receive
        {trevrpc_stream, Ref, Event} -> peek(event(Event, Stream))
    after 0 -> {none, Stream}
    end;
peek(Stream) ->
    {none, Stream}.

%% @doc The next frame the peer sent, without taking it, waiting for it
%% until `Deadline'. `none' means none will come: the peer finished or the
%% stream failed, which {@link next_frame/2} reports.
-spec peek(stream(), deadline()) -> {trevrpc_frame:frame() | none | timeout, stream()}.
peek(#stream{frames = [Frame | _]} = Stream, _Deadline) ->
    {Frame, Stream};
peek(#stream{error = undefined, fin = false, ref = Ref} = Stream, Deadline) ->
    receive
        {trevrpc_stream, Ref, Event} -> peek(event(Event, Stream), Deadline)
    after 0 ->
        Flushed = flush_credit(Stream),
        receive
            {trevrpc_stream, Ref, Event} -> peek(event(Event, Flushed), Deadline)
        after timeout(Deadline) ->
            {timeout, Flushed}
        end
    end;
peek(Stream, _Deadline) ->
    {none, Stream}.

%% @doc Takes the next frame the peer sent, or the messages of the next
%% datagram, waiting for them until `Deadline'. `eof' means the peer
%% finished at a frame boundary.
-spec next_frame(stream(), deadline()) ->
    {ok, trevrpc_frame:frame(), stream()}
    | {datagram, [trevrpc_datagram:message()], stream()}
    | {eof, stream()}
    | {error, read_error(), stream()}.
next_frame(#stream{frames = [Frame | Rest]} = Stream, _Deadline) ->
    {ok, Frame, Stream#stream{frames = Rest}};
next_frame(#stream{error = Error} = Stream, _Deadline) when Error =/= undefined ->
    {error, Error, Stream};
next_frame(#stream{datagrams = Datagrams, credit = Credit} = Stream, Deadline) ->
    case queue:out(Datagrams) of
        {{value, {Messages, Bytes}}, Rest} ->
            _ =
                case Credit of
                    undefined -> ok;
                    _ -> trevrpc_datagram_flows:take(Credit, length(Messages), Bytes)
                end,
            {datagram, Messages, Stream#stream{datagrams = Rest}};
        {empty, _} ->
            next_stream_frame(Stream, Deadline)
    end.

next_stream_frame(#stream{fin = true, reader = Reader} = Stream, _Deadline) ->
    case trevrpc_frame:at_boundary(Reader) of
        true -> {eof, Stream};
        false -> fail(truncated, Stream)
    end;
next_stream_frame(#stream{ref = Ref} = Stream, Deadline) ->
    receive
        {trevrpc_stream, Ref, Event} -> next_frame(event(Event, Stream), Deadline)
    after 0 ->
        Flushed = flush_credit(Stream),
        receive
            {trevrpc_stream, Ref, Event} -> next_frame(event(Event, Flushed), Deadline)
        after timeout(Deadline) ->
            {error, timeout, Flushed}
        end
    end.

%% @doc Reads the datagrams of the call's flow, whose credit is `Credit',
%% from now on.
-spec datagrams(trevrpc_datagram_flows:credit(), stream()) -> stream().
datagrams(Credit, Stream) ->
    Stream#stream{credit = Credit}.

%% @doc Ends the datagrams of the call's flow `Flow' as the end of the
%% stream's direction arrived, announcing `Count' datagrams in its
%% DATAGRAM_COUNT (0 for none): waits until that many arrived, `Until'
%% passes, or the stream fails, then reads no more datagrams. Those read by
%% then are still returned by {@link next_frame/2}, before the end.
-spec end_datagrams(pos_integer(), non_neg_integer(), deadline(), stream()) -> stream().
end_datagrams(_Flow, _Count, _Until, #stream{credit = undefined} = Stream) ->
    Stream;
end_datagrams(Flow, Count, Until, #stream{credit = Credit, ref = Ref, router = Router} = Stream) ->
    Waited =
        case Count of
            0 ->
                Stream;
            _ ->
                Router ! {trevrpc_await_datagrams, self(), Ref, Flow, Count},
                await_datagrams(Stream, Until)
        end,
    ok = trevrpc_datagram_flows:close(Credit),
    %% What the connection sent before the credit closed is the call's.
    (drain_datagrams(Waited))#stream{credit = undefined}.

%% @doc Takes the messages of every datagram read and not taken yet, in
%% order: those of a direction whose datagrams ended.
-spec take_datagrams(stream()) -> {[[trevrpc_datagram:message()]], stream()}.
take_datagrams(#stream{datagrams = Datagrams, credit = undefined} = Stream) ->
    {[Messages || {Messages, _Bytes} <- queue:to_list(Datagrams)], Stream#stream{
        datagrams = queue:new()
    }}.

await_datagrams(#stream{ref = Ref} = Stream, Until) ->
    receive
        {trevrpc_stream, Ref, datagrams_arrived} ->
            Stream;
        {trevrpc_stream, Ref, Event} ->
            case event(Event, Stream) of
                #stream{error = undefined} = Next -> await_datagrams(Next, Until);
                Failed -> Failed
            end
    after timeout(Until) ->
        Stream
    end.

drain_datagrams(#stream{ref = Ref} = Stream) ->
    receive
        {trevrpc_stream, Ref, {datagram, _, _} = Event} -> drain_datagrams(event(Event, Stream))
    after 0 -> Stream
    end.

event({data, Bin, Fin}, #stream{reader = Reader, unconsumed = Unconsumed} = Stream) ->
    Read = Stream#stream{unconsumed = Unconsumed + byte_size(Bin), fin = Fin},
    case trevrpc_frame:feed(Bin, Reader) of
        {ok, Frames, Next} ->
            batch_credit(Read#stream{reader = Next, frames = Frames});
        {error, Reason} ->
            Read#stream{error = Reason, frames = []}
    end;
event({reset, Code}, Stream) ->
    Stream#stream{error = {reset, Code}};
event({stop_sending, _Code}, Stream) ->
    Stream#stream{stopped = true};
event(closed, Stream) ->
    Stream#stream{error = closed};
event(send_ready, Stream) ->
    %% A stale wake-up from an earlier full send queue.
    Stream;
event({datagram, _Messages, _Bytes}, #stream{credit = undefined} = Stream) ->
    %% The call reads no datagrams, or no more.
    Stream;
event({datagram, Messages, Bytes}, #stream{datagrams = Datagrams} = Stream) ->
    Stream#stream{datagrams = queue:in({Messages, Bytes}, Datagrams)};
event(datagrams_arrived, Stream) ->
    %% A late answer to a wait that timed out.
    Stream.

fail(Reason, Stream) ->
    {error, Reason, Stream#stream{error = Reason, frames = []}}.

batch_credit(#stream{unconsumed = Unconsumed} = Stream) when Unconsumed >= ?CONSUME_BATCH ->
    flush_credit(Stream);
batch_credit(Stream) ->
    Stream.

flush_credit(#stream{unconsumed = 0} = Stream) ->
    Stream;
flush_credit(#stream{transport = Transport, id = Id, unconsumed = Unconsumed} = Stream) ->
    %% The stream may already be gone (finished or reset); its credit no
    %% longer matters then.
    _ = trevrpc_transport:consume(Transport, Id, Unconsumed),
    Stream#stream{unconsumed = 0, consumed = Stream#stream.consumed + Unconsumed}.

%% @doc Writes `Data' to the stream, finishing it with `Fin'. When the
%% connection's send queue is full, or over HTTP/2 the peer's window, waits
%% until `Deadline' for it to drain.
-spec send(stream(), iodata(), boolean(), deadline()) ->
    {ok, stream()} | {error, timeout | stopped | closed | term(), stream()}.
send(#stream{stopped = true} = Stream, _Data, _Fin, _Deadline) ->
    {error, stopped, Stream};
send(#stream{transport = Transport, id = Id} = Stream, Data, Fin, Deadline) ->
    case trevrpc_transport:send_data(Transport, Id, Data, Fin, timeout(Deadline)) of
        ok -> {ok, Stream};
        {error, send_queue_full} -> wait_send_ready(Stream, Data, Fin, Deadline);
        {error, Reason} -> {error, Reason, Stream}
    end.

%% quic_h3 tells the writer it refused itself.
wait_send_ready(#stream{transport = {h3, H3, _}, id = Id, ref = Ref} = Stream, Data, Fin, Deadline) ->
    receive
        {quic_h3, H3, {send_ready, Id}} ->
            send(Stream, Data, Fin, Deadline);
        {trevrpc_stream, Ref, closed} ->
            {error, closed, Stream#stream{error = closed}};
        {trevrpc_stream, Ref, {stop_sending, _}} ->
            {error, stopped, Stream#stream{stopped = true}}
    after timeout(Deadline) ->
        {error, timeout, Stream}
    end;
wait_send_ready(#stream{ref = Ref, router = Router} = Stream, Data, Fin, Deadline) ->
    Router ! {trevrpc_send_ready, self(), Ref},
    receive
        {trevrpc_stream, Ref, send_ready} ->
            send(Stream, Data, Fin, Deadline);
        {trevrpc_stream, Ref, closed} ->
            {error, closed, Stream#stream{error = closed}};
        {trevrpc_stream, Ref, {stop_sending, _}} ->
            {error, stopped, Stream#stream{stopped = true}}
    after timeout(Deadline) ->
        {error, timeout, Stream}
    end.

%% @doc Abandons the stream: resets the sending side and stops the receiving
%% side, both with `Code'. Errors are ignored, as the stream or the whole
%% connection may already be gone.
-spec abort(stream(), non_neg_integer()) -> ok.
abort(#stream{transport = Transport, id = Id} = Stream, Code) ->
    _ = trevrpc_transport:reset_stream(Transport, Id, Code),
    _ = trevrpc_transport:stop_sending(Transport, Id, Code),
    flush_events(Stream).

%% @doc Gives up reading: returns the credit of every byte read or routed
%% here and not consumed, those the reader holds and those of the data
%% events still in the mailbox, which it drops with the stream's other
%% events. The peer's data that nothing will read then does not hold back
%% the connection's receive window while the stream lingers.
-spec release(stream()) -> ok.
release(#stream{ref = Ref, unconsumed = Unconsumed} = Stream) ->
    _ = flush_credit(Stream#stream{unconsumed = Unconsumed + pending_data(Ref, 0)}),
    ok.

pending_data(Ref, Bytes) ->
    receive
        {trevrpc_stream, Ref, {data, Bin, _Fin}} -> pending_data(Ref, Bytes + byte_size(Bin));
        {trevrpc_stream, Ref, _Other} -> pending_data(Ref, Bytes)
    after 0 -> Bytes
    end.

%% @doc Drops the stream's events still in the mailbox.
-spec flush_events(stream() | reference()) -> ok.
flush_events(#stream{ref = Ref}) ->
    flush_events(Ref);
flush_events(Ref) when is_reference(Ref) ->
    receive
        {trevrpc_stream, Ref, _} -> flush_events(Ref)
    after 0 -> ok
    end.

timeout(infinity) -> infinity;
timeout(Deadline) -> max(0, Deadline - erlang:monotonic_time(millisecond)).
