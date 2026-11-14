%% @doc Calls on a {@link trevrpc_channel}.
%%
%% ```
%% {ok, Reply, #{headers := Headers, trailers := Trailers}} =
%%     trevrpc_client:unary(Channel, <<"/pkg.Svc/Unary">>, Request, #{timeout => 5000}),
%%
%% {ok, Call} = trevrpc_client:bidi(Channel, <<"/pkg.Svc/Bidi">>, #{}),
%% ok = trevrpc_client:send(Call, Request),
%% {ok, Response} = trevrpc_client:recv(Call),
%% ok = trevrpc_client:close_send(Call),
%% {eof, Trailers} = trevrpc_client:recv(Call).
%% '''
%%
%% Messages are opaque binaries here: the caller encodes requests and
%% decodes responses, usually through generated code.
%%
%% A call is read and written by the process that started it, which owns
%% its stream; if that process exits first, the channel cancels the call. A
%% call is finished once {@link recv/1} returns `{eof, Trailers}' or
%% `{error, Status}', or after {@link cancel/1}.
%%
%% Options: `timeout' (milliseconds) or `deadline' (in
%% `erlang:monotonic_time(millisecond)' units), sent to the server and
%% enforced locally, and `metadata', the request metadata. `request_type'
%% and `response_type' are the protobuf full names of the method's
%% messages, which pick their compression dictionaries, and `compression'
%% says how its messages may be compressed without one (see
%% {@link trevrpc_zstd:directions()}), as the method's schema options do.
%%
%% With `compression => stream', a call offers stream compression for the
%% directions it streams. Its requests are compressed optimistically, before
%% the server answers, while the connection's server accepts that: the call
%% keeps a copy of up to 1 MiB of its requests until the answer arrives, and
%% if the server refuses the call before its handler runs, replays it
%% without stream compression on a new stream, transparently. A send that
%% would keep more waits for the answer.
-module(trevrpc_client).

-export([
    unary/4,
    server_stream/4,
    client_stream/3,
    bidi/3,
    send/2,
    close_send/1,
    close_and_recv/1,
    recv/1,
    headers/1,
    cancel/1
]).

-export_type([call/0, options/0]).

-type options() :: #{
    timeout => non_neg_integer(),
    deadline => integer(),
    metadata => trevrpc_metadata:metadata(),
    request_type => binary(),
    response_type => binary(),
    compression => trevrpc_zstd:directions()
}.

-record(client_call, {
    %% Names the call's state; the stream it uses may change on a replay.
    ref :: reference(),
    channel :: pid(),
    kind :: trevrpc_wire:kind(),
    deadline :: trevrpc_stream:deadline(),
    owner :: pid()
}).

-opaque call() :: #client_call{}.

%% What changes as a call runs, in the calling process's dictionary under
%% {trevrpc_client, Ref}.
-record(cstate, {
    stream :: trevrpc_stream:stream(),
    generation :: pos_integer(),
    max_frame_size :: pos_integer(),
    %% The REQUEST's fields and how the call's directions may be
    %% compressed, for a replay.
    request :: map(),
    modes :: {trevrpc_zstd:mode(), trevrpc_zstd:mode()},
    send_closed = false :: boolean(),
    %% A known frame was read, so an ANSWER may no longer come.
    answered = false :: boolean(),
    %% The response headers, once known: a HEADERS frame, or empty once a
    %% message or the status came first.
    headers :: trevrpc_metadata:metadata() | undefined,
    %% Messages read ahead, while looking for headers or a status.
    pending = [] :: [binary()],
    terminal :: terminal() | undefined,
    %% Compression: the channel's configuration, the dictionaries response
    %% messages may use, the dictionary of request messages (see
    %% trevrpc_zstd:prepare/5), and the generation whose shared set the
    %% call's ANSWER teaches the channel, when the call queried.
    compression :: trevrpc_zstd:config(),
    accept = [] :: [non_neg_integer()],
    requests = none :: non_neg_integer() | {pending, non_neg_integer()} | none,
    learn :: pos_integer() | undefined,
    %% Stream compression: what the REQUEST offered, the response stream's
    %% decompressing context, and the request stream's sender.
    offer = none :: trevrpc_wire:stream_compression(),
    stream_decoder :: trevrpc_zstd_stream:decoder() | undefined,
    request_sender :: trevrpc_zstd_stream:sender() | undefined,
    %% An optimistic call waiting for the server's decision keeps copies of
    %% the requests it sent (newest first), and memory for them from the
    %% channel's budget.
    deciding = false :: boolean(),
    kept = [] :: [iodata()],
    kept_bytes = 0 :: non_neg_integer(),
    copy_reserved = 0 :: non_neg_integer(),
    %% The datagrams the server announced with DATAGRAM_COUNT, once read.
    %% Nothing waits for them yet, as the runtime has no datagram messages.
    datagram_count :: non_neg_integer() | undefined
}).

-type terminal() :: {eof, trevrpc_metadata:metadata()} | {error, trevrpc_status:status()}.

-define(CANCELLED, 1).
%% The most request bytes an optimistic call keeps until the server
%% decides, and how much budget it reserves for them at a time.
-define(MAX_KEPT, 1048576).
-define(COPY_CHUNK, 65536).

%% @doc A unary call. `trailers' is the trailing metadata of the OK status.
-spec unary(pid(), binary(), iodata(), options()) ->
    {ok, binary(), #{
        headers := trevrpc_metadata:metadata(), trailers := trevrpc_metadata:metadata()
    }}
    | {error, trevrpc_status:status()}.
unary(Channel, Method, Request, Opts) ->
    case start(Channel, Method, unary, Opts, {message, Request}) of
        {ok, Call} -> single_response(Call);
        Error -> Error
    end.

%% @doc Starts a server-streaming call; read its responses with
%% {@link recv/1}.
-spec server_stream(pid(), binary(), iodata(), options()) ->
    {ok, call()} | {error, trevrpc_status:status()}.
server_stream(Channel, Method, Request, Opts) ->
    start(Channel, Method, server_streaming, Opts, {message, Request}).

%% @doc Starts a client-streaming call: send its requests with
%% {@link send/2}, then get the response with {@link close_and_recv/1}.
-spec client_stream(pid(), binary(), options()) -> {ok, call()} | {error, trevrpc_status:status()}.
client_stream(Channel, Method, Opts) ->
    start(Channel, Method, client_streaming, Opts, none).

%% @doc Starts a bidirectional call.
-spec bidi(pid(), binary(), options()) -> {ok, call()} | {error, trevrpc_status:status()}.
bidi(Channel, Method, Opts) ->
    start(Channel, Method, bidirectional_streaming, Opts, none).

start(Channel, Method, Kind, Opts, First) ->
    Deadline = deadline(Opts),
    Metadata = maps:get(metadata, Opts, #{}),
    maybe
        ok ?= check_deadline(Deadline),
        ok ?= check_metadata(Metadata),
        {ok, Info} ?= trevrpc_channel:open_stream(Channel, Deadline),
        #{ref := Ref, max_frame_size := Max} = Info,
        #{compression := Compression, shared := Shared, generation := Generation} = Info,
        Call = #client_call{
            ref = Ref, channel = Channel, kind = Kind, deadline = Deadline, owner = self()
        },
        {Requests, Responses} = Modes = trevrpc_zstd:directions(maps:get(compression, Opts, none)),
        #{query := Query, accept := Accept, requests := RequestDict} = trevrpc_zstd:prepare(
            Compression,
            Shared,
            maps:get(request_type, Opts, undefined),
            maps:get(response_type, Opts, undefined),
            {Requests =/= none, Responses =/= none}
        ),
        Fields = #{
            method => Method,
            metadata => Metadata,
            kind => Kind,
            accept_dictionaries => Accept,
            dictionary_query => Query
        },
        State = offer(Call, Info, #cstate{
            stream = new_stream(Info, Channel),
            generation = Generation,
            max_frame_size = Max,
            request = Fields,
            modes = Modes,
            compression = Compression,
            accept = Accept,
            requests = RequestDict,
            learn =
                case Query of
                    [] -> undefined;
                    _ -> Generation
                end
        }),
        %% A single request goes with its REQUEST, in one write.
        {Frames, Fin, Encoded} =
            case First of
                none ->
                    {request_frame(State, Deadline), false, State};
                {message, Message} ->
                    {Frame, Next} = encode_request(Call, State, Message),
                    {[request_frame(State, Deadline), Frame], true, Next}
            end,
        put_state(Call, Encoded#cstate{send_closed = Fin}),
        case write(Call, Frames, Fin) of
            ok ->
                {ok, Call};
            {error, Status} ->
                done(Call, state(Call)),
                {error, Status}
        end
    end.

new_stream(#{transport := Transport, id := Id, ref := Ref, max_frame_size := Max}, Channel) ->
    trevrpc_stream:new(Transport, Id, Ref, Channel, trevrpc_frame:reader(Max)).

request_frame(#cstate{request = Fields, offer = Offer}, Deadline) ->
    trevrpc_frame:encode(
        request,
        trevrpc_wire:encode_request(Fields#{
            timeout_nanos => timeout_nanos(Deadline), stream_compression => Offer
        })
    ).

%% Offers stream compression for the directions the call streams and its
%% options allow, within the channel's budget. An offer says the client
%% decodes stream-compressed responses, so a call whose responses stream
%% offers only if it can reserve their decompressing context. A call whose
%% requests may be stream-compressed is optimistic while the connection's
%% server accepts that and the budget has room for its compressing context.
offer(
    #client_call{kind = Kind, channel = Channel},
    Info,
    #cstate{modes = {Requests, Responses}} = State
) ->
    #{stream_budget := Budget, optimistic := Optimistic, id := Id, ref := Ref} = Info,
    StreamRequests =
        Requests =:= stream andalso trevrpc_wire:streams_requests(Kind) andalso
            Budget =/= undefined,
    StreamResponses =
        Responses =:= stream andalso trevrpc_wire:streams_responses(Kind) andalso
            Budget =/= undefined,
    Decoder =
        case StreamResponses of
            true ->
                case
                    trevrpc_channel:reserve(Channel, Id, Ref, trevrpc_zstd_stream:decoder_cost())
                of
                    true ->
                        trevrpc_zstd_stream:decoder(State#cstate.compression, State#cstate.accept);
                    false ->
                        refused
                end;
            false ->
                undefined
        end,
    case Decoder =/= refused andalso (StreamRequests orelse StreamResponses) of
        false ->
            State;
        true ->
            Offer =
                case
                    StreamRequests andalso Optimistic andalso
                        trevrpc_zstd_stream:has_room(Budget, trevrpc_zstd_stream:encoder_cost())
                of
                    true -> optimistic;
                    false -> enabled
                end,
            State#cstate{
                offer = Offer,
                stream_decoder = Decoder,
                request_sender =
                    case StreamRequests of
                        true -> trevrpc_zstd_stream:sender(Offer =:= optimistic);
                        false -> undefined
                    end,
                deciding = Offer =:= optimistic
            }
    end.

deadline(#{deadline := Deadline}) -> Deadline;
deadline(#{timeout := Timeout}) -> erlang:monotonic_time(millisecond) + Timeout;
deadline(#{}) -> infinity.

check_deadline(infinity) ->
    ok;
check_deadline(Deadline) ->
    case Deadline > erlang:monotonic_time(millisecond) of
        true -> ok;
        false -> {error, deadline_status()}
    end.

check_metadata(Metadata) ->
    case trevrpc_metadata:validate(Metadata) of
        ok -> ok;
        {error, Message} -> {error, trevrpc_status:new(invalid_argument, Message)}
    end.

timeout_nanos(infinity) ->
    0;
timeout_nanos(Deadline) ->
    max(1, Deadline - erlang:monotonic_time(millisecond)) * 1000000.

%% @doc Sends a request message of a client-streaming or bidirectional
%% call. `{error, Status}' means the call ended, with `Status' (which is OK
%% when the server finished it successfully); its responses can still be
%% read.
-spec send(call(), iodata()) -> ok | {error, trevrpc_status:status()}.
send(#client_call{kind = Kind} = Call, Message) when
    Kind =:= client_streaming; Kind =:= bidirectional_streaming
->
    case state(Call) of
        #cstate{send_closed = true} ->
            erlang:error(send_closed);
        #cstate{terminal = undefined} = State ->
            case before_send(Call, State, iolist_size(Message)) of
                {ok, Ready} ->
                    {Frame, Encoded} = encode_request(Call, Ready, Message),
                    put_state(Call, keep(Encoded, Message)),
                    write(Call, Frame, false);
                {error, Status} ->
                    {error, Status}
            end;
        #cstate{terminal = Terminal} ->
            {error, terminal_status(Terminal)}
    end;
send(_Call, _Message) ->
    erlang:error(badarg).

%% Takes what the server decided, if it has arrived, and makes room to keep
%% a copy of a request of `Size' bytes while an optimistic call waits; with
%% no room, waits for the decision.
before_send(Call, State, Size) ->
    case check_answer(Call, State) of
        {Result, #cstate{deciding = true} = Checked} when Result =:= ok; Result =:= replayed ->
            case copy_room(Call, Checked, Size) of
                {ok, Roomy} ->
                    put_state(Call, Roomy),
                    {ok, Roomy};
                full ->
                    case await_decision(Call, Checked, false) of
                        {error, Status} -> {error, Status};
                        {_, Decided} -> before_send(Call, Decided, Size)
                    end
            end;
        {Result, Checked} when Result =:= ok; Result =:= replayed ->
            {ok, Checked};
        {error, Status} ->
            {error, Status}
    end.

copy_room(#client_call{channel = Channel}, State, Size) ->
    #cstate{kept_bytes = Kept, copy_reserved = Reserved, stream = Stream} = State,
    if
        Kept + Size > ?MAX_KEPT ->
            full;
        Kept + Size =< Reserved ->
            {ok, State};
        true ->
            More = max(?COPY_CHUNK, Kept + Size - Reserved),
            case
                trevrpc_channel:reserve(
                    Channel, trevrpc_stream:id(Stream), trevrpc_stream:ref(Stream), More
                )
            of
                true -> {ok, State#cstate{copy_reserved = Reserved + More}};
                false -> full
            end
    end.

keep(#cstate{deciding = true, kept = Kept, kept_bytes = Bytes} = State, Message) ->
    State#cstate{kept = [Message | Kept], kept_bytes = Bytes + iolist_size(Message)};
keep(State, _Message) ->
    State.

%% The frame of a request message: a ZSTD_STREAM_MESSAGE once the request
%% stream's context runs, else a ZSTD_MESSAGE when the call's request
%% dictionary is shared and makes the message shorter, else a MESSAGE.
encode_request(_Call, #cstate{request_sender = undefined} = State, Message) ->
    {per_message(State, Message), State};
encode_request(#client_call{channel = Channel}, #cstate{request_sender = Sender} = State, Message) ->
    #cstate{compression = Compression, stream = Stream} = State,
    %% The context starts from the requests' dictionary once it is shared.
    Seed =
        case State#cstate.requests of
            Id when is_integer(Id) -> Id;
            _ -> 0
        end,
    Reserve = fun() ->
        trevrpc_channel:reserve(
            Channel,
            trevrpc_stream:id(Stream),
            trevrpc_stream:ref(Stream),
            trevrpc_zstd_stream:encoder_cost()
        )
    end,
    case trevrpc_zstd_stream:next(Sender, Message, Compression, Seed, Reserve) of
        {stream, Payload, Next} ->
            {trevrpc_frame:encode(zstd_stream_message, Payload), State#cstate{request_sender = Next}};
        {message, Next} ->
            {per_message(State, Message), State#cstate{request_sender = Next}}
    end.

per_message(#cstate{requests = Id, compression = Compression}, Message) when is_integer(Id) ->
    case trevrpc_zstd:compress(Compression, Id, Message) of
        {ok, Payload} -> trevrpc_frame:encode(zstd_message, Payload);
        none -> trevrpc_frame:encode(message, Message)
    end;
per_message(_State, Message) ->
    trevrpc_frame:encode(message, Message).

%% A call waiting for its ANSWER (to confirm its request dictionary, to
%% enable its stream-compressed requests, or to accept its optimistic
%% requests) takes what the server sent first as soon as it has arrived, so
%% its requests start compressing mid-call. Returns the state the next
%% request is encoded with (`replayed' after a refusal), or the status of a
%% call that ended, as reading it would.
check_answer(Call, #cstate{stream = Stream} = State) ->
    case waits_for_answer(State) of
        true ->
            case trevrpc_stream:peek(Stream) of
                {none, Peeked} ->
                    Polled = State#cstate{stream = Peeked},
                    put_state(Call, Polled),
                    {ok, Polled};
                {Frame, Peeked} ->
                    first_frame(Call, Frame, State#cstate{stream = Peeked}, false)
            end;
        false ->
            {ok, State}
    end.

waits_for_answer(#cstate{answered = true}) -> false;
waits_for_answer(#cstate{requests = {pending, _}}) -> true;
waits_for_answer(#cstate{deciding = true}) -> true;
waits_for_answer(#cstate{request_sender = Sender}) -> Sender =/= undefined.

%% Waits for the server's decision on an optimistic call, as a send that
%% has no room to keep its request must.
await_decision(#client_call{deadline = Deadline} = Call, #cstate{stream = Stream} = State, Fin) ->
    case trevrpc_stream:peek(Stream, Deadline) of
        {timeout, Peeked} ->
            {error, abort(Call, State#cstate{stream = Peeked}, deadline_status())};
        {none, Peeked} ->
            %% The stream ended or failed before deciding; reading it says
            %% how.
            Ended = settle(Call, State#cstate{stream = Peeked}),
            put_state(Call, Ended),
            {ok, Ended};
        {Frame, Peeked} ->
            first_frame(Call, Frame, State#cstate{stream = Peeked}, Fin)
    end.

%% Acts on the first response frame of a call waiting for its ANSWER,
%% leaving any frame but the ANSWER and a refusal for the reader.
first_frame(Call, {answer, _}, #cstate{stream = Stream} = State, _Fin) ->
    {ok, {answer, Payload}, Next} = trevrpc_stream:next_frame(Stream, infinity),
    case answer(Call, Payload, State#cstate{stream = Next}) of
        {ok, Answered} ->
            put_state(Call, Answered),
            {ok, Answered};
        {error, Message} ->
            {terminal, {error, Status}, _} = protocol_error(
                Call, State#cstate{stream = Next}, Message
            ),
            {error, Status}
    end;
first_frame(Call, {status, Payload}, #cstate{deciding = true, stream = Stream} = State, Fin) ->
    case refused(Payload) of
        true ->
            {ok, _, Next} = trevrpc_stream:next_frame(Stream, infinity),
            replay(Call, State#cstate{stream = Next}, Fin);
        false ->
            Settled = settle(Call, answered(State)),
            put_state(Call, Settled),
            {ok, Settled}
    end;
first_frame(Call, {Type, _}, #cstate{deciding = true} = State, _Fin) ->
    {terminal, {error, Status}, _} = optimistic_error(Call, State, Type),
    {error, Status};
first_frame(Call, _Frame, State, _Fin) ->
    %% Another frame came first, so no ANSWER will.
    Unconfirmed = answered(State),
    put_state(Call, Unconfirmed),
    {ok, Unconfirmed}.

refused(Payload) ->
    case trevrpc_wire:decode_status(Payload) of
        {ok, #{refused_stream_compression := true}} -> true;
        _ -> false
    end.

optimistic_error(Call, State, Type) ->
    protocol_error(
        Call,
        State,
        iolist_to_binary(
            io_lib:format("optimistic call answered with ~s before ANSWER", [
                trevrpc_frame:name(Type)
            ])
        )
    ).

%% The server decided: the copies are not needed any more.
settle(#client_call{channel = Channel}, #cstate{copy_reserved = Reserved, stream = Stream} = State) ->
    _ =
        case Reserved of
            0 ->
                ok;
            _ ->
                trevrpc_channel:release(
                    Channel, trevrpc_stream:id(Stream), trevrpc_stream:ref(Stream), Reserved
                )
        end,
    State#cstate{deciding = false, kept = [], kept_bytes = 0, copy_reserved = 0}.

%% The server refused the optimistic call before its handler ran: the
%% connection's calls stop being optimistic until one is answered, and the
%% call starts again on a new stream with the same REQUEST, offering stream
%% compression without being optimistic, with the requests it kept and,
%% with `Fin' or once the caller closed the upload, its end. A call is
%% replayed at most once, since the replay is not optimistic.
replay(#client_call{channel = Channel, deadline = Deadline} = Call, State, Fin) ->
    #cstate{stream = Old, kept = Kept, generation = Generation} = State,
    trevrpc_channel:optimism(Channel, Generation, false),
    _ = trevrpc_stream:abort(Old, ?CANCELLED),
    %% The channel gives back what the old stream held.
    trevrpc_channel:stream_done(Channel, trevrpc_stream:id(Old), trevrpc_stream:ref(Old)),
    trevrpc_stream:flush_events(trevrpc_stream:ref(Old)),
    Closing = Fin orelse State#cstate.send_closed,
    case trevrpc_channel:open_stream(Channel, Deadline) of
        {ok, Info} ->
            Base = State#cstate{
                stream = new_stream(Info, Channel),
                generation = maps:get(generation, Info),
                answered = false,
                offer = none,
                stream_decoder = undefined,
                request_sender = undefined,
                deciding = false,
                kept = [],
                kept_bytes = 0,
                copy_reserved = 0
            },
            Offered = offer_again(Call, Info, Base),
            {Frames, Encoded} = lists:mapfoldl(
                fun(Message, Acc) -> encode_request(Call, Acc, Message) end,
                Offered,
                lists:reverse(Kept)
            ),
            Ready = Encoded#cstate{send_closed = Closing},
            #cstate{stream = Stream} = Ready,
            case
                trevrpc_stream:send(
                    Stream, [request_frame(Offered, Deadline) | Frames], Closing, Deadline
                )
            of
                {ok, Next} ->
                    Replayed = Ready#cstate{stream = Next},
                    put_state(Call, Replayed),
                    {replayed, Replayed};
                {error, timeout, Next} ->
                    {error, abort(Call, Ready#cstate{stream = Next}, deadline_status())};
                {error, _Reason, Next} ->
                    {error,
                        terminal_status(
                            read_terminal(Call, Ready#cstate{stream = Next, send_closed = true})
                        )}
            end;
        {error, Status} ->
            %% Nothing holds the old stream any more.
            put_state(Call, State#cstate{terminal = {error, Status}, send_closed = true}),
            {error, Status}
    end.

%% A replay offers what the call offered, but never optimistically.
offer_again(Call, Info, State) ->
    offer(Call, Info#{optimistic := false}, State).

%% @doc Ends the requests of a client-streaming or bidirectional call.
-spec close_send(call()) -> ok | {error, trevrpc_status:status()}.
close_send(Call) ->
    case state(Call) of
        #cstate{send_closed = true} -> ok;
        #cstate{terminal = undefined} -> write(Call, <<>>, true);
        #cstate{terminal = Terminal} -> {error, terminal_status(Terminal)}
    end.

%% @doc Ends the requests of a client-streaming call and returns its
%% response.
-spec close_and_recv(call()) -> {ok, binary()} | {error, trevrpc_status:status()}.
close_and_recv(Call) ->
    _ = close_send(Call),
    case single_response(Call) of
        {ok, Message, _Metadata} -> {ok, Message};
        Error -> Error
    end.

write(Call, Data, Fin) ->
    #cstate{stream = Stream} = State = state(Call),
    case trevrpc_stream:send(Stream, Data, Fin, Call#client_call.deadline) of
        {ok, Next} ->
            put_state(Call, State#cstate{
                stream = Next, send_closed = Fin orelse State#cstate.send_closed
            });
        {error, timeout, Next} ->
            {error, abort(Call, State#cstate{stream = Next}, deadline_status())};
        {error, _Reason, Next} when State#cstate.deciding ->
            %% A server refusing an optimistic call may stop its upload.
            %% The request being sent is among the kept ones, so a replay
            %% sends it.
            case await_decision(Call, State#cstate{stream = Next}, Fin) of
                {replayed, _} -> ok;
                {error, Status} -> {error, Status};
                {ok, Decided} -> ended_upload(Call, Decided)
            end;
        {error, _Reason, Next} ->
            ended_upload(Call, State#cstate{stream = Next})
    end.

%% The server ended the call or the connection is gone: the status, if any,
%% says why.
ended_upload(Call, State) ->
    {error, terminal_status(read_terminal(Call, State#cstate{send_closed = true}))}.

%% @doc Reads the next response message. `{eof, Trailers}' ends a call with
%% an OK status and its trailing metadata; `{error, Status}' ends it with
%% any other.
-spec recv(call()) -> {ok, binary()} | terminal().
recv(Call) ->
    case state(Call) of
        #cstate{pending = [Message | Rest]} = State ->
            put_state(Call, State#cstate{pending = Rest}),
            {ok, Message};
        #cstate{terminal = undefined} = State ->
            case read_event(Call, State) of
                {message, Message, Next} ->
                    put_state(Call, Next),
                    {ok, Message};
                {headers, Next} ->
                    put_state(Call, Next),
                    recv(Call);
                {terminal, Terminal, Next} ->
                    done(Call, Next),
                    Terminal
            end;
        #cstate{terminal = Terminal} = State ->
            done(Call, State),
            Terminal
    end.

%% @doc The response headers, reading as far as needed: up to the HEADERS
%% frame, or the first message or the status, after which they are empty.
-spec headers(call()) -> {ok, trevrpc_metadata:metadata()} | {error, trevrpc_status:status()}.
headers(Call) ->
    case state(Call) of
        #cstate{headers = undefined, terminal = undefined} = State ->
            case read_event(Call, State) of
                {message, Message, Next} ->
                    put_state(Call, Next#cstate{pending = Next#cstate.pending ++ [Message]}),
                    {ok, Next#cstate.headers};
                {headers, Next} ->
                    put_state(Call, Next),
                    {ok, Next#cstate.headers};
                {terminal, Terminal, Next} ->
                    put_state(Call, Next#cstate{terminal = Terminal}),
                    headers(Call)
            end;
        #cstate{headers = undefined, terminal = {error, Status}} ->
            {error, Status};
        #cstate{headers = undefined} ->
            {ok, #{}};
        #cstate{headers = Headers} ->
            {ok, Headers}
    end.

%% @doc Cancels the call: resets the request side and stops reading the
%% response.
-spec cancel(call()) -> ok.
cancel(Call) ->
    case get({trevrpc_client, Call#client_call.ref}) of
        undefined ->
            ok;
        State ->
            _ = abort(Call, State, trevrpc_status:new(cancelled, <<"call cancelled">>)),
            done(Call, state(Call))
    end.

%%% Reading the response

%% A unary or client-streaming response: exactly one message, then OK.
single_response(Call) ->
    case recv(Call) of
        {ok, Message} ->
            {ok, Headers} = headers(Call),
            case recv(Call) of
                {eof, Trailers} ->
                    {ok, Message, #{headers => Headers, trailers => Trailers}};
                {ok, _Extra} ->
                    Status = trevrpc_status:new(
                        internal, <<"response carried more than one message">>
                    ),
                    _ = abort(Call, state(Call), Status),
                    done(Call, state(Call)),
                    {error, Status};
                {error, Status} ->
                    {error, Status}
            end;
        {eof, _Trailers} ->
            {error, trevrpc_status:new(internal, <<"response carried no message">>)};
        {error, Status} ->
            {error, Status}
    end.

%% Reads response frames up to the next message, the headers, or the call's
%% end, checking the grammar: [ANSWER] [HEADERS] MESSAGE* [DATAGRAM_COUNT]
%% STATUS.
read_event(Call, #cstate{stream = Stream} = State) ->
    case trevrpc_stream:next_frame(Stream, Call#client_call.deadline) of
        {ok, Frame, Next} ->
            frame(Call, Frame, State#cstate{stream = Next});
        {eof, Next} ->
            protocol_error(
                Call, State#cstate{stream = Next}, <<"response stream ended before final status">>
            );
        {error, Reason, Next} ->
            read_failed(Call, Reason, State#cstate{stream = Next})
    end.

frame(Call, {Type, _Payload}, #cstate{datagram_count = Count} = State) when
    Count =/= undefined, Type =/= status
->
    protocol_error(
        Call,
        State,
        iolist_to_binary(
            io_lib:format("unexpected ~s after DATAGRAM_COUNT on the response stream", [
                trevrpc_frame:name(Type)
            ])
        )
    );
frame(Call, {answer, Payload}, #cstate{answered = false} = State) ->
    case answer(Call, Payload, State) of
        {ok, Answered} -> read_event(Call, Answered);
        {error, Message} -> protocol_error(Call, State, Message)
    end;
frame(Call, {status, Payload}, #cstate{deciding = true} = State) ->
    case refused(Payload) of
        true ->
            case replay(Call, State, false) of
                {replayed, Replayed} -> read_event(Call, Replayed);
                {error, Status} -> ended(Status, state(Call))
            end;
        false ->
            frame(Call, {status, Payload}, settle(Call, State))
    end;
frame(Call, {Type, _Payload}, #cstate{deciding = true} = State) ->
    optimistic_error(Call, State, Type);
frame(Call, {headers, Payload}, #cstate{headers = undefined} = State) ->
    case trevrpc_wire:decode_headers(Payload) of
        {ok, #{metadata := Metadata}} ->
            case trevrpc_metadata:validate(Metadata) of
                ok -> {headers, answered(State#cstate{headers = Metadata})};
                {error, Message} -> protocol_error(Call, State, Message)
            end;
        error ->
            protocol_error(Call, State, <<"malformed HEADERS">>)
    end;
frame(Call, {datagram_count, Payload}, State) ->
    case trevrpc_frame:decode_datagram_count(Payload) of
        {ok, Count} ->
            Counted = answered(State#cstate{
                headers = headers_or_empty(State), datagram_count = Count
            }),
            read_event(Call, Counted);
        error ->
            protocol_error(Call, State, <<"malformed DATAGRAM_COUNT">>)
    end;
frame(_Call, {message, Message}, State) ->
    {message, Message, answered(State#cstate{headers = headers_or_empty(State)})};
frame(Call, {zstd_message, Payload}, State) ->
    #cstate{compression = Compression, accept = Accept, max_frame_size = Max} = State,
    case trevrpc_zstd:inflate(Compression, Accept, Payload, Max) of
        {ok, Message, _Id} ->
            {message, Message, answered(State#cstate{headers = headers_or_empty(State)})};
        {error, {too_large, Size}} ->
            read_failed(Call, {frame_too_large, Size}, State);
        {error, {invalid, Message}} ->
            protocol_error(Call, State, Message)
    end;
frame(Call, {zstd_stream_message, Payload}, #cstate{stream_decoder = Decoder} = State) when
    Decoder =/= undefined
->
    case trevrpc_zstd_stream:decode(Decoder, Payload, State#cstate.max_frame_size) of
        {ok, Message, Next} ->
            {message, Message,
                answered(State#cstate{headers = headers_or_empty(State), stream_decoder = Next})};
        {error, {too_large, Size}} ->
            read_failed(Call, {frame_too_large, Size}, State);
        {error, {invalid, Message}} ->
            protocol_error(Call, State, Message)
    end;
frame(Call, {status, Payload}, State) ->
    case trevrpc_wire:decode_status(Payload) of
        {ok, #{code := Code, metadata := Trailers} = Status} ->
            case trevrpc_metadata:validate(Trailers) of
                ok ->
                    Ended = answered(State#cstate{headers = headers_or_empty(State)}),
                    cancel_upload(Ended),
                    Terminal =
                        case Code of
                            ok -> {eof, Trailers};
                            _ -> {error, maps:filter(fun present/2, Status)}
                        end,
                    {terminal, Terminal, Ended#cstate{terminal = Terminal}};
                {error, Message} ->
                    protocol_error(Call, State, Message)
            end;
        error ->
            protocol_error(Call, State, <<"malformed STATUS">>)
    end;
frame(Call, {Type, _Payload}, State) ->
    protocol_error(
        Call,
        State,
        iolist_to_binary(
            io_lib:format("unexpected ~s on the response stream", [
                trevrpc_frame:name(Type)
            ])
        )
    ).

%% Takes the call's ANSWER: the dictionaries its query confirmed are the
%% connection's shared set, and the request stream compresses with its
%% dictionary only if they include it. `stream_compression' enables the
%% request stream's context, accepts an optimistic call's requests, and
%% makes the connection's calls optimistic again; an optimistic call
%% answered without it broke the protocol.
answer(Call, Payload, State) ->
    case trevrpc_wire:decode_answer(Payload) of
        {ok, #{stream_compression := false}} when State#cstate.deciding ->
            {error, <<"optimistic call answered without stream compression">>};
        {ok, #{dictionaries := Dictionaries, stream_compression := Stream}} ->
            Learned = learn(Call, Dictionaries, State),
            {ok, enable_stream(Call, Stream, Learned#cstate{answered = true})};
        error ->
            {error, <<"malformed ANSWER">>}
    end.

learn(#client_call{channel = Channel}, Dictionaries, #cstate{learn = Learn} = State) when
    Learn =/= undefined
->
    Shared = trevrpc_zstd:shared(State#cstate.compression, Dictionaries),
    trevrpc_channel:learn(Channel, Learn, Shared),
    case State#cstate.requests of
        {pending, Id} ->
            case lists:member(Id, Shared) of
                true -> State#cstate{requests = Id};
                false -> State#cstate{requests = none}
            end;
        _ ->
            State
    end;
learn(_Call, _Dictionaries, State) ->
    State.

enable_stream(_Call, false, State) ->
    State;
enable_stream(Call, true, #cstate{deciding = true} = State) ->
    settle(Call, State);
enable_stream(#client_call{channel = Channel}, true, #cstate{request_sender = Sender} = State) when
    Sender =/= undefined
->
    trevrpc_channel:optimism(Channel, State#cstate.generation, true),
    State#cstate{request_sender = trevrpc_zstd_stream:enable(Sender)};
enable_stream(_Call, true, State) ->
    State.

%% No ANSWER can come once another known frame arrived, so a request
%% dictionary still waiting for one is not confirmed.
answered(#cstate{requests = {pending, _}} = State) ->
    State#cstate{answered = true, requests = none};
answered(State) ->
    State#cstate{answered = true}.

%% Fields of a decoded status worth keeping: the code, and what is set.
present(code, _) -> true;
present(refused_stream_compression, _) -> false;
present(_, <<>>) -> false;
present(_, false) -> false;
present(_, []) -> false;
present(_, Map) when map_size(Map) =:= 0 -> false;
present(_, _) -> true.

headers_or_empty(#cstate{headers = undefined}) -> #{};
headers_or_empty(#cstate{headers = Headers}) -> Headers.

%% A status that arrives while the client is still uploading ends the
%% upload.
cancel_upload(#cstate{send_closed = true}) ->
    ok;
cancel_upload(#cstate{stream = Stream}) ->
    _ = trevrpc_transport:reset_stream(
        trevrpc_stream:transport(Stream), trevrpc_stream:id(Stream), ?CANCELLED
    ),
    ok.

read_failed(Call, timeout, State) ->
    ended(abort(Call, State, deadline_status()), State);
read_failed(Call, {frame_too_large, Length}, #cstate{max_frame_size = Max} = State) ->
    Message = iolist_to_binary(io_lib:format("frame length ~b exceeds maximum ~b", [Length, Max])),
    ended(abort(Call, State, trevrpc_status:new(resource_exhausted, Message)), State);
read_failed(Call, truncated, State) ->
    protocol_error(Call, State, <<"response stream ended inside a frame">>);
read_failed(_Call, {reset, _Code}, State) ->
    ended(trevrpc_status:new(unavailable, <<"stream reset by the server">>), State);
read_failed(_Call, closed, State) ->
    ended(trevrpc_status:new(unavailable, <<"connection closed">>), State);
read_failed(Call, Other, State) ->
    protocol_error(Call, State, iolist_to_binary(io_lib:format("~p", [Other]))).

%% A response that breaks the protocol is an `internal' error, and the call
%% is abandoned.
protocol_error(Call, State, Message) ->
    ended(abort(Call, State, trevrpc_status:new(internal, Message)), State).

ended(Status, State) ->
    Terminal = {error, Status},
    {terminal, Terminal, State#cstate{terminal = Terminal}}.

%% Reads until the call ends, keeping the messages for recv, and stores how
%% it ended.
read_terminal(Call, State) ->
    case read_event(Call, State) of
        {message, Message, Next} ->
            read_terminal(Call, Next#cstate{pending = Next#cstate.pending ++ [Message]});
        {headers, Next} ->
            read_terminal(Call, Next);
        {terminal, Terminal, Next} ->
            put_state(Call, Next#cstate{terminal = Terminal}),
            Terminal
    end.

terminal_status({eof, Trailers}) -> #{code => ok, metadata => Trailers};
terminal_status({error, Status}) -> Status.

%% Abandons the stream, storing `Status' as the call's end; returns it.
abort(Call, #cstate{stream = Stream} = State, Status) ->
    _ = trevrpc_stream:abort(Stream, ?CANCELLED),
    put_state(Call, State#cstate{terminal = {error, Status}, send_closed = true}),
    Status.

%% The call is finished: the channel forgets its stream, and what the
%% stream held.
done(#client_call{ref = Ref, channel = Channel}, #cstate{stream = Stream}) ->
    trevrpc_channel:stream_done(Channel, trevrpc_stream:id(Stream), trevrpc_stream:ref(Stream)),
    trevrpc_stream:flush_events(trevrpc_stream:ref(Stream)),
    erase({trevrpc_client, Ref}),
    ok.

deadline_status() ->
    trevrpc_status:new(deadline_exceeded, <<"deadline exceeded">>).

state(#client_call{ref = Ref, owner = Owner}) when Owner =:= self() ->
    case get({trevrpc_client, Ref}) of
        undefined -> erlang:error(call_finished);
        State -> State
    end;
state(#client_call{}) ->
    erlang:error(not_owner).

put_state(#client_call{ref = Ref}, State) ->
    put({trevrpc_client, Ref}, State),
    ok.
