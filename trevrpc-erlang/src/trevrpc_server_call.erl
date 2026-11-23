%% @private The process of one server call: reads and checks the stream's
%% REQUEST, runs the handler, and writes the response.
%%
%% `trevrpc_server_conn' starts it as soon as the stream opens and routes
%% the stream's events to it. Once the REQUEST passed its checks, the call
%% asks the connection to admit it ({@link trevrpc_server_conn:admit/4}),
%% which takes its slot in the server's request limit and, for its deadline,
%% stops it if it runs too long. A stream refused before then ends with a
%% lone STATUS the call writes. Reading the REQUEST here, with the stream's
%% flow control and credit, keeps that work and the blocking calls into the
%% transport out of the connection process, which every call goes through.
-module(trevrpc_server_call).

-include("trevrpc_call.hrl").

-export([start/1, status_frame/2]).

-define(CANCELLED, 1).
%% How long a terminal STATUS may wait for the send queue to drain.
-define(STATUS_TIMEOUT, 30000).

%% @doc Starts the process of the call on stream `Id' of connection
%% `Transport', whose events `Router', the caller, routes to it tagged with
%% `Ref', linked to and monitored by the caller. The connection stops its
%% calls when it exits however it can; if it is killed, the link takes them
%% down too, unless a handler traps exits, which then only gets an `'EXIT''
%% message.
-spec start(map()) -> {pid(), reference()}.
start(Spec) ->
    {Pid, Monitor} = spawn_opt(fun() -> run(Spec) end, [link, monitor]),
    {Pid, Monitor}.

run(#{transport := Transport, id := Id, ref := Ref, router := Router, config := Config} = Spec) ->
    #{
        max_frame_size := MaxFrame,
        max_stream_body_size := MaxBody,
        initial_request_timeout := Initial
    } = Config,
    Stream = trevrpc_stream:new(
        Transport, Id, Ref, Router, trevrpc_frame:reader(MaxFrame, #{max_bytes => MaxBody})
    ),
    Sets = connection_sets(Transport, Router, maps:get(connection_metadata, Spec, undefined)),
    case request(Stream, deadline(Initial), Config, Sets) of
        {ok, Checked, Next} ->
            admit(Checked, Next, Router, Config);
        {error, Status, Next} ->
            case trevrpc_server_conn:finish(Router, Id, Ref) of
                ok -> lone_status(Status, Next, MaxFrame);
                cancelled -> ok
            end;
        gone ->
            ok
    end.

deadline(infinity) -> infinity;
deadline(Timeout) -> erlang:monotonic_time(millisecond) + Timeout.

%% Where the call finds the connection metadata sets of its connection:
%% native QUIC connections, QMux sessions and WebTransport sessions keep
%% them, in a table their connection process owns once it stored one, which
%% calls read without going through that process; the HTTP mapping keeps
%% none, since a proxy may share one connection between clients.
connection_sets(Transport, Router, Table) ->
    case trevrpc_transport:kind(Transport) of
        Kind when Kind =:= quic; Kind =:= qmux; Kind =:= wt -> {Router, Table};
        _ -> none
    end.

%% The stream's REQUEST, checked, within the initial request timeout. A
%% stream the peer reset, or whose connection closed, is gone: the
%% connection ends the call itself.
request(Stream, Deadline, Config, Sets) ->
    case trevrpc_stream:next_frame(Stream, Deadline) of
        {ok, {request, Payload}, Next} ->
            case check_request(Payload, Config, Sets) of
                {ok, Checked} -> {ok, Checked, Next};
                {error, Status} -> {error, Status, Next}
            end;
        {ok, {Type, _}, Next} ->
            {error,
                invalid_status(
                    iolist_to_binary(
                        io_lib:format("request stream must start with REQUEST, not ~s", [
                            trevrpc_frame:name(Type)
                        ])
                    )
                ),
                Next};
        {datagram, _Messages, Next} ->
            %% Datagrams are read only once the call took its flow.
            request(Next, Deadline, Config, Sets);
        {eof, Next} ->
            {error, invalid_status(<<"request stream ended before REQUEST">>), Next};
        {error, truncated, Next} ->
            {error, invalid_status(<<"request stream ended inside a frame">>), Next};
        {error, timeout, Next} ->
            {error, trevrpc_status:new(deadline_exceeded, <<"initial request frame timeout">>),
                Next};
        {error, {frame_too_large, Length}, Next} ->
            {error,
                trevrpc_status:new(
                    resource_exhausted,
                    iolist_to_binary(
                        io_lib:format("frame length ~b exceeds maximum ~b", [
                            Length, maps:get(max_frame_size, Config)
                        ])
                    )
                ),
                Next};
        {error, {body_too_large, Max}, Next} ->
            {error,
                trevrpc_status:new(
                    resource_exhausted,
                    iolist_to_binary(
                        io_lib:format("request stream exceeded maximum body size of ~b bytes", [
                            Max
                        ])
                    )
                ),
                Next};
        {error, _Gone, _Next} ->
            gone
    end.

%% Checks a REQUEST in the order the Go and Rust servers do, with their
%% messages.
check_request(Payload, #{routes := Routes}, Sets) ->
    maybe
        {ok, Request} ?= decode_request(Payload),
        #{method := Method, kind := Kind, stream_compression := Compression} = Request,
        ok ?= check_method(Method),
        ok ?= check_kind(Kind),
        ok ?= check_stream_compression(Compression),
        {ok, Metadata, Confirmed} ?= merge_metadata(Request, Sets),
        ok ?= validate_metadata(Metadata),
        {ok, Timeout} ?= timeout(Request),
        {ok, Route} ?= route(Method, Kind, Routes),
        {ok, #{
            request => Request,
            route => Route,
            metadata => Metadata,
            timeout => Timeout,
            connection_metadata => Confirmed
        }}
    end.

decode_request(Payload) ->
    case trevrpc_wire:decode_request(Payload) of
        {ok, Request} -> {ok, Request};
        error -> invalid(<<"invalid RPC request frame: malformed RpcRequest">>)
    end.

check_method(<<$/, Rest/binary>>) ->
    case binary:split(Rest, <<"/">>) of
        [Service, Method] when Service =/= <<>>, Method =/= <<>> ->
            case binary:match(Method, <<"/">>) of
                nomatch -> ok;
                _ -> malformed_method()
            end;
        _ ->
            malformed_method()
    end;
check_method(_) ->
    malformed_method().

malformed_method() ->
    invalid(<<"malformed RPC method path; expected /<service>/<method>">>).

check_kind({unknown, Value}) ->
    invalid(iolist_to_binary(io_lib:format("unsupported TrevRPC RPC kind ~b", [Value])));
check_kind(_) ->
    ok.

check_stream_compression({unknown, Value}) ->
    invalid(iolist_to_binary(io_lib:format("unsupported TrevRPC stream compression ~b", [Value])));
check_stream_compression(_) ->
    ok.

%% Merges the connection metadata set the REQUEST offers or refers to into
%% its metadata, the call's own entries winning, and returns the ID of an
%% offered set the connection stored, which the call's ANSWER confirms, or
%% 0. The connection process stores the set: this process asks it to before
%% it is admitted, through the same process, so the set is stored before
%% the ANSWER can confirm it. An offer on a connection that keeps no sets
%% serves its own call, and every reference there is unknown.
merge_metadata(#{metadata := Own, connection_metadata := undefined}, _Sets) ->
    {ok, Own, 0};
merge_metadata(#{metadata := Own, connection_metadata := #{id := Id, metadata := Set}}, Sets) when
    map_size(Set) > 0
->
    maybe
        ok ?= validate_metadata(Set),
        Confirmed =
            case Sets of
                none ->
                    0;
                {Router, _Table} ->
                    trevrpc_server_conn:store_connection_metadata(Router, Id, Set),
                    Id
            end,
        {ok, trevrpc_metadata:merge(Set, Own), Confirmed}
    end;
merge_metadata(#{metadata := Own, connection_metadata := #{id := Id}}, Sets) ->
    case trevrpc_server_conn:connection_metadata(Sets, Id) of
        {ok, Set} ->
            {ok, trevrpc_metadata:merge(Set, Own), 0};
        error ->
            invalid(iolist_to_binary(io_lib:format("unknown connection metadata set ~b", [Id])))
    end.

validate_metadata(Metadata) ->
    case trevrpc_metadata:validate(Metadata) of
        ok -> ok;
        {error, Message} -> invalid(Message)
    end.

%% Erlang timers take up to 2^32 - 1 ms; a longer timeout is rejected rather
%% than truncated.
timeout(#{timeout_nanos := 0}) ->
    {ok, infinity};
timeout(#{timeout_nanos := Nanos}) ->
    case (Nanos + 999999) div 1000000 of
        Millis when Millis =< 16#FFFFFFFF -> {ok, Millis};
        _ -> invalid(<<"RPC timeout is too large">>)
    end.

route(Method, Kind, Routes) ->
    case Routes of
        #{Method := #{kind := Kind} = Route} ->
            {ok, Route};
        #{Method := #{kind := Registered}} ->
            invalid(
                iolist_to_binary(
                    io_lib:format("streaming RPC kind mismatch for ~ts: expected ~b, got ~b", [
                        Method, trevrpc_wire:kind_to_int(Registered), trevrpc_wire:kind_to_int(Kind)
                    ])
                )
            );
        #{} ->
            {error,
                trevrpc_status:new(
                    unimplemented,
                    iolist_to_binary(io_lib:format("unknown RPC method ~ts", [Method]))
                )}
    end.

invalid(Message) ->
    {error, invalid_status(Message)}.

invalid_status(Message) ->
    trevrpc_status:new(invalid_argument, Message).

%% Asks the connection to admit the call, then serves it; the connection
%% may refuse it, and then it ends with the lone STATUS of that refusal.
admit(Checked, Stream, Router, Config) ->
    #{request := Request, route := Route, timeout := Timeout} = Checked,
    #{kind := Kind, stream_compression := Offer} = Request,
    {Requests, _} = trevrpc_zstd:directions(maps:get(compression, Route, none)),
    Admission = #{
        flow => maps:get(datagram_flow, Request, 0),
        kind => Kind,
        offer => Offer,
        requests => Requests,
        timeout => Timeout
    },
    Id = trevrpc_stream:id(Stream),
    %% From now on the stream's unread bytes count against the
    %% connection's window too, as its handler may read them slowly.
    ok = trevrpc_transport:track_flow_control(
        trevrpc_stream:transport(Stream), Id, trevrpc_stream:consumed(Stream)
    ),
    case trevrpc_server_conn:admit(Router, Id, trevrpc_stream:ref(Stream), Admission) of
        {ok, #{flow := Flow, stream_requests := Decode}} ->
            serve(Checked#{
                stream => Stream,
                router => Router,
                deadline => deadline(Timeout),
                config => Config,
                flow => Flow,
                stream_requests => Decode
            });
        {refuse, Status} ->
            lone_status(Status, Stream, maps:get(max_frame_size, Config));
        cancelled ->
            ok
    end.

%% Ends a call refused before it ran: its STATUS, and, if the peer has not
%% finished the request side, a stop to it.
lone_status(Status, Stream, Max) ->
    Deadline = erlang:monotonic_time(millisecond) + ?STATUS_TIMEOUT,
    _ = trevrpc_stream:send(Stream, status_frame(Status, Max), true, Deadline),
    _ =
        case trevrpc_stream:peer_finished(Stream) of
            true ->
                ok;
            false ->
                trevrpc_transport:stop_sending(
                    trevrpc_stream:transport(Stream), trevrpc_stream:id(Stream), ?CANCELLED
                )
        end,
    trevrpc_stream:release(Stream).

serve(#{stream := Stream0, router := Router, route := Route, config := Config} = Spec) ->
    Id = trevrpc_stream:id(Stream0),
    Ref = trevrpc_stream:ref(Stream0),
    #{kind := Kind, handler := Handler} = Route,
    #{request := #{method := Method} = Request, metadata := Metadata, deadline := Deadline} = Spec,
    #{
        max_frame_size := MaxFrame,
        max_stream_messages := MaxMessages,
        compression := Compression
    } = Config,
    {_, Responses} = trevrpc_zstd:directions(maps:get(compression, Route, none)),
    {Answer, ResponseDict} = trevrpc_zstd:negotiate(
        Compression, Request, maps:get(response_type, Route, undefined), Responses =/= none
    ),
    StreamRequests = maps:get(stream_requests, Spec),
    %% A client that offered stream compression decodes stream-compressed
    %% responses; the route decides whether they are.
    StreamResponses =
        maps:get(stream_compression, Request) =/= none andalso Responses =:= stream andalso
            trevrpc_wire:streams_responses(Kind),
    %% A request that is a single message is bounded by the frame size
    %% alone; a stream of them by the cumulative body limit too.
    Stream1 = trevrpc_stream:charge_messages(trevrpc_wire:streams_requests(Kind), Stream0),
    %% A call with a datagram flow reads the request datagrams of a request
    %% stream, and sends its responses as datagrams if the route lets them.
    {Stream, RequestFlow, Sender} = flow(maps:get(flow, Spec, undefined), Kind, Route, Stream1),
    Call = #call{
        ref = Ref,
        method = Method,
        kind = Kind,
        metadata = Metadata,
        deadline = Deadline,
        owner = self(),
        codec = maps:get(codec, Route, undefined)
    },
    trevrpc_call:put_state(Call, #call_state{
        stream = Stream,
        max_frame_size = MaxFrame,
        max_messages = MaxMessages,
        compression = Compression,
        response_dict = ResponseDict,
        answer = answer_frame(Answer, StreamRequests, maps:get(connection_metadata, Spec)),
        stream_decoder =
            case StreamRequests of
                true -> trevrpc_zstd_stream:decoder(Compression, all);
                false -> undefined
            end,
        response_sender =
            case StreamResponses of
                true -> trevrpc_zstd_stream:sender(true);
                false -> undefined
            end,
        datagram_flow = RequestFlow,
        datagram_sender = Sender,
        reserve = fun() ->
            trevrpc_server_conn:reserve(Router, Id, Ref, trevrpc_zstd_stream:encoder_cost())
        end
    }),
    %% A client streaming requests learns at once which dictionaries it may
    %% compress them with; other calls carry the ANSWER with their first
    %% response frame.
    _ =
        case trevrpc_wire:streams_requests(Kind) of
            true -> trevrpc_call:send_answer(Call);
            false -> ok
        end,
    Result = handle(Kind, Handler, Call, Config),
    finish(Call, Kind, Result, Router, Id).

flow(undefined, _Kind, _Route, Stream) ->
    {Stream, undefined, undefined};
flow({Flow, Credit}, Kind, Route, Stream) ->
    {_, Responses} = trevrpc_datagram:effective(maps:get(unreliable, Route, #{}), Kind),
    Sender =
        case Responses of
            true -> trevrpc_datagram_sender:new(Stream, Flow);
            false -> undefined
        end,
    case trevrpc_wire:streams_requests(Kind) of
        true -> {trevrpc_stream:datagrams(Credit, Stream), Flow, Sender};
        false -> {Stream, undefined, Sender}
    end.

%% The ANSWER the call owes: to a dictionary query, that it decodes
%% stream-compressed requests, and that the connection stored the
%% connection metadata set the REQUEST offered.
answer_frame(undefined, false, 0) ->
    [];
answer_frame(Dictionaries, StreamRequests, Confirmed) ->
    Answer = #{
        dictionaries =>
            case Dictionaries of
                undefined -> [];
                _ -> Dictionaries
            end,
        stream_compression => StreamRequests,
        connection_metadata => Confirmed
    },
    trevrpc_frame:encode(answer, trevrpc_wire:encode_answer(Answer)).

handle(Kind, Handler, Call, Config) when Kind =:= unary; Kind =:= server_streaming ->
    maybe
        {ok, Message} ?= read_request(Call, maps:get(initial_request_timeout, Config)),
        {ok, Request} ?= trevrpc_call:decode(Call, Message, <<"failed to decode request">>),
        invoke(Call, fun() -> encoded(Call, Handler(Request, Call)) end)
    end;
handle(_Kind, Handler, Call, _Config) ->
    invoke(Call, fun() -> encoded(Call, Handler(Call)) end).

%% A single response, encoded with the route's codec. One it cannot encode
%% ends the call with an `internal' status, which the client sees as an
%% internal server error, as for a crashing handler.
encoded(#call{kind = Kind, codec = Codec}, {ok, Response}) when
    Kind =:= unary; Kind =:= client_streaming
->
    case trevrpc_codec:encode(Codec, Response) of
        {ok, Message} -> {ok, Message};
        error -> {error, trevrpc_status:new(internal, <<"failed to encode response">>)}
    end;
encoded(_Call, Result) ->
    Result.

invoke(Call, Fun) ->
    try
        Fun()
    catch
        Class:Reason:Stack ->
            logger:error("trevrpc handler of ~ts failed: ~p", [
                trevrpc_call:method(Call), {Class, Reason, Stack}
            ]),
            {error, trevrpc_status:new(internal, <<"internal server error">>)}
    end.

%% Reads the single request message of a unary or server-streaming call,
%% then the end of the request stream, which must follow it within the
%% initial request timeout.
read_request(Call, InitialTimeout) ->
    State = trevrpc_call:state(Call),
    Deadline = trevrpc_call:deadline(Call),
    case trevrpc_call:read(State, Deadline) of
        {{ok, {message, Request}}, Next} ->
            Completion = earliest(Deadline, InitialTimeout),
            case trevrpc_call:read(Next, Completion) of
                {eof, Ended} ->
                    trevrpc_call:put_state(Call, Ended#call_state{request_end = eof}),
                    {ok, Request};
                {{ok, {message, _}}, Ended} ->
                    request_error(
                        Call, Ended, <<"request stream carried MESSAGE after the request message">>
                    );
                {{error, timeout}, Ended} ->
                    trevrpc_call:put_state(Call, Ended),
                    {error,
                        trevrpc_status:new(
                            deadline_exceeded, <<"initial request completion timeout">>
                        )};
                {{error, Reason}, Ended} ->
                    trevrpc_call:put_state(Call, Ended),
                    {error, trevrpc_call:read_status(Reason, Ended)}
            end;
        {eof, Next} ->
            trevrpc_call:put_state(Call, Next#call_state{request_end = eof}),
            {error,
                trevrpc_status:new(
                    invalid_argument, <<"request stream ended before the request message">>
                )};
        {{error, {unexpected, Type}}, Next} ->
            request_error(
                Call,
                Next,
                iolist_to_binary(
                    io_lib:format("request stream carried ~s where the request message belongs", [
                        trevrpc_frame:name(Type)
                    ])
                )
            );
        {{error, timeout}, Next} ->
            trevrpc_call:put_state(Call, Next),
            {error,
                trevrpc_status:new(deadline_exceeded, <<"initial request completion timeout">>)};
        {{error, Reason}, Next} ->
            trevrpc_call:put_state(Call, Next),
            {error, trevrpc_call:read_status(Reason, Next)}
    end.

request_error(Call, State, Message) ->
    trevrpc_call:put_state(Call, State),
    {error, trevrpc_status:new(invalid_argument, Message)}.

earliest(infinity, infinity) -> infinity;
earliest(Deadline, infinity) -> Deadline;
earliest(Deadline, Timeout) -> min(Deadline, erlang:monotonic_time(millisecond) + Timeout).

%% Writes the call's end: its single response message, if it has one, and
%% its terminal STATUS, once the connection released its slots.
finish(Call, Kind, Result, Router, Id) ->
    State = trevrpc_call:state(Call),
    #call_state{trailers = Trailers, max_frame_size = Max, answer = Answer} = State,
    %% A call that sent response datagrams announces them before its STATUS.
    Count = trevrpc_datagram_sender:finish(
        earliest(Call#call.deadline, ?STATUS_TIMEOUT), State#call_state.datagram_sender
    ),
    Frames =
        case response(Kind, Result, Max) of
            {ok, Message} ->
                [
                    Answer,
                    element(1, trevrpc_call:encode_message(State, Message)),
                    status_frame(#{metadata => Trailers}, Max)
                ];
            ok ->
                [Answer, Count | status_frame(#{metadata => Trailers}, Max)];
            {error, Status} ->
                log_internal(Call, Status),
                [Answer, Count | status_frame(maps:merge(#{metadata => Trailers}, Status), Max)]
        end,
    case trevrpc_server_conn:finish(Router, Id, Call#call.ref) of
        ok ->
            Deadline = erlang:monotonic_time(millisecond) + ?STATUS_TIMEOUT,
            Stream = State#call_state.stream,
            _ = trevrpc_stream:send(Stream, Frames, true, Deadline),
            %% Release a client still uploading, even before it reads the
            %% status.
            _ =
                case State#call_state.request_end of
                    eof ->
                        ok;
                    _ ->
                        trevrpc_transport:stop_sending(
                            trevrpc_stream:transport(Stream), Id, ?CANCELLED
                        )
                end,
            %% Requests routed here that the handler never read give their
            %% credit back. The connection routed them before it took the
            %% finish request, so they are all in the mailbox now, and it
            %% returns the credit of whatever arrives later itself.
            trevrpc_stream:release(Stream);
        cancelled ->
            ok
    end.

%% A handler's result, checked against what the method's kind responds with.
response(Kind, {ok, Message}, Max) when Kind =:= unary; Kind =:= client_streaming ->
    case iolist_size(Message) of
        Size when Size > Max ->
            {error,
                trevrpc_status:new(
                    resource_exhausted,
                    iolist_to_binary(
                        io_lib:format("frame length ~b exceeds maximum ~b", [Size, Max])
                    )
                )};
        _ ->
            {ok, Message}
    end;
response(Kind, ok, _Max) when Kind =:= server_streaming; Kind =:= bidirectional_streaming ->
    ok;
response(_Kind, {error, #{code := Code} = Status}, _Max) when Code =/= ok ->
    {error, Status};
response(_Kind, Other, _Max) ->
    logger:error("trevrpc handler returned ~p", [Other]),
    {error, trevrpc_status:new(internal, <<"internal server error">>)}.

log_internal(Call, #{code := internal} = Status) ->
    case trevrpc_status:message(Status) of
        <<"internal server error">> ->
            ok;
        Message ->
            logger:error("trevrpc handler of ~ts failed: ~ts", [trevrpc_call:method(Call), Message])
    end;
log_internal(_Call, _Status) ->
    ok.

%% @doc The STATUS frame of a call's end. An `internal' status reaches the
%% client as "internal server error", without details, so a handler's own
%% message does not leak. A status too large for a frame goes without its
%% details, then without its trailing metadata and with its message cut to
%% fit, so the call still ends with its code.
-spec status_frame(trevrpc_status:status(), pos_integer()) -> iodata().
status_frame(#{code := internal} = Status, Max) ->
    fit(Status#{message => <<"internal server error">>, details => []}, Max);
status_frame(Status, Max) ->
    fit(Status, Max).

fit(Status, Max) ->
    Payload = iolist_to_binary(trevrpc_wire:encode_status(Status)),
    if
        byte_size(Payload) =< Max ->
            trevrpc_frame:encode(status, Payload);
        is_map_key(details, Status) ->
            fit(maps:remove(details, Status), Max);
        is_map_key(metadata, Status) ->
            fit(maps:remove(metadata, Status), Max);
        true ->
            Message = trevrpc_status:message(Status),
            Cut = truncate(Message, byte_size(Message) - (byte_size(Payload) - Max)),
            trevrpc_frame:encode(status, trevrpc_wire:encode_status(Status#{message => Cut}))
    end.

%% The longest prefix of a UTF-8 string within Size bytes, cut between
%% characters.
truncate(_Message, Size) when Size =< 0 ->
    <<>>;
truncate(Message, Size) ->
    <<Prefix:Size/binary, _/binary>> = Message,
    case unicode:characters_to_binary(Prefix) of
        Prefix -> Prefix;
        _ -> truncate(Message, Size - 1)
    end.
