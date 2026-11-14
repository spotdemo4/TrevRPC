%% @private The process of one server call: reads the request, runs the
%% handler, and writes the response.
%%
%% `trevrpc_server_conn' starts it once the stream's REQUEST passed its
%% checks, routes the stream's events to it, and stops it if the call ends
%% for any other reason first.
-module(trevrpc_server_call).

-include("trevrpc_call.hrl").

-export([start/1, status_frame/2]).

-define(CANCELLED, 1).
%% How long a terminal STATUS may wait for the send queue to drain.
-define(STATUS_TIMEOUT, 30000).

%% @doc Starts the call process described by `Spec', linked to and
%% monitored by the caller. The connection stops its calls when it exits
%% however it can; if it is killed, the link takes them down too, unless a
%% handler traps exits, which then only gets an `'EXIT'' message.
-spec start(map()) -> {pid(), reference()}.
start(Spec) ->
    {Pid, Monitor} = spawn_opt(fun() -> run(Spec) end, [link, monitor]),
    {Pid, Monitor}.

run(
    #{
        transport := Transport,
        id := Id,
        ref := Ref,
        router := Router,
        route := Route,
        config := Config
    } = Spec
) ->
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
    Stream0 = trevrpc_stream:resume(
        maps:get(frames, Spec),
        maps:get(fin, Spec),
        trevrpc_stream:new(Transport, Id, Ref, Router, maps:get(reader, Spec))
    ),
    %% A request that is a single message is bounded by the frame size
    %% alone; a stream of them by the cumulative body limit too.
    Stream = trevrpc_stream:charge_messages(trevrpc_wire:streams_requests(Kind), Stream0),
    Call = #call{
        ref = Ref,
        method = Method,
        kind = Kind,
        metadata = Metadata,
        deadline = Deadline,
        owner = self()
    },
    trevrpc_call:put_state(Call, #call_state{
        stream = Stream,
        max_frame_size = MaxFrame,
        max_messages = MaxMessages,
        compression = Compression,
        response_dict = ResponseDict,
        answer = answer_frame(Answer, StreamRequests),
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

%% The ANSWER the call owes: to a dictionary query, and that it decodes
%% stream-compressed requests.
answer_frame(undefined, false) ->
    [];
answer_frame(Dictionaries, StreamRequests) ->
    Answer = #{
        dictionaries =>
            case Dictionaries of
                undefined -> [];
                _ -> Dictionaries
            end,
        stream_compression => StreamRequests
    },
    trevrpc_frame:encode(answer, trevrpc_wire:encode_answer(Answer)).

handle(Kind, Handler, Call, Config) when Kind =:= unary; Kind =:= server_streaming ->
    case read_request(Call, maps:get(initial_request_timeout, Config)) of
        {ok, Request} -> invoke(Call, fun() -> Handler(Request, Call) end);
        {error, Status} -> {error, Status}
    end;
handle(_Kind, Handler, Call, _Config) ->
    invoke(Call, fun() -> Handler(Call) end).

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
                        string:uppercase(atom_to_list(Type))
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
    Frames =
        case response(Kind, Result, Max) of
            {ok, Message} ->
                [
                    Answer,
                    element(1, trevrpc_call:encode_message(State, Message)),
                    status_frame(#{metadata => Trailers}, Max)
                ];
            ok ->
                [Answer | status_frame(#{metadata => Trailers}, Max)];
            {error, Status} ->
                log_internal(Call, Status),
                [Answer | status_frame(maps:merge(#{metadata => Trailers}, Status), Max)]
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
