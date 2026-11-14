%% @doc A server call, as its handler sees it.
%%
%% A handler runs in a process of its own, and only that process can read
%% and write the call. The server ends the process when the call's deadline
%% passes, when the client cancels it, and when the server shuts down
%% (giving handlers that trap exits its graceful shutdown timeout first).
%%
%% ```
%% %% A bidirectional handler echoing every request.
%% fun(Call) -> echo(Call) end.
%%
%% echo(Call) ->
%%     case trevrpc_call:recv(Call) of
%%         {ok, Request} ->
%%             ok = trevrpc_call:send(Call, Request),
%%             echo(Call);
%%         eof ->
%%             ok;
%%         {error, Status} ->
%%             {error, Status}
%%     end.
%% '''
-module(trevrpc_call).

-include("trevrpc_call.hrl").

-export([
    method/1,
    metadata/1,
    deadline/1,
    recv/1,
    send/2,
    send_headers/2,
    set_trailers/2,
    compressed_received/1
]).

%% Used by trevrpc_server_call.
-export([state/1, put_state/2, read/2, read_status/2, encode_message/2, send_answer/1]).

-export_type([call/0]).

%% Opaque to handlers; trevrpc_server_call builds it.
-type call() :: #call{}.

%% @doc The method path, `/package.Service/Method'.
-spec method(call()) -> binary().
method(#call{method = Method}) -> Method.

%% @doc The request metadata, with the connection metadata merged in.
-spec metadata(call()) -> trevrpc_metadata:metadata().
metadata(#call{metadata = Metadata}) -> Metadata.

%% @doc The call's deadline in `erlang:monotonic_time(millisecond)' units,
%% or `infinity'.
-spec deadline(call()) -> trevrpc_stream:deadline().
deadline(#call{deadline = Deadline}) -> Deadline.

%% @doc Reads the next request message of a client-streaming or
%% bidirectional call. `eof' means the client sent every request.
-spec recv(call()) -> {ok, binary()} | eof | {error, trevrpc_status:status()}.
recv(#call{kind = Kind} = Call) when Kind =:= client_streaming; Kind =:= bidirectional_streaming ->
    State = state(Call),
    case State#call_state.request_end of
        undefined ->
            {Result, Next} = read(State, Call#call.deadline),
            put_state(Call, ended(Result, Next)),
            case Result of
                {ok, {message, Message}} -> {ok, Message};
                eof -> eof;
                {error, Reason} -> {error, read_status(Reason, State)}
            end;
        End ->
            End
    end;
recv(_Call) ->
    erlang:error(badarg).

ended({ok, _}, State) ->
    State;
ended(eof, State) ->
    State#call_state{request_end = eof};
ended({error, Reason}, State) ->
    State#call_state{request_end = {error, read_status(Reason, State)}}.

%% @doc Sends a response message of a server-streaming or bidirectional
%% call, after the headers if they are waiting.
-spec send(call(), iodata()) -> ok | {error, trevrpc_status:status()}.
send(#call{kind = Kind} = Call, Message) when
    Kind =:= server_streaming; Kind =:= bidirectional_streaming
->
    #call_state{max_frame_size = Max} = State = state(Call),
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
            write(Call, State#call_state{messages_sent = true}, encode_message(State, Message))
    end;
send(_Call, _Message) ->
    erlang:error(badarg).

%% @doc Sends the response headers at once, before any response message.
%% They can be sent once, and only before the first response message.
-spec send_headers(call(), trevrpc_metadata:metadata()) -> ok | {error, trevrpc_status:status()}.
send_headers(Call, Metadata) ->
    State = state(Call),
    if
        State#call_state.headers_sent; State#call_state.messages_sent ->
            {error, trevrpc_status:new(internal, <<"response headers already sent">>)};
        true ->
            case trevrpc_metadata:validate(Metadata) of
                ok ->
                    Frame = trevrpc_frame:encode(
                        headers, trevrpc_wire:encode_headers(#{metadata => Metadata})
                    ),
                    write(Call, State#call_state{headers_sent = true}, Frame);
                {error, Message} ->
                    {error, trevrpc_status:new(internal, Message)}
            end
    end.

%% @doc Sets the trailing metadata the call's terminal status carries.
-spec set_trailers(call(), trevrpc_metadata:metadata()) -> ok.
set_trailers(Call, Metadata) ->
    put_state(Call, (state(Call))#call_state{trailers = Metadata}).

%% @doc How many of the request messages read so far arrived compressed
%% with a dictionary (`dictionary') and without one (`plain').
-spec compressed_received(call()) ->
    #{dictionary := non_neg_integer(), plain := non_neg_integer()}.
compressed_received(Call) ->
    #call_state{dictionary_received = Dictionary, plain_received = Plain} = state(Call),
    #{dictionary => Dictionary, plain => Plain}.

%% Any ANSWER still waiting goes first.
write(Call, #call_state{stream = Stream, answer = Answer} = State, Frame) ->
    case trevrpc_stream:send(Stream, [Answer | Frame], false, Call#call.deadline) of
        {ok, Next} ->
            put_state(Call, State#call_state{stream = Next, answer = []});
        {error, Reason, Next} ->
            put_state(Call, State#call_state{stream = Next, answer = []}),
            {error, write_status(Reason)}
    end.

write_status(timeout) -> trevrpc_status:new(deadline_exceeded, <<"deadline exceeded">>);
write_status(closed) -> trevrpc_status:new(unavailable, <<"connection closed">>);
write_status(_Reason) -> trevrpc_status:new(cancelled, <<"client stopped reading">>).

%%% Shared with trevrpc_server_call

%% @private The frame of a response message: a ZSTD_MESSAGE when the call
%% negotiated a dictionary for its responses and it makes the message
%% shorter, a MESSAGE otherwise.
-spec encode_message(#call_state{}, iodata()) -> iodata().
encode_message(#call_state{response_dict = none}, Message) ->
    trevrpc_frame:encode(message, Message);
encode_message(#call_state{response_dict = Id, compression = Compression}, Message) ->
    case trevrpc_zstd:compress(Compression, Id, Message) of
        {ok, Payload} -> trevrpc_frame:encode(zstd_message, Payload);
        none -> trevrpc_frame:encode(message, Message)
    end.

%% @private Sends the call's ANSWER now, if one is waiting.
-spec send_answer(call()) -> ok | {error, trevrpc_status:status()}.
send_answer(Call) ->
    case state(Call) of
        #call_state{answer = []} -> ok;
        State -> write(Call, State, [])
    end.

%% @private
-spec state(call()) -> #call_state{}.
state(#call{ref = Ref, owner = Owner}) when Owner =:= self() ->
    get({trevrpc_call, Ref});
state(#call{}) ->
    erlang:error(not_owner).

%% @private
-spec put_state(call(), #call_state{}) -> ok.
put_state(#call{ref = Ref}, State) ->
    put({trevrpc_call, Ref}, State),
    ok.

%% @private Reads the next request frame, counting messages against the
%% limit and decompressing ZSTD_MESSAGE frames. Frames other than MESSAGE
%% and ZSTD_MESSAGE are errors on a request stream past its REQUEST.
-spec read(#call_state{}, trevrpc_stream:deadline()) ->
    {{ok, {message, binary()}} | eof | {error, term()}, #call_state{}}.
read(#call_state{stream = Stream, received = Received, max_messages = Max} = State, Deadline) ->
    case trevrpc_stream:next_frame(Stream, Deadline) of
        {ok, {Type, _}, Next} when Type =:= message orelse Type =:= zstd_message, Received >= Max ->
            {{error, {too_many_messages, Max}}, State#call_state{stream = Next}};
        {ok, {message, _} = Frame, Next} ->
            {{ok, Frame}, State#call_state{stream = Next, received = Received + 1}};
        {ok, {zstd_message, Payload}, Next} ->
            inflate(Payload, State#call_state{stream = Next});
        {ok, {Type, _}, Next} ->
            {{error, {unexpected, Type}}, State#call_state{stream = Next}};
        {eof, Next} ->
            {eof, State#call_state{stream = Next}};
        {error, Reason, Next} ->
            {{error, Reason}, State#call_state{stream = Next}}
    end.

%% A server accepts every dictionary it holds, and compression without one.
%% The decompressed size counts towards the cumulative limit, as a
%% MESSAGE's payload would.
inflate(Payload, #call_state{compression = Compression, max_frame_size = Max} = State) ->
    case trevrpc_zstd:inflate(Compression, all, Payload, Max) of
        {ok, Message, Id} ->
            case trevrpc_stream:charge_message(byte_size(Message), State#call_state.stream) of
                {ok, Charged} ->
                    Read = State#call_state{
                        stream = Charged, received = State#call_state.received + 1
                    },
                    {{ok, {message, Message}}, count_compressed(Id, Read)};
                {error, Reason, Failed} ->
                    {{error, Reason}, State#call_state{stream = Failed}}
            end;
        {error, {too_large, Size}} ->
            {{error, {frame_too_large, Size}}, State};
        {error, {invalid, Message}} ->
            {{error, {invalid, Message}}, State}
    end.

count_compressed(0, #call_state{plain_received = Plain} = State) ->
    State#call_state{plain_received = Plain + 1};
count_compressed(_Id, #call_state{dictionary_received = Dictionary} = State) ->
    State#call_state{dictionary_received = Dictionary + 1}.

%% @private The status a failed read of the request stream ends the call
%% with.
-spec read_status(term(), #call_state{}) -> trevrpc_status:status().
read_status({frame_too_large, Length}, #call_state{max_frame_size = Max}) ->
    exhausted(io_lib:format("frame length ~b exceeds maximum ~b", [Length, Max]));
read_status({body_too_large, Max}, _State) ->
    exhausted(io_lib:format("request stream exceeded maximum body size of ~b bytes", [Max]));
read_status({too_many_messages, Max}, _State) ->
    exhausted(io_lib:format("request stream exceeded maximum of ~b messages", [Max]));
read_status({invalid, Message}, _State) ->
    trevrpc_status:new(invalid_argument, Message);
read_status({unexpected, zstd_stream_message}, _State) ->
    trevrpc_status:new(
        invalid_argument,
        <<"request stream carried ZSTD_STREAM_MESSAGE without stream compression">>
    );
read_status({unexpected, Type}, _State) ->
    trevrpc_status:new(
        invalid_argument,
        iolist_to_binary(
            io_lib:format("request stream carried ~s after REQUEST", [frame_name(Type)])
        )
    );
read_status(truncated, _State) ->
    trevrpc_status:new(invalid_argument, <<"request stream ended inside a frame">>);
read_status({reset, _Code}, _State) ->
    trevrpc_status:new(cancelled, <<"request stream reset by the client">>);
read_status(closed, _State) ->
    trevrpc_status:new(unavailable, <<"connection closed">>);
read_status(timeout, _State) ->
    trevrpc_status:new(deadline_exceeded, <<"deadline exceeded">>).

exhausted(Message) ->
    trevrpc_status:new(resource_exhausted, iolist_to_binary(Message)).

frame_name(request) -> "REQUEST";
frame_name(headers) -> "HEADERS";
frame_name(status) -> "STATUS";
frame_name(answer) -> "ANSWER";
frame_name(message) -> "MESSAGE";
frame_name(zstd_message) -> "ZSTD_MESSAGE";
frame_name(zstd_stream_message) -> "ZSTD_STREAM_MESSAGE".
