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
%% enforced locally, and `metadata', the request metadata.
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
    metadata => trevrpc_metadata:metadata()
}.

-record(client_call, {
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
    max_frame_size :: pos_integer(),
    send_closed = false :: boolean(),
    %% A known frame was read, so an ANSWER may no longer come.
    answered = false :: boolean(),
    %% The response headers, once known: a HEADERS frame, or empty once a
    %% message or the status came first.
    headers :: trevrpc_metadata:metadata() | undefined,
    %% Messages read ahead, while looking for headers or a status.
    pending = [] :: [binary()],
    terminal :: terminal() | undefined
}).

-type terminal() :: {eof, trevrpc_metadata:metadata()} | {error, trevrpc_status:status()}.

-define(CANCELLED, 1).

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
        #{conn := Conn, id := Id, ref := Ref, max_frame_size := Max} = Info,
        Stream = trevrpc_stream:new(Conn, Id, Ref, Channel, trevrpc_frame:reader(Max)),
        Call = #client_call{
            ref = Ref, channel = Channel, kind = Kind, deadline = Deadline, owner = self()
        },
        Request = trevrpc_frame:encode(
            request,
            trevrpc_wire:encode_request(#{
                method => Method,
                metadata => Metadata,
                kind => Kind,
                timeout_nanos => timeout_nanos(Deadline)
            })
        ),
        %% A single request goes with its REQUEST, in one write.
        {Frames, Fin} =
            case First of
                none -> {Request, false};
                {message, Message} -> {[Request, trevrpc_frame:encode(message, Message)], true}
            end,
        put_state(Call, #cstate{stream = Stream, max_frame_size = Max, send_closed = Fin}),
        case write(Call, Frames, Fin) of
            ok ->
                {ok, Call};
            {error, Status} ->
                done(Call, state(Call)),
                {error, Status}
        end
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
        #cstate{send_closed = true} -> erlang:error(send_closed);
        #cstate{terminal = undefined} -> write(Call, trevrpc_frame:encode(message, Message), false);
        #cstate{terminal = Terminal} -> {error, terminal_status(Terminal)}
    end;
send(_Call, _Message) ->
    erlang:error(badarg).

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
        {error, _Reason, Next} ->
            %% The server ended the call or the connection is gone: the
            %% status, if any, says why.
            {error,
                terminal_status(
                    read_terminal(Call, State#cstate{stream = Next, send_closed = true})
                )}
    end.

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
%% end, checking the grammar: [ANSWER] [HEADERS] MESSAGE* STATUS.
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

frame(Call, {answer, Payload}, #cstate{answered = false} = State) ->
    %% Nothing this client negotiates is answered yet; the ANSWER is checked
    %% and skipped.
    case trevrpc_wire:decode_answer(Payload) of
        {ok, _Answer} -> read_event(Call, State#cstate{answered = true});
        error -> protocol_error(Call, State, <<"malformed ANSWER">>)
    end;
frame(Call, {headers, Payload}, #cstate{headers = undefined} = State) ->
    case trevrpc_wire:decode_headers(Payload) of
        {ok, #{metadata := Metadata}} ->
            case trevrpc_metadata:validate(Metadata) of
                ok -> {headers, State#cstate{answered = true, headers = Metadata}};
                {error, Message} -> protocol_error(Call, State, Message)
            end;
        error ->
            protocol_error(Call, State, <<"malformed HEADERS">>)
    end;
frame(_Call, {message, Message}, State) ->
    {message, Message, State#cstate{answered = true, headers = headers_or_empty(State)}};
frame(Call, {status, Payload}, State) ->
    case trevrpc_wire:decode_status(Payload) of
        {ok, #{code := Code, metadata := Trailers} = Status} ->
            case trevrpc_metadata:validate(Trailers) of
                ok ->
                    Ended = State#cstate{answered = true, headers = headers_or_empty(State)},
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
                string:uppercase(atom_to_list(Type))
            ])
        )
    ).

%% Fields of a decoded status worth keeping: the code, and what is set.
present(code, _) -> true;
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
    _ = catch quic:reset_stream(trevrpc_stream:conn(Stream), trevrpc_stream:id(Stream), ?CANCELLED),
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

%% The call is finished: the channel forgets its stream.
done(#client_call{ref = Ref, channel = Channel}, #cstate{stream = Stream}) ->
    trevrpc_channel:stream_done(Channel, trevrpc_stream:id(Stream), Ref),
    trevrpc_stream:flush_events(Ref),
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
