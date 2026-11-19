%% @private The Gleam binding's calls into the Erlang runtime: Gleam's
%% records, which are tuples tagged with their constructor, and Options
%% (`none' or `{some, X}') to and from the runtime's maps.
-module(trevrpc_gleam_ffi).

-export([connect/2, close/1]).
-export([unary/4, server_stream/4, client_stream/3, bidi/3]).
-export([send/2, close_send/1, recv/1, recv_within/2, close_and_recv/1, headers/1, cancel/1]).
-export([route/3, start/1, port/1, tcp_port/1, stop/1]).
-export([handler_recv/1, handler_send/2, send_headers/2, set_trailers/2]).

%% Gleam's values as this module sees them: a custom type's value is a tuple
%% tagged with its constructor's name in snake case, `Nil' is `nil'.
-type option(T) :: none | {some, T}.
-type status() ::
    {status, trevrpc_status:code(), binary(), trevrpc_metadata:metadata(), [
        {detail, binary(), binary()}
    ]}.
-type result(T) :: {ok, T} | {error, status()}.
-type codec() :: {codec, fun((term()) -> binary()), fun((binary()) -> {ok, term()} | {error, nil})}.
-type compression() :: no_compression | message_compression | stream_compression.
-type method() ::
    {method, binary(), codec(), codec(), binary(), binary(), compression(), compression(),
        boolean(), boolean()}.
-type call_options() :: {options, option(non_neg_integer()), trevrpc_metadata:metadata()}.
-type channel_config() ::
    {config, binary(), inet:port_number(), option(inet:port_number()), auto | quic | tcp,
        option(binary()), option(binary()), boolean(), option(pos_integer()), option(pos_integer()),
        option(pos_integer())}.
-type kind() :: unary | server_streaming | client_streaming | bidirectional_streaming.
-type server_config() ::
    {config, inet:port_number(), binary(), binary(), [{binary(), trevrpc_server:route()}],
        boolean(), option(inet:port_number()), boolean(), option(pos_integer()),
        option(pos_integer()), option(pos_integer()), option(non_neg_integer())}.

%%% Channels

-spec connect(channel_config(), timeout()) -> result(pid()).
connect(
    {config, Host, Port, TcpPort, Transport, ServerName, CaFile, Verify, MaxFrame, ConnectTimeout,
        IdleTimeout},
    Timeout
) ->
    Opts = options(
        #{host => Host, port => Port, transport => Transport, verify => Verify},
        [
            {tcp_port, TcpPort},
            {server_name, map_option(ServerName, fun binary_to_list/1)},
            {cacertfile, CaFile},
            {max_frame_size, MaxFrame},
            {connect_timeout, ConnectTimeout},
            {idle_timeout, IdleTimeout}
        ]
    ),
    result(trevrpc_channel:connect(Opts, Timeout)).

-spec close(pid()) -> nil.
close(Channel) ->
    ok = trevrpc_channel:close(Channel),
    nil.

%%% Client calls

-spec unary(pid(), method(), term(), call_options()) ->
    result({reply, term(), trevrpc_metadata:metadata(), trevrpc_metadata:metadata()}).
unary(Channel, {method, Path, _, _, _, _, _, _, _, _} = Method, Request, Options) ->
    case trevrpc_client:unary(Channel, Path, Request, call_options(Method, Options)) of
        {ok, Reply, #{headers := Headers, trailers := Trailers}} ->
            {ok, {reply, Reply, Headers, Trailers}};
        {error, Status} ->
            {error, status(Status)}
    end.

-spec server_stream(pid(), method(), term(), call_options()) -> result(trevrpc_client:call()).
server_stream(Channel, {method, Path, _, _, _, _, _, _, _, _} = Method, Request, Options) ->
    result(trevrpc_client:server_stream(Channel, Path, Request, call_options(Method, Options))).

-spec client_stream(pid(), method(), call_options()) -> result(trevrpc_client:call()).
client_stream(Channel, {method, Path, _, _, _, _, _, _, _, _} = Method, Options) ->
    result(trevrpc_client:client_stream(Channel, Path, call_options(Method, Options))).

-spec bidi(pid(), method(), call_options()) -> result(trevrpc_client:call()).
bidi(Channel, {method, Path, _, _, _, _, _, _, _, _} = Method, Options) ->
    result(trevrpc_client:bidi(Channel, Path, call_options(Method, Options))).

-spec send(trevrpc_client:call(), term()) -> result(nil).
send(Call, Request) -> usable(fun() -> done(trevrpc_client:send(Call, Request)) end).

-spec close_send(trevrpc_client:call()) -> result(nil).
close_send(Call) -> usable(fun() -> done(trevrpc_client:close_send(Call)) end).

-spec recv(trevrpc_client:call()) ->
    result({message, term()} | {'end', trevrpc_metadata:metadata()}).
recv(Call) ->
    usable(fun() ->
        case trevrpc_client:recv(Call) of
            {ok, Response} -> {ok, {message, Response}};
            {eof, Trailers} -> {ok, {'end', Trailers}};
            {error, Status} -> {error, status(Status)}
        end
    end).

-spec recv_within(trevrpc_client:call(), integer()) ->
    result(option({message, term()} | {'end', trevrpc_metadata:metadata()})).
recv_within(Call, Milliseconds) ->
    usable(fun() ->
        case trevrpc_client:recv(Call, max(0, Milliseconds)) of
            timeout -> {ok, none};
            {ok, Response} -> {ok, {some, {message, Response}}};
            {eof, Trailers} -> {ok, {some, {'end', Trailers}}};
            {error, Status} -> {error, status(Status)}
        end
    end).

-spec close_and_recv(trevrpc_client:call()) -> result(term()).
close_and_recv(Call) -> usable(fun() -> result(trevrpc_client:close_and_recv(Call)) end).

-spec headers(trevrpc_client:call()) -> result(trevrpc_metadata:metadata()).
headers(Call) -> usable(fun() -> result(trevrpc_client:headers(Call)) end).

%% The runtime raises for a call that is finished (after its end, an error or
%% a cancel), whose state it forgot, or that another process started; Gleam's
%% types cannot rule either out, so both are errors.
usable(Fun) ->
    try
        Fun()
    catch
        error:call_finished ->
            {error, status(trevrpc_status:new(failed_precondition, <<"call is finished">>))};
        error:not_owner ->
            {error,
                status(
                    trevrpc_status:new(
                        failed_precondition, <<"call belongs to the process that started it">>
                    )
                )}
    end.

-spec cancel(trevrpc_client:call()) -> nil.
cancel(Call) ->
    ok = trevrpc_client:cancel(Call),
    nil.

%% A client call's options: the method's schema, with a codec that encodes
%% requests and decodes responses, and the caller's.
call_options(Method, {options, Timeout, Metadata}) ->
    {method, _Path, Request, Response, _, _, _, _, _, _} = Method,
    options(
        (schema(Method))#{metadata => Metadata, codec => codec(Request, Response)},
        [{timeout, Timeout}]
    ).

%%% Servers

-spec route(kind(), method(), function()) -> {binary(), trevrpc_server:route()}.
route(Kind, {method, Path, Request, Response, _, _, _, _, _, _} = Method, Handler) ->
    Route = (schema(Method))#{
        kind => kind(Kind),
        handler => handler(kind(Kind), Handler),
        codec => codec(Response, Request)
    },
    {Path, Route}.

kind(unary) -> unary;
kind(server_streaming) -> server_streaming;
kind(client_streaming) -> client_streaming;
kind(bidirectional_streaming) -> bidirectional_streaming.

%% Handlers return Results; the runtime takes `{ok, Response}' or `ok'.
handler(unary, Handler) ->
    fun(Request, Call) -> single(Handler(Request, Call)) end;
handler(server_streaming, Handler) ->
    fun(Request, Call) -> streamed(Handler(Request, Call)) end;
handler(client_streaming, Handler) ->
    fun(Call) -> single(Handler(Call)) end;
handler(bidirectional_streaming, Handler) ->
    fun(Call) -> streamed(Handler(Call)) end.

single({ok, Response}) -> {ok, Response};
single({error, Status}) -> {error, to_status(Status)}.

streamed({ok, nil}) -> ok;
streamed({error, Status}) -> {error, to_status(Status)}.

-spec start(server_config()) -> {ok, pid()} | {error, binary()}.
start(
    {config, Port, CertFile, KeyFile, Routes, Tcp, TcpPort, Http, MaxFrame, MaxStreams, MaxRequests,
        Grace}
) ->
    case [Path || {Path, _} <- Routes] -- lists:usort([Path || {Path, _} <- Routes]) of
        [] ->
            Opts = options(
                #{
                    port => Port,
                    certfile => CertFile,
                    keyfile => KeyFile,
                    routes => maps:from_list(Routes),
                    tcp => Tcp,
                    http => Http
                },
                [
                    {tcp_port, TcpPort},
                    {max_frame_size, MaxFrame},
                    {max_streams_per_connection, MaxStreams},
                    {max_concurrent_requests, MaxRequests},
                    {graceful_shutdown_timeout, Grace}
                ]
            ),
            %% Linked, so that the server stops with the process that started
            %% it; a failed start returns its error without taking it down.
            case trevrpc_server:start_link(Opts) of
                {ok, Server} -> {ok, Server};
                {error, Reason} -> {error, format(Reason)};
                ignore -> {error, <<"ignore">>}
            end;
        [Path | _] ->
            {error, <<"two routes for ", Path/binary>>}
    end.

-spec port(pid()) -> {ok, inet:port_number()} | {error, nil}.
port(Server) ->
    listening(fun() -> trevrpc_server:port(Server) end).

-spec tcp_port(pid()) -> {ok, inet:port_number()} | {error, nil}.
tcp_port(Server) ->
    listening(fun() -> trevrpc_server:tcp_port(Server) end).

listening(Port) ->
    case unless_down(Port, {error, down}) of
        {ok, Number} -> {ok, Number};
        {error, _} -> {error, nil}
    end.

%% Stopping a server that is already down does nothing.
-spec stop(pid()) -> nil.
stop(Server) ->
    _ = unless_down(fun() -> trevrpc_server:stop(Server) end, ok),
    nil.

%% A call to a server that is down, or goes down while the call waits (as
%% during a stop), exits with the server's exit reason; that is `Down'. A
%% timeout leaves the server up, so it still exits.
unless_down(Call, Down) ->
    try
        Call()
    catch
        exit:{Reason, {gen_server, call, _}} when Reason =/= timeout -> Down
    end.

%%% Server calls

-spec handler_recv(trevrpc_call:call()) -> result(option(term())).
handler_recv(Call) ->
    case trevrpc_call:recv(Call) of
        {ok, Request} -> {ok, {some, Request}};
        eof -> {ok, none};
        {error, Status} -> {error, status(Status)}
    end.

-spec handler_send(trevrpc_call:call(), term()) -> result(nil).
handler_send(Call, Response) -> done(trevrpc_call:send(Call, Response)).

-spec send_headers(trevrpc_call:call(), trevrpc_metadata:metadata()) -> result(nil).
send_headers(Call, Headers) -> done(trevrpc_call:send_headers(Call, Headers)).

-spec set_trailers(trevrpc_call:call(), trevrpc_metadata:metadata()) -> nil.
set_trailers(Call, Trailers) ->
    ok = trevrpc_call:set_trailers(Call, Trailers),
    nil.

%%% Conversions

%% A method's schema options, as routes and calls take them.
schema({method, _Path, _, _, RequestType, ResponseType, RequestMode, ResponseMode, UReq, UResp}) ->
    Types = maps:from_list(
        [{request_type, RequestType} || RequestType =/= <<>>] ++
            [{response_type, ResponseType} || ResponseType =/= <<>>]
    ),
    Types#{
        compression => #{requests => mode(RequestMode), responses => mode(ResponseMode)},
        unreliable => #{requests => UReq, responses => UResp}
    }.

mode(no_compression) -> none;
mode(message_compression) -> message;
mode(stream_compression) -> stream.

%% A codec that encodes with `Out''s encoder and decodes with `In''s
%% decoder, raising for a message it cannot decode, as the runtime expects.
codec({codec, Encode, _}, {codec, _, Decode}) ->
    #{
        encode => Encode,
        decode => fun(Bin) ->
            case Decode(Bin) of
                {ok, Message} -> Message;
                {error, nil} -> erlang:error(badarg)
            end
        end
    }.

format(Reason) ->
    unicode:characters_to_binary(io_lib:format("~0p", [Reason])).

%% Options set to `{some, Value}' added to `Map'.
options(Map, Options) ->
    maps:merge(Map, maps:from_list([{Key, Value} || {Key, {some, Value}} <- Options])).

map_option(none, _Fun) -> none;
map_option({some, Value}, Fun) -> {some, Fun(Value)}.

result({ok, Value}) -> {ok, Value};
result({error, Status}) -> {error, status(Status)}.

done(ok) -> {ok, nil};
done({error, Status}) -> {error, status(Status)}.

status(Status) ->
    {status, maps:get(code, Status, ok), maps:get(message, Status, <<>>),
        maps:get(metadata, Status, #{}), [
            {detail, maps:get(type, Detail, <<>>), maps:get(value, Detail, <<>>)}
         || Detail <- maps:get(details, Status, [])
        ]}.

to_status({status, Code, Message, Metadata, Details}) ->
    #{
        code => Code,
        message => Message,
        metadata => Metadata,
        details => [#{type => Type, value => Value} || {detail, Type, Value} <- Details]
    }.
