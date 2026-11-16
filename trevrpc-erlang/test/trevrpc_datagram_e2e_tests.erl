%% Datagram messages between an Erlang client and server on loopback: over
%% native QUIC, where they travel as datagrams, and over QMux on TLS, where
%% they stay on the stream. Raw streams send what the client never does.
-module(trevrpc_datagram_e2e_tests).

-include_lib("eunit/include/eunit.hrl").

-define(BIDI, <<"/test.Datagrams/Bidi">>).
-define(SINK, <<"/test.Datagrams/Sink">>).
-define(FLOOD, <<"/test.Datagrams/Flood">>).
-define(UNARY, <<"/test.Datagrams/Unary">>).
-define(BOTH, #{requests => true, responses => true}).

%%% Fixture

routes() ->
    #{
        %% Echoes each request, and ends with how many arrived, and how many
        %% of them as datagrams, in its trailers.
        ?BIDI => #{kind => bidirectional_streaming, handler => fun bidi/1, unreliable => ?BOTH},
        %% Answers "<requests>:<datagrams>".
        ?SINK => #{
            kind => client_streaming,
            handler => fun(Call) -> sink(Call, 0) end,
            unreliable => #{requests => true}
        },
        %% "<count>:<size>": sends `count' responses of `size' bytes.
        ?FLOOD => #{
            kind => server_streaming, handler => fun flood/2, unreliable => #{responses => true}
        },
        ?UNARY => #{kind => unary, handler => fun(Request, _Call) -> {ok, Request} end}
    }.

bidi(Call) ->
    bidi(Call, 0).

bidi(Call, Received) ->
    case trevrpc_call:recv(Call) of
        {ok, Message} ->
            ok = trevrpc_call:send(Call, Message),
            bidi(Call, Received + 1);
        eof ->
            trevrpc_call:set_trailers(Call, #{
                <<"x-received">> => integer_to_binary(Received),
                <<"x-datagrams">> => integer_to_binary(trevrpc_call:datagrams_received(Call))
            }),
            ok;
        {error, Status} ->
            {error, Status}
    end.

sink(Call, Received) ->
    case trevrpc_call:recv(Call) of
        {ok, _} ->
            sink(Call, Received + 1);
        eof ->
            Datagrams = trevrpc_call:datagrams_received(Call),
            {ok, iolist_to_binary(io_lib:format("~b:~b", [Received, Datagrams]))};
        {error, Status} ->
            {error, Status}
    end.

flood(Request, Call) ->
    [Count, Size] = [binary_to_integer(Part) || Part <- binary:split(Request, <<":">>)],
    Message = binary:copy(<<"f">>, Size),
    lists:foreach(fun(_) -> ok = trevrpc_call:send(Call, Message) end, lists:seq(1, Count)).

start_server(Credentials, Opts) ->
    {ok, Server} = trevrpc_server:start(
        maps:merge(
            #{
                ip => {127, 0, 0, 1},
                cert => maps:get(cert, Credentials),
                key => maps:get(key, Credentials),
                routes => routes()
            },
            Opts
        )
    ),
    Server.

connect(Credentials, Server) ->
    {ok, Port} = trevrpc_server:port(Server),
    {ok, TcpPort} = trevrpc_server:tcp_port(Server),
    {ok, Channel} = trevrpc_channel:start(#{
        host => {127, 0, 0, 1},
        port => Port,
        tcp_port => TcpPort,
        transport => maps:get(transport, Credentials),
        server_name => "localhost",
        cacerts => maps:get(cacerts, Credentials)
    }),
    ok = trevrpc_channel:wait_until_ready(Channel, 30000),
    Channel.

setup(Transport) ->
    {ok, _} = application:ensure_all_started(quic),
    logger:set_primary_config(level, none),
    Credentials = (trevrpc_test_tls:credentials())#{transport => Transport},
    Server = start_server(Credentials, #{}),
    #{
        transport => Transport,
        credentials => Credentials,
        server => Server,
        channel => connect(Credentials, Server)
    }.

cleanup(#{server := Server, channel := Channel}) ->
    trevrpc_channel:close(Channel),
    trevrpc_server:stop(Server),
    logger:set_primary_config(level, notice).

datagram_test_() ->
    [{atom_to_list(Transport), tests(Transport)} || Transport <- [quic, tcp]].

tests(Transport) ->
    {setup, fun() -> setup(Transport) end, fun cleanup/1, fun(Env) ->
        [
            {Name, {timeout, 30, fun() -> Test(Env) end}}
         || {Name, Test} <- [
                {"a bidirectional call's messages", fun bidi_test/1},
                {"a message too large for a datagram takes the stream", fun large_test/1},
                {"server-streamed responses", fun flood_test/1},
                {"a flow in use fails the call", fun flow_in_use_test/1},
                {"request datagrams before the REQUEST", fun held_test/1},
                {"DATAGRAM_COUNT waits for missing datagrams", fun count_wait_test/1},
                {"a unary call drops request datagrams", fun unary_test/1},
                {"datagrams count towards the message limit", fun message_limit_test/1},
                {"response datagrams end with DATAGRAM_COUNT", fun response_count_test/1}
            ]
        ]
    end}.

%% How many datagrams the client's QUIC connection delivered to it.
delivered(#{transport := quic, channel := Channel}) ->
    #{delivered := Delivered} = trevrpc_channel:datagram_stats(Channel),
    Delivered;
delivered(#{transport := tcp, channel := Channel}) ->
    none = trevrpc_channel:datagram_stats(Channel),
    0.

%%% Tests

bidi_test(#{transport := Transport} = Env) ->
    Before = delivered(Env),
    {ok, Call} = trevrpc_client:bidi(env_channel(Env), ?BIDI, #{unreliable => ?BOTH}),
    Requests = [integer_to_binary(N) || N <- lists:seq(1, 20)],
    _ = [ok = trevrpc_client:send(Call, Request) || Request <- Requests],
    ok = trevrpc_client:close_send(Call),
    {Responses, Trailers} = drain(Call, []),
    Received = binary_to_integer(maps:get(<<"x-received">>, Trailers)),
    Datagrams = binary_to_integer(maps:get(<<"x-datagrams">>, Trailers)),
    case Transport of
        quic ->
            %% Datagrams may be lost, even on loopback, and come in any
            %% order, but never twice; some travelled as datagrams.
            ?assertEqual(lists:usort(Responses), lists:sort(Responses)),
            ?assertEqual([], Responses -- Requests),
            ?assert(Received >= 1 andalso Received =< 20, Received),
            ?assert(Datagrams >= 1, Datagrams),
            ?assert(delivered(Env) > Before);
        tcp ->
            ?assertEqual(Requests, Responses),
            ?assertEqual({20, 0}, {Received, Datagrams})
    end.

large_test(Env) ->
    {ok, Call} = trevrpc_client:bidi(env_channel(Env), ?BIDI, #{unreliable => ?BOTH}),
    Large = binary:copy(<<"L">>, 4096),
    ok = trevrpc_client:send(Call, Large),
    ok = trevrpc_client:close_send(Call),
    ?assertMatch({[Large], #{<<"x-datagrams">> := <<"0">>}}, drain(Call, [])).

flood_test(#{transport := Transport} = Env) ->
    Before = delivered(Env),
    {ok, Call} = trevrpc_client:server_stream(
        env_channel(Env), ?FLOOD, <<"50:100">>, #{unreliable => ?BOTH}
    ),
    {Responses, _Trailers} = drain(Call, []),
    case Transport of
        quic ->
            ?assert(length(Responses) >= 1 andalso length(Responses) =< 50),
            ?assert(delivered(Env) > Before);
        tcp ->
            ?assertEqual(50, length(Responses))
    end.

flow_in_use_test(#{transport := Transport} = Env) ->
    Channel = env_channel(Env),
    Request = request(#{method => ?BIDI, kind => bidirectional_streaming, datagram_flow => 99}),
    First = open(Channel, Request),
    Second = raw(Channel, Request, true),
    case Transport of
        quic ->
            ?assertMatch(
                [
                    {status, #{
                        code := invalid_argument,
                        message := <<"datagram flow 99 is already in use">>
                    }}
                ],
                Second
            );
        tcp ->
            %% QMux has no datagrams: the flow is ignored.
            ?assertMatch([{status, #{code := ok}}], Second)
    end,
    close(Channel, First).

%% Request datagrams sent right before the REQUEST, which they may overtake:
%% held until it arrives, or delivered once it has. Any may be lost, so the
%% handler counts no more than were sent, all of them datagrams. The holding
%% itself is trevrpc_datagram_flows_tests'.
held_test(#{transport := quic} = Env) ->
    Channel = env_channel(Env),
    _ = [ok = send_datagram(Channel, 77, [<<"early">>]) || _ <- lists:seq(1, 5)],
    Request = request(#{method => ?SINK, kind => client_streaming, datagram_flow => 77}),
    [{message, Counts}, {status, #{code := ok}}] =
        raw(Channel, [Request, trevrpc_frame:encode_datagram_count(5)], true),
    {Received, Datagrams} = counts(Counts),
    ?assertEqual(Received, Datagrams),
    ?assert(Datagrams =< 5, Datagrams);
held_test(#{transport := tcp}) ->
    ok.

count_wait_test(#{transport := quic} = Env) ->
    Channel = env_channel(Env),
    Request = request(#{method => ?SINK, kind => client_streaming, datagram_flow => 78}),
    Stream = open(Channel, Request),
    ok = send_datagram(Channel, 78, [<<"a">>]),
    ok = send_datagram(Channel, 78, [<<"b">>]),
    %% A third that was announced but never sent: the server waits for it,
    %% 10 ms.
    Started = erlang:monotonic_time(millisecond),
    [{message, Counts}, {status, #{code := ok}}] =
        finish(Channel, Stream, trevrpc_frame:encode_datagram_count(3)),
    ?assert(erlang:monotonic_time(millisecond) - Started >= 10),
    %% Either of the two sent may have been lost.
    {Received, Datagrams} = counts(Counts),
    ?assertEqual(Received, Datagrams),
    ?assert(Datagrams =< 2, Datagrams);
count_wait_test(#{transport := tcp}) ->
    ok.

unary_test(#{transport := quic} = Env) ->
    Channel = env_channel(Env),
    Request = request(#{method => ?UNARY, kind => unary, datagram_flow => 79}),
    Stream = open(Channel, Request),
    ok = send_datagram(Channel, 79, [<<"dropped">>]),
    ?assertMatch(
        [{message, <<"one">>}, {status, #{code := ok}}],
        finish(Channel, Stream, trevrpc_frame:encode(message, <<"one">>))
    );
unary_test(#{transport := tcp}) ->
    ok.

message_limit_test(#{transport := quic, credentials := Credentials}) ->
    Server = start_server(Credentials, #{max_stream_messages => 5}),
    Channel = connect(Credentials, Server),
    try
        Request = request(#{method => ?SINK, kind => client_streaming, datagram_flow => 1}),
        Stream = open(Channel, Request),
        %% Enough that more than 5 arrive even if some are lost.
        _ = [ok = send_datagram(Channel, 1, [<<"m">>]) || _ <- lists:seq(1, 20)],
        ?assertMatch(
            [{status, #{code := resource_exhausted}}],
            finish(Channel, Stream, trevrpc_frame:encode_datagram_count(20))
        )
    after
        trevrpc_channel:close(Channel),
        trevrpc_server:stop(Server)
    end;
message_limit_test(#{transport := tcp}) ->
    ok.

%% A raw stream's flow is unknown to the channel, which drops the server's
%% datagrams; its stream shows how the responses ended.
response_count_test(#{transport := quic} = Env) ->
    Request = request(#{method => ?FLOOD, kind => server_streaming, datagram_flow => 80}),
    Frames = raw(env_channel(Env), [Request, trevrpc_frame:encode(message, <<"3:10">>)], true),
    ?assertMatch([headers, {datagram_count, 3}, {status, #{code := ok}}], Frames);
response_count_test(#{transport := tcp}) ->
    ok.

%%% Helpers

env_channel(#{channel := Channel}) -> Channel.

%% The Sink's answer, "<requests>:<datagrams>".
counts(Answer) ->
    [Received, Datagrams] = binary:split(Answer, <<":">>),
    {binary_to_integer(Received), binary_to_integer(Datagrams)}.

%% Reads a call's responses up to its end.
drain(Call, Acc) ->
    case trevrpc_client:recv(Call) of
        {ok, Message} -> drain(Call, [Message | Acc]);
        {eof, Trailers} -> {lists:reverse(Acc), Trailers}
    end.

request(Fields) ->
    trevrpc_frame:encode(request, trevrpc_wire:encode_request(Fields)).

send_datagram(Channel, Flow, Messages) ->
    quic:send_datagram(conn(Channel), trevrpc_datagram:encode_messages(Flow, Messages)).

%% The channel's QUIC connection, which a stream's information carries.
conn(Channel) ->
    {ok, #{transport := {quic, Conn}, id := Id, ref := Ref}} = trevrpc_channel:open_stream(
        Channel, infinity
    ),
    _ = quic:reset_stream(Conn, Id, 1),
    trevrpc_channel:stream_done(Channel, Id, Ref),
    Conn.

%% Opens a raw stream and writes `Frames' on it, without finishing it.
open(Channel, Frames) ->
    {ok, #{transport := Transport, id := Id, ref := Ref}} = trevrpc_channel:open_stream(
        Channel, infinity
    ),
    Stream = trevrpc_stream:new(Transport, Id, Ref, Channel, trevrpc_frame:reader(1 bsl 22)),
    {ok, Sent} = trevrpc_stream:send(Stream, Frames, false, infinity),
    Sent.

%% Finishes a raw stream with `Frames', and reads the response, which may
%% have ended the stream already.
finish(Channel, Stream, Frames) ->
    Sent =
        case trevrpc_stream:send(Stream, Frames, true, infinity) of
            {ok, Next} -> Next;
            {error, _Reason, Next} -> Next
        end,
    Result = collect(Sent, erlang:monotonic_time(millisecond) + 5000, []),
    trevrpc_channel:stream_done(Channel, trevrpc_stream:id(Stream), trevrpc_stream:ref(Stream)),
    Result.

raw(Channel, Frames, true) ->
    finish(Channel, open(Channel, Frames), []).

close(Channel, Stream) ->
    _ = trevrpc_stream:abort(Stream, 1),
    trevrpc_channel:stream_done(Channel, trevrpc_stream:id(Stream), trevrpc_stream:ref(Stream)).

collect(Stream, Deadline, Acc) ->
    case trevrpc_stream:next_frame(Stream, Deadline) of
        {ok, {status, Payload}, _} ->
            {ok, Status} = trevrpc_wire:decode_status(Payload),
            lists:reverse(Acc, [{status, maps:with([code, message], Status)}]);
        {ok, {message, Message}, Next} ->
            collect(Next, Deadline, [{message, Message} | Acc]);
        {ok, {headers, _}, Next} ->
            collect(Next, Deadline, [headers | Acc]);
        {ok, {datagram_count, Payload}, Next} ->
            {ok, Count} = trevrpc_frame:decode_datagram_count(Payload),
            collect(Next, Deadline, [{datagram_count, Count} | Acc]);
        {ok, _Other, Next} ->
            collect(Next, Deadline, Acc);
        Other ->
            lists:reverse(Acc, [Other])
    end.
