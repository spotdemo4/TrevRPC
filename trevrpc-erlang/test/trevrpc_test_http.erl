%% HTTP clients for the tests of the HTTP fallback, in Erlang and in Gleam:
%% quic_h3 for HTTP/3, erlang_h2 for HTTP/2 and erlang_h1 for HTTP/1.1, all
%% sending their events to the process that connected. A client is
%% `{Tag, Conn}', with `Tag' the tag of its events.
-module(trevrpc_test_http).

-export([h3_opts/0, connect/2, connect/3, close/1, open/3, cancel/2, send_data/4]).
-export([exchange/3, response/2, response_head/2, body/3, request/3, header/2]).

-define(CHUNK, 65536).

h3_opts() ->
    #{verify => verify_none, sync => true}.

%% Connects to 127.0.0.1 over `h3', on the UDP port, or `h2' or `h1', on
%% the TCP port. It starts the client's application, which the Erlang tests
%% get from starting trevrpc, and the Gleam tests, whose runner starts no
%% applications, from nothing else.
connect(Version, Port) ->
    {ok, _} = application:ensure_all_started(application(Version)),
    connect_started(Version, Port, []).

%% Connects like connect/2, sending `ServerName' as the TLS server name.
connect(Version, Port, ServerName) ->
    {ok, _} = application:ensure_all_started(application(Version)),
    connect_started(Version, Port, [{server_name_indication, ServerName}]).

application(h3) -> quic;
application(h2) -> h2;
application(h1) -> h1.

%% `Sni' holds the server name to send, or nothing for the clients' own:
%% none from quic_h3 and erlang_h1 to an IP address, the address itself from
%% erlang_h2.
connect_started(h3, Port, Sni) ->
    Quic = maps:from_list([{server_name, Name} || {server_name_indication, Name} <- Sni]),
    {ok, Conn} = quic_h3:connect("127.0.0.1", Port, (h3_opts())#{quic_opts => Quic}),
    {quic_h3, Conn};
connect_started(h2, Port, Sni) ->
    {ok, Conn} = h2:connect("127.0.0.1", Port, #{verify => verify_none, ssl_opts => Sni}),
    %% A connection the server closes exits abnormally, and takes its linked
    %% caller with it.
    true = unlink(Conn),
    ok = h2:wait_connected(Conn),
    {h2, Conn};
connect_started(h1, Port, Sni) ->
    {ok, Conn} = h1:connect("127.0.0.1", Port, #{
        transport => ssl, ssl_opts => [{verify, verify_none} | Sni]
    }),
    true = unlink(Conn),
    ok = h1:wait_connected(Conn),
    {h1, Conn}.

close({quic_h3, Conn}) -> quic_h3:close(Conn);
close({h2, Conn}) -> h2:close(Conn);
close({h1, Conn}) -> h1:close(Conn).

%% Opens a request; `Ended' says the request has no body.
open({quic_h3, Conn}, Headers, Ended) -> quic_h3:request(Conn, Headers, #{end_stream => Ended});
open({h2, Conn}, Headers, Ended) -> h2:request(Conn, Headers, #{end_stream => Ended});
open({h1, Conn}, Headers, true) -> h1:request(Conn, Headers, #{}).

cancel({quic_h3, Conn}, Id) -> quic_h3:cancel(Conn, Id);
cancel({h2, Conn}, Id) -> h2:cancel(Conn, Id).

%% Sends a request with `Body' and reads the whole response.
exchange({h1, Conn} = Client, Headers, Body) ->
    Opts =
        case Body of
            <<>> -> #{};
            _ -> #{body => Body}
        end,
    {ok, Id} = h1:request(Conn, Headers, Opts),
    response(Client, Id);
exchange(Client, Headers, <<>>) ->
    {ok, Id} = open(Client, Headers, true),
    response(Client, Id);
exchange(Client, Headers, Body) ->
    {ok, Id} = open(Client, Headers, false),
    ok = send_body(Client, Id, Body),
    response(Client, Id).

%% The body in chunks, so that HTTP/2 flow control holds the client back
%% while the server reads.
send_body(Client, Id, Body) when byte_size(Body) > ?CHUNK ->
    <<Chunk:?CHUNK/binary, Rest/binary>> = Body,
    ok = send_data(Client, Id, Chunk, false),
    send_body(Client, Id, Rest);
send_body(Client, Id, Body) ->
    send_data(Client, Id, Body, true).

%% Sends part of a request body over HTTP/3 or HTTP/2, the last with `Fin'.
send_data({quic_h3, Conn}, Id, Data, Fin) -> quic_h3:send_data(Conn, Id, Data, Fin);
send_data({h2, Conn}, Id, Data, Fin) -> h2:send_data(Conn, Id, Data, Fin).

%% The response to request `Id': `{Status, Headers, Body}'.
response(Client, Id) ->
    case response_head(Client, Id) of
        {Status, Headers, Data, true} ->
            {Status, Headers, Data};
        {Status, Headers, Data, false} ->
            {Rest, true} = body(Client, Id, Data),
            {Status, Headers, Rest}
    end.

response_head({Tag, Conn}, Id) ->
    receive
        {Tag, Conn, {response, Id, Status, Headers}} ->
            {Status, Headers, <<>>, false};
        {Tag, Conn, {stream_reset, Id, Code}} ->
            error({reset, Code})
    after 10000 -> error(no_response)
    end.

%% The rest of the response body after `Acc', and whether it ended.
body({Tag, Conn} = Client, Id, Acc) ->
    receive
        {Tag, Conn, {data, Id, Data, true}} -> {<<Acc/binary, Data/binary>>, true};
        {Tag, Conn, {data, Id, Data, false}} -> body(Client, Id, <<Acc/binary, Data/binary>>);
        {Tag, Conn, {trailers, Id, _}} -> {Acc, true};
        {Tag, Conn, {stream_reset, Id, Code}} -> error({reset, Code})
    after 10000 -> error({no_body, byte_size(Acc)})
    end.

%% A request's headers, for `localhost'.
request(Method, Path, Extra) ->
    [
        {<<":method">>, Method},
        {<<":scheme">>, <<"https">>},
        {<<":authority">>, <<"localhost">>},
        {<<":path">>, Path}
        | Extra
    ].

%% The first value of header `Name', or `<<>>'.
header(Name, Headers) ->
    case lists:keyfind(Name, 1, Headers) of
        {_, Value} -> iolist_to_binary(Value);
        false -> <<>>
    end.
