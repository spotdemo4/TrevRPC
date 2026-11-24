%% What the Gleam tests need from Erlang: running them, a self-signed
%% certificate for localhost in PEM files, waiting, watching a process by a
%% name it registers, quieting the logger, and HTTP requests.
-module(trevrpc_test_ffi).

-export([run_tests/0]).
-export([credentials/0, remove/1, sleep/1, register_self/1, is_alive/1, monitor/1, wait_down/2]).
-export([quietly/1]).
-export([http_exchange/6, http_unfinished/3, http_upload_in_two/5]).

%% Runs the tests in trevrpc_test, with eunit's timeouts scaled as gleeunit
%% scales them, and halts with their result.
run_tests() ->
    case eunit:test(trevrpc_test, [verbose, {scale_timeouts, 10}]) of
        ok -> erlang:halt(0);
        error -> erlang:halt(1)
    end.

%% The certificate's and key's files, and the directory that holds them.
credentials() ->
    San = {'Extension', {2, 5, 29, 17}, false, [{dNSName, "localhost"}]},
    #{cert := Cert, key := Key} = public_key:pkix_test_root_cert("localhost", [
        {key, {namedCurve, secp256r1}}, {digest, sha256}, {extensions, [San]}
    ]),
    Dir = filename:join(
        os:getenv("TMPDIR", "/tmp"),
        "trevrpc_test_" ++ integer_to_list(erlang:unique_integer([positive]))
    ),
    ok = filelib:ensure_path(Dir),
    CertFile = filename:join(Dir, "cert.pem"),
    KeyFile = filename:join(Dir, "key.pem"),
    ok = file:write_file(CertFile, public_key:pem_encode([{'Certificate', Cert, not_encrypted}])),
    ok = file:write_file(
        KeyFile, public_key:pem_encode([public_key:pem_entry_encode('ECPrivateKey', Key)])
    ),
    {list_to_binary(CertFile), list_to_binary(KeyFile), list_to_binary(Dir)}.

remove(Dir) ->
    ok = file:del_dir_r(Dir),
    nil.

sleep(Milliseconds) ->
    timer:sleep(Milliseconds),
    nil.

register_self(Name) ->
    true = register(binary_to_atom(Name), self()),
    nil.

is_alive(Name) ->
    whereis(binary_to_atom(Name)) =/= undefined.

%% Monitors the process registered as `Name'.
monitor(Name) ->
    erlang:monitor(process, binary_to_atom(Name)).

%% Whether the monitored process exits within `Timeout' milliseconds.
wait_down(Ref, Timeout) ->
    receive
        {'DOWN', Ref, process, _, _} -> true
    after Timeout ->
        erlang:demonitor(Ref, [flush]),
        false
    end.

%% Sends a request over `Version' (`h3', `h2' or `h1') to the server on
%% `Port' of 127.0.0.1, on a connection of its own, and returns the response.
http_exchange(Version, Port, Method, Path, Headers, Body) ->
    Client = trevrpc_test_http:connect(Version, Port),
    {Status, ResponseHeaders, ResponseBody} = trevrpc_test_http:exchange(
        Client, trevrpc_test_http:request(Method, Path, Headers), Body
    ),
    trevrpc_test_http:close(Client),
    {Status, [{iolist_to_binary(N), iolist_to_binary(V)} || {N, V} <- ResponseHeaders],
        ResponseBody}.

%% Opens a request over HTTP/3 or HTTP/2 whose body never comes, and
%% returns the response's status.
http_unfinished(Version, Port, Path) ->
    Client = trevrpc_test_http:connect(Version, Port),
    {ok, Id} = trevrpc_test_http:open(
        Client, trevrpc_test_http:request(<<"POST">>, Path, []), false
    ),
    {Status, _, _} = trevrpc_test_http:response(Client, Id),
    trevrpc_test_http:close(Client),
    Status.

%% Sends a request body over HTTP/3 or HTTP/2 in two parts, the second only
%% once the response head arrived, and returns the response's status and
%% body.
http_upload_in_two(Version, Port, Path, First, Rest) ->
    Client = trevrpc_test_http:connect(Version, Port),
    {ok, Id} = trevrpc_test_http:open(
        Client, trevrpc_test_http:request(<<"POST">>, Path, []), false
    ),
    ok = trevrpc_test_http:send_data(Client, Id, First, false),
    {Status, _, <<>>, false} = trevrpc_test_http:response_head(Client, Id),
    ok = trevrpc_test_http:send_data(Client, Id, Rest, true),
    {Body, true} = trevrpc_test_http:body(Client, Id, <<>>),
    trevrpc_test_http:close(Client),
    {Status, Body}.

%% Runs `Fun' with the logger off, for handlers that fail on purpose.
quietly(Fun) ->
    #{level := Level} = logger:get_primary_config(),
    ok = logger:set_primary_config(level, none),
    try
        Fun()
    after
        logger:set_primary_config(level, Level)
    end.
