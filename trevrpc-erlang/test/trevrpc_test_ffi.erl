%% What the Gleam tests need from Erlang: running them, a self-signed
%% certificate for localhost in PEM files, waiting, and watching a process by
%% a name it registers.
-module(trevrpc_test_ffi).

-export([run_tests/0]).
-export([credentials/0, remove/1, sleep/1, register_self/1, is_alive/1, monitor/1, wait_down/2]).

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
