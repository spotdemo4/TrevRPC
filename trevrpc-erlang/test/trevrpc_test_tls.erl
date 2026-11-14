%% Test certificates: a server certificate for localhost and the CA that
%% trusts it.
-module(trevrpc_test_tls).

-export([credentials/0, self_signed/0, self_signed/1]).

%% Server credentials in the form trevrpc_server takes them, with the
%% client's `cacerts'. Each call makes a new CA.
credentials() ->
    %% #'Extension'{extnID = ?'id-ce-subjectAltName', critical = false, extnValue = ...}
    San = {'Extension', {2, 5, 29, 17}, false, [{dNSName, "localhost"}]},
    %% TLS 1.3 takes neither DSA keys nor SHA-1 signatures, pkix_test_data's
    %% defaults.
    Key = [{key, {namedCurve, secp256r1}}, {digest, sha256}],
    #{server_config := Server, client_config := Client} = public_key:pkix_test_data(#{
        server_chain => #{root => Key, intermediates => [], peer => [{extensions, [San]} | Key]},
        client_chain => #{root => Key, intermediates => [], peer => Key}
    }),
    {KeyType, KeyDer} = proplists:get_value(key, Server),
    #{
        cert => proplists:get_value(cert, Server),
        key => public_key:der_decode(KeyType, KeyDer),
        cacerts => proplists:get_value(cacerts, Client)
    }.

%% A self-signed certificate for localhost that clients trust by itself, as
%% the interop runner's is.
self_signed() ->
    self_signed([]).

%% The same with more pkix_test_root_cert options, such as `validity'.
self_signed(Opts) ->
    San = {'Extension', {2, 5, 29, 17}, false, [{dNSName, "localhost"}]},
    #{cert := Cert, key := Key} = public_key:pkix_test_root_cert("localhost", [
        {key, {namedCurve, secp256r1}}, {digest, sha256}, {extensions, [San]} | Opts
    ]),
    #{cert => Cert, key => Key, cacerts => [Cert]}.
