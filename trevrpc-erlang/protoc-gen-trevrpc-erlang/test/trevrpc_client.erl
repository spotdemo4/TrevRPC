%% A stand-in for the runtime's trevrpc_client in the generator's tests:
%% each call returns what it was given, so the tests see what the generated
%% client functions pass.
-module(trevrpc_client).

-export([unary/4, server_stream/4, client_stream/3, bidi/3]).

unary(Channel, Method, Request, Options) -> {unary, Channel, Method, Request, Options}.
server_stream(Channel, Method, Request, Options) ->
    {server_stream, Channel, Method, Request, Options}.
client_stream(Channel, Method, Options) -> {client_stream, Channel, Method, Options}.
bidi(Channel, Method, Options) -> {bidi, Channel, Method, Options}.
