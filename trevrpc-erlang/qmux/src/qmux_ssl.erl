%% @doc QMux over TLS: the `qmux_transport' for an `ssl' socket whose
%% handshake completed, which closes with close_notify.
-module(qmux_ssl).

-behaviour(qmux_transport).

-export([messages/0, controlling_process/2, setopts/2, send/2, shutdown/2, close/1, peername/1]).

-spec messages() -> {ssl, ssl_closed, ssl_error}.
messages() -> {ssl, ssl_closed, ssl_error}.

-spec controlling_process(ssl:sslsocket(), pid()) -> ok | {error, term()}.
controlling_process(Socket, Pid) -> ssl:controlling_process(Socket, Pid).

-spec setopts(ssl:sslsocket(), [qmux_transport:option()]) -> ok | {error, term()}.
setopts(Socket, Opts) -> ssl:setopts(Socket, Opts).

-spec send(ssl:sslsocket(), iodata()) -> ok | {error, term()}.
send(Socket, Data) -> ssl:send(Socket, Data).

-spec shutdown(ssl:sslsocket(), write) -> ok | {error, term()}.
shutdown(Socket, How) -> ssl:shutdown(Socket, How).

-spec close(ssl:sslsocket()) -> ok | {error, term()}.
close(Socket) -> ssl:close(Socket).

-spec peername(ssl:sslsocket()) -> {ok, {inet:ip_address(), inet:port_number()}} | {error, term()}.
peername(Socket) -> ssl:peername(Socket).
