%% @doc The `qmux_transport' for a `gen_tcp' socket, for QMux over a byte
%% stream that is secured some other way, or not at all, such as a
%% loopback connection.
-module(qmux_tcp).

-behaviour(qmux_transport).

-export([messages/0, controlling_process/2, setopts/2, send/2, shutdown/2, close/1, peername/1]).

-spec messages() -> {tcp, tcp_closed, tcp_error}.
messages() -> {tcp, tcp_closed, tcp_error}.

-spec controlling_process(gen_tcp:socket(), pid()) -> ok | {error, term()}.
controlling_process(Socket, Pid) -> gen_tcp:controlling_process(Socket, Pid).

-spec setopts(gen_tcp:socket(), [qmux_transport:option()]) -> ok | {error, term()}.
setopts(Socket, Opts) -> inet:setopts(Socket, Opts).

-spec send(gen_tcp:socket(), iodata()) -> ok | {error, term()}.
send(Socket, Data) -> gen_tcp:send(Socket, Data).

-spec shutdown(gen_tcp:socket(), write) -> ok | {error, term()}.
shutdown(Socket, How) -> gen_tcp:shutdown(Socket, How).

-spec close(gen_tcp:socket()) -> ok.
close(Socket) -> gen_tcp:close(Socket).

-spec peername(gen_tcp:socket()) -> {ok, {inet:ip_address(), inet:port_number()}} | {error, term()}.
peername(Socket) -> inet:peername(Socket).
