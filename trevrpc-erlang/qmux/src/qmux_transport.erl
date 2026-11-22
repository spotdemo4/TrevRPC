%% @doc The socket a QMux session reads and writes: a reliable, ordered byte
%% stream, such as a TLS connection (`qmux_ssl') or a TCP connection
%% (`qmux_tcp'). A session takes `{Module, Socket}', where `Module'
%% implements this behaviour for `Socket'.
%%
%% The session takes the socket over with `controlling_process/2', sets it
%% to binary mode and to a send timeout, then reads it in active mode: it
%% expects the messages `messages/0' names, `{Data, Socket, Bin}',
%% `{Closed, Socket}' and `{Error, Socket, Reason}', as `ssl' and
%% `gen_tcp' send them. `send/2' and `shutdown/2' are called by a writer
%% process of the session's own, the rest by the session.
%%
%% How records travel over the socket is up to the session's framing (see
%% `qmux_framing').
-module(qmux_transport).

-export_type([t/0, socket/0, option/0]).

%% A transport module and its socket.
-type t() :: {module(), socket()}.

-type socket() :: term().

%% The options the session sets: binary mode, active mode, and a send
%% timeout that closes the socket when it expires, so a write to a peer that
%% stopped reading cannot block for good.
-type option() ::
    {mode, binary}
    | {active, once | true}
    | {send_timeout, pos_integer()}
    | {send_timeout_close, true}.

%% The tags of the active-mode messages the socket sends its controlling
%% process: data, closed and error.
-callback messages() -> {Data :: atom(), Closed :: atom(), Error :: atom()}.

-callback controlling_process(socket(), pid()) -> ok | {error, term()}.

-callback setopts(socket(), [option()]) -> ok | {error, term()}.

-callback send(socket(), iodata()) -> ok | {error, term()}.

%% Ends the sending direction, once the last record went out.
-callback shutdown(socket(), write) -> ok | {error, term()}.

-callback close(socket()) -> ok | {error, term()}.

-callback peername(socket()) ->
    {ok, {inet:ip_address(), inet:port_number()}} | {error, term()}.
