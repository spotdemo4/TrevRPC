%% @private Socket options shared by the TCP listener and the channel's
%% QMux over TLS connections.
-module(trevrpc_socket).

-export([user_timeout/1, check_user_timeout/1]).

%% The largest TCP_USER_TIMEOUT the socket option holds, in milliseconds.
-define(MAX_USER_TIMEOUT, 16#FFFFFFFF).

%% The socket options that set TCP_USER_TIMEOUT to Ms milliseconds: the
%% kernel drops a connection whose sent data stays unacknowledged that long.
%% QMux restarts its idle timeout on every record sent, keep-alives included
%% (draft-ietf-quic-qmux-02 section 7.1), so without it a peer that vanishes
%% while its connection still takes writes holds the connection until TCP
%% gives up retransmitting, many minutes later. Sockets a listener accepts
%% inherit it. Only Linux has the option; elsewhere, and for `infinity',
%% there are none.
-spec user_timeout(pos_integer() | infinity) -> [gen_tcp:option()].
user_timeout(infinity) ->
    [];
user_timeout(Ms) when is_integer(Ms), Ms > 0, Ms =< ?MAX_USER_TIMEOUT ->
    case os:type() of
        %% IPPROTO_TCP, TCP_USER_TIMEOUT.
        {unix, linux} -> [{raw, 6, 18, <<Ms:32/native>>}];
        _ -> []
    end.

%% Checks the `tcp_user_timeout' option of a server's or a channel's
%% options: `infinity', or a positive number of milliseconds that fits the
%% socket option.
-spec check_user_timeout(map()) -> ok | {error, {invalid_option, tcp_user_timeout}}.
check_user_timeout(#{tcp_user_timeout := infinity}) ->
    ok;
check_user_timeout(#{tcp_user_timeout := Ms}) when
    is_integer(Ms), Ms > 0, Ms =< ?MAX_USER_TIMEOUT
->
    ok;
check_user_timeout(#{tcp_user_timeout := _}) ->
    {error, {invalid_option, tcp_user_timeout}};
check_user_timeout(#{}) ->
    ok.
