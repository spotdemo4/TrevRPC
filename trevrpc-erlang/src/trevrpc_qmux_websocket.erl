%% @private QMux over WebSocket (draft-lcurley-qmux-websocket-00): the
%% `qmux_framing' of a session over a TLS connection that upgraded to a
%% WebSocket, one record per binary message, where message boundaries
%% replace the Size field and text messages are forbidden.
%%
%% Pings are answered here, and the closing handshake replaces QMux's
%% draining: a session that ends sends a Close frame after its last record.
-module(trevrpc_qmux_websocket).

-behaviour(qmux_framing).

-export([init/2, decode/2, encode/2, close/2]).

-export_type([state/0]).

-record(websocket, {
    role :: client | server,
    parser :: ws_frame:parser(),
    %% The code of the Close frame that ends the session, or `sent' once
    %% one went out.
    close = 1000 :: 1000..4999 | sent
}).

-opaque state() :: #websocket{}.

%% WebSocket close codes (RFC 6455 section 7.4.1).
-define(WS_NORMAL, 1000).
-define(WS_PROTOCOL_ERROR, 1002).
-define(WS_UNSUPPORTED_DATA, 1003).
-define(WS_TOO_BIG, 1009).

-spec init(client | server, pos_integer()) -> state().
init(Role, MaxRecordSize) ->
    #websocket{
        role = Role,
        parser = ws_frame:init_parser(#{
            role => Role, max_frame => MaxRecordSize, max_message => MaxRecordSize
        })
    }.

%% @doc Takes the records out of bytes that arrived, with the Pongs and the
%% Close that answer the peer's Pings and Close.
-spec decode(binary(), state()) -> qmux_framing:decoded(state()).
decode(Bin, #websocket{parser = Parser} = Framing) ->
    case ws_frame:parse(Parser, Bin) of
        {ok, Messages, Next} ->
            messages(Messages, Framing#websocket{parser = Next}, [], []);
        {error, message_too_big, Next} ->
            {error, {frame_encoding, 0, <<"message exceeds max_record_size">>}, [], [],
                Framing#websocket{parser = Next, close = ?WS_TOO_BIG}};
        {error, Reason, Next} ->
            {error, {protocol_violation, 0, atom_to_binary(Reason)}, [], [], Framing#websocket{
                parser = Next, close = ?WS_PROTOCOL_ERROR
            }}
    end.

messages([], Framing, Records, Replies) ->
    {ok, lists:reverse(Records), lists:reverse(Replies), Framing};
messages([{binary, Record} | Rest], Framing, Records, Replies) ->
    messages(Rest, Framing, [Record | Records], Replies);
messages([{ping, Payload} | Rest], #websocket{role = Role} = Framing, Records, Replies) ->
    messages(Rest, Framing, Records, [ws_frame:encode({pong, Payload}, Role) | Replies]);
messages([{pong, _} | Rest], Framing, Records, Replies) ->
    messages(Rest, Framing, Records, Replies);
messages([{text, _} | _], Framing, Records, Replies) ->
    {error, {protocol_violation, 0, <<"QMux over WebSocket forbids text messages">>},
        lists:reverse(Records), lists:reverse(Replies), Framing#websocket{
            close = ?WS_UNSUPPORTED_DATA
        }};
messages([Close | _], Framing, Records, Replies) when
    Close =:= close; element(1, Close) =:= close
->
    %% The peer started the closing handshake, which our Close completes.
    {closed, websocket_closed, lists:reverse(Records),
        lists:reverse([ws_close(?WS_NORMAL, Framing) | Replies]), Framing#websocket{close = sent}}.

%% @doc A binary message carrying `Frames'.
-spec encode(iodata(), state()) -> iodata().
encode(Frames, #websocket{role = Role}) ->
    ws_frame:encode({binary, Frames}, Role).

%% @doc The last record, carrying `Frame' unless it is `none', then a Close
%% frame, unless one already answered the peer's.
-spec close(iodata() | none, state()) -> iodata().
close(Frame, #websocket{close = Code} = Framing) ->
    Record =
        case Frame of
            none -> [];
            _ -> encode(Frame, Framing)
        end,
    case Code of
        sent -> Record;
        _ -> [Record, ws_close(Code, Framing)]
    end.

ws_close(Code, #websocket{role = Role}) ->
    ws_frame:encode({close, Code, <<>>}, Role).
