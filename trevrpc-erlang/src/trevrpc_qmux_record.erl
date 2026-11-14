%% @private How a QMux session's records travel: Size-prefixed over a TLS
%% byte stream (draft-ietf-quic-qmux-02), or one record per binary message
%% over a WebSocket (draft-lcurley-qmux-websocket-00), where message
%% boundaries replace the Size field and text messages are forbidden.
%%
%% Over a WebSocket, pings are answered here, and the closing handshake
%% replaces QMux's draining: a session that ends sends a Close frame after
%% its last record.
-module(trevrpc_qmux_record).

-export([new/3, decode/2, encode/2, close/2]).

-export_type([t/0, framing/0, decoded/0]).

%% `tls' for QMux over TLS, `websocket' for QMux over WebSocket.
-type framing() :: tls | websocket.

-record(tls, {
    max_record_size :: pos_integer(),
    buffer = <<>> :: binary()
}).

-record(websocket, {
    role :: client | server,
    parser :: ws_frame:parser(),
    %% The code of the Close frame that ends the session, or `sent' once
    %% one went out.
    close = 1000 :: 1000..4999 | sent
}).

-opaque t() :: #tls{} | #websocket{}.

%% What a decode found: the records that arrived, in order, and what must go
%% back to the peer at once (pongs, and the Close that answers the peer's).
%% `{closed, ...}' means the peer ended the connection after those records,
%% `{error, ...}' that it broke the framing.
-type decoded() ::
    {ok, [binary()], iodata(), t()}
    | {closed, term(), [binary()], iodata(), t()}
    | {error, trevrpc_qmux_frame:error(), [binary()], iodata(), t()}.

%% WebSocket close codes (RFC 6455 section 7.4.1).
-define(WS_NORMAL, 1000).
-define(WS_PROTOCOL_ERROR, 1002).
-define(WS_UNSUPPORTED_DATA, 1003).
-define(WS_TOO_BIG, 1009).

%% @doc The framing of one end of a session, whose records may hold up to
%% `MaxRecordSize' bytes.
-spec new(framing(), client | server, pos_integer()) -> t().
new(tls, _Role, MaxRecordSize) ->
    #tls{max_record_size = MaxRecordSize};
new(websocket, Role, MaxRecordSize) ->
    #websocket{
        role = Role,
        parser = ws_frame:init_parser(#{
            role => Role, max_frame => MaxRecordSize, max_message => MaxRecordSize
        })
    }.

%% @doc Takes the records out of bytes that arrived.
-spec decode(binary(), t()) -> decoded().
decode(Bin, #tls{buffer = Buffer} = Framing) ->
    tls_records(<<Buffer/binary, Bin/binary>>, Framing, []);
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

tls_records(Bin, #tls{max_record_size = Max} = Framing, Acc) ->
    case trevrpc_qmux_frame:decode_record(Bin, Max) of
        more ->
            {ok, lists:reverse(Acc), [], Framing#tls{buffer = Bin}};
        {ok, Record, Rest} ->
            tls_records(Rest, Framing, [Record | Acc]);
        {error, Error} ->
            {error, Error, lists:reverse(Acc), [], Framing#tls{buffer = <<>>}}
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

%% @doc The bytes of a record carrying `Frames'.
-spec encode(iodata(), t()) -> iodata().
encode(Frames, #tls{}) ->
    trevrpc_qmux_frame:record(Frames);
encode(Frames, #websocket{role = Role}) ->
    ws_frame:encode({binary, Frames}, Role).

%% @doc The bytes that end a session: the last record, carrying `Frame'
%% unless it is `none', then over a WebSocket a Close frame, unless one
%% already answered the peer's.
-spec close(iodata() | none, t()) -> iodata().
close(none, #tls{}) ->
    [];
close(Frame, #tls{} = Framing) ->
    encode(Frame, Framing);
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
