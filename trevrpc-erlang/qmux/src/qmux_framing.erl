%% @doc How a QMux session's records travel over its transport. The default,
%% `qmux_stream_framing', prefixes each record with its Size over a byte
%% stream (draft-ietf-quic-qmux-02 Section 4); a message-based framing
%% sends one record per message instead, as QMux over WebSocket does
%% (draft-lcurley-qmux-websocket-00). A session takes the framing's module
%% in its `framing' option.
%%
%% A framing may also answer the peer itself (WebSocket Pongs, say) and end
%% the connection its own way (a WebSocket Close): `decode/2' returns its
%% replies with the records, and `close/2' the bytes that end the session.
%%
%% Replies are latest-wins, as WebSocket Pongs are (RFC 6455 Section
%% 5.5.3): while the session reads on, only the last reply of a decode goes
%% out, with the next flush that finds room in the writer, and a later one
%% replaces it if it has not gone out yet. That bounds what a peer that
%% sends and never reads can make the session hold. When a decode ends the
%% connection (`closed' or `error'), all its replies go out at once. A
%% framing whose replies must all arrive cannot use them.
-module(qmux_framing).

-export([new/3, decode/2, encode/2, close/2]).

-export_type([t/0, decoded/1]).

%% A framing module and its state.
-opaque t() :: {module(), term()}.

%% What a decode found: the records that arrived, in order, and the
%% replies to the peer, of which only the last counts while the session
%% reads on. `{closed, ...}' means the peer ended the connection after those
%% records, `{error, ...}' that it broke the framing, which the session
%% answers with a transport CONNECTION_CLOSE; either way, all the replies go
%% out.
-type decoded(State) ::
    {ok, [binary()], [iodata()], State}
    | {closed, term(), [binary()], [iodata()], State}
    | {error, qmux_frame:error(), [binary()], [iodata()], State}.

%% The state of one end of a session, whose records may hold up to
%% `MaxRecordSize' bytes.
-callback init(Role :: client | server, MaxRecordSize :: pos_integer()) -> State :: term().

%% Takes the records out of bytes that arrived, with the replies they call
%% for.
-callback decode(binary(), State) -> decoded(State) when State :: term().

%% The bytes of a record carrying the frames.
-callback encode(Frames :: iodata(), State :: term()) -> iodata().

%% The bytes that end the session: the last record, carrying the frame
%% unless it is `none', and whatever else the framing ends a connection
%% with.
-callback close(Frame :: iodata() | none, State :: term()) -> iodata().

-spec new(module(), client | server, pos_integer()) -> t().
new(Module, Role, MaxRecordSize) ->
    {Module, Module:init(Role, MaxRecordSize)}.

-spec decode(binary(), t()) -> decoded(t()).
decode(Bin, {Module, State}) ->
    case Module:decode(Bin, State) of
        {ok, Records, Replies, Next} ->
            {ok, Records, Replies, {Module, Next}};
        {closed, Reason, Records, Replies, Next} ->
            {closed, Reason, Records, Replies, {Module, Next}};
        {error, Error, Records, Replies, Next} ->
            {error, Error, Records, Replies, {Module, Next}}
    end.

-spec encode(iodata(), t()) -> iodata().
encode(Frames, {Module, State}) ->
    Module:encode(Frames, State).

-spec close(iodata() | none, t()) -> iodata().
close(Frame, {Module, State}) ->
    Module:close(Frame, State).
