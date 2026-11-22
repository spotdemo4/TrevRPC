%% @doc The default `qmux_framing': records prefixed with their Size over a
%% byte stream, such as QMux over TLS (draft-ietf-quic-qmux-02 Section 4).
-module(qmux_stream_framing).

-behaviour(qmux_framing).

-export([init/2, decode/2, encode/2, close/2]).

-record(stream_framing, {
    max_record_size :: pos_integer(),
    %% The start of a record that has not all arrived yet.
    buffer = <<>> :: binary()
}).

-opaque state() :: #stream_framing{}.

-export_type([state/0]).

-spec init(client | server, pos_integer()) -> state().
init(_Role, MaxRecordSize) ->
    #stream_framing{max_record_size = MaxRecordSize}.

-spec decode(binary(), state()) -> qmux_framing:decoded(state()).
decode(Bin, #stream_framing{buffer = Buffer} = State) ->
    records(<<Buffer/binary, Bin/binary>>, State, []).

records(Bin, #stream_framing{max_record_size = Max} = State, Acc) ->
    case qmux_frame:decode_record(Bin, Max) of
        more ->
            {ok, lists:reverse(Acc), [], State#stream_framing{buffer = Bin}};
        {ok, Record, Rest} ->
            records(Rest, State, [Record | Acc]);
        {error, Error} ->
            {error, Error, lists:reverse(Acc), [], State#stream_framing{buffer = <<>>}}
    end.

-spec encode(iodata(), state()) -> iodata().
encode(Frames, #stream_framing{}) ->
    qmux_frame:record(Frames).

-spec close(iodata() | none, state()) -> iodata().
close(none, #stream_framing{}) ->
    [];
close(Frame, #stream_framing{}) ->
    qmux_frame:record(Frame).
