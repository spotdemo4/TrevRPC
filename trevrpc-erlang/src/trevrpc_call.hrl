%% The state a server call keeps in its process, shared by
%% trevrpc_server_call, which runs the call, and trevrpc_call, the handler
%% API.

%% What a handler holds: the call's immutable facts.
-record(call, {
    ref :: reference(),
    method :: binary(),
    kind :: trevrpc_wire:kind(),
    metadata :: trevrpc_metadata:metadata(),
    deadline :: trevrpc_stream:deadline(),
    %% The call process: the only one that can read and write the call.
    owner :: pid()
}).

%% What changes as the call runs, kept in the call process's dictionary
%% under {trevrpc_call, Ref}.
-record(call_state, {
    stream :: trevrpc_stream:stream(),
    max_frame_size :: pos_integer(),
    max_messages :: pos_integer() | infinity,
    received = 0 :: non_neg_integer(),
    headers_sent = false :: boolean(),
    messages_sent = false :: boolean(),
    trailers = #{} :: trevrpc_metadata:metadata(),
    %% The server's compression, the dictionary response messages are
    %% compressed with, and the ANSWER that goes out before the first
    %% response frame, while it waits.
    compression :: trevrpc_zstd:config(),
    response_dict = none :: non_neg_integer() | none,
    answer = [] :: iodata(),
    %% Request messages that arrived compressed with a dictionary, and
    %% without one.
    dictionary_received = 0 :: non_neg_integer(),
    plain_received = 0 :: non_neg_integer(),
    %% The request side ended: eof, or the status that ended it.
    request_end :: undefined | eof | {error, trevrpc_status:status()}
}).
