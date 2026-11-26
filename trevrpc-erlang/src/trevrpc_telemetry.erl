%% @doc The server's call metrics, as `telemetry' events.
%%
%% A server emits two events for every call whose REQUEST arrived, whatever
%% becomes of it, calls the protocol's checks, the admission, the
%% authorizer or an unknown method rejected before a handler ran included,
%% as the Go and Rust servers' metrics see them:
%%
%% <ul>
%% <li>`[trevrpc, server, call, start]', with the measurements
%%   `monotonic_time' and `system_time', when the stream opened, in native
%%   units.</li>
%% <li>`[trevrpc, server, call, stop]', once the call ended, with the
%%   measurements `duration' (native units since the stream opened),
%%   `monotonic_time', and `request_body_len' and `response_body_len', the
%%   bytes of a call's single request and single response messages (0 for
%%   a direction that streams, and for a message that never came).</li>
%% </ul>
%%
%% Both have the metadata `method' (the path), `service', `method_name',
%% `kind', `registered', `peer' (the client's address, or `undefined') and
%% `telemetry_span_context', a reference shared by a call's two events; the
%% stop event also has `status', the status the call ended with, which is
%% what the client got, except that an `internal' one keeps the message the
%% client does not see. `registered' is false when the server has no such
%% method: `method', `service' and `method_name' are then whatever the
%% client sent, unchecked (empty for a REQUEST that did not decode), so
%% metrics should not use them as labels. A call that its deadline, the
%% client, the server's shutdown or the end of its connection stopped
%% still emits its stop, with `deadline_exceeded', `cancelled' or
%% `unavailable'.
%%
%% ```
%% telemetry:attach(my_metrics, [trevrpc, server, call, stop],
%%     fun(_Event, #{duration := Duration}, #{status := Status, registered := true} = Meta, _) ->
%%         record(maps:get(method, Meta), trevrpc_status:code(Status), Duration);
%%        (_Event, _Measurements, _Meta, _) ->
%%         ok
%%     end, []).
%% '''
%%
%% Each call's events are emitted once and in pairs: an admitted call's by
%% its connection's process, which admits it and alone learns how it ended,
%% however it ended, and a call rejected before it was admitted by its own
%% process, both at once. Handlers run inline there, so they must not block;
%% `telemetry' detaches one that crashes. Neither event holds the request's
%% metadata or messages.
-module(trevrpc_telemetry).

-export([meta/4, start/2, stop/4, rejected/3]).

-export_type([meta/0]).

-define(START, [trevrpc, server, call, start]).
-define(STOP, [trevrpc, server, call, stop]).

%% A call's metadata for its events.
-type meta() :: #{
    method := binary(),
    service := binary(),
    method_name := binary(),
    kind := trevrpc_wire:kind() | undefined,
    registered := boolean(),
    peer := {inet:ip_address(), inet:port_number()} | undefined,
    telemetry_span_context := reference()
}.

%% @private The metadata of a call to `Method' (empty for a REQUEST that did
%% not decode), of `Kind'.
-spec meta(binary(), trevrpc_wire:kind() | undefined, boolean(), meta_peer()) -> meta().
meta(Method, Kind, Registered, Peer) ->
    {Service, Name} = trevrpc_wire:method_names(Method),
    #{
        method => Method,
        service => Service,
        method_name => Name,
        kind => Kind,
        registered => Registered,
        peer => Peer,
        telemetry_span_context => make_ref()
    }.

-type meta_peer() :: {inet:ip_address(), inet:port_number()} | undefined.

%% @private The start of a call whose stream opened at `Started'
%% (`erlang:monotonic_time()').
-spec start(integer(), meta()) -> ok.
start(Started, Meta) ->
    telemetry:execute(
        ?START, #{monotonic_time => Started, system_time => Started + erlang:time_offset()}, Meta
    ).

%% @private The end of a call with `Status', and the bytes of its single
%% request and response messages.
-spec stop(integer(), meta(), trevrpc_status:status(), {non_neg_integer(), non_neg_integer()}) ->
    ok.
stop(Started, Meta, Status, {RequestBytes, ResponseBytes}) ->
    Now = erlang:monotonic_time(),
    telemetry:execute(
        ?STOP,
        #{
            duration => Now - Started,
            monotonic_time => Now,
            request_body_len => RequestBytes,
            response_body_len => ResponseBytes
        },
        Meta#{status => Status}
    ).

%% @private Both events of a call rejected before it was admitted.
-spec rejected(integer(), meta(), trevrpc_status:status()) -> ok.
rejected(Started, Meta, Status) ->
    start(Started, Meta),
    stop(Started, Meta, Status, {0, 0}).
