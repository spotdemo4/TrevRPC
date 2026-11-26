%% @doc Request validation: a server checks request messages after they
%% are decoded and before a handler sees them (see the `validator' option
%% of {@link trevrpc_server}), and a channel checks the ones its calls send
%% before they leave (see the `validator' option of {@link
%% trevrpc_channel}).
%%
%% A validator gets the protobuf full name of each request type of the
%% server's routes once, when the server starts, and answers `none' when its
%% messages never need checking, which costs the routes nothing, or `{ok,
%% Validate}' with the function that checks them, which every route of the
%% type shares. Each request message goes through
%% `Validate(Request, Encoded)': the term the route's codec decoded (the
%% bytes without one) and the message's encoding, so that a validator can
%% check whichever suits it. `ok' lets the handler see it; `{error, Status}'
%% fails it with `Status', normally {@link trevrpc_violations:status/1}'s
%% `invalid_argument' with a `buf.validate.Violations' detail.
%%
%% A unary or server-streaming call whose request is invalid fails before
%% its handler runs. On a client-streaming or bidirectional call, the
%% `trevrpc_call:recv/1' that read the message returns the status, as for a
%% message the codec cannot decode, and the handler decides whether the
%% call goes on.
%%
%% A validator that raises, or answers anything else, for a request type
%% fails the server's start: rules that cannot be enforced serve no calls.
%% A rule that compiles but cannot be evaluated for some values, such as a
%% division by a field that is 0 by default, is not such a rule: `Validate'
%% fails just the messages it cannot evaluate, with `internal', as the Go,
%% Rust and Python validators do. A validator that compiles a type's rules
%% by checking an empty message must not refuse the type for that.
%% A `Validate' that raises or answers anything else fails the call with
%% `internal'. One that answers something else is a bug of the validator,
%% logged as an error; one that raises may be a message it cannot evaluate,
%% which a peer can send on every call, so it is logged at debug level, as
%% a message the codec cannot decode is, with the exception's class, its
%% reason's tag and where it was raised, never the message.
%%
%% A channel's validator takes the same form, and checks the requests of
%% the calls whose options name their `request_type', as generated code's
%% do, after they are encoded and before anything is sent: a unary or
%% server-streaming call whose request is invalid fails without opening a
%% stream, and {@link trevrpc_client:send/2} returns `{invalid, Status}'
%% for a streamed request, which is not sent, and the call goes on. The
%% channel asks its validator about each request type once, when the first
%% call of the type starts, and keeps the answer for its calls; a validator
%% that raises or answers anything else for a type fails each call of the
%% type with `internal' as it starts, before it opens a stream, whatever
%% its kind. Without a validator, calls pay nothing.
%%
%% Calls find a channel's validator without asking the channel, by the
%% channel's pid in a `persistent_term', which only a channel with a
%% validator stores. Closing such a channel erases it, which makes the VM
%% scan every process for references to it: nothing for a channel that
%% lives as long as the node's connection to its server, as channels are
%% meant to, but a node that opens and closes many channels with validators
%% pays it each time. A channel killed without running its `terminate/2'
%% (`exit(Channel, kill)', a supervisor's `brutal_kill') leaves the entry
%% behind.
-module(trevrpc_validator).

-export([check/1, bind/2, validate/4]).

%% Used by trevrpc_channel and trevrpc_client.
-export([install/1, uninstall/0, request_check/3]).

-export_type([validator/0, validate/0]).

-type validator() :: fun((RequestType :: binary()) -> none | {ok, validate()}).

-type validate() :: fun(
    (Request :: term(), Encoded :: binary()) -> ok | {error, trevrpc_status:status()}
).

%% @doc Checks that `Validator' is a validator, or `undefined' (none).
-spec check(term()) -> ok | error.
check(undefined) -> ok;
check(Validator) when is_function(Validator, 1) -> ok;
check(_Validator) -> error.

%% @doc The validation functions of the routes whose request messages
%% `Validator' checks, by method path. `Validator' is asked once per request
%% type, however many routes share it, and routes without a `request_type'
%% are not checked. A type it cannot bind fails with `{validator, Type,
%% Reason}', for the first such type in order.
-spec bind(validator() | undefined, trevrpc_server:routes()) ->
    {ok, #{binary() => validate()}} | {error, {validator, binary(), term()}}.
bind(undefined, _Routes) ->
    {ok, #{}};
bind(Validator, Routes) ->
    Types = lists:usort([Type || _ := #{request_type := Type} <- Routes]),
    Bound = [{Type, bind_type(Validator, Type)} || Type <- Types],
    case [{validator, Type, Reason} || {Type, {error, Reason}} <- Bound] of
        [] ->
            Validates = #{Type => Validate || {Type, {ok, Validate}} <- Bound},
            {ok,
                #{
                    Method => Validate
                 || Method := #{request_type := Type} <- Routes,
                    {ok, Validate} <- [maps:find(Type, Validates)]
                }};
        [Error | _] ->
            {error, Error}
    end.

bind_type(Validator, Type) ->
    try Validator(Type) of
        none -> none;
        {ok, Validate} when is_function(Validate, 2) -> {ok, Validate};
        Other -> {error, {bad_return, Other}}
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

%% @doc Checks the request message `Request', encoded as `Encoded', of a
%% call to `Method' with `Validate', if any.
-spec validate(validate() | undefined, binary(), term(), binary()) ->
    ok | {error, trevrpc_status:status()}.
validate(undefined, _Method, _Request, _Encoded) ->
    ok;
validate(Validate, Method, Request, Encoded) ->
    try Validate(Request, Encoded) of
        ok ->
            ok;
        {error, #{code := Code} = Status} when is_atom(Code), Code =/= ok ->
            {error, Status};
        Other ->
            failed(error, invalid_response, #{
                method => Method, returned => trevrpc_log:shape(Other)
            })
    catch
        Class:Reason:Stack ->
            failed(debug, validator_panic, (trevrpc_log:exception(Class, Reason, Stack))#{
                method => Method
            })
    end.

failed(Level, Phase, Fields) ->
    trevrpc_log:log(Level, validator, Phase, Fields),
    {error, trevrpc_status:new(internal, <<"request validation failed">>)}.

%%% Channels

%% A channel's validator and the answers it gave, by request type, in a
%% table the channel owns and its callers fill, found by the channel's pid:
%% a call reads it without asking the channel, and a channel without a
%% validator stores nothing.
-define(CHANNEL_KEY(Channel), {?MODULE, Channel}).

%% @private Makes `Validator' the calling channel's, for request_check/3.
-spec install(validator() | undefined) -> ok.
install(undefined) ->
    ok;
install(Validator) ->
    Table = ets:new(?MODULE, [set, public, {read_concurrency, true}]),
    persistent_term:put(?CHANNEL_KEY(self()), {Validator, Table}).

%% @private Removes the calling channel's validator, if it has one.
-spec uninstall() -> ok.
uninstall() ->
    _ = persistent_term:erase(?CHANNEL_KEY(self())),
    ok.

%% @private The check of the requests of type `Type' that a call to
%% `Method' on `Channel' sends, `undefined' when they need none, or `{error,
%% Status}' when the validator cannot check them, which fails the call.
-spec request_check(pid(), binary(), binary() | undefined) ->
    fun((term(), binary()) -> ok | {error, trevrpc_status:status()})
    | undefined
    | {error, trevrpc_status:status()}.
request_check(_Channel, _Method, undefined) ->
    undefined;
request_check(Channel, Method, Type) ->
    case persistent_term:get(?CHANNEL_KEY(Channel), undefined) of
        undefined ->
            undefined;
        {Validator, Table} ->
            case bound(Validator, Table, Type) of
                none ->
                    undefined;
                {ok, Validate} ->
                    fun(Request, Encoded) -> validate(Validate, Method, Request, Encoded) end;
                {error, _Reason} ->
                    {error, trevrpc_status:new(internal, <<"cannot validate ", Type/binary>>)}
            end
    end.

%% A type's answer, asked of the validator when the first call of the type
%% starts. Calls that start first at once may each ask; the first answer
%% stored is the one every call uses, and a type that cannot be bound is
%% logged once.
bound(Validator, Table, Type) ->
    try
        case ets:lookup(Table, Type) of
            [{_, Bound}] -> Bound;
            [] -> bind(Validator, Table, Type)
        end
    catch
        %% The channel is gone, and its table with it, before the lookup or
        %% while the validator answered: the call fails as it opens its
        %% stream. bind_type/2 catches what the validator raises, so only
        %% the table raises this.
        error:badarg -> none
    end.

bind(Validator, Table, Type) ->
    Bound = bind_type(Validator, Type),
    case ets:insert_new(Table, {Type, Bound}) of
        true ->
            _ = log_unbound(Type, Bound),
            Bound;
        false ->
            [{_, Kept}] = ets:lookup(Table, Type),
            Kept
    end.

log_unbound(Type, {error, Reason}) ->
    trevrpc_log:log(error, client, validator_failed, #{
        request_type => Type, reason => trevrpc_log:shape(Reason)
    });
log_unbound(_Type, _Bound) ->
    ok.
