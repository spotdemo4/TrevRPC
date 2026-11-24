%% @doc Request validation: checks request messages after they are decoded
%% and before a handler sees them (see the `validator' option of {@link
%% trevrpc_server}).
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
-module(trevrpc_validator).

-export([check/1, bind/2, validate/4]).

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
        _Other ->
            failed(error, Method, bad_return)
    catch
        Class:Reason:Stack ->
            failed(debug, Method, {Class, trevrpc_codec:exception(Reason, Stack)})
    end.

failed(Level, Method, Why) ->
    logger:log(Level, "trevrpc request validation of ~ts failed: ~p", [Method, Why]),
    {error, trevrpc_status:new(internal, <<"request validation failed">>)}.
