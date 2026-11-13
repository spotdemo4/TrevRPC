%% @doc TrevRPC status codes, whose numeric values match gRPC's.
-module(trevrpc_status).

-export([to_int/1, from_int/1, codes/0]).

-export_type([code/0]).

-type code() ::
    ok
    | cancelled
    | unknown
    | invalid_argument
    | deadline_exceeded
    | not_found
    | already_exists
    | permission_denied
    | resource_exhausted
    | failed_precondition
    | aborted
    | out_of_range
    | unimplemented
    | internal
    | unavailable
    | data_loss
    | unauthenticated.

%% @doc Every code, in numeric order.
-spec codes() -> [code(), ...].
codes() ->
    [
        ok,
        cancelled,
        unknown,
        invalid_argument,
        deadline_exceeded,
        not_found,
        already_exists,
        permission_denied,
        resource_exhausted,
        failed_precondition,
        aborted,
        out_of_range,
        unimplemented,
        internal,
        unavailable,
        data_loss,
        unauthenticated
    ].

%% @doc The numeric value of a code.
-spec to_int(code()) -> 0..16.
to_int(ok) -> 0;
to_int(cancelled) -> 1;
to_int(unknown) -> 2;
to_int(invalid_argument) -> 3;
to_int(deadline_exceeded) -> 4;
to_int(not_found) -> 5;
to_int(already_exists) -> 6;
to_int(permission_denied) -> 7;
to_int(resource_exhausted) -> 8;
to_int(failed_precondition) -> 9;
to_int(aborted) -> 10;
to_int(out_of_range) -> 11;
to_int(unimplemented) -> 12;
to_int(internal) -> 13;
to_int(unavailable) -> 14;
to_int(data_loss) -> 15;
to_int(unauthenticated) -> 16.

%% @doc The code of a numeric value. Values the protocol does not define
%% map to `unknown'.
-spec from_int(integer()) -> code().
from_int(0) -> ok;
from_int(1) -> cancelled;
from_int(2) -> unknown;
from_int(3) -> invalid_argument;
from_int(4) -> deadline_exceeded;
from_int(5) -> not_found;
from_int(6) -> already_exists;
from_int(7) -> permission_denied;
from_int(8) -> resource_exhausted;
from_int(9) -> failed_precondition;
from_int(10) -> aborted;
from_int(11) -> out_of_range;
from_int(12) -> unimplemented;
from_int(13) -> internal;
from_int(14) -> unavailable;
from_int(15) -> data_loss;
from_int(16) -> unauthenticated;
from_int(Value) when is_integer(Value) -> unknown.
