%% @doc The Erlang names of protobuf things.
-module(trevrpc_gen_names).

-export([snake/1, atom/1, message_module/2, service_module/3]).

%% @doc A protobuf identifier in snake case: `SayHello' is `say_hello',
%% `HTTPRequest' `http_request' and `GetV2' `get_v2'. A capital starts a
%% word after a lower-case letter or a digit, and so does the last capital
%% of a run before a lower-case letter.
-spec snake(binary()) -> binary().
snake(Name) ->
    list_to_binary(snake(binary_to_list(Name), undefined)).

snake([C | Rest], Previous) when C >= $A, C =< $Z ->
    Lower = C - $A + $a,
    Next =
        case Rest of
            [N | _] -> N;
            [] -> undefined
        end,
    Starts =
        Previous =/= undefined andalso Previous =/= $_ andalso
            (lower_or_digit(Previous) orelse (upper(Previous) andalso lower(Next))),
    case Starts of
        true -> [$_, Lower | snake(Rest, C)];
        false -> [Lower | snake(Rest, C)]
    end;
snake([C | Rest], _Previous) ->
    [C | snake(Rest, C)];
snake([], _Previous) ->
    [].

lower(C) -> is_integer(C) andalso C >= $a andalso C =< $z.
upper(C) -> is_integer(C) andalso C >= $A andalso C =< $Z.
lower_or_digit(C) -> lower(C) orelse (C >= $0 andalso C =< $9).

%% @doc `Name' as an atom in Erlang source, quoted where it must be (a
%% reserved word such as `receive', or one that starts with an
%% underscore).
-spec atom(binary()) -> iodata().
atom(Name) ->
    io_lib:write_atom(binary_to_atom(Name, utf8)).

%% @doc The module of a proto file's messages: the file's base name in
%% snake case, with `Prefix' before and `Suffix' after; `foo/bar.proto' is
%% `bar_pb' by default, as gpb names it.
-spec message_module(binary(), #{prefix := binary(), pb_suffix := binary(), term() => term()}) ->
    binary().
message_module(File, #{prefix := Prefix, pb_suffix := Suffix}) ->
    Base = filename:basename(File, <<".proto">>),
    <<Prefix/binary, (identifier(Base))/binary, Suffix/binary>>.

%% @doc The module of a service: its package and name in snake case, joined
%% by underscores; `trevrpc.interop.v1.Echo' is `trevrpc_interop_v1_echo'.
-spec service_module(
    binary(), binary(), #{prefix := binary(), service_suffix := binary(), term() => term()}
) -> binary().
service_module(Package, Service, #{prefix := Prefix, service_suffix := Suffix}) ->
    Parts = [identifier(Part) || Part <- binary:split(Package, <<".">>, [global]), Part =/= <<>>],
    Name = iolist_to_binary(lists:join(<<"_">>, Parts ++ [identifier(Service)])),
    <<Prefix/binary, Name/binary, Suffix/binary>>.

%% A name in snake case, its characters that cannot be in an unquoted atom
%% replaced by underscores.
identifier(Name) ->
    <<
        <<
            (case C of
                _ when C >= $a, C =< $z; C >= $0, C =< $9; C =:= $_ -> C;
                _ -> $_
            end)
        >>
     || <<C>> <= snake(Name)
    >>.
