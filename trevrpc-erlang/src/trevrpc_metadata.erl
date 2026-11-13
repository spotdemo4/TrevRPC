%% @doc Metadata: maps from lowercase ASCII keys to byte values.
%%
%% Request metadata, response headers and trailing metadata share these
%% rules. Keys are non-empty, at most 128 bytes of lowercase ASCII letters,
%% digits, `.', `_' and `-', and do not start with the reserved prefix
%% `trevrpc-'. A map holds at most 64 entries, each value is at most 8 KiB,
%% and keys and values total at most 64 KiB.
-module(trevrpc_metadata).

-export([validate/1, validate_key/1, normalize_key/1, merge/2]).

-export_type([metadata/0]).

-type metadata() :: #{binary() => binary()}.

-define(MAX_ENTRIES, 64).
-define(MAX_KEY_LEN, 128).
-define(MAX_VALUE_LEN, 8192).
-define(MAX_TOTAL_SIZE, 65536).
-define(RESERVED_PREFIX, "trevrpc-").

%% @doc Checks a metadata map against the key rules and size limits.
-spec validate(metadata()) -> ok | {error, binary()}.
validate(Metadata) when map_size(Metadata) > ?MAX_ENTRIES ->
    {error,
        iolist_to_binary(
            io_lib:format("metadata has ~b entries, maximum is ~b", [
                map_size(Metadata), ?MAX_ENTRIES
            ])
        )};
validate(Metadata) ->
    validate_entries(maps:iterator(Metadata), 0).

validate_entries(Iterator, Total) ->
    case maps:next(Iterator) of
        none when Total > ?MAX_TOTAL_SIZE ->
            {error,
                iolist_to_binary(
                    io_lib:format("metadata is ~b bytes, maximum is ~b", [Total, ?MAX_TOTAL_SIZE])
                )};
        none ->
            ok;
        {Key, Value, Next} ->
            case validate_key(Key) of
                ok when byte_size(Value) > ?MAX_VALUE_LEN ->
                    {error,
                        iolist_to_binary(
                            io_lib:format("metadata value ~s is ~b bytes, maximum is ~b", [
                                quote(Key), byte_size(Value), ?MAX_VALUE_LEN
                            ])
                        )};
                ok ->
                    validate_entries(Next, Total + byte_size(Key) + byte_size(Value));
                Error ->
                    Error
            end
    end.

%% @doc Checks one key against the key rules.
-spec validate_key(binary()) -> ok | {error, binary()}.
validate_key(<<>>) ->
    {error, <<"metadata key is empty">>};
validate_key(Key) when byte_size(Key) > ?MAX_KEY_LEN ->
    {error,
        iolist_to_binary(
            io_lib:format("metadata key ~s is ~b bytes, maximum is ~b", [
                quote(Key), byte_size(Key), ?MAX_KEY_LEN
            ])
        )};
validate_key(<<?RESERVED_PREFIX, _/binary>> = Key) ->
    {error,
        iolist_to_binary(
            io_lib:format("metadata key ~s uses reserved prefix \"~s\"", [
                quote(Key), ?RESERVED_PREFIX
            ])
        )};
validate_key(Key) ->
    case valid_key_bytes(Key) of
        true ->
            ok;
        false ->
            {error,
                iolist_to_binary(
                    io_lib:format(
                        "metadata key ~s must use lowercase ASCII letters, digits, '.', '_' or '-'",
                        [quote(Key)]
                    )
                )}
    end.

valid_key_bytes(<<C, Rest/binary>>) when
    (C >= $a andalso C =< $z);
    (C >= $0 andalso C =< $9);
    C =:= $.;
    C =:= $_;
    C =:= $-
->
    valid_key_bytes(Rest);
valid_key_bytes(<<>>) ->
    true;
valid_key_bytes(<<_/binary>>) ->
    false.

%% A key in double quotes for an error message, with bytes outside printable
%% ASCII escaped.
quote(Key) ->
    [$", [quote_byte(C) || <<C>> <= Key], $"].

quote_byte($") -> "\\\"";
quote_byte($\\) -> "\\\\";
quote_byte(C) when C >= 16#20, C < 16#7F -> C;
quote_byte(C) -> io_lib:format("\\x~2.16.0b", [C]).

%% @doc Lowercases the ASCII letters of a key, as keys in received maps must
%% be.
-spec normalize_key(binary()) -> binary().
normalize_key(Key) ->
    <<<<(lower(C))>> || <<C>> <= Key>>.

lower(C) when C >= $A, C =< $Z -> C + 32;
lower(C) -> C.

%% @doc Merges a base map, such as a connection metadata set, under a call's
%% own entries: the call's entry wins for a key both have.
-spec merge(metadata(), metadata()) -> metadata().
merge(Base, Own) -> maps:merge(Base, Own).
