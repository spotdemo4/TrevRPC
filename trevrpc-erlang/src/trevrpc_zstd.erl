%% @doc Compressed messages: ZSTD_MESSAGE frames, which carry one message
%% compressed with zstd and a dictionary both peers hold, or without a
%% dictionary on methods that allow it. See the wiki's Compressed Messages
%% section of Protocol and Wire Format.
%%
%% A configuration ({@link config/1}) holds the dictionaries, the message
%% types each compresses, and the compression level. Every configuration also
%% holds compression without a dictionary, which the negotiation treats as
%% the dictionary with ID 0.
%%
%% Payloads are magicless zstd frames. OTP's `zstd' writes and reads frames
%% with their Magic_Number, so it is stripped from what it writes and put
%% back before what it reads.
%%
%% OTP's zstd contexts work only in the process that created them, so they
%% cannot be pooled across processes. Each process keeps one per dictionary
%% (and level) in its process dictionary and reuses it for its later
%% messages, except after a message larger than 128 KiB, whose buffers it
%% does not keep. A client process reuses its contexts across calls; on a
%% server, where every call runs in a process of its own, a call creates
%% the contexts it needs and reuses them for its own messages. A new context
%% costs about a microsecond per message on top of the 0.5 to 1 us a 71-byte
%% message takes to compress or decompress with a kept one (measured with
%% the golden dictionary); the context is freed with the call's process.
-module(trevrpc_zstd).

-export([
    config/1,
    plain/0,
    level/1,
    query/1,
    answer/2,
    shared/2,
    dict_for/4,
    accepted/3,
    directions/1,
    negotiate/4,
    prepare/5,
    compress/3,
    inflate/4,
    parse_header/1,
    dictionaries/2,
    dictionary_id/1,
    fingerprint/1,
    plain_min_size/0
]).

-export_type([config/0, settings/0, dictionary/0, accept/0, mode/0, directions/0]).

%% The compression a peer configures: zstd-format dictionaries, each with the
%% protobuf message types (full names) it compresses, and the zstd level of
%% outgoing messages (default 1).
-type settings() :: #{
    dictionaries => [dictionary()],
    level => integer()
}.

-type dictionary() :: #{data := binary(), messages => [binary()]}.

%% How a method's messages may be compressed without a dictionary, from the
%% `compression' of its `trevrpc.method' schema option: not at all (`none',
%% for COMPRESSION_UNSPECIFIED), one message at a time (`message', for
%% COMPRESSION_MESSAGE), or also with one zstd context per call (`stream',
%% for COMPRESSION_STREAM). A map sets the two directions apart, so that a
%% direction whose message type has a field marked `debug_redact' can be
%% `none'.
-type mode() :: none | message | stream.
-type directions() :: mode() | #{requests => mode(), responses => mode()}.

%% The dictionaries a receiver accepts on ZSTD_MESSAGE frames: every one it
%% holds (a server), those listed (a client, for one call), or none.
-type accept() :: all | [non_neg_integer()] | none.

%% A dictionary's Dictionary_ID and fingerprint, as DictionaryInfo carries
%% them.
-type info() :: #{id := non_neg_integer(), fingerprint := binary()}.

-record(dict, {
    info :: info(),
    %% Undefined for compression without a dictionary.
    cdict :: zstd:dict() | undefined,
    ddict :: zstd:dict() | undefined
}).

-record(config, {
    level :: integer(),
    dicts :: #{non_neg_integer() => #dict{}},
    types :: #{binary() => non_neg_integer()},
    %% Every dictionary, compression without one last, as a client's
    %% dictionary_query.
    query :: [info()]
}).

-opaque config() :: #config{}.

%% The smallest message compressed without a dictionary: a lone smaller
%% message rarely shrinks.
-define(PLAIN_MIN_SIZE, 256).
-define(PLAIN_ID, 0).
%% Opens every zstd-format dictionary (RFC 8878, section 5).
-define(DICTIONARY_MAGIC, 16#EC30A437).
%% Opens every zstd frame that is not magicless.
-define(FRAME_MAGIC, <<16#28, 16#B5, 16#2F, 16#FD>>).
%% Contexts that handled a larger message are not kept.
-define(MAX_KEPT_MESSAGE, 131072).

%% @doc Validates and digests `Settings'. Fails on a dictionary that is not
%% a zstd-format dictionary with an ID, on two dictionaries with one ID, on
%% a message type that is not a protobuf full name, and on a message type in
%% two dictionaries.
-spec config(settings()) -> {ok, config()} | {error, binary()}.
config(Settings) when is_map(Settings) ->
    case maps:get(level, Settings, 1) of
        Level when is_integer(Level) ->
            Base = base(level(Level)),
            add_dictionaries(maps:get(dictionaries, Settings, []), Base);
        _ ->
            {error, <<"compression level is not an integer">>}
    end;
config(_Settings) ->
    {error, <<"compression settings are not a map">>}.

%% @doc The configuration of a peer that configured no compression: only
%% compression without a dictionary, at level 1.
-spec plain() -> config().
plain() ->
    base(1).

%% @doc The zstd level of outgoing messages; 0 asks for the default, 1.
-spec level(config() | integer()) -> integer().
level(#config{level = Level}) -> Level;
level(0) -> 1;
level(Level) when is_integer(Level) -> Level.

base(Level) ->
    Plain = #dict{info = #{id => ?PLAIN_ID, fingerprint => <<>>}},
    #config{
        level = Level,
        dicts = #{?PLAIN_ID => Plain},
        types = #{},
        query = [Plain#dict.info]
    }.

add_dictionaries([], #config{query = Query} = Config) ->
    %% Compression without a dictionary goes last.
    {ok, Config#config{query = tl(Query) ++ [hd(Query)]}};
add_dictionaries([#{data := Data} = Dictionary | Rest], Config) when is_binary(Data) ->
    maybe
        {ok, Id} ?= valid_id(Data),
        ok ?= unique_id(Id, Config),
        {ok, Dict} ?= digest(Id, Data, Config#config.level),
        {ok, Types} ?= add_types(maps:get(messages, Dictionary, []), Id, Config#config.types),
        add_dictionaries(Rest, Config#config{
            dicts = (Config#config.dicts)#{Id => Dict},
            types = Types,
            query = Config#config.query ++ [Dict#dict.info]
        })
    end;
add_dictionaries([_ | _], _Config) ->
    {error, <<"compression dictionary has no binary data">>};
add_dictionaries(_, _Config) ->
    {error, <<"compression dictionaries are not a list">>}.

valid_id(Data) ->
    case dictionary_id(Data) of
        {ok, Id} -> {ok, Id};
        error -> {error, <<"compression dictionary is not a zstd-format dictionary with an ID">>}
    end.

unique_id(Id, #config{dicts = Dicts}) ->
    case is_map_key(Id, Dicts) of
        true ->
            {error, iolist_to_binary(io_lib:format("compression dictionaries share ID ~b", [Id]))};
        false ->
            ok
    end.

%% Digesting the dictionary once, for each direction, proves libzstd accepts
%% it.
digest(Id, Data, Level) ->
    try
        {ok, CDict} = zstd:dict(compress, Data, #{compressionLevel => Level}),
        {ok, DDict} = zstd:dict(decompress, Data),
        {ok, #dict{
            info = #{id => Id, fingerprint => fingerprint(Data)}, cdict = CDict, ddict = DDict
        }}
    catch
        error:_ ->
            {error, iolist_to_binary(io_lib:format("compression dictionary ~b is not valid", [Id]))}
    end.

add_types([], _Id, Types) ->
    {ok, Types};
add_types([Name | Rest], Id, Types) ->
    case {valid_full_name(Name), Types} of
        {false, _} ->
            {error,
                iolist_to_binary(
                    io_lib:format("invalid message type ~0p in compression dictionary ~b", [
                        Name, Id
                    ])
                )};
        {true, #{Name := _}} ->
            {error,
                iolist_to_binary(
                    io_lib:format("message type ~ts is in two compression dictionaries", [Name])
                )};
        {true, _} ->
            add_types(Rest, Id, Types#{Name => Id})
    end;
add_types(_, Id, _Types) ->
    {error,
        iolist_to_binary(
            io_lib:format("the message types of compression dictionary ~b are not a list", [Id])
        )}.

%% Dot-separated identifiers.
valid_full_name(Name) when is_binary(Name), Name =/= <<>> ->
    lists:all(fun valid_identifier/1, binary:split(Name, <<".">>, [global]));
valid_full_name(_) ->
    false.

valid_identifier(<<First, Rest/binary>>) when
    First >= $a, First =< $z; First >= $A, First =< $Z; First =:= $_
->
    lists:all(
        fun(C) ->
            (C >= $a andalso C =< $z) orelse (C >= $A andalso C =< $Z) orelse
                (C >= $0 andalso C =< $9) orelse C =:= $_
        end,
        binary_to_list(Rest)
    );
valid_identifier(_) ->
    false.

%% @doc The Dictionary_ID of a zstd-format dictionary. Raw-content
%% dictionaries have none.
-spec dictionary_id(binary()) -> {ok, pos_integer()} | error.
dictionary_id(<<?DICTIONARY_MAGIC:32/little, Id:32/little, _/binary>>) when Id =/= 0 ->
    {ok, Id};
dictionary_id(_) ->
    error.

%% @doc A dictionary's fingerprint: the first 8 bytes of its SHA-256 digest.
-spec fingerprint(binary()) -> binary().
fingerprint(Data) ->
    <<Fingerprint:8/binary, _/binary>> = crypto:hash(sha256, Data),
    Fingerprint.

%% @doc The smallest message compressed without a dictionary.
-spec plain_min_size() -> pos_integer().
plain_min_size() -> ?PLAIN_MIN_SIZE.

%%% Negotiation

%% @doc The `dictionary_query' of a client with `Config': every dictionary,
%% and compression without one.
-spec query(config()) -> [info()].
query(#config{query = Query}) -> Query.

%% @doc A server's answer to a `dictionary_query': each queried dictionary it
%% holds with the same ID and fingerprint, once.
-spec answer(config(), [map()]) -> [info()].
answer(Config, Query) ->
    lists:reverse(
        lists:foldl(
            fun(Info, Answer) ->
                case matches(Config, Info) of
                    {ok, Dict} ->
                        case lists:member(Dict#dict.info, Answer) of
                            true -> Answer;
                            false -> [Dict#dict.info | Answer]
                        end;
                    error ->
                        Answer
                end
            end,
            [],
            Query
        )
    ).

%% @doc A client's shared set from the server's answer: the IDs of the
%% answered dictionaries it holds with the same fingerprint.
-spec shared(config(), [map()]) -> [non_neg_integer()].
shared(Config, Answer) ->
    lists:usort([Id || Info <- Answer, {ok, #dict{info = #{id := Id}}} <- [matches(Config, Info)]]).

matches(#config{dicts = Dicts}, Info) ->
    Id = maps:get(id, Info, 0),
    Fingerprint = maps:get(fingerprint, Info, <<>>),
    case Dicts of
        #{Id := #dict{info = #{fingerprint := Fingerprint}} = Dict} -> {ok, Dict};
        #{} -> error
    end.

%% @doc The dictionary of a direction whose messages are of type `Type': the
%% type's dictionary if `Usable' allows it, or else, where `Plain' allows it
%% and `Usable' too, compression without a dictionary (ID 0). A peer that
%% lacks the type's dictionary can thus still receive the direction's
%% messages compressed.
-spec dict_for(config(), binary() | undefined, boolean(), fun((non_neg_integer()) -> boolean())) ->
    non_neg_integer() | none.
dict_for(#config{types = Types}, Type, Plain, Usable) ->
    Typed =
        case Types of
            #{Type := Id} -> Id;
            #{} -> none
        end,
    case Typed =/= none andalso Usable(Typed) of
        true ->
            Typed;
        false ->
            case Plain andalso Usable(?PLAIN_ID) of
                true -> ?PLAIN_ID;
                false -> none
            end
    end.

%% @doc A server's negotiation for one call: the answer to the request's
%% `dictionary_query' (`undefined' when it has none, which makes no ANSWER
%% go out), and the dictionary of the response messages, of type
%% `ResponseType': the type's dictionary, or compression without one where
%% `Plain' allows it, if the client accepts it and, on a call that queries,
%% the answer confirms it. A server without dictionaries still answers, so
%% clients stop asking, and confirms compression without a dictionary.
-spec negotiate(config(), map(), binary() | undefined, boolean()) ->
    {[info()] | undefined, non_neg_integer() | none}.
negotiate(Config, Request, ResponseType, Plain) ->
    Answer =
        case maps:get(dictionary_query, Request, []) of
            [] -> undefined;
            Query -> answer(Config, Query)
        end,
    Accepted = maps:get(accept_dictionaries, Request, []),
    Usable = fun(Id) ->
        lists:member(Id, Accepted) andalso
            (Answer =:= undefined orelse lists:any(fun(#{id := A}) -> A =:= Id end, Answer))
    end,
    {Answer, dict_for(Config, ResponseType, Plain, Usable)}.

%% @doc A client's negotiation for one call on a connection whose shared
%% set is `Shared' (`undefined' until a call learned it): the
%% `dictionary_query' to send, which is empty once the set is known, the
%% dictionaries the call accepts on its responses, and the dictionary of its
%% request messages. That is `{pending, Id}' when the set is not known yet:
%% the request stream may start compressing with `Id' as soon as the call's
%% ANSWER confirms it.
%%
%% A call that queries may already accept the dictionary it asks about: the
%% server compresses with it only if its answer confirms it.
-spec prepare(
    config(),
    [non_neg_integer()] | undefined,
    binary() | undefined,
    binary() | undefined,
    {boolean(), boolean()}
) ->
    #{
        query := [info()],
        accept := [non_neg_integer()],
        requests := non_neg_integer() | {pending, non_neg_integer()} | none
    }.
prepare(Config, Shared, RequestType, ResponseType, {PlainRequests, PlainResponses}) ->
    Usable = fun(Id) -> Shared =:= undefined orelse lists:member(Id, Shared) end,
    Accept =
        case dict_for(Config, ResponseType, PlainResponses, Usable) of
            none -> [];
            ResponseId -> [ResponseId]
        end,
    Requests =
        case {dict_for(Config, RequestType, PlainRequests, Usable), Shared} of
            {none, _} -> none;
            {RequestId, undefined} -> {pending, RequestId};
            {RequestId, _} -> RequestId
        end,
    Query =
        case Shared of
            undefined -> query(Config);
            _ -> []
        end,
    #{query => Query, accept => Accept, requests => Requests}.

%% @doc Whether a receiver that accepts `Accept' with `Config' decodes
%% dictionary `Id'.
-spec accepted(config(), accept(), non_neg_integer()) -> boolean().
accepted(#config{dicts = Dicts}, all, Id) ->
    is_map_key(Id, Dicts);
accepted(#config{dicts = Dicts}, Ids, Id) when is_list(Ids) ->
    lists:member(Id, Ids) andalso is_map_key(Id, Dicts);
accepted(_Config, none, _Id) ->
    false.

%% @doc Which directions of a method may be compressed without a dictionary,
%% as `{Requests, Responses}'.
-spec directions(directions()) -> {mode(), mode()}.
directions(Mode) when is_atom(Mode) ->
    {mode(Mode), mode(Mode)};
directions(#{} = Directions) ->
    {mode(maps:get(requests, Directions, none)), mode(maps:get(responses, Directions, none))}.

mode(none) -> none;
mode(message) -> message;
mode(stream) -> stream;
mode(Other) -> erlang:error({badarg, {compression, Other}}).

%%% Messages

%% @doc Compresses `Message' with dictionary `Id' into a ZSTD_MESSAGE
%% payload, or returns `none' when that would not be shorter than the
%% message, which then travels uncompressed. Without a dictionary, messages
%% under 256 bytes are not tried.
-spec compress(config(), non_neg_integer(), iodata()) -> {ok, binary()} | none.
compress(#config{dicts = Dicts, level = Level}, Id, Message) ->
    Size = iolist_size(Message),
    case Dicts of
        #{?PLAIN_ID := _} when Id =:= ?PLAIN_ID, Size < ?PLAIN_MIN_SIZE ->
            none;
        #{Id := #dict{cdict = CDict} = Dict} ->
            Key = {?MODULE, compress, cache_key(Dict), Level},
            Context = context(Key, compress, compress_options(CDict, Level)),
            try zstd:compress(Message, Context) of
                Frame ->
                    keep(Key, Context, Size),
                    case iolist_to_binary(Frame) of
                        <<_:4/binary, Payload/binary>> when byte_size(Payload) < Size ->
                            {ok, Payload};
                        _ ->
                            none
                    end
            catch
                error:_ ->
                    erase(Key),
                    none
            end;
        #{} ->
            none
    end.

compress_options(undefined, Level) ->
    #{compressionLevel => Level, checksumFlag => false, contentSizeFlag => true};
compress_options(CDict, _Level) ->
    %% The dictionary's level applies.
    #{dictionary => CDict, checksumFlag => false, contentSizeFlag => true, dictIDFlag => true}.

%% Contexts are kept by what their dictionary is, not by which
%% configuration holds it, so a process that sees many configurations keeps
%% one context per dictionary.
cache_key(#dict{info = #{id := Id, fingerprint := Fingerprint}}) -> {Id, Fingerprint}.

context(Key, Mode, Options) ->
    case get(Key) of
        undefined ->
            {ok, Context} = zstd:context(Mode, Options),
            Context;
        Context ->
            Context
    end.

%% Keeps a context for the next message, unless it grew for a large one.
keep(Key, Context, Size) when Size =< ?MAX_KEPT_MESSAGE ->
    put(Key, Context),
    ok;
keep(Key, Context, _Size) ->
    erase(Key),
    zstd:close(Context).

%% @doc Decompresses a ZSTD_MESSAGE payload with the dictionaries `Accept'
%% allows. A message larger than `Max' is rejected before it is
%% decompressed. Returns the message and the ID of its dictionary.
-spec inflate(config(), accept(), binary(), non_neg_integer()) ->
    {ok, binary(), non_neg_integer()}
    | {error, {too_large, non_neg_integer()} | {invalid, binary()}}.
inflate(_Config, none, _Payload, _Max) ->
    {error, {invalid, <<"compressed message where no dictionary is accepted">>}};
inflate(Config, Accept, Payload, Max) ->
    case parse_header(Payload) of
        error ->
            {error, {invalid, <<"invalid ZSTD_MESSAGE payload">>}};
        {ok, _Id, Size} when Size > Max ->
            {error, {too_large, Size}};
        {ok, Id, Size} ->
            case accepted(Config, Accept, Id) of
                true -> decompress(maps:get(Id, Config#config.dicts), Payload, Size);
                false -> {error, {invalid, not_accepted(Id)}}
            end
    end.

not_accepted(?PLAIN_ID) ->
    <<"compressed message without a dictionary, which is not accepted">>;
not_accepted(Id) ->
    iolist_to_binary(
        io_lib:format("compressed message uses dictionary ~b, which is not accepted", [Id])
    ).

decompress(#dict{info = #{id := Id}, ddict = DDict} = Dict, Payload, Size) ->
    Key = {?MODULE, decompress, cache_key(Dict)},
    Options =
        case DDict of
            undefined -> #{};
            _ -> #{dictionary => DDict}
        end,
    Context = context(Key, decompress, Options),
    try iolist_to_binary(zstd:decompress([?FRAME_MAGIC, Payload], Context)) of
        Message when byte_size(Message) =:= Size ->
            keep(Key, Context, Size),
            {ok, Message, Id};
        _ ->
            erase(Key),
            {error, {invalid, <<"compressed message decompressed to the wrong size">>}}
    catch
        error:{zstd_error, Reason} ->
            erase(Key),
            {error, {invalid, iolist_to_binary(["compressed message is corrupt: ", Reason])}};
        error:_ ->
            erase(Key),
            {error, {invalid, <<"compressed message is corrupt">>}}
    end.

%% @doc The digested dictionary `Id' of `Config', for compressing and for
%% decompressing; `undefined' for compression without a dictionary (ID 0),
%% `error' for one it does not hold.
-spec dictionaries(config(), non_neg_integer()) ->
    {zstd:dict() | undefined, zstd:dict() | undefined} | error.
dictionaries(#config{dicts = Dicts}, Id) ->
    case Dicts of
        #{Id := #dict{cdict = CDict, ddict = DDict}} -> {CDict, DDict};
        #{} -> error
    end.

%% @doc Validates that `Payload' is exactly one magicless zstd frame with a
%% Frame_Content_Size, and returns its Dictionary_ID (0 when it has none)
%% and content size. It walks the block headers to find the frame's end, so
%% trailing bytes, a second frame included, are rejected. Block contents are
%% not checked; decompression does that.
-spec parse_header(binary()) -> {ok, non_neg_integer(), non_neg_integer()} | error.
parse_header(<<FcsFlag:2, Single:1, _Unused:1, 0:1, Checksum:1, DictFlag:2, Rest0/binary>>) ->
    DictLength = element(DictFlag + 1, {0, 1, 2, 4}),
    SizeLength =
        case {FcsFlag, Single} of
            {0, 1} -> 1;
            {0, 0} -> 0;
            {1, _} -> 2;
            {2, _} -> 4;
            {3, _} -> 8
        end,
    WindowLength = 1 - Single,
    case Rest0 of
        _ when SizeLength =:= 0 ->
            error;
        <<_:WindowLength/binary, DictId:DictLength/little-unit:8, Size0:SizeLength/little-unit:8,
            Blocks/binary>> ->
            Size =
                case SizeLength of
                    2 -> Size0 + 256;
                    _ -> Size0
                end,
            case blocks(Blocks) of
                {ok, Rest} when Checksum =:= 0, Rest =:= <<>> -> {ok, DictId, Size};
                {ok, <<_:4/binary>>} when Checksum =:= 1 -> {ok, DictId, Size};
                _ -> error
            end;
        _ ->
            error
    end;
parse_header(_) ->
    error.

%% Walks blocks up to the Last_Block, returning what follows it.
blocks(<<Header:24/little, Rest/binary>>) ->
    Last = Header band 1,
    Size = Header bsr 3,
    Length =
        case (Header bsr 1) band 3 of
            %% Raw_Block and Compressed_Block.
            0 -> Size;
            2 -> Size;
            %% RLE_Block.
            1 -> 1;
            3 -> reserved
        end,
    case Rest of
        _ when Length =:= reserved -> error;
        <<_:Length/binary, Next/binary>> when Last =:= 1 -> {ok, Next};
        <<_:Length/binary, Next/binary>> -> blocks(Next);
        _ -> error
    end;
blocks(_) ->
    error.
