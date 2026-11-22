%% Test helpers that protoc-gen-trevrpc-erlang's and
%% protoc-gen-trevrpc-gleam's tests share: a `CodeGeneratorRequest' built
%% from the descriptors protoc writes, and the files or error of a
%% response.
%%
%% protoc must be on the PATH.
-module(trevrpc_gen_test).

-include_lib("eunit/include/eunit.hrl").

-export([generating/1, options_dir/1, request/3, response/1, fields/1, scratch/0]).

%% How long a test that generates code may take, in seconds.
-define(GENERATING_TIMEOUT, 60).

%% `Tests', each under a timeout long enough for a test that runs protoc
%% and the generator. gpb compiles the generated code, and its modules load
%% on first use, which on a loaded machine can outlast eunit's 5 s default
%% for a test. A list's tests each get the timeout, because a group's
%% timeout does not lift the 5 s default of the tests in it.
generating(Tests) when is_list(Tests) ->
    [generating(Test) || Test <- Tests];
generating(Test) ->
    {timeout, ?GENERATING_TIMEOUT, Test}.

%% The directory of trevrpc/options.proto: TREVRPC_PROTO, or `Default'.
options_dir(Default) ->
    case os:getenv("TREVRPC_PROTO") of
        false -> Default;
        Dir -> Dir
    end.

%% A CodeGeneratorRequest for `Protos', found in the directories `Dirs'.
request(Protos, Parameter, Dirs) ->
    Set = filename:join(scratch(), "set.bin"),
    Protoc = os:find_executable("protoc"),
    ?assertNotEqual(false, Protoc),
    Args =
        ["--include_imports", "--descriptor_set_out=" ++ Set] ++
            lists:append([["-I", Dir] || Dir <- Dirs]) ++
            Protos,
    Port = open_port({spawn_executable, Protoc}, [
        {args, Args}, exit_status, stderr_to_stdout, binary
    ]),
    ?assertEqual({0, <<>>}, wait(Port, <<>>)),
    {ok, Bin} = file:read_file(Set),
    iolist_to_binary([
        [bytes(1, Proto) || Proto <- Protos],
        bytes(2, Parameter),
        [bytes(15, File) || {1, {bytes, File}} <- fields(Bin)]
    ]).

wait(Port, Output) ->
    receive
        {Port, {data, Data}} -> wait(Port, <<Output/binary, Data/binary>>);
        {Port, {exit_status, Status}} -> {Status, Output}
    end.

%% The files of a CodeGeneratorResponse, or its error.
response(Response) ->
    Fields = fields(iolist_to_binary(Response)),
    case [Error || {1, {bytes, Error}} <- Fields] of
        [Error] ->
            {error, Error};
        [] ->
            {ok, [
                {Name, Content}
             || {15, {bytes, File}} <- Fields,
                {1, {bytes, Name}} <- fields(File),
                {15, {bytes, Content}} <- fields(File)
            ]}
    end.

%% A new directory for a test's files.
scratch() ->
    Dir = filename:join([
        os:getenv("TMPDIR", "/tmp"),
        "trevrpc_gen_tests",
        integer_to_list(erlang:unique_integer([positive]))
    ]),
    ok = filelib:ensure_path(Dir),
    Dir.

bytes(Number, Payload) ->
    Data = iolist_to_binary(Payload),
    [varint((Number bsl 3) bor 2), varint(byte_size(Data)), Data].

varint(Value) when Value < 128 -> <<Value>>;
varint(Value) -> <<1:1, (Value band 127):7, (varint(Value bsr 7))/binary>>.

%% A protobuf message's fields, varints and length-delimited ones.
fields(<<>>) ->
    [];
fields(Bin) ->
    {Tag, Rest} = read_varint(Bin, 0, 0),
    case Tag band 7 of
        0 ->
            {Value, Next} = read_varint(Rest, 0, 0),
            [{Tag bsr 3, {varint, Value}} | fields(Next)];
        2 ->
            {Length, R} = read_varint(Rest, 0, 0),
            <<Value:Length/binary, Next/binary>> = R,
            [{Tag bsr 3, {bytes, Value}} | fields(Next)]
    end.

read_varint(<<0:1, B:7, Rest/binary>>, Shift, Acc) ->
    {Acc bor (B bsl Shift), Rest};
read_varint(<<1:1, B:7, Rest/binary>>, Shift, Acc) ->
    read_varint(Rest, Shift + 7, Acc bor (B bsl Shift)).
