%% @doc The `bench' subcommand: the load client of bench/README.md. It
%% connects, runs one scenario from the start of the warmup to the end of the
%% measured window, and prints the result as one JSON line.
%%
%% It runs every scenario and compression mode of the contract except what
%% the runtime does not have yet: the validation scenarios (no
%% `ValidationBench' bindings) and connection metadata, for which it exits
%% with a usage error. It reports no `wire_bytes_per_msg': erlang_quic counts
%% application bytes, not the UDP payload the other clients count.
-module(trevrpc_interop_bench).

-export([run/1, bench/1]).

-define(ECHO, trevrpc_interop_v1_echo).
-define(COMPRESSED_ECHO, trevrpc_interop_v1_compressed_echo).
-define(UNRELIABLE_ECHO, trevrpc_interop_v1_unreliable_echo).
%% How long operations still running when the window ends get to finish
%% before their deadline cancels them, in milliseconds.
-define(GRACE_MS, 10000).
%% How long a worker pauses after a failed operation, so that a dead
%% connection does not turn into a busy loop.
-define(ERROR_BACKOFF_MS, 1).
%% How long after its send a paced message's echo may arrive and still count
%% as delivered, in nanoseconds.
-define(PACED_DEADLINE, 500000000).
-define(BENCH_PAD, <<"bench-pad">>).
-define(MAX_METADATA_SIZE, 8192).
-define(RECEIVED, <<"interop-received">>).
-define(SECOND, 1000000000).

-define(SCENARIOS, [
    "unary",
    "server-stream",
    "client-stream",
    "bidi",
    "unreliable-server-stream",
    "unreliable-client-stream",
    "paced",
    "unreliable-paced",
    "validation-unary",
    "validation-client-stream"
]).

-record(config, {
    flags :: #{string() => string()},
    transport :: quic | tcp,
    scenario :: string(),
    size :: non_neg_integer(),
    metadata :: non_neg_integer(),
    metadata_scope :: string(),
    concurrency :: pos_integer(),
    connections :: pos_integer(),
    messages :: pos_integer(),
    rate :: float(),
    duration :: float(),
    warmup :: float(),
    payload :: repeat | records,
    compression :: string(),
    validation :: string()
}).

%% What one worker measured: latencies in nanoseconds.
-record(tally, {
    latencies = [] :: [non_neg_integer()],
    messages = 0 :: non_neg_integer(),
    lost = 0 :: non_neg_integer(),
    errors = 0 :: non_neg_integer()
}).

%% The window every worker measures, in erlang:monotonic_time(nanosecond)
%% units, and the deadline of every call, in millisecond units.
-record(window, {
    start :: integer(),
    'end' :: integer(),
    deadline :: integer()
}).

%% @doc Runs the bench client and returns its exit status.
-spec run(#{string() => string()}) -> non_neg_integer().
run(Flags) ->
    case bench(Flags) of
        {ok, Result} ->
            io:put_chars([json:encode(Result), $\n]),
            0;
        {error, Message} ->
            %% On one line: run.py retries a client whose last line says it
            %% could not connect or completed no operation.
            io:format(standard_error, "bench: ~ts~n", [Message]),
            1
    end.

%% @doc Runs the scenario the flags describe and returns the result line's
%% fields, or why it measured nothing. Bad flags throw `{usage, Message}'.
-spec bench(#{string() => string()}) -> {ok, map()} | {error, unicode:chardata()}.
bench(Flags) ->
    Config = config(Flags),
    case connect(Config) of
        {ok, Channels} ->
            try
                measure(Config, Channels)
            after
                lists:foreach(fun trevrpc_channel:close/1, Channels)
            end;
        {error, #{code := Code, message := Message}} ->
            {error, io_lib:format("could not connect: ~s: ~ts", [Code, Message])}
    end.

%%% Configuration

config(Flags) ->
    Config = #config{
        flags = Flags,
        transport =
            case flag(Flags, "transport", "quic") of
                "quic" -> quic;
                "tcp" -> tcp;
                Other -> usage("unknown --transport ~s", [Other])
            end,
        scenario = flag(Flags, "scenario", ""),
        size = integer(Flags, "size", 64),
        metadata = integer(Flags, "metadata-size", 0),
        metadata_scope = flag(Flags, "metadata-scope", "call"),
        concurrency = integer(Flags, "concurrency", 1),
        connections = integer(Flags, "connections", 1),
        messages = integer(Flags, "messages", 100),
        rate = number(Flags, "rate", 100.0),
        duration = number(Flags, "duration", 5.0),
        warmup = number(Flags, "warmup", 1.0),
        payload =
            case flag(Flags, "payload", "repeat") of
                "repeat" -> repeat;
                "records" -> records;
                Other -> usage("unknown --payload ~s", [Other])
            end,
        compression = flag(Flags, "compression", "none"),
        validation = flag(Flags, "validation", "none")
    },
    validate(Config),
    Config.

validate(#config{scenario = Scenario} = Config) ->
    lists:member(Scenario, ?SCENARIOS) orelse usage("unknown --scenario ~p", [Scenario]),
    case compression_scenarios(Config#config.compression) of
        unknown ->
            usage("unknown --compression ~p", [Config#config.compression]);
        all ->
            ok;
        Scenarios ->
            lists:member(Scenario, Scenarios) orelse
                usage("--compression ~s does not run the ~s scenario", [
                    Config#config.compression, Scenario
                ])
    end,
    uses_dictionary(Config) =:= maps:is_key("dictionary", Config#config.flags) orelse
        usage("--compression ~s and --dictionary go together", [Config#config.compression]),
    lists:member(Config#config.metadata_scope, ["call", "connection"]) orelse
        usage("unknown --metadata-scope ~p", [Config#config.metadata_scope]),
    lists:member(Config#config.validation, ["none", "standard", "cel"]) orelse
        usage("unknown --validation ~p", [Config#config.validation]),
    Config#config.validation =:= "none" orelse validated(Config) orelse
        usage("--validation ~s runs the validation scenarios only", [Config#config.validation]),
    Checks = [
        {Config#config.size >= 0, "--size must not be negative"},
        {
            Config#config.metadata >= 0 andalso Config#config.metadata =< ?MAX_METADATA_SIZE,
            "--metadata-size must be between 0 and 8192"
        },
        {Config#config.concurrency >= 1, "--concurrency must be at least 1"},
        {
            Config#config.connections >= 1 andalso
                Config#config.connections =< Config#config.concurrency,
            "--connections must be between 1 and --concurrency"
        },
        {
            Config#config.messages >= 1 andalso Config#config.messages < 1 bsl 32,
            "--messages must be between 1 and 2^32-1"
        },
        {
            Config#config.rate > 0 andalso Config#config.rate =< 1.0e9,
            "--rate must be positive and at most 10^9"
        },
        {Config#config.duration > 0, "--duration must be positive"},
        {Config#config.warmup >= 0, "--warmup must not be negative"}
    ],
    [usage(Message, []) || {false, Message} <- Checks],
    %% What the runtime cannot do yet.
    validated(Config) andalso
        usage("the ~s scenario is not implemented: there are no ValidationBench bindings", [
            Scenario
        ]),
    Config#config.metadata_scope =:= "connection" andalso
        usage("--metadata-scope connection is not implemented: no connection metadata", []),
    ok.

%% The scenarios each --compression mode runs. The stream modes call
%% CompressedEcho, whose streaming methods allow stream compression and whose
%% Unary allows compression without a dictionary.
compression_scenarios("none") -> all;
compression_scenarios("dictionary") -> ["unary", "client-stream", "bidi"];
compression_scenarios("stream") -> ["unary", "client-stream", "bidi"];
compression_scenarios("stream-dictionary") -> ["unary", "client-stream", "bidi"];
compression_scenarios(_) -> unknown.

uses_dictionary(#config{compression = Mode}) ->
    Mode =:= "dictionary" orelse Mode =:= "stream-dictionary".

stream_compressed(#config{compression = Mode}) ->
    Mode =:= "stream" orelse Mode =:= "stream-dictionary".

paced(#config{scenario = Scenario}) ->
    Scenario =:= "paced" orelse Scenario =:= "unreliable-paced".

validated(#config{scenario = Scenario}) ->
    Scenario =:= "validation-unary" orelse Scenario =:= "validation-client-stream".

flag(Flags, Name, Default) ->
    maps:get(Name, Flags, Default).

integer(Flags, Name, Default) ->
    case Flags of
        #{Name := Value} ->
            try
                list_to_integer(Value)
            catch
                error:badarg -> usage("--~s: expected an integer, got ~p", [Name, Value])
            end;
        #{} ->
            Default
    end.

number(Flags, Name, Default) ->
    case Flags of
        #{Name := Value} ->
            case string:to_float(Value) of
                {Float, []} ->
                    Float;
                _ ->
                    try
                        float(list_to_integer(Value))
                    catch
                        error:badarg -> usage("--~s: expected a number, got ~p", [Name, Value])
                    end
            end;
        #{} ->
            Default
    end.

-spec usage(string(), list()) -> no_return().
usage(Format, Args) ->
    throw({usage, "bench: " ++ lists:flatten(io_lib:format(Format, Args))}).

%%% Running

%% One channel per --connections, each with its own connection.
connect(#config{connections = Count, transport = Transport, flags = Flags}) ->
    connect(Count, Transport, Flags, []).

connect(0, _Transport, _Flags, Channels) ->
    {ok, lists:reverse(Channels)};
connect(Count, Transport, Flags, Channels) ->
    case trevrpc_interop_client:connect(Flags, Transport) of
        {ok, Channel} ->
            connect(Count - 1, Transport, Flags, [Channel | Channels]);
        {error, _} = Error ->
            lists:foreach(fun trevrpc_channel:close/1, Channels),
            Error
    end.

measure(Config, Channels) ->
    Now = erlang:monotonic_time(nanosecond),
    Start = Now + round(Config#config.warmup * ?SECOND),
    End = Start + round(Config#config.duration * ?SECOND),
    Window = #window{
        start = Start,
        'end' = End,
        deadline = erlang:convert_time_unit(End, nanosecond, millisecond) + ?GRACE_MS
    },
    Parent = self(),
    Count = length(Channels),
    Workers = [
        spawn_link(fun() ->
            Channel = lists:nth(1 + Index rem Count, Channels),
            Payload = trevrpc_interop_payload:new(
                Config#config.payload, Config#config.size, Index
            ),
            Parent ! {self(), work(Config, Channel, Payload, Window)}
        end)
     || Index <- lists:seq(0, Config#config.concurrency - 1)
    ],
    Tallies = [
        receive
            {Worker, Tally} -> Tally
        end
     || Worker <- Workers
    ],
    summarize(Config, trevrpc_channel:transport(hd(Channels)), Tallies).

%% What one worker runs and measures.
work(Config, Channel, Payload, Window) ->
    case paced(Config) of
        true -> paced(Config, Channel, Payload, Window);
        false -> loop(operation(Config, Channel, Window), init(Config, Payload), Window, #tally{})
    end.

%% A worker's state between operations: its payload, the fixed message of
%% the unreliable scenarios, and the Bidi call it keeps open.
init(#config{scenario = "unreliable-" ++ _}, Payload) ->
    {Message, Next} = trevrpc_interop_payload:next(Payload),
    #{payload => Next, message => Message, call => undefined};
init(_Config, Payload) ->
    #{payload => Payload, call => undefined}.

%% Runs the operation back to back until the window ends, counting only the
%% operations that start and finish within it.
loop(Operation, State, #window{start = Start, 'end' = End} = Window, Tally) ->
    Began = erlang:monotonic_time(nanosecond),
    case Began >= End of
        true ->
            close(State),
            Tally;
        false ->
            Result = Operation(State),
            Finished = erlang:monotonic_time(nanosecond),
            Inside = Began >= Start andalso Finished =< End,
            case Result of
                {ok, Messages, Lost, Next} when Inside ->
                    loop(Operation, Next, Window, Tally#tally{
                        latencies = [Finished - Began | Tally#tally.latencies],
                        messages = Tally#tally.messages + Messages,
                        lost = Tally#tally.lost + Lost
                    });
                {ok, _Messages, _Lost, Next} ->
                    loop(Operation, Next, Window, Tally);
                {error, Next} ->
                    timer:sleep(?ERROR_BACKOFF_MS),
                    Errors =
                        Tally#tally.errors +
                            case Inside of
                                true -> 1;
                                false -> 0
                            end,
                    loop(Operation, Next, Window, Tally#tally{errors = Errors})
            end
    end.

close(#{call := undefined}) -> ok;
close(#{call := Call}) -> trevrpc_client:cancel(Call).

%% The options every call carries: the deadline that ends what is left
%% after the window, and the bench-pad entry.
options(#config{metadata = 0}, #window{deadline = Deadline}) ->
    #{deadline => Deadline};
options(#config{metadata = Size}, #window{deadline = Deadline}) ->
    #{deadline => Deadline, metadata => #{?BENCH_PAD => binary:copy(<<"x">>, Size)}}.

%% The operation of each scenario of bench/README.md: a function of the
%% worker's state that returns the application messages it carried and lost,
%% and the next state.
operation(#config{scenario = "unary"} = Config, Channel, Window) ->
    Service = service(Config),
    Options = options(Config, Window),
    fun(#{payload := Payload} = State) ->
        {Message, Next} = trevrpc_interop_payload:next(Payload),
        Sent = byte_size(Message),
        case Service:unary(Channel, #{message => Message}, Options) of
            {ok, #{message := <<_:Sent/binary>>}, _} -> {ok, 1, 0, State#{payload := Next}};
            _ -> {error, State#{payload := Next}}
        end
    end;
operation(#config{scenario = "server-stream", messages = Count} = Config, Channel, Window) ->
    Options = options(Config, Window),
    fun(#{payload := Payload} = State) ->
        {Message, Next} = trevrpc_interop_payload:next(Payload),
        Request = #{message => Message, count => Count},
        case ?ECHO:server_stream(Channel, Request, Options) of
            {ok, Call} ->
                Received = finish(Call, receive_all(Call, byte_size(Message), 0)),
                case Received of
                    {ok, Count, _Trailers} -> {ok, Count, 0, State#{payload := Next}};
                    _ -> {error, State#{payload := Next}}
                end;
            {error, _} ->
                {error, State#{payload := Next}}
        end
    end;
operation(#config{scenario = "client-stream", messages = Count} = Config, Channel, Window) ->
    Service = service(Config),
    Options = options(Config, Window),
    fun(#{payload := Payload} = State) ->
        case Service:client_stream(Channel, Options) of
            {ok, Call} ->
                {Sent, Next} = send_payloads(Call, Payload, Count, 0),
                Result = finish(
                    Call,
                    case Sent of
                        error -> error;
                        _ -> trevrpc_client:close_and_recv(Call)
                    end
                ),
                case Result of
                    {ok, #{index := Count, message := Echo}} when byte_size(Echo) =:= Sent ->
                        {ok, Count, 0, State#{payload := Next}};
                    _ ->
                        {error, State#{payload := Next}}
                end;
            {error, _} ->
                {error, State}
        end
    end;
operation(#config{scenario = "bidi"} = Config, Channel, Window) ->
    Service = service(Config),
    Options = options(Config, Window),
    fun(#{payload := Payload, call := Open} = State) ->
        {Message, Next} = trevrpc_interop_payload:next(Payload),
        Sent = byte_size(Message),
        maybe
            {ok, Call} ?=
                case Open of
                    undefined -> Service:bidi(Channel, Options);
                    _ -> {ok, Open}
                end,
            ok ?= trevrpc_client:send(Call, #{message => Message}),
            {ok, #{message := <<_:Sent/binary>>}} ?= trevrpc_client:recv(Call),
            {ok, 1, 0, State#{payload := Next, call := Call}}
        else
            _ ->
                close(State),
                {error, State#{payload := Next, call := undefined}}
        end
    end;
operation(
    #config{scenario = "unreliable-server-stream", messages = Count} = Config, Channel, Window
) ->
    Options = options(Config, Window),
    fun(#{message := Message} = State) ->
        Result =
            case ?UNRELIABLE_ECHO:bidi(Channel, Options) of
                {ok, Call} ->
                    finish(
                        Call,
                        maybe
                            ok ?= trevrpc_client:send(Call, #{message => Message, count => Count}),
                            ok ?= trevrpc_client:close_send(Call),
                            receive_all(Call, byte_size(Message), 0)
                        end
                    );
                Error ->
                    Error
            end,
        case Result of
            {ok, Received, _Trailers} when Received =< Count ->
                {ok, Received, Count - Received, State};
            _ ->
                {error, State}
        end
    end;
operation(
    #config{scenario = "unreliable-client-stream", messages = Count} = Config, Channel, Window
) ->
    Options = options(Config, Window),
    fun(#{message := Message} = State) ->
        Result =
            case ?UNRELIABLE_ECHO:bidi(Channel, Options) of
                {ok, Call} ->
                    finish(
                        Call,
                        maybe
                            ok ?= send_n(Call, #{message => Message}, Count),
                            ok ?= trevrpc_client:close_send(Call),
                            {ok, 0, Trailers} ?= receive_all(Call, byte_size(Message), 0),
                            received(Trailers)
                        end
                    );
                Error ->
                    Error
            end,
        case Result of
            {ok, Received} when Received =< Count -> {ok, Received, Count - Received, State};
            _ -> {error, State}
        end
    end.

%% The service whose calls the compression mode uses.
service(Config) ->
    case stream_compressed(Config) of
        true -> ?COMPRESSED_ECHO;
        false -> ?ECHO
    end.

%% Cancels the call unless it already ended, and returns `Result'.
finish(Call, Result) ->
    trevrpc_client:cancel(Call),
    Result.

%% Reads responses of `Size' bytes until the call ends.
receive_all(Call, Size, Received) ->
    case trevrpc_client:recv(Call) of
        {ok, #{message := <<_:Size/binary>>}} -> receive_all(Call, Size, Received + 1);
        {eof, Trailers} -> {ok, Received, Trailers};
        Other -> Other
    end.

%% Sends `Count' requests, each with the payload's next message, and
%% returns how many bytes they carried, or `error'.
send_payloads(_Call, Payload, 0, Sent) ->
    {Sent, Payload};
send_payloads(Call, Payload, Count, Sent) ->
    {Message, Next} = trevrpc_interop_payload:next(Payload),
    case trevrpc_client:send(Call, #{message => Message}) of
        ok -> send_payloads(Call, Next, Count - 1, Sent + byte_size(Message));
        {error, _} -> {error, Next}
    end.

send_n(_Call, _Request, 0) ->
    ok;
send_n(Call, Request, Count) ->
    case trevrpc_client:send(Call, Request) of
        ok -> send_n(Call, Request, Count - 1);
        Error -> Error
    end.

%% How many requests the server received, from the status's trailing
%% metadata.
received(#{?RECEIVED := Value}) ->
    try binary_to_integer(Value) of
        Count when Count >= 0 -> {ok, Count};
        _ -> error
    catch
        error:badarg -> error
    end;
received(#{}) ->
    error.

%%% Paced scenarios

%% A paced worker keeps one Bidi stream open, on Echo for paced and on
%% UnreliableEcho, whose messages may travel as datagrams, for
%% unreliable-paced. It sends a request asking for one echo every interval,
%% from the start of the warmup until PACED_DEADLINE after the window, and
%% reads the echoes in between. Each message scheduled inside the window is
%% one operation: delivered, with the time from its send to its echo, if the
%% echo arrives within PACED_DEADLINE of the send, and lost otherwise.
paced(Config, Channel, Payload, #window{start = Start, 'end' = End} = Window) ->
    Interval = max(1, trunc(?SECOND / Config#config.rate)),
    Base = erlang:monotonic_time(nanosecond),
    Total = (End + ?PACED_DEADLINE - Base) div Interval + 1,
    Schedule = #{base => Base, interval => Interval, total => Total},
    Open =
        case Config#config.scenario of
            "paced" -> fun(Options) -> ?ECHO:bidi(Channel, Options) end;
            "unreliable-paced" -> fun(Options) -> ?UNRELIABLE_ECHO:bidi(Channel, Options) end
        end,
    {Sent, Echoed, Errors} = paced_streams(
        Open, options(Config, Window), Schedule, Window, Payload, 0, #{}, #{}, 0
    ),
    lists:foldl(
        fun(Index, Tally) ->
            Scheduled = Base + Index * Interval,
            Delivered =
                case {Sent, Echoed} of
                    {#{Index := At}, #{Index := Arrived}} when
                        Arrived - At =< ?PACED_DEADLINE
                    ->
                        {ok, Arrived - At};
                    _ ->
                        lost
                end,
            case Scheduled >= Start andalso Scheduled < End of
                false ->
                    Tally;
                true when Delivered =:= lost ->
                    Tally#tally{lost = Tally#tally.lost + 1};
                true ->
                    {ok, Rtt} = Delivered,
                    Tally#tally{
                        latencies = [Rtt | Tally#tally.latencies],
                        messages = Tally#tally.messages + 1
                    }
            end
        end,
        #tally{errors = Errors},
        lists:seq(0, Total - 1)
    ).

%% Opens streams until every scheduled message was sent: after a failure,
%% the messages whose time passed while the stream was down are lost, not
%% sent late. `Sent' and `Echoed' map each message's number to when it was
%% sent and when its first echo arrived.
paced_streams(
    _Open, _Options, #{total := Total}, _Window, _Payload, Next, Sent, Echoed, Errors
) when
    Next >= Total
->
    {Sent, Echoed, Errors};
paced_streams(Open, Options, Schedule, Window, Payload, Next, Sent, Echoed, Errors) ->
    case Open(Options) of
        {ok, Call} ->
            case paced_stream(Call, Schedule, Payload, Next, Sent, Echoed) of
                {ok, Sent1, Echoed1} ->
                    {Sent1, Echoed1, Errors};
                {error, Payload1, Next1, Sent1, Echoed1} ->
                    trevrpc_client:cancel(Call),
                    paced_failed(
                        Open, Options, Schedule, Window, Payload1, Next1, Sent1, Echoed1, Errors
                    )
            end;
        {error, _} ->
            paced_failed(Open, Options, Schedule, Window, Payload, Next, Sent, Echoed, Errors)
    end.

paced_failed(Open, Options, Schedule, Window, Payload, Next, Sent, Echoed, Errors) ->
    #{base := Base, interval := Interval} = Schedule,
    #window{start = Start, 'end' = End} = Window,
    Now = erlang:monotonic_time(nanosecond),
    Counted =
        case Now >= Start andalso Now =< End of
            true -> Errors + 1;
            false -> Errors
        end,
    timer:sleep(?ERROR_BACKOFF_MS),
    Resume = max(Next, (erlang:monotonic_time(nanosecond) - Base) div Interval + 1),
    paced_streams(Open, Options, Schedule, Window, Payload, Resume, Sent, Echoed, Counted).

%% Sends the messages from `Next' on, each at its time, reading echoes while
%% it waits, then closes the request side and reads the rest.
paced_stream(Call, #{total := Total}, _Payload, Next, Sent, Echoed) when Next >= Total ->
    case trevrpc_client:close_send(Call) of
        ok -> paced_drain(Call, Total, Sent, Echoed);
        {error, _} -> {error, undefined, Next, Sent, Echoed}
    end;
paced_stream(Call, Schedule, Payload, Next, Sent, Echoed) ->
    #{base := Base, interval := Interval} = Schedule,
    Now = erlang:monotonic_time(nanosecond),
    Due = Base + Next * Interval,
    case Now >= Due of
        true ->
            %% The echoes that already arrived first, so that a worker that
            %% fell behind its schedule, and sends without waiting, still
            %% reads them as they come, as the other runtimes' workers do on
            %% a receiver of their own.
            case paced_arrived(Call, Sent, Echoed) of
                {ok, Echoed1} ->
                    {Message, Payload1} = trevrpc_interop_payload:next(Payload),
                    Request = #{message => paced_message(Next, Message), count => 1},
                    At = erlang:monotonic_time(nanosecond),
                    case trevrpc_client:send(Call, Request) of
                        ok ->
                            paced_stream(
                                Call, Schedule, Payload1, Next + 1, Sent#{Next => At}, Echoed1
                            );
                        {error, _} ->
                            {error, Payload1, Next + 1, Sent#{Next => At}, Echoed1}
                    end;
                {error, Echoed1} ->
                    {error, Payload, Next, Sent, Echoed1}
            end;
        false ->
            %% Rounded up, so that the worker waits instead of spinning
            %% through the last fraction of a millisecond: a send that goes
            %% a little late is timed from when it went.
            Wait = (Due - Now + 999999) div 1000000,
            case trevrpc_client:recv(Call, Wait) of
                timeout ->
                    paced_stream(Call, Schedule, Payload, Next, Sent, Echoed);
                {ok, #{message := Echo}} ->
                    case echoed(Echo, Sent, Echoed) of
                        {ok, Echoed1} -> paced_stream(Call, Schedule, Payload, Next, Sent, Echoed1);
                        error -> {error, Payload, Next, Sent, Echoed}
                    end;
                _Ended ->
                    {error, Payload, Next, Sent, Echoed}
            end
    end.

%% Records the echoes that already arrived, without waiting for more.
paced_arrived(Call, Sent, Echoed) ->
    case trevrpc_client:recv(Call, 0) of
        timeout ->
            {ok, Echoed};
        {ok, #{message := Echo}} ->
            case echoed(Echo, Sent, Echoed) of
                {ok, Echoed1} -> paced_arrived(Call, Sent, Echoed1);
                error -> {error, Echoed}
            end;
        _Ended ->
            {error, Echoed}
    end.

paced_drain(Call, Next, Sent, Echoed) ->
    case trevrpc_client:recv(Call) of
        {ok, #{message := Echo}} ->
            case echoed(Echo, Sent, Echoed) of
                {ok, Echoed1} -> paced_drain(Call, Next, Sent, Echoed1);
                error -> {error, undefined, Next, Sent, Echoed}
            end;
        {eof, _Trailers} ->
            {ok, Sent, Echoed};
        {error, _} ->
            {error, undefined, Next, Sent, Echoed}
    end.

%% Records when the echo of the message it carries arrived, unless an echo
%% of it already did.
echoed(Echo, Sent, Echoed) ->
    Arrived = erlang:monotonic_time(nanosecond),
    case paced_sequence(Echo) of
        {ok, Sequence} when is_map_key(Sequence, Echoed) -> {ok, Echoed};
        {ok, Sequence} when is_map_key(Sequence, Sent) -> {ok, Echoed#{Sequence => Arrived}};
        _ -> error
    end.

%% The request message of paced message number `Sequence': the number in
%% decimal and a space in place of the start of the payload.
paced_message(Sequence, Payload) ->
    Prefix = <<(integer_to_binary(Sequence))/binary, " ">>,
    case Payload of
        <<_:(byte_size(Prefix))/binary, Rest/binary>> -> <<Prefix/binary, Rest/binary>>;
        _ -> Prefix
    end.

paced_sequence(Echo) ->
    case binary:split(Echo, <<" ">>) of
        [Digits, _] ->
            try binary_to_integer(Digits) of
                Sequence when Sequence >= 0 -> {ok, Sequence};
                _ -> error
            catch
                error:badarg -> error
            end;
        _ ->
            error
    end.

%%% Result

summarize(Config, Transport, Tallies) ->
    Latencies = lists:sort(lists:append([T#tally.latencies || T <- Tallies])),
    Messages = lists:sum([T#tally.messages || T <- Tallies]),
    Lost = lists:sum([T#tally.lost || T <- Tallies]),
    Errors = lists:sum([T#tally.errors || T <- Tallies]),
    Seconds = Config#config.duration,
    case length(Latencies) of
        0 ->
            {error,
                io_lib:format("no operation completed in the ~ss measured window (~b failed)", [
                    float_to_list(Seconds, [short]), Errors
                ])};
        Ops ->
            Sorted = list_to_tuple(Latencies),
            Result = #{
                scenario => list_to_binary(Config#config.scenario),
                transport =>
                    case Transport of
                        quic -> <<"quic">>;
                        _ -> <<"qmux">>
                    end,
                size => Config#config.size,
                metadata_size => Config#config.metadata,
                metadata_scope => list_to_binary(Config#config.metadata_scope),
                connections => Config#config.connections,
                concurrency => Config#config.concurrency,
                duration_s => Seconds,
                ops => Ops,
                msgs => Messages,
                errors => Errors,
                lost => Lost,
                ops_per_sec => Ops / Seconds,
                msgs_per_sec => Messages / Seconds,
                bytes_per_sec => Messages / Seconds * Config#config.size,
                latency_us => #{
                    p50 => percentile_us(Sorted, 0.50),
                    p90 => percentile_us(Sorted, 0.90),
                    p99 => percentile_us(Sorted, 0.99),
                    p999 => percentile_us(Sorted, 0.999)
                },
                payload => atom_to_binary(Config#config.payload),
                compression => list_to_binary(Config#config.compression)
            },
            {ok,
                case paced(Config) of
                    true -> Result#{rate => Config#config.rate};
                    false -> Result
                end}
    end.

%% The nearest-rank percentile `P' of the sorted latencies, in microseconds.
percentile_us(Sorted, P) ->
    Rank = max(1, ceil(P * tuple_size(Sorted))),
    element(Rank, Sorted) / 1000.
