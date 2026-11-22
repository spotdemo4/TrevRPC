%% @doc protoc-gen-trevrpc-elixir's files: gpb's message modules, and an
%% Elixir module per service over the Erlang runtime.
%%
%% Each proto file gets gpb's module, as protoc-gen-trevrpc-erlang makes it
%% with `msg_format=maps' (`greeter_pb.erl' for `greeter.proto'), at the
%% root of the output directory, as Erlang modules share one namespace.
%% Messages are gpb's maps, with strings as binaries and message names that
%% carry their package.
%%
%% Each service gets an Elixir module named after its package and name
%% (`Example.Greeter.V1.Greeter' for `example.greeter.v1.Greeter'), at the
%% path Mix's conventions give it (`example/greeter/v1/greeter.ex'), with:
%%
%% - a behaviour, one callback per method, for the module that handles the
%%   service;
%% - `routes/1', the service's routes for `:trevrpc_server', with handlers
%%   from a module that implements the behaviour or from a map of funs, and
%%   each method's message types, compression, datagrams and codec;
%% - per method, a function that calls it on a `:trevrpc_channel', with
%%   optional call options.
%%
%% It is the Erlang plugin's service module (see {@link trevrpc_gen_service})
%% in Elixir: the same routes and call options, from the same checks.
-module(trevrpc_gen_elixir).

-export([files/2, check_prefix/1, service_module/3, camelize/1]).

-export_type([options/0]).

-type options() :: #{
    module_prefix := binary(),
    prefix := binary(),
    pb_suffix := binary()
}.

%% Elixir's reserved words, which a function's definition takes only
%% through `unquote', and the special forms that work the same.
-define(UNQUOTED, [
    <<"true">>,
    <<"false">>,
    <<"nil">>,
    <<"fn">>,
    <<"do">>,
    <<"end">>,
    <<"in">>,
    <<"not">>,
    <<"and">>,
    <<"or">>,
    <<"catch">>,
    <<"rescue">>,
    <<"after">>,
    <<"else">>,
    <<"unquote">>,
    <<"unquote_splicing">>
]).

%% @doc The files for the proto files a `CodeGeneratorRequest' asks for.
-spec files(trevrpc_gen_descriptor:request(), options()) ->
    {ok, [{binary(), iodata()}]} | {error, binary()}.
files(#{file_set := FileSet} = Request, Options) ->
    {Generate, Messages} = trevrpc_gen_descriptor:to_generate(Request),
    maybe
        ok ?= trevrpc_gen_service:check_files(Generate),
        ok ?= check_services(Generate),
        {ok, Services} ?= services(Generate, Options),
        ok ?= check_modules(Generate, Services, Options),
        {ok, Defs} ?= trevrpc_gen_messages:defs(FileSet),
        {ok, MessageFiles} ?=
            trevrpc_gen_messages:generate_files(
                Generate, fun(Name) -> message_module(Name, Options) end, Defs, maps
            ),
        {ok, MessageFiles ++ [service_file(Service, Messages, Options) || Service <- Services]}
    end.

message_module(File, Options) ->
    trevrpc_gen_names:message_module(File, Options).

%% @doc Checks parameter `module_prefix': an Elixir alias, or nothing.
-spec check_prefix(binary()) -> ok | {error, binary()}.
check_prefix(<<>>) ->
    ok;
check_prefix(Prefix) ->
    case re:run(Prefix, <<"^[A-Z][A-Za-z0-9_]*(\\.[A-Z][A-Za-z0-9_]*)*$">>, [{capture, none}]) of
        match ->
            ok;
        nomatch ->
            {error, <<
                "invalid module_prefix \"",
                Prefix/binary,
                "\": expected an Elixir alias, such as MyApp.Rpc"
            >>}
    end.

%% @doc The Elixir module of a service, as its alias's parts: `Prefix''s,
%% then its package's and its name, each camelized (see camelize/1);
%% `example.greeter.v1.Greeter' is `Example.Greeter.V1.Greeter'. An error
%% when a part cannot be one of an alias.
-spec service_module(binary(), binary(), binary()) -> {ok, [binary()]} | {error, binary()}.
service_module(Prefix, Package, Service) ->
    Parts = [P || P <- binary:split(Package, <<".">>, [global]), P =/= <<>>] ++ [Service],
    Alias = [camelize(Part) || Part <- Parts],
    case [Part || Part <- Alias, not alias_part(Part)] of
        [] ->
            {ok, [P || P <- binary:split(Prefix, <<".">>, [global]), P =/= <<>>] ++ Alias};
        _ ->
            trevrpc_gen_errors:new(
                "service ~ts needs Elixir module ~ts, which is no alias: each part must start "
                "with a letter",
                [trevrpc_gen_names:qualified(Package, Service), lists:join(".", Alias)]
            )
    end.

%% @doc A protobuf identifier as Mix's `Macro.camelize/1' has it: its first
%% letter, and every letter after an underscore, upper-case, and the
%% underscores dropped; `greeter_v1' is `GreeterV1', `Greeter' stays.
-spec camelize(binary()) -> binary().
camelize(Name) ->
    iolist_to_binary([upper_first(Word) || Word <- binary:split(Name, <<"_">>, [global])]).

upper_first(<<C, Rest/binary>>) when C >= $a, C =< $z -> <<(C - $a + $A), Rest/binary>>;
upper_first(Word) -> Word.

alias_part(Part) ->
    re:run(Part, <<"^[A-Z][A-Za-z0-9_]*$">>, [{capture, none}]) =:= match.

%% The functions Elixir gives every module, and those that cannot be
%% defined, which no method may be named after.
check_services(Files) ->
    trevrpc_gen_errors:first([
        trevrpc_gen_errors:new(
            "service ~ts RPC method ~ts requires Elixir function ~ts, which Elixir does not allow",
            [trevrpc_gen_names:qualified(Package, Service), Name, Snake]
        )
     || #{package := Package, services := Services} <- Files,
        #{name := Service, methods := Methods} <- Services,
        #{name := Name} <- Methods,
        Snake <- [trevrpc_gen_names:snake(Name)],
        not definable(Snake)
    ]).

%% `when' cannot be a function's name, nor the special forms such as
%% `__block__', nor `__info__', which Elixir defines.
definable(<<"when">>) ->
    false;
definable(Name) ->
    re:run(Name, <<"^__.*__$">>, [{capture, none}]) =:= nomatch.

%% The services of `Files', each with its file, package and Elixir module.
services(Files, #{module_prefix := Prefix}) ->
    lists:foldl(
        fun
            (_, {error, _} = Error) ->
                Error;
            ({File, #{name := Name} = Service}, {ok, Acc}) ->
                #{package := Package} = File,
                case service_module(Prefix, Package, Name) of
                    {ok, Module} ->
                        {ok, Acc ++ [#{file => File, service => Service, module => Module}]};
                    {error, _} = Error ->
                        Error
                end
        end,
        {ok, []},
        [{File, Service} || #{services := Services} = File <- Files, Service <- Services]
    ).

%% Every generated module needs a name, and every Elixir file a path, no
%% other one has.
check_modules(Files, Services, Options) ->
    Described = [
        {Module, <<"service ", (trevrpc_gen_names:qualified(Package, Name))/binary>>}
     || #{file := #{package := Package}, service := #{name := Name}, module := Module} <- Services
    ],
    trevrpc_gen_errors:first([
        trevrpc_gen_errors:distinct(
            [
                {message_module(Name, Options), <<"the messages of ", Name/binary>>}
             || #{name := Name} <- Files
            ],
            <<"module">>
        ),
        trevrpc_gen_errors:distinct(
            [{alias_of(Module), Thing} || {Module, Thing} <- Described], <<"module">>
        ),
        trevrpc_gen_errors:distinct(
            [{path(Module), Thing} || {Module, Thing} <- Described], <<"file">>
        )
    ]).

alias_of(Module) ->
    iolist_to_binary(lists:join(".", Module)).

%% Mix's path of a module: its alias's parts in snake case, as directories
%% and the file.
path(Module) ->
    iolist_to_binary([lists:join("/", [trevrpc_gen_names:snake(Part) || Part <- Module]), ".ex"]).

service_file(#{file := File, service := Service, module := Module}, Messages, Options) ->
    #{name := Name, package := Package} = File,
    Context = #{
        messages_module => message_module(Name, Options),
        messages => Messages
    },
    {path(Module), generate(Name, Package, Service, alias_of(Module), Context)}.

%%% The service module

generate(Source, Package, #{name := Name, methods := Methods}, Module, Context) ->
    Full = trevrpc_gen_names:qualified(Package, Name),
    #{messages_module := Messages} = Context,
    [
        "# Code generated by protoc-gen-trevrpc-elixir. DO NOT EDIT.\n"
        "# Source: ",
        Source,
        "\n\n"
        "defmodule ",
        Module,
        " do\n"
        "  @moduledoc \"\"\"\n"
        "  The `",
        Full,
        "` service: the behaviour of a module that handles it,\n"
        "  its routes for `:trevrpc_server`, and a function per method that calls\n"
        "  it on a `:trevrpc_channel`.\n\n"
        "  Messages are the maps of gpb's `",
        atom(Messages),
        "`, with strings as binaries.\n"
        "  \"\"\"\n",
        [callback(Context, Method) || Method <- Methods],
        handlers_type(Context, Methods),
        routes(Context, Full, Methods),
        [client(Context, Full, Method) || Method <- Methods],
        "end\n"
    ].

callback(Context, #{name := Name} = Method) ->
    [
        "\n  @doc \"Handles `",
        Name,
        "`.\"\n"
        "  @callback ",
        definition(function(Method)),
        "(",
        lists:join(", ", handler_args(Context, Method)),
        ") ::\n              ",
        handler_result(Context, Method),
        "\n"
    ].

handler_args(Context, #{client_streaming := false, input := Input}) ->
    [type(Context, Input), ":trevrpc_call.call()"];
handler_args(_Context, #{client_streaming := true}) ->
    [":trevrpc_call.call()"].

handler_result(Context, #{server_streaming := false, output := Output}) ->
    ["{:ok, ", type(Context, Output), "} | {:error, :trevrpc_status.status()}"];
handler_result(_Context, #{server_streaming := true}) ->
    ":ok | {:error, :trevrpc_status.status()}".

%% A message's type: gpb's type of its map.
type(#{messages_module := Messages}, Name) ->
    [atom(Messages), ".", call(Name), "()"].

handlers_type(_Context, []) ->
    "\n  @typedoc \"A module that implements the behaviour.\"\n  @type handlers :: module() | %{}\n";
handlers_type(Context, Methods) ->
    [
        "\n  @typedoc \"A module that implements the behaviour, or handler funs by method name.\"\n"
        "  @type handlers ::\n"
        "          module()\n"
        "          | %{\n",
        lists:join(",\n", [
            [
                "              optional(",
                atom(function(Method)),
                ") =>\n                (",
                lists:join(", ", handler_args(Context, Method)),
                " ->\n                   ",
                handler_result(Context, Method),
                ")"
            ]
         || Method <- Methods
        ]),
        "\n            }\n"
    ].

routes(_Context, _Service, []) ->
    [
        "\n  @doc \"\"\"\n"
        "  The service's routes, for `:trevrpc_server`: none, as it has no\n"
        "  methods. A handler in the map raises `{:unknown_method, name, []}`.\n"
        "  \"\"\"\n"
        "  @spec routes(handlers()) :: :trevrpc_server.routes()\n"
        "  def routes(module) when Kernel.is_atom(module), do: %{}\n\n"
        "  def routes(handlers) when Kernel.is_map(handlers) do\n"
        "    Enum.each(handlers, fn {name, _} -> :erlang.error({:unknown_method, name, []}) end)\n"
        "    %{}\n"
        "  end\n"
    ];
routes(Context, Service, Methods) ->
    [
        "\n  @doc \"\"\"\n"
        "  The service's routes, for the `routes` of `:trevrpc_server`.\n\n"
        "  `handlers` is a module that implements the behaviour, or a map of\n"
        "  handler funs by method name; a method missing from the map is not\n"
        "  routed, and calls to it fail as `unimplemented`. A name that is no\n"
        "  method raises `{:unknown_method, name, methods}`.\n"
        "  \"\"\"\n"
        "  @spec routes(handlers()) :: :trevrpc_server.routes()\n"
        "  def routes(module) when Kernel.is_atom(module) do\n"
        "    routes(%{\n",
        lists:join(",\n", [
            [
                "      ",
                key(function(Method)),
                "&module.",
                call(function(Method)),
                "/",
                integer_to_list(handler_arity(Method))
            ]
         || Method <- Methods
        ]),
        "\n    })\n"
        "  end\n\n"
        "  def routes(handlers) when Kernel.is_map(handlers) do\n"
        "    routes = %{\n",
        lists:join(",\n", [route(Context, Service, Method) || Method <- Methods]),
        "\n    }\n\n"
        "    Map.new(handlers, fn {name, handler} ->\n"
        "      case routes do\n"
        "        %{^name => {path, route}} -> {path, Map.put(route, :handler, handler)}\n"
        "        %{} -> :erlang.error({:unknown_method, name, Enum.sort(Map.keys(routes))})\n"
        "      end\n"
        "    end)\n"
        "  end\n"
    ].

handler_arity(#{client_streaming := true}) -> 1;
handler_arity(#{client_streaming := false}) -> 2.

route(Context, Service, #{input := Input, output := Output} = Method) ->
    Indent = "           ",
    [
        "      ",
        string_key(function(Method)),
        ":\n        {",
        path(Service, Method),
        ",\n         %{\n",
        lists:join(",\n", [
            [Indent, Field]
         || Field <- [
                ["kind: ", atom(atom_to_binary(trevrpc_gen_service:kind(Method)))]
                | schema_options(Context, Method, {Input, decode}, {Output, encode}, Indent)
            ]
        ]),
        "\n         }}"
    ].

client(Context, Service, #{name := Name, input := Input, output := Output} = Method) ->
    F = function(Method),
    Indent = "          ",
    Schema = [
        "      Map.merge(\n"
        "        %{\n",
        lists:join(",\n", [
            [Indent, Field]
         || Field <- schema_options(Context, Method, {Input, encode}, {Output, decode}, Indent)
        ]),
        "\n        },\n"
        "        options\n"
        "      )\n"
    ],
    {Call, Arity, Parameters, Arguments, Result} =
        case Method of
            #{client_streaming := false, server_streaming := false} ->
                {"unary", 4, [type(Context, Input), ", "], "request, ",
                    client_result(Context, Output)};
            #{client_streaming := false, server_streaming := true} ->
                {"server_stream", 4, [type(Context, Input), ", "], "request, ", call_result()};
            #{client_streaming := true, server_streaming := false} ->
                {"client_stream", 3, [], "", call_result()};
            #{client_streaming := true, server_streaming := true} ->
                {"bidi", 3, [], "", call_result()}
        end,
    Verb =
        case Method of
            #{client_streaming := false} -> "Calls `";
            #{client_streaming := true} -> "Starts a call to `"
        end,
    [
        "\n  @doc \"\"\"\n  ",
        Verb,
        Name,
        "`.\n\n"
        "  `options` are those of `:trevrpc_client.",
        Call,
        "/",
        integer_to_list(Arity),
        "`, and win over the schema's.\n"
        "  \"\"\"\n"
        "  @spec ",
        definition(F),
        "(pid(), ",
        Parameters,
        ":trevrpc_client.options()) ::\n          ",
        Result,
        "\n  def ",
        definition(F),
        "(channel, ",
        Arguments,
        "options \\\\ %{}) do\n"
        "    :trevrpc_client.",
        Call,
        "(\n"
        "      channel,\n"
        "      ",
        path(Service, Method),
        ",\n",
        case Arguments of
            "" -> [];
            _ -> "      request,\n"
        end,
        Schema,
        "    )\n"
        "  end\n"
    ].

client_result(Context, Output) ->
    [
        "{:ok, ",
        type(Context, Output),
        ",\n"
        "           %{headers: :trevrpc_metadata.metadata(), trailers: :trevrpc_metadata.metadata()}}\n"
        "          | {:error, :trevrpc_status.status()}"
    ].

call_result() ->
    "{:ok, :trevrpc_client.call()} | {:error, :trevrpc_status.status()}".

%% The options a method's schema gives its routes and calls, as the Erlang
%% plugin gives them: its message types, how its messages may be compressed
%% and travel as datagrams, and its codec, which encodes one side's
%% messages and decodes the other's.
schema_options(Context, Method, {Input, InputOp}, {Output, OutputOp}, Indent) ->
    #{messages_module := Messages, messages := Descriptors} = Context,
    Codec = fun(Op, Type) ->
        [
            atom_to_binary(Op),
            ": &",
            atom(Messages),
            ".",
            atom_to_binary(Op),
            "_msg(&1, ",
            atom(Type),
            ")"
        ]
    end,
    Ops = lists:sort([{InputOp, Input}, {OutputOp, Output}]),
    [
        ["request_type: ", string(Input)],
        ["response_type: ", string(Output)]
    ] ++
        compression(trevrpc_gen_service:compression(Descriptors, Method)) ++
        unreliable(trevrpc_gen_service:unreliable(Method)) ++
        [
            [
                "codec: %{\n",
                lists:join(",\n", [[Indent, "  ", Codec(Op, Type)] || {Op, Type} <- Ops]),
                "\n",
                Indent,
                "}"
            ]
        ].

compression(none) ->
    [];
compression(#{requests := Requests, responses := Responses}) ->
    [
        [
            "compression: %{requests: ",
            atom(atom_to_binary(Requests)),
            ", responses: ",
            atom(atom_to_binary(Responses)),
            "}"
        ]
    ];
compression(Mode) ->
    [["compression: ", atom(atom_to_binary(Mode))]].

unreliable([]) ->
    [];
unreliable(Keys) ->
    [["unreliable: %{", lists:join(", ", [[atom_to_binary(Key), ": true"] || Key <- Keys]), "}"]].

%% The method path, `/package.Service/Method', as a string literal.
path(Service, #{name := Name}) ->
    string(<<"/", Service/binary, "/", Name/binary>>).

%%% Elixir names

%% A method's function: its name in snake case, as in Erlang.
function(#{name := Name}) ->
    trevrpc_gen_names:snake(Name).

%% A function's name where it is defined, through `unquote' where Elixir
%% reserves it.
definition(Name) ->
    case lists:member(Name, ?UNQUOTED) of
        true -> ["unquote(", atom(Name), ")"];
        false -> Name
    end.

%% `Name' as an atom literal, quoted where it must be, and only there:
%% Elixir warns of quotes it does not need.
atom(Name) ->
    case plain_atom(Name) of
        true -> [":", Name];
        false -> [":", string(Name)]
    end.

%% `Name' as a map key in keyword syntax, followed by its space.
key(Name) ->
    [string_key(Name), ": "].

%% `Name' as a map key in keyword syntax, without its colon.
string_key(Name) ->
    case plain_atom(Name) of
        true -> Name;
        false -> string(Name)
    end.

%% `Name' as the name of a remote call or type, quoted where it must be.
call(Name) ->
    case re:run(Name, <<"^[a-z_][A-Za-z0-9_]*$">>, [{capture, none}]) of
        match -> Name;
        nomatch -> string(Name)
    end.

plain_atom(Name) ->
    re:run(Name, <<"^[A-Za-z_][A-Za-z0-9_]*[?!]?$">>, [{capture, none}]) =:= match.

%% A string literal. Protobuf names hold no characters to escape, but the
%% ones Elixir would read otherwise are escaped all the same.
string(Value) ->
    [
        $",
        [
            case C of
                $" -> "\\\"";
                $\\ -> "\\\\";
                $# -> "\\#";
                _ -> C
            end
         || <<C>> <= Value
        ],
        $"
    ].
