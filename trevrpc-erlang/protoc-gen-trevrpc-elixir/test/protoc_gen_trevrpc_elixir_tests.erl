%% The generator end to end: protoc parses the protos in test/proto, and
%% protoc-gen-trevrpc-erlang's for the services they share, the generator
%% answers the request built from them, and the files it generates are
%% checked. test/elixir compiles and runs generated code.
%%
%% protoc must be on the PATH. TREVRPC_PROTO names the directory of
%% trevrpc/options.proto, the repository's proto/ by default.
-module(protoc_gen_trevrpc_elixir_tests).

-include_lib("eunit/include/eunit.hrl").

%% gpb's module goes to the root, as Erlang modules share one namespace,
%% and the service's Elixir module where Mix's conventions put it.
files_test_() ->
    trevrpc_gen_test:generating(fun files/0).

files() ->
    {ok, Files} = generate(["methods.proto", "nested/v1/point.proto"], <<>>),
    ?assertEqual(
        [
            <<"example/methods/v1/methods.ex">>,
            <<"methods_pb.erl">>,
            <<"point_pb.erl">>
        ],
        lists:sort([Name || {Name, _} <- Files])
    ),
    %% Maps, as gpb's maps option generates them.
    {_, Pb} = lists:keyfind(<<"methods_pb.erl">>, 1, Files),
    ?assertNotEqual(nomatch, binary:match(Pb, <<"-type 'example.methods.v1.Request'() ::">>)).

%% The service module: a behaviour, routes and a client function per
%% method, with the schema options the Erlang plugin gives them.
service_test_() ->
    trevrpc_gen_test:generating(fun service/0).

service() ->
    {ok, Files} = generate(["methods.proto"], <<>>),
    {_, Service} = lists:keyfind(<<"example/methods/v1/methods.ex">>, 1, Files),
    Has = fun(Text) -> ?assertNotEqual(nomatch, binary:match(Service, Text), Text) end,
    Has(<<"defmodule Example.Methods.V1.Methods do\n">>),
    Has(<<
        "  @callback unary(:methods_pb.\"example.methods.v1.Request\"(), :trevrpc_call.call()) ::\n"
    >>),
    Has(<<"  @callback client_stream(:trevrpc_call.call()) ::\n">>),
    Has(<<"      get_http_status: &module.get_http_status/2\n">>),
    Has(<<"      bidi: &module.bidi/1">>),
    Has(<<"        {\"/example.methods.v1.Methods/GetHTTPStatus\",\n">>),
    Has(<<"           kind: :bidirectional_streaming,\n">>),
    Has(<<"decode: &:methods_pb.decode_msg(&1, :\"example.methods.v1.Request\")">>),
    %% receive is a special form, whose name a function may take.
    Has(<<"  def receive(channel, request, options \\\\ %{}) do\n">>),
    Has(<<"  def client_stream(channel, options \\\\ %{}) do\n">>),
    Has(<<"    :trevrpc_client.server_stream(\n">>),
    %% The trevrpc.method options of the routes.
    Has(<<"           unreliable: %{requests: true, responses: true},\n">>),
    Has(<<"           compression: %{requests: :none, responses: :stream},\n">>),
    Has(<<"           compression: :message,\n">>),
    %% And of the client functions' calls, whose codecs encode requests and
    %% decode replies, the other way round from the routes'.
    Has(<<
        "  def unreliable(channel, options \\\\ %{}) do\n"
        "    :trevrpc_client.bidi(\n"
        "      channel,\n"
        "      \"/example.methods.v1.Methods/Unreliable\",\n"
        "      Map.merge(\n"
        "        %{\n"
        "          request_type: \"example.methods.v1.Request\",\n"
        "          response_type: \"example.methods.v1.Reply\",\n"
        "          unreliable: %{requests: true, responses: true},\n"
        "          codec: %{\n"
        "            decode: &:methods_pb.decode_msg(&1, :\"example.methods.v1.Reply\"),\n"
        "            encode: &:methods_pb.encode_msg(&1, :\"example.methods.v1.Request\")\n"
        "          }\n"
        "        },\n"
        "        options\n"
        "      )\n"
        "    )\n"
        "  end\n"
    >>),
    Has(<<
        "      \"/example.methods.v1.Methods/Redacted\",\n"
        "      Map.merge(\n"
        "        %{\n"
        "          request_type: \"example.methods.v1.Secret\",\n"
        "          response_type: \"example.methods.v1.Reply\",\n"
        "          compression: %{requests: :none, responses: :stream},\n"
    >>),
    Has(<<
        "      \"/example.methods.v1.Methods/Compressed\",\n"
        "      request,\n"
        "      Map.merge(\n"
        "        %{\n"
        "          request_type: \"example.methods.v1.Request\",\n"
        "          response_type: \"example.methods.v1.Reply\",\n"
        "          compression: :message,\n"
    >>).

%% A service without methods or package, and no message module of a file
%% without services is left out.
empty_test_() ->
    trevrpc_gen_test:generating(fun empty/0).

empty() ->
    {ok, Files} = generate(["empty.proto"], <<>>),
    ?assertEqual([<<"empty.ex">>, <<"empty_pb.erl">>], lists:sort([Name || {Name, _} <- Files])),
    {_, Service} = lists:keyfind(<<"empty.ex">>, 1, Files),
    ?assertNotEqual(nomatch, binary:match(Service, <<"defmodule Empty do\n">>)),
    ?assertNotEqual(nomatch, binary:match(Service, <<"@type handlers :: module() | %{}\n">>)).

options_test_() ->
    trevrpc_gen_test:generating(fun options/0).

options() ->
    {ok, Files} = generate(
        ["methods.proto"], <<"module_prefix=MyApp.Rpc,pb_prefix=my_,pb_suffix=_proto">>
    ),
    ?assertEqual(
        [<<"my_app/rpc/example/methods/v1/methods.ex">>, <<"my_methods_proto.erl">>],
        lists:sort([Name || {Name, _} <- Files])
    ),
    {_, Service} = lists:keyfind(<<"my_app/rpc/example/methods/v1/methods.ex">>, 1, Files),
    ?assertNotEqual(
        nomatch, binary:match(Service, <<"defmodule MyApp.Rpc.Example.Methods.V1.Methods do">>)
    ),
    ?assertNotEqual(nomatch, binary:match(Service, <<"&:my_methods_proto.encode_msg(&1, ">>)).

errors_test_() ->
    trevrpc_gen_test:generating([
        ?_assertEqual(
            {error, <<"unknown trevrpc-elixir option \"msg_format\"">>},
            generate(["empty.proto"], <<"msg_format=records">>)
        ),
        ?_assertEqual(
            {error,
                <<"invalid module_prefix \"my_app\": expected an Elixir alias, such as MyApp.Rpc">>},
            generate(["empty.proto"], <<"module_prefix=my_app">>)
        ),
        ?_assertEqual(
            {error,
                <<"invalid pb_suffix \"-pb\": expected lower-case letters, digits and underscores">>},
            generate(["empty.proto"], <<"pb_suffix=-pb">>)
        ),
        ?_assertEqual(
            {error, <<
                "service elixirgen.Guards RPC method When requires Elixir function when, which "
                "Elixir does not allow"
            >>},
            generate(["elixirgen/when.proto"], <<>>)
        ),
        ?_assertEqual(
            {error, <<
                "service elixirgen.v1._2fa.Codes needs Elixir module Elixirgen.V1.2fa.Codes, which "
                "is no alias: each part must start with a letter"
            >>},
            generate(["elixirgen/digit.proto"], <<>>)
        ),
        ?_assertEqual(
            {error, <<
                "service elixirgen.fooBar.Svc and service elixirgen.foo_bar.Svc both require "
                "module Elixirgen.FooBar.Svc"
            >>},
            generate(["elixirgen/same.proto", "elixirgen/same_other.proto"], <<>>)
        ),
        ?_assertEqual(
            {error, <<
                "service elixirgen.HTTPServer and service elixirgen.HttpServer both require file "
                "elixirgen/http_server.ex"
            >>},
            generate(["elixirgen/paths.proto"], <<>>)
        ),
        %% The Erlang plugin's checks of methods apply.
        ?_assertEqual(
            {error, <<
                "service example.errors.Collision RPC methods GetFoo and Get_Foo both require "
                "generated function get_foo"
            >>},
            generate(["errors/collision.proto"], <<>>)
        )
    ]).

camelize_test_() ->
    [
        ?_assertEqual(Want, trevrpc_gen_elixir:camelize(Name))
     || {Name, Want} <- [
            {<<"greeter">>, <<"Greeter">>},
            {<<"Greeter">>, <<"Greeter">>},
            {<<"greeter_v1">>, <<"GreeterV1">>},
            {<<"v1">>, <<"V1">>},
            {<<"fooBar">>, <<"FooBar">>},
            {<<"foo__bar_">>, <<"FooBar">>},
            {<<"_2fa">>, <<"2fa">>}
        ]
    ].

%%% Helpers

generate(Protos, Parameter) ->
    Test = filename:dirname(?FILE),
    Dirs = [
        filename:join(Test, "proto"),
        filename:join([Test, "..", "_checkouts", "protoc_gen_trevrpc_erlang", "test", "proto"]),
        trevrpc_gen_test:options_dir(filename:join([Test, "..", "..", "..", "proto"]))
    ],
    Request = trevrpc_gen_test:request(Protos, Parameter, Dirs),
    trevrpc_gen_test:response(protoc_gen_trevrpc_elixir:generate(Request)).
