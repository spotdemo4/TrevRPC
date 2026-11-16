%% The generator end to end: protoc parses the protos in test/proto, the
%% generator answers the request built from them, and the modules it
%% generates are compiled with warnings as errors and loaded.
%%
%% protoc must be on the PATH. TREVRPC_PROTO names the directory of
%% trevrpc/options.proto, the repository's proto/ by default.
-module(protoc_gen_trevrpc_erlang_tests).

-include_lib("eunit/include/eunit.hrl").

-define(METHODS, example_methods_v1_methods).
-define(REQUEST, 'example.methods.v1.Request').
-define(REPLY, 'example.methods.v1.Reply').
-define(PATH(Method), <<"/example.methods.v1.Methods/", Method>>).

%%% Generated code

maps_test_() ->
    {setup, fun() -> load(generated(["methods.proto"], <<>>)) end, fun(Files) ->
        [
            ?_assertEqual([<<"example_methods_v1_methods.erl">>, <<"methods_pb.erl">>], Files),
            {"messages", fun messages/0},
            {"message module holds what its file uses", fun message_closure/0},
            {"routes from handler funs", fun routes_from_funs/0},
            {"routes from a module", fun routes_from_module/0},
            {"compression", fun compression/0},
            {"route codecs", fun route_codecs/0},
            {"client functions", fun client_functions/0},
            {"behaviour", fun behaviour/0}
        ]
    end}.

messages() ->
    Request = #{
        text => <<"hé"/utf8>>,
        count => 3,
        at => #{seconds => 1, nanos => 2},
        kind => 'KIND_ONE',
        nested => #{data => <<1, 2>>},
        by_name => #{<<"k">> => #{data => <<3>>}},
        choice => {name, <<"n">>}
    },
    Bin = methods_pb:encode_msg(Request, ?REQUEST),
    ?assertEqual(Request, methods_pb:decode_msg(Bin, ?REQUEST)),
    %% Strings are binaries, and an unset optional field is left out.
    ?assertEqual(
        #{text => <<>>, kind => 'KIND_UNSPECIFIED', by_name => #{}},
        methods_pb:decode_msg(
            <<>>, ?REQUEST
        )
    ).

%% The file's own messages and the types they use, Timestamp among them,
%% but not descriptor.proto, which trevrpc/options.proto extends.
message_closure() ->
    Names = methods_pb:get_msg_names(),
    ?assert(lists:member('example.methods.v1.Unused', Names)),
    ?assert(lists:member('google.protobuf.Timestamp', Names)),
    ?assertNot(lists:member('google.protobuf.MethodOptions', Names)),
    ?assertNot(lists:member('trevrpc.MethodOptions', Names)),
    ?assertEqual(["methods", "timestamp"], lists:sort(methods_pb:get_all_proto_names())).

routes_from_funs() ->
    Unary = fun(_Request, _Call) -> {ok, #{}} end,
    Bidi = fun(_Call) -> ok end,
    Routes = ?METHODS:routes(#{unary => Unary, bidi => Bidi}),
    ?assertEqual([?PATH("Bidi"), ?PATH("Unary")], lists:sort(maps:keys(Routes))),
    ?assertMatch(
        #{
            kind := unary,
            handler := Unary,
            request_type := <<"example.methods.v1.Request">>,
            response_type := <<"example.methods.v1.Reply">>,
            codec := #{}
        },
        maps:get(?PATH("Unary"), Routes)
    ),
    ?assertNot(is_map_key(compression, maps:get(?PATH("Unary"), Routes))),
    ?assertMatch(
        #{kind := bidirectional_streaming, handler := Bidi}, maps:get(?PATH("Bidi"), Routes)
    ),
    ?assertError(
        {unknown_method, nope, [
            bidi,
            client_stream,
            compressed,
            get_http_status,
            'receive',
            redacted,
            server_stream,
            stream_compressed,
            unary,
            unreliable
        ]},
        ?METHODS:routes(#{unary => Unary, nope => Bidi})
    ).

routes_from_module() ->
    Routes = ?METHODS:routes(handler_module),
    ?assertEqual(10, map_size(Routes)),
    Arity = fun(Path) ->
        #{handler := Handler} = maps:get(Path, Routes),
        {module, handler_module} = erlang:fun_info(Handler, module),
        element(2, erlang:fun_info(Handler, arity))
    end,
    ?assertEqual(2, Arity(?PATH("Unary"))),
    ?assertEqual(1, Arity(?PATH("ClientStream"))),
    ?assertEqual(2, Arity(?PATH("ServerStream"))),
    ?assertEqual(1, Arity(?PATH("Bidi"))),
    ?assertMatch(#{kind := unary}, maps:get(?PATH("Receive"), Routes)),
    ?assertMatch(#{kind := unary}, maps:get(?PATH("GetHTTPStatus"), Routes)).

%% As the Go generator has it: no direction with a redacted field, and a
%% context per call only where a direction streams. The unreliable flags
%% are a route's and a call's `unreliable'.
compression() ->
    Routes = ?METHODS:routes(handler_module),
    Compression = fun(Method) ->
        Path = <<"/example.methods.v1.Methods/", (list_to_binary(Method))/binary>>,
        maps:get(compression, maps:get(Path, Routes), none)
    end,
    ?assertEqual(message, Compression("Compressed")),
    ?assertEqual(#{requests => stream, responses => message}, Compression("StreamCompressed")),
    ?assertEqual(#{requests => none, responses => stream}, Compression("Redacted")),
    ?assertEqual(none, Compression("Unreliable")),
    ?assertEqual(none, Compression("Unary")),
    ?assertMatch(
        #{unreliable := #{requests := true, responses := true}},
        maps:get(?PATH("Unreliable"), Routes)
    ),
    ?assertNot(is_map_key(unreliable, maps:get(?PATH("Bidi"), Routes))),
    ?assertMatch(
        {bidi, channel, _, #{unreliable := #{requests := true, responses := true}}},
        ?METHODS:unreliable(channel)
    ).

%% A route decodes requests and encodes replies.
route_codecs() ->
    #{codec := #{encode := Encode, decode := Decode}} =
        maps:get(?PATH("Unary"), ?METHODS:routes(handler_module)),
    Request = methods_pb:encode_msg(#{text => <<"in">>}, ?REQUEST),
    ?assertMatch(#{text := <<"in">>}, Decode(Request)),
    ?assertEqual(
        #{text => <<"out">>},
        methods_pb:decode_msg(iolist_to_binary(Encode(#{text => <<"out">>})), ?REPLY)
    ),
    ?assertError(_, Decode(<<255>>)).

%% A call encodes requests and decodes replies, and its own options win
%% over the schema's.
client_functions() ->
    Request = #{text => <<"in">>},
    {unary, channel, Path, Request, Options} = ?METHODS:unary(channel, Request),
    ?assertEqual(?PATH("Unary"), Path),
    #{codec := #{encode := Encode, decode := Decode}} = Options,
    ?assertEqual(
        #{text => <<"in">>},
        maps:with([text], methods_pb:decode_msg(iolist_to_binary(Encode(Request)), ?REQUEST))
    ),
    ?assertEqual(#{text => <<"out">>}, Decode(methods_pb:encode_msg(#{text => <<"out">>}, ?REPLY))),
    ?assertMatch(
        #{
            request_type := <<"example.methods.v1.Request">>,
            response_type := <<"example.methods.v1.Reply">>
        },
        Options
    ),
    {unary, channel, _, _, Overridden} =
        ?METHODS:compressed(channel, Request, #{timeout => 5, compression => none}),
    ?assertMatch(#{timeout := 5, compression := none, codec := #{}}, Overridden),
    ?assertMatch(
        {unary, channel, _, _, #{compression := message}}, ?METHODS:compressed(channel, Request)
    ),
    ?assertMatch(
        {server_stream, channel, <<"/example.methods.v1.Methods/ServerStream">>, Request, #{}},
        ?METHODS:server_stream(channel, Request)
    ),
    ?assertMatch(
        {client_stream, channel, <<"/example.methods.v1.Methods/ClientStream">>, #{timeout := 1}},
        ?METHODS:client_stream(channel, #{timeout => 1})
    ),
    ?assertMatch(
        {bidi, channel, <<"/example.methods.v1.Methods/Bidi">>, #{codec := #{}}},
        ?METHODS:bidi(channel)
    ),
    ?assertMatch(
        {unary, channel, <<"/example.methods.v1.Methods/Receive">>, _, _},
        ?METHODS:'receive'(channel, Request)
    ),
    ?assertMatch(
        {unary, channel, <<"/example.methods.v1.Methods/GetHTTPStatus">>, _, _},
        ?METHODS:get_http_status(channel, Request)
    ).

behaviour() ->
    ?assertEqual(
        lists:sort([
            {unary, 2},
            {client_stream, 1},
            {server_stream, 2},
            {bidi, 1},
            {compressed, 2},
            {stream_compressed, 1},
            {redacted, 1},
            {unreliable, 1},
            {'receive', 2},
            {get_http_status, 2}
        ]),
        lists:sort(?METHODS:behaviour_info(callbacks))
    ).

records_test() ->
    Files = load(generated(["methods.proto"], <<"msg_format=records">>)),
    ?assertEqual(
        [<<"example_methods_v1_methods.erl">>, <<"methods_pb.erl">>, <<"methods_pb.hrl">>], Files
    ),
    Bin = methods_pb:encode_msg({?REPLY, <<"text">>}, ?REPLY),
    ?assertEqual({?REPLY, <<"text">>}, methods_pb:decode_msg(Bin, ?REPLY)).

%% A service without methods, in a file without a package.
empty_test() ->
    ?assertEqual([<<"empty.erl">>, <<"empty_pb.erl">>], load(generated(["empty.proto"], <<>>))),
    ?assertEqual(#{}, empty:routes(handler_module)),
    ?assertEqual(#{}, empty:routes(#{})),
    ?assertError({unknown_method, nope, []}, empty:routes(#{nope => fun(_) -> ok end})),
    ?assertEqual(
        #{value => 7}, empty_pb:decode_msg(empty_pb:encode_msg(#{value => 7}, 'Lonely'), 'Lonely')
    ).

module_names_test() ->
    {ok, Files} = generate(
        ["methods.proto"], <<"module_prefix=my_,pb_suffix=_proto,service_suffix=_svc">>
    ),
    ?assertEqual(
        [<<"my_example_methods_v1_methods_svc.erl">>, <<"my_methods_proto.erl">>],
        lists:sort([Name || {Name, _} <- Files])
    ),
    {_, Service} = lists:keyfind(<<"my_example_methods_v1_methods_svc.erl">>, 1, Files),
    ?assertMatch({match, _}, re:run(Service, <<"my_methods_proto:decode_msg">>)).

%% A file in a subdirectory, which gpb names by its base name.
subdirectory_test() ->
    {ok, Files} = generate(["nested/v1/point.proto"], <<>>),
    ?assertEqual([<<"point_pb.erl">>], [Name || {Name, _} <- Files]).

%%% Failures

errors_test_() ->
    [
        ?_assertEqual(
            {error, <<
                "service example.errors.Collision RPC methods GetFoo and Get_Foo both require "
                "generated function get_foo"
            >>},
            generate(["errors/collision.proto"], <<>>)
        ),
        ?_assertEqual(
            {error, <<
                "service example.errors.Reserved RPC method Routes requires generated function "
                "routes, which is taken"
            >>},
            generate(["errors/reserved.proto"], <<>>)
        ),
        ?_assertEqual(
            {error,
                <<"example.errors.StreamUnary.Unary: COMPRESSION_STREAM requires a streaming method">>},
            generate(["errors/stream_unary.proto"], <<>>)
        ),
        ?_assertEqual(
            {error, <<
                "example.errors.UnreliableRequests.ServerStream: unreliable_requests requires a "
                "client-streaming or bidirectional method"
            >>},
            generate(["errors/unreliable_requests.proto"], <<>>)
        ),
        ?_assertEqual(
            {error, <<
                "example.errors.UnreliableResponses.ClientStream: unreliable_responses requires a "
                "server-streaming or bidirectional method"
            >>},
            generate(["errors/unreliable_responses.proto"], <<>>)
        ),
        ?_assertEqual(
            {error, <<"service Empty and the messages of empty.proto both require module empty">>},
            generate(["empty.proto"], <<"pb_suffix=">>)
        ),
        ?_assertEqual(
            {error, <<"unknown trevrpc-erlang option \"paths\"">>},
            generate(["empty.proto"], <<"paths=source_relative">>)
        ),
        ?_assertEqual(
            {error, <<"invalid msg_format \"tuples\": expected maps or records">>},
            generate(["empty.proto"], <<"msg_format=tuples">>)
        ),
        ?_assertEqual(
            {error,
                <<"invalid module_prefix \"My-\": expected lower-case letters, digits and underscores">>},
            generate(["empty.proto"], <<"module_prefix=My-">>)
        ),
        ?_assertEqual(
            {error, <<"malformed CodeGeneratorRequest">>}, response(<<255>>)
        )
    ].

%% Proto3 optional fields, and no editions, which protoc then refuses to
%% hand the plugin.
features_test() ->
    Fields = trevrpc_gen_test:fields(
        iolist_to_binary(protoc_gen_trevrpc_erlang:generate(request(["empty.proto"], <<>>)))
    ),
    ?assertEqual([1], [Features || {2, {varint, Features}} <- Fields]).

%%% Names

snake_test_() ->
    [
        ?_assertEqual(Want, trevrpc_gen_names:snake(Name))
     || {Name, Want} <- [
            {<<"SayHello">>, <<"say_hello">>},
            {<<"HTTPRequest">>, <<"http_request">>},
            {<<"GetHTTPStatus">>, <<"get_http_status">>},
            {<<"GetV2">>, <<"get_v2">>},
            {<<"already_snake">>, <<"already_snake">>},
            {<<"Get_Foo">>, <<"get_foo">>},
            {<<"ID">>, <<"id">>},
            {<<"x">>, <<"x">>}
        ]
    ].

atom_test() ->
    ?assertEqual("'receive'", lists:flatten(trevrpc_gen_names:atom(<<"receive">>))),
    ?assertEqual("unary", lists:flatten(trevrpc_gen_names:atom(<<"unary">>))),
    ?assertEqual("'pkg.Msg'", lists:flatten(trevrpc_gen_names:atom(<<"pkg.Msg">>))).

module_name_test() ->
    Options = #{prefix => <<>>, pb_suffix => <<"_pb">>, service_suffix => <<>>},
    ?assertEqual(<<"bar_pb">>, trevrpc_gen_names:message_module(<<"foo/bar.proto">>, Options)),
    ?assertEqual(<<"my_file_pb">>, trevrpc_gen_names:message_module(<<"My-File.proto">>, Options)),
    ?assertEqual(
        <<"trevrpc_interop_v1_compressed_echo">>,
        trevrpc_gen_names:service_module(<<"trevrpc.interop.v1">>, <<"CompressedEcho">>, Options)
    ),
    ?assertEqual(<<"echo">>, trevrpc_gen_names:service_module(<<>>, <<"Echo">>, Options)).

%%% Helpers

proto_dir() ->
    filename:join(filename:dirname(?FILE), "proto").

%% The files generated for `Protos', compiled and loaded; returns their
%% names.
load({ok, Files}) ->
    Dir = trevrpc_gen_test:scratch(),
    [ok = file:write_file(filename:join(Dir, Name), Content) || {Name, Content} <- Files],
    [
        begin
            Path = filename:join(Dir, binary_to_list(Name)),
            Opts = [binary, return, warnings_as_errors, warn_missing_spec, {i, Dir}],
            {ok, Module, Beam, []} = compile:file(Path, Opts),
            {module, Module} = code:load_binary(Module, Path, Beam)
        end
     || {Name, _} <- Files, filename:extension(Name) =:= <<".erl">>
    ],
    lists:sort([Name || {Name, _} <- Files]).

generated(Protos, Parameter) ->
    {ok, _} = generate(Protos, Parameter).

generate(Protos, Parameter) ->
    response(request(Protos, Parameter)).

request(Protos, Parameter) ->
    Options = trevrpc_gen_test:options_dir(
        filename:join([filename:dirname(?FILE), "..", "..", "..", "proto"])
    ),
    trevrpc_gen_test:request(Protos, Parameter, [proto_dir(), Options]).

response(Request) ->
    trevrpc_gen_test:response(protoc_gen_trevrpc_erlang:generate(Request)).
