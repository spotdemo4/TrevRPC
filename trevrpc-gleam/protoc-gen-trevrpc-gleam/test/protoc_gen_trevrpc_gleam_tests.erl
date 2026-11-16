%% The generator end to end: protoc parses the protos in test/proto, and
%% protoc-gen-trevrpc-erlang's for the services they share, the generator
%% answers the request built from them, and the files it generates are
%% checked. test/gleam compiles and runs generated code.
%%
%% protoc must be on the PATH. TREVRPC_PROTO names the directory of
%% trevrpc/options.proto, the repository's proto/ by default.
-module(protoc_gen_trevrpc_gleam_tests).

-include_lib("eunit/include/eunit.hrl").

%% The files of a proto file: its Gleam module where its path says, and
%% gpb's module and the shim beside it.
gleam_files_test() ->
    {ok, Files} = generate(["nested/v1/point.proto"], <<>>),
    ?assertEqual(
        [
            <<"nested/v1/nested@v1@point_pb.erl">>,
            <<"nested/v1/nested@v1@point_pb.hrl">>,
            <<"nested/v1/nested@v1@point_shim.erl">>,
            <<"nested/v1/point.gleam">>
        ],
        lists:sort([Name || {Name, _} <- Files])
    ),
    {_, Gleam} = lists:keyfind(<<"nested/v1/point.gleam">>, 1, Files),
    ?assertNotEqual(nomatch, binary:match(Gleam, <<"pub type Point {\n  Point(\n    x: Int,">>)),
    {_, Pb} = lists:keyfind(<<"nested/v1/nested@v1@point_pb.hrl">>, 1, Files),
    %% gpb's record takes the Gleam constructor's name.
    ?assertNotEqual(nomatch, binary:match(Pb, <<"-record(point,">>)).

%% A service gets a module beside its file's, over trevrpc-gleam: a method
%% description, a handler and a client function per method, and routes.
gleam_service_test() ->
    {ok, Files} = generate(["methods.proto"], <<>>),
    {_, Service} = lists:keyfind(<<"methods_trevrpc.gleam">>, 1, Files),
    Has = fun(Text) -> ?assertNotEqual(nomatch, binary:match(Service, Text), Text) end,
    Has(<<"import methods\n">>),
    Has(<<"pub type Handlers {">>),
    Has(<<"pub fn routes(handlers: Handlers) -> List(server.Route) {">>),
    Has(<<"server.unary(unary_method(), handlers.unary),">>),
    Has(<<"server.bidi(bidi_method(), handlers.bidi),">>),
    Has(<<"path: \"/example.methods.v1.Methods/GetHTTPStatus\",">>),
    Has(<<"pub fn get_http_status(\n">>),
    %% receive is no Gleam keyword, but its Erlang atom is quoted.
    Has(<<"pub fn receive(\n">>),
    Has(<<"client.client_stream(channel, client_stream_method(), options)">>),
    %% The trevrpc.method options.
    Has(<<"unreliable_requests: True,">>),
    Has(<<"response_compression: schema.StreamCompression,">>).

%% A file with enums and no messages: gpb's module has no records.
gleam_enums_test() ->
    {ok, Files} = generate(["gleamgen/enums.proto"], <<>>),
    {_, Gleam} = lists:keyfind(<<"gleamgen/enums.gleam">>, 1, Files),
    ?assertNotEqual(nomatch, binary:match(Gleam, <<"pub type Level {\n  LevelUnspecified\n">>)),
    {_, Shim} = lists:keyfind(<<"gleamgen/gleamgen@enums_shim.erl">>, 1, Files),
    ?assertEqual(nomatch, binary:match(Shim, <<"-export">>)).

gleam_errors_test_() ->
    [
        ?_assertEqual(
            {error, <<"protoc-gen-trevrpc-gleam takes no parameters, got \"x=y\"">>},
            generate(["empty.proto"], <<"x=y">>)
        ),
        ?_assertEqual(
            {error, <<
                "gleamgen/both.proto: two messages this file uses would both be Gleam's point, "
                "which a gpb module cannot tell apart"
            >>},
            generate(["gleamgen/both.proto"], <<>>)
        ),
        ?_assertEqual(
            {error,
                <<"gleamgen/names.proto: service gleamgen.Names: two Gleam functions are named foo_method">>},
            generate(["gleamgen/names.proto"], <<>>)
        ),
        %% The Erlang plugin's checks of methods apply.
        ?_assertEqual(
            {error, <<
                "service example.errors.Collision RPC methods GetFoo and Get_Foo both require "
                "generated function get_foo"
            >>},
            generate(["errors/collision.proto"], <<>>)
        ),
        ?_assertEqual(
            {error, <<"two Gleam types are named ShapeKind">>},
            generate(["gleamgen/types.proto"], <<>>)
        ),
        ?_assertEqual(
            {error, <<"gleamgen.Labels: two fields are labelled ab in Gleam">>},
            generate(["gleamgen/labels.proto"], <<>>)
        ),
        ?_assertEqual(
            {error, <<"Gleam type Result is taken by Gleam itself">>},
            generate(["gleamgen/taken.proto"], <<>>)
        ),
        ?_assertEqual(
            {error, <<
                "gleamgen/Upper.proto: a Gleam module needs a path of lower-case letters, digits "
                "and underscores, each part starting with a letter"
            >>},
            generate(["gleamgen/Upper.proto"], <<>>)
        )
    ].

%% As the Gleam compiler's to_snake_case and to_upper_camel_case, with
%% which it names constructors' atoms and modules' types.
gleam_names_test_() ->
    [
        ?_assertEqual(Want, trevrpc_gen_gleam:gleam_snake(Name))
     || {Name, Want} <- [
            {<<"ShapeStyle">>, <<"shape_style">>},
            {<<"HTTPStatus">>, <<"h_t_t_p_status">>},
            {<<"Point3D">>, <<"point3_d">>},
            {<<"End">>, <<"end">>},
            {<<"Ok">>, <<"ok">>}
        ]
    ] ++
        [
            ?_assertEqual(Want, trevrpc_gen_gleam:upper_camel(Name))
         || {Name, Want} <- [
                {<<"login_user">>, <<"LoginUser">>},
                {<<"Int_Value">>, <<"IntValue">>},
                {<<"wibble">>, <<"Wibble">>},
                {<<"Wibble">>, <<"Wibble">>},
                {<<"wibble__wobble_">>, <<"WibbleWobble">>},
                {<<"Shape_Style">>, <<"ShapeStyle">>},
                {<<>>, <<>>}
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
    trevrpc_gen_test:response(protoc_gen_trevrpc_gleam:generate(Request)).
