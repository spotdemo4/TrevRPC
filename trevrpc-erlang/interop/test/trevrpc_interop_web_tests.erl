%% The files the interop server's HTTP fallback serves: those under the
%% web root, and nothing outside it.
-module(trevrpc_interop_web_tests).

-include_lib("eunit/include/eunit.hrl").

file_path_test_() ->
    {setup,
        fun() ->
            Root = filename:join(
                [
                    filename:basedir(user_cache, "trevrpc-interop-web-tests"),
                    integer_to_list(
                        erlang:unique_integer([positive])
                    )
                ]
            ),
            ok = filelib:ensure_path(filename:join(Root, "sub")),
            Root
        end,
        fun(Root) -> file:del_dir_r(Root) end, fun(Root) ->
            [
                ?_assertEqual({ok, bin_join([Root, "index.html"])}, file_path(Root, <<"/">>)),
                ?_assertEqual({ok, bin_join([Root, "page.js"])}, file_path(Root, <<"/page.js">>)),
                ?_assertEqual(
                    {ok, bin_join([Root, "sub", "index.html"])}, file_path(Root, <<"/sub/">>)
                ),
                ?_assertEqual({ok, bin_join([Root, "a b"])}, file_path(Root, <<"/a%20b">>)),
                ?_assertEqual(error, file_path(Root, <<"/../secret">>)),
                ?_assertEqual(error, file_path(Root, <<"/%2e%2e/secret">>)),
                ?_assertEqual(error, file_path(Root, <<"/sub/../../secret">>)),
                ?_assertEqual(error, file_path(Root, <<"/%zz">>))
            ]
        end}.

bin_join(Parts) -> iolist_to_binary(filename:join(Parts)).

file_path(Root, Path) ->
    case trevrpc_interop_web:file_path(Root, Path) of
        {ok, File} -> {ok, iolist_to_binary(File)};
        error -> error
    end.
