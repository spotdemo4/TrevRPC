%% @doc The HTTP fallback of the interop server: the files of `--web-root',
%% such as the browser interop page, as Go's `http.FileServer' and Rust's
%% `ServeDir' serve them for the other servers (see interop/README.md).
%%
%% `GET' and `HEAD' requests get the file at their path, `index.html' for a
%% directory, with a content type from its extension: a module script must
%% arrive as JavaScript. A path that leaves the root, or names no file,
%% gets `404', and other methods `405'.
-module(trevrpc_interop_web).

-export([handler/1]).

-ifdef(TEST).
-export([file_path/2]).
-endif.

%% @doc The fallback handler serving the files under `Root'.
-spec handler(file:filename_all()) -> trevrpc_http_request:handler().
handler(Root) ->
    fun(Req) -> serve(Root, Req) end.

serve(Root, Req) ->
    case trevrpc_http_request:method(Req) of
        Method when Method =:= <<"GET">>; Method =:= <<"HEAD">> ->
            case file_path(Root, trevrpc_http_request:path(Req)) of
                {ok, Path} ->
                    case file:read_file(Path) of
                        {ok, Data} ->
                            trevrpc_http_request:reply(
                                Req, 200, [{<<"content-type">>, content_type(Path)}], Data
                            );
                        {error, _} ->
                            not_found(Req)
                    end;
                error ->
                    not_found(Req)
            end;
        _ ->
            trevrpc_http_request:reply(
                Req, 405, [{<<"allow">>, <<"GET, HEAD">>}], <<"method not allowed\n">>
            )
    end.

not_found(Req) ->
    trevrpc_http_request:reply(
        Req, 404, [{<<"content-type">>, <<"text/plain; charset=utf-8">>}], <<"not found\n">>
    ).

%% The file a request path names under Root, unless it leaves Root.
-spec file_path(file:filename_all(), binary()) -> {ok, file:filename_all()} | error.
file_path(Root, Path) ->
    %% OTP 29's percent_decode/1 throws on an invalid escape.
    Decoded =
        try
            uri_string:percent_decode(Path)
        catch
            throw:{error, _, _} = Error -> Error
        end,
    case Decoded of
        Binary when is_binary(Binary) ->
            case string:trim(Binary, leading, "/") of
                <<>> ->
                    {ok, filename:join(Root, "index.html")};
                Relative ->
                    case filelib:safe_relative_path(Relative, Root) of
                        unsafe -> error;
                        Safe -> in_root(filename:join(Root, Safe))
                    end
            end;
        _ ->
            error
    end.

in_root(Full) ->
    case filelib:is_dir(Full) of
        true -> {ok, filename:join(Full, "index.html")};
        false -> {ok, Full}
    end.

content_type(Path) ->
    case string:lowercase(unicode:characters_to_binary(filename:extension(Path))) of
        <<".html">> -> <<"text/html; charset=utf-8">>;
        <<".js">> -> <<"text/javascript; charset=utf-8">>;
        <<".mjs">> -> <<"text/javascript; charset=utf-8">>;
        <<".css">> -> <<"text/css; charset=utf-8">>;
        <<".json">> -> <<"application/json">>;
        <<".wasm">> -> <<"application/wasm">>;
        <<".svg">> -> <<"image/svg+xml">>;
        <<".png">> -> <<"image/png">>;
        <<".txt">> -> <<"text/plain; charset=utf-8">>;
        _ -> <<"application/octet-stream">>
    end.
