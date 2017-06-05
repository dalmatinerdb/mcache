-module(mcache).

-on_load(init/0).

%% API exports
-export([new/1, insert/4, print/1, stats/1, get/2, age/1]).

-type mcache_handle() :: binary().

-type mcache_error() :: {error, term()}.
%%====================================================================
%% API functions
%%====================================================================

-spec new(pos_integer()) ->
                 {ok, Handle::mcache_handle()} |
                 mcache_error().
new(_MaxSize) ->
    erlang:nif_error(nif_library_not_loaded).

insert(_Handle, _Name, _Offset, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

get(_Handle, _Name) ->
    erlang:nif_error(nif_library_not_loaded).

print(_Handle) ->
    erlang:nif_error(nif_library_not_loaded).

stats(_Handle) ->
    erlang:nif_error(nif_library_not_loaded).


age(_Handle) ->
    erlang:nif_error(nif_library_not_loaded).

%%====================================================================
%% Internal functions
%%====================================================================

init() ->
    PrivDir = case code:priv_dir(?MODULE) of
                  {error, _} ->
                      EbinDir = filename:dirname(code:which(?MODULE)),
                      AppPath = filename:dirname(EbinDir),
                      filename:join(AppPath, "priv");
                  Path ->
                      Path
              end,
    erlang:load_nif(filename:join(PrivDir, "mcache_drv"), 0).
