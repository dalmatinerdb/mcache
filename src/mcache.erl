-module(mcache).

-on_load(init/0).

%% API exports
-export([new/2, pop/1, insert/4, print/1, stats/1, get/2, age/1]).

-type mcache_handle() :: binary().

-type mcache_error() :: {error, term()}.
-type mc_opt() :: {buckets, pos_integer()} |
                  {age_cycle, pos_integer()} |
                  {initial_data_size, pos_integer()} |
                  {initial_entries, pos_integer()} |
                  {hash_seed, pos_integer()}.

%%====================================================================
%% API functions
%%====================================================================

-spec new(pos_integer(), [mc_opt()]) ->
                 {ok, Handle::mcache_handle()} |
                 mcache_error().

new(MaxAlloc, Opts) ->
    Buckets = proplists:get_value(buckets, Opts, 512),
    AgeCycle = proplists:get_value(age_cycle, Opts, 1000000),
    InitialDataSize = proplists:get_value(initial_data_size, Opts, 32),
    InitialEntries = proplists:get_value(initial_entries, Opts, 4),
    HashSeed = proplists:get_value(hash_seed, Opts, 42),
      new(MaxAlloc, Buckets, AgeCycle, InitialDataSize, InitialEntries,
          HashSeed).
new(_MaxAlloc, _Buckets, _AgeCycle, _InitialDataSize, _InitialEntries,
    _HashSeed) ->
    erlang:nif_error(nif_library_not_loaded).

pop(_Handle) ->
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
