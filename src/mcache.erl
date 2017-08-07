-module(mcache).

-on_load(init/0).

%% API exports
-export([new/2, pop/1, insert/5, print/1, stats/1, get/3, age/1,
         take/3 , is_empty/1, remove_prefix/2
        ]).

-type mcache_handle() :: binary().
-type bucket() :: binary().
-type key() :: binary().
-type offset() :: non_neg_integer().
-type values() :: <<_:_*64>>.
-type chunk() :: {offset(), values()}.
-type chunks() :: [chunk()].

-type conf_stats() ::
        [
         {buckets, non_neg_integer()} |
         {age_cycle, non_neg_integer()} |
         {initial_data_size, non_neg_integer()} |
         {initial_entries, non_neg_integer()} |
         {max_alloc, non_neg_integer()}
        ].

-type gen_stats() ::
        [
         {alloc, non_neg_integer()} |
         {count, non_neg_integer()} |
         {size, non_neg_integer()}
        ].
-type stats() ::
        [
         {age, non_neg_integer()} |
         {inserts, non_neg_integer()} |
         {conf, conf_stats()} |
         {total_alloc, non_neg_integer()} |
         {gen0, gen_stats()} |
         {gen1, gen_stats()} |
         {gen2, gen_stats()}
        ].

-type mcache_error() :: {error, term()}.
-type mc_opt() :: {buckets, pos_integer()} |
                  {age_cycle, pos_integer()} |
                  {initial_data_size, pos_integer()} |
                  {initial_entries, pos_integer()} |
                  {max_gap, pos_integer()} |
                  {hash_seed, pos_integer()}.

%%====================================================================
%% API functions
%%====================================================================

-spec new(pos_integer(), [mc_opt()]) ->
                 Handle::mcache_handle() |
                         mcache_error().

new(MaxAlloc, Opts) ->
    Buckets = proplists:get_value(buckets, Opts, 512),
    AgeCycle = proplists:get_value(age_cycle, Opts, 1000000),
    InitialDataSize = proplists:get_value(initial_data_size, Opts, 32),
    InitialEntries = proplists:get_value(initial_entries, Opts, 4),
    HashSeed = proplists:get_value(hash_seed, Opts, 42),
    MaxGap = proplists:get_value(max_gap, Opts, 0),
    new(MaxAlloc, Buckets, AgeCycle, InitialDataSize, InitialEntries,
        HashSeed, MaxGap).
-spec pop(mcache_handle()) ->
                 undefined |
                 {ok, key(), chunks()}.

pop(_Handle) ->
    erlang:nif_error(nif_library_not_loaded).

-spec insert(mcache_handle(), bucket(), key(), offset(), values()) ->
                    ok |
                    {overflow, key(), chunks()}.
insert(_Handle, _Bucket, _Name, _Offset, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

-spec get(mcache_handle(), bucket(), key()) ->
                 undefined |
                 {ok, chunks()}.

get(_Handle, _Bucket, _Name) ->
    erlang:nif_error(nif_library_not_loaded).

-spec remove_prefix(mcache_handle(), key()) ->
                           undefined |
                           {ok, non_neg_integer()}.

remove_prefix(_Handle, _Name) ->
    {ok, 0}.
%%erlang:nif_error(nif_library_not_loaded).

-spec take(mcache_handle(), bucket(), key()) ->
                  undefined |
                  {ok, chunks()}.

take(_Handle, _Bucket, _Name) ->
    erlang:nif_error(nif_library_not_loaded).


-spec stats(mcache_handle()) ->
                   stats().
stats(_Handle) ->
    erlang:nif_error(nif_library_not_loaded).

-spec is_empty(mcache_handle()) ->
                      boolean().
is_empty(_Handle) ->
    erlang:nif_error(nif_library_not_loaded).


-spec age(mcache_handle()) ->
                 ok.
age(_Handle) ->
    erlang:nif_error(nif_library_not_loaded).

-spec print(mcache_handle()) ->
                   ok.
print(_Handle) ->
    erlang:nif_error(nif_library_not_loaded).

%%====================================================================
%% Internal functions
%%====================================================================

new(_MaxAlloc, _Buckets, _AgeCycle, _InitialDataSize, _InitialEntries,
    _HashSeed, _MaxGap) ->
    erlang:nif_error(nif_library_not_loaded).

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
