-module(mcache_eqc).

-include_lib("eqc/include/eqc.hrl").

-compile(export_all).

%%-define(LOCAL, 1).

%%====================================================================
%% Generators & Helpers
%%====================================================================

add_t(T, N, O, V) ->
    maps:update_with(
      N, fun(Acc) ->
                 set_points(O, V, Acc)
         end,
      set_points(O, V, #{}), T).

set_points(_, <<>>, Acc) ->
    Acc;
set_points(O, <<V:8/binary, Vs/binary>>, Acc) ->
    set_points(O + 1, Vs, maps:put(O, V, Acc)).

new(Size, Opts) ->
    H = mcache:new(Size, Opts),
    {H, #{}, []}.

insert({H, T, Ds}, Gap, B, N, O, V) ->
    case mcache:insert(H, B, N, O, V) of
        ok ->
            T1 = add_t(T, {B, N}, O, V),
            {H, T1, Ds};
        {overflow, {B1, K}, Data} ->
            T1 = add_t(T, {B, N}, O, V),
            T2 = maps:remove(K, T1),
            {H, T2, [{shrink_t(Data, 0), shrink_t(maps:get({B1, K}, T1), Gap)} | Ds]}
    end.

stats({H, T, Ds}) ->
    mcache:stats(H),
    {H, T, Ds}.

age({H, T, Ds}) ->
    mcache:age(H),
    {H, T, Ds}.

pop({H, T, Ds}, Gap) ->
    case mcache:pop(H) of
        undefined ->
            {H, T, Ds};
        {ok, {B, K} , Data} ->
            T1 = maps:remove({B, K}, T),
            {H, T1, [{shrink_t(Data, 0), shrink_t(maps:get({B, K}, T), Gap)} | Ds]}
    end.

take({H, T, Ds}, Gap, B, N) ->
    case mcache:take(H, B, N) of
        undefined ->
            {H, T, Ds};
        {ok, Data} ->
            T1 = maps:remove({B, N}, T),
            {H, T1, [{shrink_t(Data, 0), shrink_t(maps:get({B, N}, T), Gap)} | Ds]}
    end.

get({H, T, Ds}, B, N) ->
    mcache:get(H, B, N),
    {H, T, Ds}.

val() ->
    ?LET(Ns, ?SUCHTHAT(L, list(nat()), L /= []), << <<N:64>> || N <- Ns >>).

key() ->
    utf8().

bucket() ->
    <<>>.

v_time() ->
    nat().

pnat() ->
    ?SUCHTHAT(N, nat(), N > 0).

opts() ->
    [
     {buckets, pnat()},
     {age_cycle, pnat()},
     {initial_data_size, pnat()},
     {initial_entries, pnat()},
     {hash_seed, pnat()}
    ].


cache(MaxSize, Gap, Opts) ->
    ?SIZED(Size, cache(MaxSize, Gap, Opts, Size)).


cache(MaxSize, Gap, Opts, 0) ->
    {call, ?MODULE, new, [MaxSize, [{max_gap, Gap} | Opts]]};

cache(MaxSize, Gap, Opts, Size) ->
    ?LAZY(
       ?LETSHRINK(
          [H], [cache(MaxSize, Gap, Opts, Size -1)],
          frequency(
            [
             {1,   {call, ?MODULE, age, [H]}},
             {10,  {call, ?MODULE, stats, [H]}},
             {100, {call, ?MODULE, insert, [H, Gap, bucket(), key(), v_time(), val()]}},
             {15,  {call, ?MODULE, pop, [H, Gap]}},
             {15,  {call, ?MODULE, take, [H, Gap, bucket(), key()]}},
             {20,  {call, ?MODULE, get, [H, bucket(), key()]}}
            ]))).

c_size() ->
    ?LET(I, largeint(), erlang:abs(I)).


%%====================================================================
%% compare helpers
%%====================================================================

all_keys_c(H, Acc) ->
    case mcache:pop(H) of
        undefined ->
            lists:sort(Acc);
        {ok, {B, K} , Data} ->
            all_keys_c(H, [{{B, K}, shrink_t(Data, 0)} | Acc])
    end.

size_c([], N) ->
    N;
size_c([{_O, D} | R], N) ->
    size_c(R, N + byte_size(D) div 8).

all_keys_t(T, Gap) ->
    L = maps:to_list(T),
    apply_t_size(L, Gap, []).

apply_t_size([], _Gap, Acc) ->
    lists:sort(Acc);
apply_t_size([{K, Data} | R], Gap, Acc) ->
    apply_t_size(R, Gap, [{K, shrink_t(Data, Gap)} | Acc]).

shrink_t(M, Gap) when is_map(M) ->

    shrink_t(lists:sort(maps:to_list(M)), Gap, []);
shrink_t(L, Gap) when is_list(L)->
    shrink_t(lists:sort(L), Gap, []).

shrink_t([{N, D}, {N1, D1} | R], Gap, Acc)
  when N  + (byte_size(D) div 8) + Gap >= N1 ->
    Missing = (N1 - (N + (byte_size(D) div 8))) * 8 * 8,
    shrink_t([{N, <<D/binary, 0:Missing, D1/binary>>} | R], Gap, Acc);
shrink_t([E | R], Gap, Acc) ->
    shrink_t(R, Gap, [E | Acc]);
shrink_t([], _Gap, Acc) ->
    lists:sort(Acc).

check_elements([]) ->
    [];
check_elements([{D, D} | R]) ->
    check_elements(R);
check_elements([H | _T]) ->
    [H].


empty(C) ->
    case mcache:pop(C) of
        undefined ->
            ok;
        _ ->
            empty(C)
    end.

filter_bucket([{{Bkt, _K}, _V} | R], Bkt, Acc) ->
    filter_bucket(R, Bkt, Acc);

filter_bucket([E | R], Bkt, Acc) ->
    filter_bucket(R, Bkt, [E | Acc]);

filter_bucket([], _Pfx, Acc) ->
    lists:reverse(Acc).

%%====================================================================
%% Remote helpers
%%====================================================================

-ifdef(LOCAL).
maybe_client() ->
    {ok, node()}.
-else.
maybe_client() ->
    case ct_slave:start(eqc_client) of
        {ok, Client} ->
            rpc:call(Client, code, set_path, [code:get_path()]),
            {ok, Client};
        {error, already_started, Client} ->
            {ok, Client};
        E ->
            io:format(user, ">>>> E: ~p~n", [E]),
            E
    end.
-endif.

remote_eval(Fn, Args) ->
    {ok, Client} = maybe_client(),
    rpc:call(Client, ?MODULE, Fn, Args).



%%====================================================================
%% Properties
%%====================================================================


limit_body(Cache) ->
    {H, _, _} = eval(Cache),
    Stats = mcache:stats(H),
    Max = proplists:get_value(max_alloc, Stats),
    Total = proplists:get_value(total_alloc, Stats),
    {Max, Total}.

prop_limit() ->
    ?SETUP(
       fun setup/0,
       ?FORALL(
          {MaxSize, Gap, Opts}, {c_size(), nat(), opts()},
          ?FORALL(Cache, cache(MaxSize, Gap, Opts),
                  begin
                      {Max, Total} = remote_eval(limit_body, [Cache]),
                      ?WHENFAIL(io:format(user, "Max: ~p~nTotal:~p~n", [Max, Total]),
                                Max >= Total)
                  end))).

is_empty_body(Cache) ->
    {H, _, _} = eval(Cache),
    empty(H),
    mcache:is_empty(H).

prop_is_empty() ->
    ?SETUP(
       fun setup/0,
       ?FORALL(
          {MaxSize, Gap, Opts}, {c_size(), nat(), opts()},
          ?FORALL(
             Cache, cache(MaxSize, Gap, Opts),
             remote_eval(is_empty_body, [Cache])
            ))).

insert_pop_body(S, B, K, T, V) ->
    In = {B, K, T, V},
    H = mcache:new(S, []),
    R1 = case mcache:insert(H, B, K, T, V) of
             {overflow, BM ,Vs} ->
                 {ok, BM, Vs};
             _O ->
                 mcache:pop(H)
         end,
    R2 = mcache:pop(H),
    {In, R1, R2}.

prop_insert_pop() ->
    ?SETUP(
       fun setup/0,
       ?FORALL(
          {S, B, K, T, V}, {c_size(), bucket(), key(), v_time(), val()},
          begin
              {In, R1, R2} =
                  remote_eval(insert_pop_body, [S, B, K, T, V]),
              ?WHENFAIL(io:format(user, "In: ~p~nR1: ~p~nR2: ~p~n",
                                  [In, R1, R2]),
                        R1 == {ok, {B, K} ,[{T, V}]} andalso
                        R2 == undefined)
          end)).

remove_bucket_body(Cache, Bucket, Gap) ->
    {H, T, Ds} = eval(Cache),
    mcache:remove_bucket(H, Bucket),
    TreeKs = all_keys_t(T, Gap),
    TreeKs1 = filter_bucket(TreeKs, Bucket, []),
    CacheKs = all_keys_c(H, []),
    Ds1 = check_elements(Ds),
    {CacheKs, TreeKs, TreeKs1, T, Ds1}.

prop_remove_bucket() ->
    ?SETUP(
       fun setup/0,
       ?FORALL(
          {MaxSize, Gap, Opts, Bkt}, {c_size(), nat(), opts(), bucket()},
          ?FORALL(
             Cache, cache(MaxSize, Gap, Opts),
             begin
                 {CacheKs, TreeKs, TreeKs1, T, Ds1} =
                     remote_eval(remove_bucket_body, [Cache, Bkt, Gap]),
                 ?WHENFAIL(io:format(user, "Cache: ~p~nTree:~p / ~p~nDs: ~p~n",
                                     [CacheKs, TreeKs, T, Ds1]),
                           CacheKs == TreeKs1 andalso
                           Ds1 == [])
             end))).


stats_body(Cache) ->
    {H, _T, _Ds} = eval(Cache),
    mcache:stats(H).

filter_buckets([], Acc) ->
    Acc;

filter_buckets([{_,
                 [{age, _},
                  {inserts, _},
                  {total_inserts, _},
                  {evictions, _},
                  {alloc, _},
                  {count, _},
                  {gen0,[{alloc, _}, {count, C1}, {g_count, C1}, {size, _}]},
                  {gen1,[{alloc, _}, {count, C2}, {g_count, C2}, {size, _}]},
                  {gen2,[{alloc, _}, {count, C3}, {g_count, C3}, {size, _}]}]} | R],
              Acc) ->
    filter_buckets(R, Acc);
filter_buckets([E | R], Acc) ->
    filter_buckets(R, [E | Acc]).


prop_stats() ->
    ?SETUP(
       fun setup/0,
       ?FORALL(
          {MaxSize, Gap, Opts}, {c_size(), nat(), opts()},
          ?FORALL(
             Cache, cache(MaxSize, Gap, Opts),
             begin
                 [{conf, _Cfg},
                  {alloc, _Alloc},
                  {count, _Count},
                  {evict_multiplyer, _},
                  {buckets, Bkts}] = remote_eval(stats_body, [Cache]),
                 R = filter_buckets(Bkts, []),
                 ?WHENFAIL(io:format(user, "Bkts: ~p~n",
                                     [R]),
                           R == [])
             end))).

map_comp_body(Cache, MaxGap) ->
    {H, T, Ds} = eval(Cache),
    TreeKs = all_keys_t(T, MaxGap),
    CacheKs = all_keys_c(H, []),
    Ds1 = check_elements(Ds),
    {CacheKs, TreeKs, T, Ds1}.


prop_map_comp() ->
    ?SETUP(
       fun setup/0,
       ?FORALL(
          {MaxSize, MaxGap, Opts}, {c_size(), nat(), opts()},
          ?FORALL(
             Cache, cache(MaxSize, MaxGap,  Opts),
             begin
                 {CacheKs, TreeKs, T, Ds1} =
                     remote_eval(map_comp_body, [Cache, MaxGap]),
                 ?WHENFAIL(io:format(user, "Cache: ~p~nTree:~p / ~p~nDs: ~p~n",
                                     [CacheKs, TreeKs, T, Ds1]),
                           CacheKs == TreeKs andalso
                           Ds1 == [])
             end))).

setup() ->
    fun() -> ct_slave:stop(eqc_client) end.
