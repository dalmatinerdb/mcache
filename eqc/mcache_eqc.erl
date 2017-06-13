-module(mcache_eqc).

-include_lib("eqc/include/eqc.hrl").

-compile(export_all).


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

insert({H, T, Ds}, N, O, V) ->
    case mcache:insert(H, N, O, V) of
        ok ->
            T1 = add_t(T, N, O, V),
            {H, T1, Ds};
        {overflow, K, Data} ->
            T1 = add_t(T, N, O, V),
            T2 = maps:remove(K, T1),
            {H, T2, [{shrink_t(Data), shrink_t(maps:get(K, T1))} | Ds]}
    end.

stats({H, T, Ds}) ->
    mcache:stats(H),
    {H, T, Ds}.

age({H, T, Ds}) ->
    mcache:age(H),
    {H, T, Ds}.

pop({H, T, Ds}) ->
    case mcache:pop(H) of
        undefined ->
            {H, T, Ds};
        {ok, K , Data} ->
            T1 = maps:remove(K, T),
            {H, T1, [{shrink_t(Data), shrink_t(maps:get(K, T))} | Ds]}
    end.

take({H, T, Ds}, N) ->
    case mcache:take(H, N) of
        undefined ->
            {H, T, Ds};
        {ok, Data} ->
            T1 = maps:remove(N, T),
            {H, T1, [{shrink_t(Data), shrink_t(maps:get(N, T))} | Ds]}
    end.

get({H, T, Ds}, N) ->
    mcache:get(H, N),
    {H, T, Ds}.

val() ->
    ?LET(Ns, ?SUCHTHAT(L, list(nat()), L /= []), << <<N:64>> || N <- Ns >>).

cache(MaxSize, Opts) ->
    ?SIZED(Size, cache(MaxSize, Opts, Size)).

key() ->
    utf8().

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

cache(MaxSize, Opts, 0) ->
    {call, ?MODULE, new, [MaxSize, Opts]};

cache(MaxSize, Opts, Size) ->
    ?LAZY(
       ?LETSHRINK(
          [H], [cache(MaxSize, Opts, Size -1)],
          frequency(
            [
             {1,   {call, ?MODULE, age, [H]}},
             {10,  {call, ?MODULE, stats, [H]}},
             {100, {call, ?MODULE, insert, [H, key(), v_time(), val()]}},
             {15,  {call, ?MODULE, pop, [H]}},
             {15,  {call, ?MODULE, take, [H, key()]}},
             {20,  {call, ?MODULE, get, [H, key()]}}
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
        {ok, K , Data} ->
            all_keys_c(H, [{K, shrink_t(Data)} | Acc])
    end.

size_c([], N) ->
    N;
size_c([{_O, D} | R], N) ->
    size_c(R, N + byte_size(D) div 8).

all_keys_t(T) ->
    L = maps:to_list(T),
    apply_t_size(L, []).

apply_t_size([], Acc) ->
    lists:sort(Acc);
apply_t_size([{K, Data} | R], Acc) ->
    apply_t_size(R, [{K, shrink_t(Data)} | Acc]).

shrink_t(M) when is_map(M) ->
    shrink_t(lists:sort(maps:to_list(M)), []);
shrink_t(L) when is_list(L)->
    shrink_t(lists:sort(L), []).

shrink_t([{N, D}, {N1, D1} | R], Acc) when N1 == N + (byte_size(D) div 8) ->
    shrink_t([{N, <<D/binary, D1/binary>>} | R], Acc);
shrink_t([E | R], Acc) ->
    shrink_t(R, [E | Acc]);
shrink_t([], Acc) ->
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

filter_pfx([{K, V} | R], Pfx, Acc) ->
    S = byte_size(Pfx),
    case K of
        <<Pfx:S/binary, _/binary>> ->
            filter_pfx(R, Pfx, Acc);
        _ ->
            filter_pfx(R, Pfx, [{K, V} | Acc])
    end;

filter_pfx([], _Pfx, Acc) ->
    lists:reverse(Acc).

%%====================================================================
%% Remote helpers
%%====================================================================

                                                %-define(LOCAL, 1).
-ifdef(LOCAL).
maybe_client() ->
    node().
-else.
maybe_client() ->
    case ct_slave:start(eqc_client) of
        {ok, Client} ->
            rpc:call(Client, code, set_path, [code:get_path()]),
            {ok, Client};
        {error, already_started, Client} ->
            {ok, Client};
        E ->
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
          {MaxSize, Opts}, {c_size(), opts()},
          ?FORALL(Cache, cache(MaxSize, Opts),
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
          {MaxSize, Opts}, {c_size(), opts()},
          ?FORALL(
             Cache, cache(MaxSize, Opts),
             remote_eval(is_empty_body, [Cache])
            ))).

insert_pop_body(S, K, T, V) ->
    In = {K, T, V},
    H = mcache:new(S, []),
    mcache:insert(H, K, T, V),
    R1 = mcache:pop(H),
    R2 = mcache:pop(H),
    {In, R1, R2}.

prop_insert_pop() ->
    ?SETUP(
       fun setup/0,
       ?FORALL(
          {S, K, T, V}, {c_size(), key(), v_time(), val()},
          begin
              {In, R1, R2} =
                  remote_eval(insert_pop_body, [S, K, T, V]),
              ?WHENFAIL(io:format(user, "In: ~p~nR1: ~p~nR2: ~p~n",
                                  [In, R1, R2]),
                        R1 == {ok,K ,[{T, V}]} andalso
                        R2 == undefined)
          end)).

remove_prefix_body(Cache, Pfx) ->
    {H, T, Ds} = eval(Cache),
    mcache:remove_prefix(H, Pfx),
    TreeKs = all_keys_t(T),
    TreeKs1 = filter_pfx(TreeKs, Pfx, []),
    CacheKs = all_keys_c(H, []),
    Ds1 = check_elements(Ds),
    {CacheKs, TreeKs, TreeKs1, T, Ds1}.

prop_remove_prefix() ->
    ?SETUP(
       fun setup/0,
       ?FORALL(
          {MaxSize, Opts, Pfx}, {c_size(), opts(), key()},
          ?FORALL(
             Cache, cache(MaxSize, Opts),
             begin
                 {CacheKs, TreeKs, TreeKs1, T, Ds1} =
                     remote_eval(remove_prefix_body, [Cache, Pfx]),
                 ?WHENFAIL(io:format(user, "Cache: ~p~nTree:~p / ~p~nDs: ~p~n",
                                     [CacheKs, TreeKs, T, Ds1]),
                           CacheKs == TreeKs1 andalso
                           Ds1 == [])
             end))).

map_comp_body(Cache) ->
    {H, T, Ds} = eval(Cache),
    TreeKs = all_keys_t(T),
    CacheKs = all_keys_c(H, []),
    Ds1 = check_elements(Ds),
    {CacheKs, TreeKs, T, Ds1}.


prop_map_comp() ->
    ?SETUP(
       fun setup/0,
       ?FORALL(
          {MaxSize, Opts}, {c_size(), opts()},
          ?FORALL(
             Cache, cache(MaxSize, Opts),
             begin
                 {CacheKs, TreeKs, T, Ds1} =
                     remote_eval(map_comp_body, [Cache]),
                 ?WHENFAIL(io:format(user, "Cache: ~p~nTree:~p / ~p~nDs: ~p~n",
                                     [CacheKs, TreeKs, T, Ds1]),
                           CacheKs == TreeKs andalso
                           Ds1 == [])
             end))).

setup() ->
    fun() -> ct_slave:stop(eqc_client) end.
