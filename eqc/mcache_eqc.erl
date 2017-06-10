-module(mcache_eqc).

-include_lib("eqc/include/eqc.hrl").

-compile(export_all).


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
    {ok, H} = mcache:new(Size, Opts),
    {H, #{}, []}.

insert({H, T, Ds}, N, O, V) ->
    case mcache:insert(H, N, O, V) of
        ok ->
            T1 = add_t(T, N, O, V),
            {H, T1, Ds};
        %% TODO: record overflow!
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
        %% TODO: record removes!
        {ok, K , Data} ->
            T1 = maps:remove(K, T),
            {H, T1, [{shrink_t(Data), shrink_t(maps:get(K, T))} | Ds]}
    end.

take({H, T, Ds}, N) ->
    case mcache:take(H, N) of
        undefined ->
            {H, T, Ds};
        %% TODO: record removes!
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
    shrink_t(L, []).

shrink_t([{N, D}, {N1, D1} | R], Acc) when N1 == N + (byte_size(D) div 8) ->
    shrink_t([{N, <<D/binary, D1/binary>>} | R], Acc);
shrink_t([E | R], Acc) ->
    shrink_t(R, [E | Acc]);
shrink_t([], Acc) ->
    lists:reverse(Acc).

check_elements([]) ->
    [];
check_elements([{D, D} | R]) ->
    check_elements(R);
check_elements([H | _T]) ->
    [H].


prop_limit_ok() ->
    ?FORALL(
       {MaxSize, Opts}, {c_size(), opts()},
       ?FORALL(Cache, cache(MaxSize, Opts),
               ?TIMEOUT(1000,
                        begin
                            %% io:format("~p~n", [Cache]),
                            {H, _, _} = eval(Cache),
                            Stats = mcache:stats(H),
                            Max = proplists:get_value(max_alloc, Stats),
                            Total = proplists:get_value(total_alloc, Stats),
                            ?WHENFAIL(io:format(user, "Max: ~p~nTotal:~p~n", [Max, Total]),
                                      Max >= Total)
                        end))).


prop_insert_pop() ->
    ?FORALL(
       {S, K, T, V}, {c_size(), key(), v_time(), val()},
       ?TIMEOUT(1000,
                begin
                    In = {K, T, V},
                    {ok, H} = mcache:new(S, []),
                    mcache:insert(H, K, T, V),
                    R1 = mcache:pop(H),
                    R2 = mcache:pop(H),
                    ?WHENFAIL(io:format(user, "In: ~p~nR1: ~p~nR2: ~p~n",
                                        [In, R1, R2]),
                              R1 == {ok,K ,[{T, V}]} andalso
                              R2 == undefined)
                end)).

prop_map_comp() ->
    ?FORALL(
       {MaxSize, Opts}, {c_size(), opts()},
       ?FORALL(
          Cache, cache(MaxSize, Opts),
          ?TIMEOUT(1000,
                   begin
                       {H, T, Ds} = eval(Cache),
                       TreeKs = all_keys_t(T),
                       CacheKs = all_keys_c(H, []),
                       Ds1 = check_elements(Ds),
                       ?WHENFAIL(io:format(user, "Cache: ~p~nTree:~p / ~p~nDs: ~p~n",
                                           [CacheKs, TreeKs, T, Ds1]),
                                 CacheKs == TreeKs andalso
                                 Ds1 == [])
                   end))).
