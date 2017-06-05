-module(mcache_eqc).

-include_lib("eqc/include/eqc.hrl").

-compile(export_all).


new(Size) ->
    {ok, H} = mcache:new(Size),
    H.

insert(H, N, O, V) ->
    mcache:insert(H, N, O, V),
    H.

stats(H) ->
    mcache:stats(H),
    H.

age(H) ->
    mcache:age(H),
    H.

get(H, N) ->
    mcache:get(H, N),
    H.

val() ->
    ?LET(N, nat(),  <<N:64>>).

cache(MaxSize) ->
    ?SIZED(Size, cache(MaxSize, Size)).

cache(MaxSize, 0) ->
    {call, ?MODULE, new, [MaxSize]};

cache(MaxSize, Size) ->
    ?LAZY(
       ?LETSHRINK(
          [H], [cache(MaxSize, Size -1)],
          frequency(
            [{1, {call, ?MODULE, age, [H]}},
             {10, {call, ?MODULE, stats, [H]}},
             {100, {call, ?MODULE, insert, [H, utf8(), nat(), val()]}},
             {20, {call, ?MODULE, get, [H, utf8()]}}]))).

prop_limit_ok() ->
    ?FORALL(
       MaxSize, nat(),
       ?FORALL(Cache, cache(MaxSize),
               begin
                   H = eval(Cache),
                   Stats = mcache:stats(H),
                   Max = proplists:get_value(max_alloc, Stats),
                   Total = proplists:get_value(total_alloc, Stats),
                   ?WHENFAIL(io:format(user, "Max: ~p~nTotal:~p~n", [Max, Total]),
                             Max >= Total)
               end)).
