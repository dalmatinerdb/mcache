-module(mc_new_cache).

-export([do_put/4, init/1]).

init(TotalSize) ->
    {ok, C} = mcache:new(TotalSize),
    C.

do_put(BM, Time, Data, C) ->
    do_put(BM, Time, Data, C, []).

do_put(_BM, _Time, <<>>, _C, Acc) ->
    Acc;

do_put(BM, Time, <<V:8/binary, Rest/binary>>, C, Acc)
  when is_binary(BM), is_integer(Time) ->
    %%io:format(user, "mcache:insert(H, ~p, ~p, ~p).\n", [BM, Time, V]),
    Acc1 = case mcache:insert(C, BM, Time, V) of
               ok ->
                   Acc;
               {overflow, _, Overflow} ->
                   lists:foldl(fun({_, Vs}, AccIn) ->
                                      [{write, byte_size(Vs)}
                                       | AccIn]
                              end, Acc, Overflow)
           end,
    do_put(BM, Time + 1, Rest, C, Acc1).
