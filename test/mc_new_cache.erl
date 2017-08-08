-module(mc_new_cache).

-export([do_put/5, init/1]).

init([TotalSize, Opts]) ->
    mcache:new(TotalSize, Opts).

do_put(Bucket, Metric, Time, Data, C) ->
    %%io:format(user, "mcache:insert(H, ~p, ~p, ~p).\n", [BM, Time, V]),
    case mcache:insert(C, Bucket, Metric, Time, Data) of
        ok ->
            [];
        {overflow, _, Overflow} ->
            lists:foldl(fun({_, Vs}, AccIn) ->
                                [{write, byte_size(Vs)}
                                 | AccIn]
                        end, [], Overflow)
    end.
