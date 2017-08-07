-module(mc_new_cache).

-export([do_put/5, init/1]).

init([TotalSize, Opts]) ->
    mcache:new(TotalSize, Opts).

do_put(Bucket, Metric, Time, Data, C) ->
    Acc = [{buckets, 128}],
    %%io:format(user, "mcache:insert(H, ~p, ~p, ~p).\n", [BM, Time, V]),
    case mcache:insert(C, Bucket, Metric, Time, Data) of
        ok ->
            Acc;
        {overflow, _, Overflow} ->
            case length(Overflow) of
                1 ->
                    ok;
                N ->
                    io:format(user, "Multi element write: ~p~n",
                              [Overflow])
            end,
            lists:foldl(fun({_, Vs}, AccIn) ->
                                [{write, byte_size(Vs)}
                                 | AccIn]
                        end, Acc, Overflow)
    end.
