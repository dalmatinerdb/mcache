-module(mc_new_cache).

-export([do_put/5, init/1]).

init([TotalSize, Opts]) ->
    mcache:new(TotalSize, Opts).

do_put(Bucket, Metric, Time, Data, C) ->
    Acc = [{buckets, 128}],
    case mcache:insert(C, Bucket, Metric, Time, Data) of
        ok ->
            Acc;
        {overflow, _, _, Overflow} ->
            case length(Overflow) of
                1 ->
                    ok;
                _N ->
                    io:format(user, "Multi element write: ~p~n",
                              [Overflow])
            end,
            lists:foldl(fun({_, Vs}, AccIn) ->
                                [{write, byte_size(Vs)}
                                 | AccIn]
                        end, Acc, Overflow)
    end.

%%    do_put(Bucket, Metric, Time, Data, C, ).

%% do_put(_Bucjet, _Metric, _Time, <<>>, _C, Acc) ->
%%     Acc;

%% do_put(Bucket, Metric, Time, <<V:8/binary, Rest/binary>>, C, Acc)
%%   when is_binary(Bucket), is_binary(Metric), is_integer(Time) ->
%%     Acc1 = case mcache:insert(C, Bucket, Metric, Time, V) of
%%                ok ->
%%                    Acc;
%%                {overflow, _, _, Overflow} ->
%%                    case length(Overflow) of
%%                        1 ->
%%                            ok;
%%                        _N ->
%%                            io:format(user, "Multi element write: ~p~n",
%%                                      [Overflow])
%%                    end,
%%                    lists:foldl(fun({_, Vs}, AccIn) ->
%%                                       [{write, byte_size(Vs)}
%%                                        | AccIn]
%%                               end, Acc, Overflow)
%%            end,
%%     do_put(Bucket, Metric, Time + 1, Rest, C, Acc1).
