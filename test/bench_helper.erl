-module(bench_helper).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

run_bench(Mod, State, MetricCount, PointsPerMetric, PointsPerWrite) ->
    Acc = {0, 0},
    run_all_points(Mod, State, MetricCount, 1, PointsPerMetric, PointsPerWrite,
                   Acc).


run_all_points(_Mod, _State, _MetricCount, _PointsPerMetric, _PointsPerMetric,
               _PointsPerWrite, Acc) ->
    Acc;

run_all_points(Mod, State, MetricCount, I, PointsPerMetric,
               PointsPerWrite, Acc) ->
    Acc1 = write_all_metrics(Mod, State, MetricCount, I, PointsPerWrite, Acc),
    run_all_points(Mod, State, MetricCount, I + 1, PointsPerMetric,
                   PointsPerWrite, Acc1).

write_all_metrics(_Mod, _State, 0, _I, _PointsPerWrite, Acc) ->
    Acc;
write_all_metrics(Mod, State, MetricCount, I, PointsPerWrite, {C, W}) ->
    Metric = integer_to_binary(MetricCount),
    Value = <<I:64>>,
    Time = I,
    Res = Mod:do_put(Metric, Time, Value, State),
    Acc1 = {C + length(Res),
            W + lists:sum([N || {write, N} <- Res])},
    write_all_metrics(Mod, State, MetricCount - 1, I, PointsPerWrite, Acc1).

-define(CACHE_POINTS, 50).

bench_new(MetricCount, PointsPerMetric, PointsPerWrite) ->
    Mod = mc_new_cache,
    CacheBytes = ?CACHE_POINTS * MetricCount * 8, %% equivalent to cache points
    State = Mod:init(CacheBytes),
    run_bench(Mod, State, MetricCount, PointsPerMetric, PointsPerWrite).

bench_old(MetricCount, PointsPerMetric, PointsPerWrite) ->
    Mod = mc_old_cache,
    CachePoints = ?CACHE_POINTS,
    State = Mod:init(CachePoints),
    run_bench(Mod, State, MetricCount, PointsPerMetric, PointsPerWrite).

bench_test_() ->
    {timeout, 60,
     fun () ->
             MetricCount = 10,
             PointsPerMetric = 1000,
             PointsPerWrite = 1,
             Args = [MetricCount, PointsPerMetric, PointsPerWrite],
             {TN, {WritesN, WrittenN}} = timer:tc(?MODULE, bench_new, Args),
             io:format(user, "~p / ~p / ~p~n", [TN, WritesN, WrittenN]),
             {TO, {WritesO, WrittenO}} = timer:tc(?MODULE, bench_old, Args),
             io:format(user, "~p / ~p / ~p~n", [TO, WritesO, WrittenO]),
             ?assert(true)
     end}.
