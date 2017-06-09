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

-define(CACHE_POINTS, 100).

bench_new(MetricCount, PointsPerMetric, PointsPerWrite) ->
    Mod = mc_new_cache,
    %% equivalent to cache points *2 to take into account ets overhead
    %% handled by
    %% MetricCount * 8 * 100 (100 64 byte values)
    CacheBytes = MetricCount * 8 * ?CACHE_POINTS
        %% for the the names (average 4 bytes, ignore length and so on)
        + MetricCount * 4
        %% start, end and false value
        + MetricCount * 3
        %% ETS overhead (measured as 200094 for 10k then scale)
        + (200094 * 8 div 10000) * MetricCount
        ,
    State = Mod:init(CacheBytes),
    {WritesO, WrittenO} = run_bench(Mod, State, MetricCount, PointsPerMetric, PointsPerWrite),
    %%io:format(user, "~p~n", [mcache:stats(State)]),
    {WritesO, WrittenO, CacheBytes}.

bench_old(MetricCount, PointsPerMetric, PointsPerWrite) ->
    Mod = mc_old_cache,
    CachePoints = ?CACHE_POINTS,
    State = Mod:init(CachePoints),
    {WritesO, WrittenO} = run_bench(Mod, State, MetricCount, PointsPerMetric, PointsPerWrite),
    Info = Mod:info(State),
    Mem = proplists:get_value(memory, Info),
    Cnt = proplists:get_value(size, Info),
    {WritesO, WrittenO, Mem * 8 + Cnt * ?CACHE_POINTS * 8 + MetricCount * 4
     + MetricCount * 3}.

-define(MCOUNT, 10000).
-define(PPM, 1000).
-define(PPW, 1).
bench_new_test_() ->
    {timeout, 60,
     fun () ->
             MetricCount = ?MCOUNT,
             PointsPerMetric = ?PPM,
             PointsPerWrite = ?PPW,
             Args = [MetricCount, PointsPerMetric, PointsPerWrite],
             {TN, {WritesN, WrittenN, Mem}} =
                 timer:tc(?MODULE, bench_new, Args),
             io:format(user, "~n     ~20s | ~20s | ~20s | ~20s",
                       ["Time", "Writes", "Written Bytes", "Memory"]),
             io:format(user, "~nNew: ~20b | ~20b | ~20b | ~20b",
                       [TN, WritesN, WrittenN, Mem]),
             ?assert(true)
     end}.

bench_old_test_() ->
    {timeout, 60,
     fun () ->
             MetricCount = ?MCOUNT,
             PointsPerMetric = ?PPM,
             PointsPerWrite = ?PPW,
             Args = [MetricCount, PointsPerMetric, PointsPerWrite],
             {TO, {WritesO, WrittenO, Mem}} =
                 timer:tc(?MODULE, bench_old, Args),
             io:format(user, "~nOld: ~20b | ~20b | ~20b | ~20b",
                       [TO, WritesO, WrittenO, Mem]),
             ?assert(true)
     end}.

%%43720
%%54000
