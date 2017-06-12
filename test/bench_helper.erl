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
    Value = << <<I:64>> || _ <- lists:seq(1, PointsPerWrite) >>,
    Time = I,
    Res = Mod:do_put(Metric, Time*PointsPerWrite, Value, State),
    Acc1 = {C + length(Res),
            W + lists:sum([N || {write, N} <- Res])},
    write_all_metrics(Mod, State, MetricCount - 1, I, PointsPerWrite, Acc1).

-define(CACHE_POINTS, 100).

bench_new(MetricCount, Opts, PointsPerMetric, PointsPerWrite) ->
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
    State = Mod:init([CacheBytes, Opts]),
    {WritesO, WrittenO} = run_bench(Mod, State, MetricCount, PointsPerMetric, PointsPerWrite),
    %%io:format(user, "~p~n", [mcache:stats(State)]),
    {WritesO, WrittenO, CacheBytes}.

bench_old(MetricCount, PointsPerMetric, PointsPerWrite) ->
    Mod = mc_old_cache,
    CachePoints = ?CACHE_POINTS,
    State = Mod:init(CachePoints),
    {WritesO, WrittenO} = run_bench(Mod, State, MetricCount, PointsPerMetric, PointsPerWrite),
    Info = Mod:info(State),
    Mod:stop(State),
    Mem = proplists:get_value(memory, Info),
    Cnt = proplists:get_value(size, Info),
    {WritesO, WrittenO, Mem * 8 + Cnt * ?CACHE_POINTS * 8 + MetricCount * 4
     + MetricCount * 3}.

-define(MCOUNT, 10000).
-define(PPM, 1000).
-define(PPW, 1).

new_run(MetricCount, PointsPerMetric, PointsPerWrite, Opts) ->
    Args = [MetricCount, Opts, PointsPerMetric div PointsPerWrite, PointsPerWrite],
    {TN, {WritesN, WrittenN, Mem}} =
        timer:tc(?MODULE, bench_new, Args),
    io:format(user, "~nNew[~3b]: ~15b | ~15b | ~15b | ~15b | ~w",
              [PointsPerWrite, TN, WritesN, WrittenN, Mem, Opts]).


old_run(MetricCount, PointsPerMetric, PointsPerWrite) ->
    Args = [MetricCount, PointsPerMetric div PointsPerWrite, PointsPerWrite],
    {TO, {WritesO, WrittenO, Mem}} =
        timer:tc(?MODULE, bench_old, Args),
    io:format(user, "~nOld[~3b]: ~15b | ~15b | ~15b | ~15b",
              [PointsPerWrite, TO, WritesO, WrittenO, Mem]).

bench_01_old_test_() ->
    {timeout, 60,
     fun () ->
             MetricCount = ?MCOUNT,
             PointsPerMetric = ?PPM,
             io:format(user, "~n     ~20s | ~15s | ~15s | ~15s | Opts",
                       ["Time", "Writes", "Written Bytes", "Memory"]),
             old_run(MetricCount, PointsPerMetric, 1),
             erlang:garbage_collect(),
             old_run(MetricCount, PointsPerMetric, 10),
             erlang:garbage_collect(),
             old_run(MetricCount, PointsPerMetric, 100),
             ?assert(true)
     end}.

bench_11_new_test_() ->
    {timeout, 600,
     fun () ->
             MetricCount = ?MCOUNT,
             PointsPerMetric = ?PPM,
             lists:foreach(fun (Bkts) ->
                                   Opts = [{buckets, Bkts}],
                                   new_run(MetricCount, PointsPerMetric, 1,
                                           Opts),
                                   erlang:garbage_collect(),
                                   new_run(MetricCount, PointsPerMetric,
                                           10, Opts),
                                   erlang:garbage_collect(),
                                   new_run(MetricCount, PointsPerMetric,
                                           100, Opts),
                                   erlang:garbage_collect()
                           end, [32, 64, 128, 256, 512]),

             ?assert(true)
     end}.

%%43720
%%54000
