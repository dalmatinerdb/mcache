-module(bench_helper).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

run_bench(_Mod, _State, [], Acc) ->
    Acc;
run_bench(Mod, State, [{Bucket, Metric, Time, Value} | R], {C, W}) ->
    Res = Mod:do_put(Bucket, Metric, Time, Value, State),
    Acc = {C + length(Res),
           W + lists:sum([N || {write, N} <- Res])},
    run_bench(Mod, State, R, Acc).

-define(CACHE_POINTS, 100).

bench_new(MetricCount, Opts, Data) ->
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
    {WritesO, WrittenO} = run_bench(Mod, State, Data, {0, 0}),
    %%io:format(user, "~p~n", [mcache:stats(State)]),
    {WritesO, WrittenO, CacheBytes}.

bench_old(MetricCount, Data) ->
    Mod = mc_old_cache,
    CachePoints = ?CACHE_POINTS,
    State = Mod:init(CachePoints),
    {WritesO, WrittenO} = run_bench(Mod, State, Data, {0, 0}),
    Info = Mod:info(State),
    Mod:stop(State),
    Mem = proplists:get_value(memory, Info),
    Cnt = proplists:get_value(size, Info),
    {WritesO, WrittenO, Mem * 8 + Cnt * ?CACHE_POINTS * 8 + MetricCount * 4
     + MetricCount * 3}.

-define(MCOUNT, 1000).
-define(PPM, 1000).
-define(PPW, 1).


make_datapoints(MetricCount, PointsPerMetric, PointsPerWrite) ->
    Acc = make_all_points(MetricCount, 0, PointsPerMetric, PointsPerWrite, []),
    lists:reverse(Acc).

make_all_points(_MetricCount, _PointsPerMetric, _PointsPerMetric, _PointsPerWrite, Acc) ->
    Acc;

make_all_points(MetricCount, I, PointsPerMetric, PointsPerWrite, Acc) ->
    Acc1 = points_for_time(MetricCount, I, PointsPerWrite, Acc),
    make_all_points(MetricCount, I + 1, PointsPerMetric, PointsPerWrite, Acc1).

points_for_time(0, _I, _PointsPerWrite, Acc) ->
    Acc;
points_for_time(MetricCount, I, PointsPerWrite, Acc) ->
    Metric = integer_to_binary(MetricCount),
    Bucket = integer_to_binary(MetricCount rem 10),
    Value = << <<I:64>> || _ <- lists:seq(1, PointsPerWrite) >>,
    Time = I * PointsPerWrite,
    Acc1 = [{Bucket, Metric, Time, Value} | Acc],
    points_for_time(MetricCount - 1, I, PointsPerWrite, Acc1).

new_run(MetricCount, PointsPerWrite, Opts, Data) ->
    Args = [MetricCount, Opts, Data],
    {TN, {WritesN, WrittenN, Mem}} =
        timer:tc(?MODULE, bench_new, Args),
    io:format(user, "~nNew[~3b]: ~15b | ~15b | ~15b | ~15b | ~w",
              [PointsPerWrite, TN, WritesN, WrittenN, Mem, Opts]).


old_run(MetricCount, PointsPerMetric, PointsPerWrite) ->
    Data = make_datapoints(MetricCount, PointsPerMetric, PointsPerWrite),
    Args = [MetricCount, Data],
    {TO, {WritesO, WrittenO, Mem}} =
        timer:tc(?MODULE, bench_old, Args),
    io:format(user, "~nOld[~3b]: ~15b | ~15b | ~15b | ~15b",
              [PointsPerWrite, TO, WritesO, WrittenO, Mem]).

points_for_time_test_() ->
    {timeout, 60,
     fun() ->
             ok == make_datapoints(3, 2, 1)
     end}.

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
             lists:foreach(
               fun (PointsPerWrite) ->
                       Data = make_datapoints(MetricCount, PointsPerMetric, PointsPerWrite),
                       lists:foreach(
                         fun(Bkts) ->
                                 Opts = [{buckets, Bkts}],
                                 new_run(MetricCount, PointsPerWrite, Opts, Data),
                                 erlang:garbage_collect()
                         end,
                         [32, 64, 128, 256, 512]),
                       erlang:garbage_collect()
               end, [1, 10, 100]),
             ?assert(true)
     end}.

%%43720
%%54000
