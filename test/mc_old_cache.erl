-module(mc_old_cache).

-define(DATA_SIZE, 8).
-record(state, {tbl, cache_size}).
-export([do_put/5, init/1, stop/1, info/1]).

init(CacheSize) ->
    #state{
       tbl = ets:new(?MODULE, [public, ordered_set]),
       cache_size = CacheSize
      }.

stop(#state{tbl = T}) ->
    ets:delete(T).

info(#state{tbl = T}) ->
    ets:info(T).

do_put(Bucket, Metric, Time, Value, #state{tbl = T, cache_size = CacheSize}) ->
    BM = term_to_binary({Bucket, Metric}),
    Len = byte_size(Value) div ?DATA_SIZE,
    %% Technically, we could still write data that falls within a range that is
    %% to be deleted by the vacuum.  See the `timestamp()' function doc.
    %% Elemements of the cacgh have the following logic:
    %% 1: Bucket + Metric
    %% 2: The start time (index in the file)
    %% 3: Last written part of the buffer
    %% 4: pre-computed end (end in the file)
    %% 5: The binary cache
    case ets:lookup(T, BM) of
        %% If the data is before the first package in the cache we just
        %% don't cache it this way we prevent overwriting already
        %% written data.
        [{BM, _Start, _Size, _End, _V}]
          when Time < _Start ->
            [{write, byte_size(Value)}];
        %% When the Delta of start time and this package is greater
        %% then the cache time we flush the cache and start a new cache
        %% with a new package
        [{BM, _Start, Size, _End, Array}]
          when (Time + Len) >= _End, Len < CacheSize ->
            Bin = k6_bytea:get(Array, 0, Size * ?DATA_SIZE),
            k6_bytea:set(Array, 0, Value),
            k6_bytea:set(Array, Len * ?DATA_SIZE,
                         <<0:(?DATA_SIZE * 8 * (CacheSize - Len))>>),
            %% We need this for mock
            End = cache_end(Time, CacheSize),
            ets:update_element(T, BM, [{2, Time}, {3, Len},
                                       {4, End}]),
            [{write, byte_size(Bin)}];
        %% In the case the data is already longer then the cache we
        %% flush the cache
        [{BM, _Start, Size, _End, Array}]
          when (Time + Len) >= _End ->
            ets:delete(T, BM),
            Bin = k6_bytea:get(Array, 0, Size * ?DATA_SIZE),
            k6_bytea:delete(Array),
            [{write, byte_size(Bin)},
             {write, byte_size(Value)}];
        %% Only update the Size section when the new
        %% size is larger then the old size.
        [{BM, Start, Size, _End, Array}]
          when (Time - Start) + Len > Size ->
            Idx = Time - Start,
            k6_bytea:set(Array, Idx * ?DATA_SIZE, Value),
            ets:update_element(T, BM, [{3, Idx + Len}]),
            [];
        [{BM, Start, _Size, _End, Array}] ->
            Idx = Time - Start,
            k6_bytea:set(Array, Idx * ?DATA_SIZE, Value),
            [];
        %% We don't have a cache yet and our data is smaller then
        %% the current cache limit
        [] when Len < CacheSize ->
            Array = k6_bytea:new(CacheSize * ?DATA_SIZE),
            k6_bytea:set(Array, 0, Value),
            %% We need this for mock
            End = cache_end(Time, CacheSize),
            ets:insert(T, {BM, Time, Len, End, Array}),
            [];
        %% If we don't have a cache but our data is too big for the
        %% cache we happiely write it directly
        [] ->
            [{write, byte_size(Value)}]
    end.

cache_end(Start, CacheSize) ->
    Jitter = rand:uniform(CacheSize div 2),
    Start + CacheSize - Jitter.
