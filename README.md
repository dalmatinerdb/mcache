# mcache
MCache is a cache specialised to deal with ingesting metrics. While it is written in the context of DalmatinerDB it is currently functional to store any array 64 bit allinged binary values.


## Goals
* Fast insert times
* Fast overflow removal
* Eventually size bonded
* moderate memory overhead per datapoint
* Handling of out of order writes (penalty is OK)



## Assumptions
MCache makes a number of assumptions on how the cached data behaved, understanding them will help understanding the methodology used as well as implementation details.

* Data comes mostly in order
* Data is not overwritten
* Gaps happen but are not the norm
* Data is either hot or cold, but does not consistantly flap between those two.
* Ingestion rate may differ on different keys


## Implementation

```
┌────────────┐
│            │
│   Cache    │
│            │
└────────────┘
       │  ┌────────────┐
       ├─▶│    gen1    │
       │  └────────────┘
       │         │          ┌────┬────┬────┬────┬────┐
       │         └─────────▶│ H1 │ H2 │ H3 │....│ Hn │
       │                    └────┴────┴────┴────┴────┘
       │  ┌────────────┐
       ├─▶│    gen2    │
       │  └────────────┘
       │         │          ┌────┬────┬────┬────┬────┐
       │         └─────────▶│ H1 │ H2 │ H3 │....│ Hn │
       │                    └────┴────┴────┴────┴────┘
       │  ┌────────────┐
       └─▶│    gen3    │
          └────────────┘
                 │          ┌────┬────┬────┬────┬────┐
                 └─────────▶│ H1 │ H2 │ H3 │....│ Hn │
                            └────┴────┴────┴────┴────┘
                               │
                               ▼
                            ┌────┐
                            │ M1 │
                            ├────┤
                            │ M2 │
                            ├────┤
                            │ M3 │
                            ├────┤
                            │....│
                            ├────┤
                            │ Mn │
                            └────┘
```

MCache implements a 3 generational caching algorithm, where hot data is expected to live in gen1 and gold data gravitates towards gen3.

Each time the cache ages gen2 data is merged into gen3, and the entire gen1 data is moved to gen2. Eviction on overflow happens the opposite away around, it first attempts gen3, then gen3 and if nothing helps gen1.

### Storage
Each generation uses a hash table of unsorted keys to store data. Each key has uses a linked list of arrays to represent data with potential gaps.

### Inserting
When data is inserted the lookup order is gen1 -> gen3, with the key being moved to gen1 when it is in gen2 or gen 3. This guarantees that hot keys gravitate towards gen1. The simplified algorithm for this would be:

```

if not key in gen1
    if key in gen2
      move key to gen1
    else if key in gen3
      move key to gen1
    else
      create key in gen1
    end
end
add point to key
```

### Overflow
The cache keeps track of the allocated data, if an insert causes the total allocation exceed the maximum a key is evicted. This eviction happens starting from gen3 then moving to gen2 and finally gen1 if no data to evict can be found.
To avoid freeing freshly written data eviction moves through the buckets starting with the next hash bucket in order of the most recent added key. In a bucket eviction will pick the largest key to evict.

### Adding points to a key
For each key a linked list of arrays is kept, this allows for adding points before the current first element by inserting a new array there. As well as dealing with gaps, by adding a new array behind the gap. 

When new inserts fill gaps then those are mended and the two list elements combined into a single one to improve memory usage as well as read performance.

Build
-----

    $ rebar3 compile
