#ifndef MC_H_INCLUDED
#define MC_H_INCLUDED

#define DEBUG

#include "erl_nif.h"
#include "stdio.h"
#include <string.h>
#include "xxhash.h"
#include <stdint.h>

#define SUBS 64
#define LCOUNT 10
#define BKT_GROWTH 20

/*
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
  │          └────┴────┴────┴────┴────┘
  │
  │
  ▼
  ┌────┬────┬────┬────┬────┐
  │ S1 │ S2 │ S3 │....│ Sn │
  └────┴────┴────┴────┴────┘
  │                   │
  ▼                   ▼
  ┌────┐              ┌────┐
  │ M1 │              │ M1 │
  ├────┤              ├────┤
  │ M2 │              │ M2 │
  ├────┤              ├────┤
  │ M3 │              │ M3 │
  ├────┤              ├────┤
  │....│              │....│
  ├────┤              ├────┤
  │ Mn │              │ Mn │
  └────┘              └────┘
*/

//FFS C!
#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))


// defining alloc and free functions so we can easiely switch between them
#define mc_alloc(size) malloc(size)
#define mc_free(ptr) free(ptr)

#define subid(x) (x >> 56) % SUBS

#ifdef DEBUG
#define DPRINT 1
#define TAGGED 1

#define TAG_ENTRY   0xFFFFFF01
#define TAG_METRIC  0xFFFFFF02
#define TAG_SUB     0xFFFFFF03
#define TAG_SLOT    0xFFFFFF04
#define TAG_GEN     0xFFFFFF05
#define TAG_CONF    0xFFFFFF06
#define TAG_CACHE   0xFFFFFF07
#define TAG_BKT     0xFFFFFF08

#define TAG_DATA_L    0xFFFFFFFFFFFFFF11
#define TAG_METRIC_L  0xFFFFFFFFFFFFFF12
#else
#define DPRINT 0
#endif

#define dprint(args ...) do {if(DPRINT){fprintf(stdout, args); fflush(stdout);}} while (0)

typedef struct  mc_entry {
#ifdef TAGGED
  uint32_t tag;
#endif
  ErlNifSInt64 start;
  uint32_t count;
  uint32_t size;
  ErlNifUInt64 *data;
  struct mc_entry *next;
} mc_entry_t;

typedef struct {
#ifdef TAGGED
  uint32_t tag;
#endif
  size_t alloc;
  uint8_t *name;
  uint16_t name_len;
  uint64_t hash;
  mc_entry_t *head;
  mc_entry_t *tail;
} mc_metric_t;


typedef struct {
#ifdef TAGGED
  uint32_t tag;
#endif
  uint32_t size;
  uint32_t count;
  mc_metric_t **metrics;
} mc_sub_slot_t;

typedef struct {
#ifdef TAGGED
  uint32_t tag;
#endif
  mc_sub_slot_t subs[SUBS];
  mc_metric_t *largest[LCOUNT];
} mc_slot_t;

typedef struct {
#ifdef TAGGED
  uint32_t tag;
#endif
  uint8_t v;
  size_t alloc;
  mc_slot_t *slots;
} mc_gen_t;

typedef struct {
#ifdef TAGGED
  uint32_t tag;
#endif
  uint64_t max_alloc;
  uint32_t slots;
  uint64_t age_cycle;
  uint16_t initial_data_size;
  uint16_t initial_entries;
  uint64_t hash_seed;
  uint64_t max_gap;
} mc_conf_t;

typedef struct {
#ifdef TAGGED
  uint32_t tag;
#endif
  uint8_t *name;
  uint16_t name_len;
  uint64_t hash;
  mc_gen_t g0;
  mc_gen_t g1;
  mc_gen_t g2;
} mc_bucket_t;

typedef struct {
#ifdef TAGGED
  uint32_t tag;
#endif
  mc_conf_t conf;
  uint32_t inserts;
  uint32_t age;
  uint32_t bkt_count;
  uint32_t bkt_size;
  mc_bucket_t *buckets;
} mcache_t;

typedef struct {
  mc_bucket_t *bucket;
  mc_metric_t *metric;
} bucket_metric_t;

void print_bkt(mc_conf_t conf, mc_bucket_t bucket);
int64_t find_bkt(mcache_t *cache, uint16_t bkt_len, uint8_t *bkt);

#endif // MC_H_INCLUDED
