#ifndef MC_H_INCLUDED
#define MC_H_INCLUDED

//#define DEBUG

#include "erl_nif.h"
#include <string.h>
#include "stdio.h"
#include "xxhash.h"
#include <stdint.h>

#define SUBS 64
#define LCOUNT 10

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
                             └────┴────┴────┴────┴────┘
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
/*
  #define mc_alloc(size) malloc(size)
  #define mc_free(ptr) free(ptr)
*/

#define mc_alloc(size) enif_alloc(size)
#define mc_free(ptr) enif_free(ptr)

#define subid(x) (x >> 56) % SUBS

#ifdef DEBUG
#define DPRINT 1
#else
#define DPRINT 0
#endif

#define dprint(args ...) do {if(DPRINT){fprintf(stdout, args); fflush(stdout);}} while (0)

// used for debugging memory leaks
/*
#define TAGGED 1

#define TAG_ENTRY   0xFFFFFF01
#define TAG_METRIC  0xFFFFFF02
#define TAG_SUB     0xFFFFFF03
#define TAG_BKT     0xFFFFFF04
#define TAG_GEN     0xFFFFFF05
#define TAG_CONF    0xFFFFFF06
#define TAG_CACHE   0xFFFFFF07

#define TAG_DATA_L    0xFFFFFFFFFFFFFF11
#define TAG_METRIC_L  0xFFFFFFFFFFFFFF12
*/

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
  uint64_t hash;
  uint16_t name_len;
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
  uint64_t slots;
  uint64_t age_cycle;
  uint64_t initial_data_size;
  uint64_t initial_entries;
  uint64_t hash_seed;
  uint64_t max_gap;
} mc_conf_t;

typedef struct {
  #ifdef TAGGED
  uint32_t tag;
  #endif
  uint32_t inserts;
  uint32_t age;
  mc_gen_t g0;
  mc_gen_t g1;
  mc_gen_t g2;
} mc_bucket_t;

typedef struct {
  #ifdef TAGGED
  uint32_t tag;
  #endif
  mc_conf_t conf;
  mc_bucket_t *bucket;
} mcache_t;


ErlNifResourceType* mcache_t_handle;
static ERL_NIF_TERM atom_ok;
static ERL_NIF_TERM atom_undefined;
static ERL_NIF_TERM atom_overflow;

// Print functions
void print_cache(mcache_t *cache);

// init functions
void init_slots(mc_conf_t conf,/*@out@*/ mc_gen_t *gen);

// info
mcache_t* init_cache(mc_conf_t config);
ERL_NIF_TERM cache_info(ErlNifEnv* env, mcache_t *cache);

#endif // MC_H_INCLUDED
