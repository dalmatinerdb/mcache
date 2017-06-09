#define INITIAL_DATA_SIZE 40
#define INITIAL_ENTRIES 8
#define BUCKETS 128
#define HASH_SEED 42
// age every 1.000.000 inserts
#define AGE_CYCLE 1000000
//FFS C!
#define MAX(x, y) (((x) > (y)) ? (x) : (y))


typedef struct  mc_entry {
  ErlNifSInt64 start;
  uint32_t count;
  uint32_t size;
  uint8_t *data;
  uint8_t *end;
  struct mc_entry *next;
} mc_entry_t;

typedef struct {
  size_t alloc;
  uint8_t *name;
  uint64_t hash;
  uint16_t name_len;
  mc_entry_t *head;
  mc_entry_t *tail;
} mc_metric_t;

typedef struct {
  uint32_t size;
  uint32_t count;
  mc_metric_t **metrics;
} mc_bucket_t;

typedef struct {
  uint8_t v;
  size_t alloc;
  mc_bucket_t buckets[BUCKETS];
} mc_gen_t;

typedef struct {
  uint64_t max_alloc;
  uint32_t inserts;
  uint32_t age;
  mc_gen_t g0;
  mc_gen_t g1;
  mc_gen_t g2;
} mcache_t;

/*
  {ok, H} = mcache:new(50*10*8).
  mcache:insert(H, <<"1">>,  1, <<1:64>>).
  mcache:insert(H, <<"1">>,  2, <<2:64>>).
  mcache:get(H, <<"1">>).
*/
