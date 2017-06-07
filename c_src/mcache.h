#define INITIAL_DATA_SIZE 20
#define INITIAL_ENTRIES 20
#define MAX_CHUNK 255
#define BUCKETS 255
#define HASH_SEED 42

//FFS C!
#define MAX(x, y) (((x) > (y)) ? (x) : (y))


typedef struct mc_entry {
  ErlNifSInt64 start;
  uint64_t count;
  uint64_t size;
  ErlNifSInt64 *data;
  struct mc_entry *next;
} mc_entry_t;

typedef struct {
  size_t alloc;
  uint8_t *name;
  uint16_t name_len;
  mc_entry_t *head;
} mc_metric_t;


typedef struct {
  uint64_t size;
  uint64_t count;
  mc_metric_t **metrics;
} mc_bucket_t;

typedef struct {
  uint8_t v;
  size_t alloc;
  mc_bucket_t buckets[BUCKETS];
} mc_gen_t;

typedef struct {
  uint64_t max_alloc;
  mc_gen_t g0;
  mc_gen_t g1;
  mc_gen_t g2;
} mcache_t;
