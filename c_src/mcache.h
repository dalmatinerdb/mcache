#define INITIAL_DATA_SIZE 20
#define INITIAL_ENTRIES 100
#define MAX_CHUNK 255

//FFS C!
#define MAX(x, y) (((x) > (y)) ? (x) : (y))


typedef struct mc_entry {
  ErlNifSInt64 start;
  uint8_t count;
  uint8_t size;
  ErlNifSInt64 *data;
  struct mc_entry *next;
} mc_entry_t;

typedef struct {
  uint8_t *name;
  uint16_t name_len;
  size_t alloc;
  mc_entry_t *head;
} mc_metric_t;


typedef struct {
  size_t alloc;
  int size;
  int count;
  mc_metric_t **metrics;
} mc_gen_t;

typedef struct {
  uint64_t max_alloc;

  mc_gen_t g0;
  mc_gen_t g1;
  mc_gen_t g2;
} mcache_t;
