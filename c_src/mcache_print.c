#include "mcache.h"

static void print_entry(mc_entry_t *entry) {
  {};
  uint8_t i;
  if (!entry) {
    printf("\r\n");
    return;
  }
  printf("    @%ld:", entry->start);

  for (i=0; i < entry->count; i++) {
    printf(" %ld", entry->data[i]);
  };

  if (entry->next) {
    printf(" |");
    print_entry(entry->next);
  } else {
    printf("\r\n");
  };
}

static void print_metric(mc_metric_t *metric) {
  uint16_t i;
  printf("  ");
  for(i = 0; i < metric->name_len; i++)
    printf("%c", (int) metric->name[i]);
  printf("[%zu]:\r\n", metric->alloc);
  print_entry(metric->head);
}

static void print_gen(mc_conf_t conf, mc_gen_t gen) {
  int size = 0;
  int count = 0;
  for (int i = 0; i < conf.slots; i++) {
    for (int sub = 0; sub < SUBS; sub++) {
      size += gen.slots[i].subs[sub].size;
      count += gen.slots[i].subs[sub].count;
    }
  };
  printf("Cache: [c: %d |s: %d|a: %zu]:\r\n",  count, size, gen.alloc);
  for (int i = 0; i < conf.slots; i++) {
    for (int sub = 0; sub < SUBS; sub++) {
      for(int j = 0; j < gen.slots[i].subs[sub].count; j++) {
        print_metric(gen.slots[i].subs[sub].metrics[j]);
      }
    }
  }
}

static void print_bucket(mc_bucket_t *bucket) {
  print_gen(bucket->conf, bucket->g0);
  print_gen(bucket->conf, bucket->g1);
  print_gen(bucket->conf, bucket->g2);
};

void print_cache(mcache_t *cache) {
  print_bucket(cache->bucket);
};
