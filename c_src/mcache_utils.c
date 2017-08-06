#include "mcache.h"

int64_t find_bkt(mcache_t *cache, uint16_t name_len, uint8_t *name) {
  dprint("find_bkt\r\n");
  uint64_t hash = XXH64(name, name_len, cache->conf.hash_seed);
  for (uint32_t b = 0; b < cache->bkt_count; b++) {
    mc_bucket_t bkt = cache->buckets[b];
    if (bkt.hash == hash &&
        name_len == bkt.name_len &&
        memcmp(bkt.name, name, name_len) == 0) {
      dprint(" [%llu]> %llu == %llu\r\n", b, hash, bkt.hash);
      return b;
    }
    dprint(" [%u]len > %hu != %hu\r\n", b, name_len, bkt.name_len);
    dprint(" [%u]hash> %llu != %llu\r\n", b, hash, bkt.hash);
  }
  return -1;
}
