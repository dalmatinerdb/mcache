#include "mcache.h"
#include "mcache_bucket.h"

void cache_free(mcache_t *cache) {
  bucket_free(cache->bucket, cache->conf);
}

void age(mcache_t* cache) {
  bucket_age(cache->bucket, cache->conf);
}

void is_empty(mcache_t* cache) {
  bucket_is_empty(cache->bucket);
}

mc_metric_t* find_metric(mcache_t* cache, uint64_t hash, uint16_t name_len, uint8_t *name) {
  return bucket_find_metric(cache->bucket, cache->conf, hash, name_len, name);
}

mc_metric_t* find_metric_and_remove(mcache_t* cache, uint64_t hash, uint16_t name_len, uint8_t *name) {
  return bucket_find_metric_and_remove(cache->bucket, cache->conf, hash, name_len, name);
}

mc_metric_t *get_metric(mcache_t* cache, uint64_t hash, uint16_t name_len, uint8_t *name) {
  return bucket_get_metric(cache->bucket, cache->conf, hash, name_len, name);
}


mc_metric_t * check_limit(mcache_t* cache, uint64_t max_alloc, uint64_t slot) {
  return bucket_check_limit(cache->bucket, cache->conf, max_alloc, slot);
}

mc_metric_t *insert(mcache_t* cache, uint8_t *bkt, size_t bkt_len, uint8_t *name, size_t name_len, uint64_t offset, uint64_t *value, size_t value_len) {
  return bucket_insert(cache->bucket, cache->conf, name, name_len, offset, value, value_len);
}

mc_metric_t* take(mcache_t* cache, uint8_t *bkt, size_t bkt_len, uint8_t *name, size_t name_len) {
  uint64_t hash = XXH64(name, name_len, cache->conf.hash_seed);
  return find_metric_and_remove(cache, hash, name_len, name);
}

mc_metric_t* get(mcache_t* cache, uint8_t *bkt, size_t bkt_len, uint8_t *name, size_t name_len) {
  uint64_t hash = XXH64(name, name_len, cache->conf.hash_seed);
  return find_metric(cache, hash, name_len, name);
}

mc_metric_t* pop(mcache_t* cache) {
  return check_limit(cache, 0, 0);
}

mcache_t* cache_init(mc_conf_t config) {
  dprint("ctor\r\n");

  mcache_t *cache;
  cache = (mcache_t *) enif_alloc_resource(mcache_t_handle, sizeof(mcache_t));
#ifdef TAGGED
  cache->tag = TAG_CACHE;
#endif
  cache->conf = config;
  cache->bucket = bucket_init(config);
  return cache;
}
