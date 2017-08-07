#include "mcache.h"
#include "mcache_bucket.h"

void cache_free(mcache_t *cache) {
  bucket_free(cache->bucket);
}

void age(mcache_t* cache) {
  bucket_age(cache->bucket);
}

void is_empty(mcache_t* cache) {
  bucket_is_empty(cache->bucket);
}

mc_metric_t* find_metric(mcache_t* cache, uint64_t hash, uint16_t name_len, uint8_t *name) {
  return bucket_find_metric(cache->bucket, hash, name_len, name);
}

mc_metric_t* find_metric_and_remove(mcache_t* cache, uint64_t hash, uint16_t name_len, uint8_t *name) {
  return bucket_find_metric_and_remove(cache->bucket, hash, name_len, name);
}

mc_metric_t *get_metric(mcache_t* cache, uint64_t hash, uint16_t name_len, uint8_t *name) {
  return bucket_get_metric(cache->bucket, hash, name_len, name);
}


mc_metric_t * check_limit(mcache_t* cache, uint64_t max_alloc, uint64_t slot) {
  return bucket_check_limit(cache->bucket, max_alloc, slot);
}

mc_metric_t *insert(mcache_t* cache, ErlNifBinary name, uint64_t offset, ErlNifBinary value) {
  return bucket_insert(cache->bucket, name, offset, value);
}

mc_metric_t* take(mcache_t* cache, ErlNifBinary name) {
  uint64_t hash = XXH64(name.data, name.size, cache->conf.hash_seed);
  return find_metric_and_remove(cache, hash, name.size, name.data);
}

mc_metric_t* get(mcache_t* cache, ErlNifBinary name) {
  uint64_t hash = XXH64(name.data, name.size, cache->conf.hash_seed);
  return find_metric(cache, hash, name.size, name.data);
}

mc_metric_t* pop(mcache_t* cache) {
  return check_limit(cache, 0, 0);
}
