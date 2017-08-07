#ifndef MC_IMPL_H_INCLUDED
#define MC_IMPL_H_INCLUDED

//impl
mc_metric_t * check_limit(mcache_t *cache, uint64_t max_alloc, uint64_t slot);

void age(mcache_t *cache);
uint8_t is_empty(mcache_t *cache);
mc_metric_t *get_metric(mcache_t *cache, uint64_t hash, uint16_t name_len, uint8_t *name);
mc_metric_t *find_metric(mcache_t *cache, uint64_t hash, uint16_t name_len, uint8_t *name);
mc_metric_t *find_metric_and_remove(mcache_t *cache, uint64_t hash, uint16_t name_len, uint8_t *name);

mc_metric_t* take(mcache_t *cache, ErlNifBinary name);
mc_metric_t* get(mcache_t *cache, ErlNifBinary name);
mc_metric_t* pop(mcache_t *cache);
mc_metric_t* insert(mcache_t *cache, ErlNifBinary name, uint64_t offset, ErlNifBinary data);
void cache_free(mcache_t* c);

#endif // MC_IMPL_H_INCLUDED
