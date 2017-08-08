#ifndef MC_IMPL_H_INCLUDED
#define MC_IMPL_H_INCLUDED

//impl

mcache_t* cache_init(mc_conf_t config);

void age(mcache_t *cache);
uint8_t is_empty(mcache_t *cache);

mc_reply_t take(mcache_t* cache, uint8_t *bkt, size_t bkt_len, uint8_t *name, size_t name_len);
mc_reply_t get(mcache_t* cache, uint8_t *bkt, size_t bkt_len, uint8_t *name, size_t name_len);
mc_reply_t pop(mcache_t *cache);
mc_reply_t insert(mcache_t* cache, uint8_t *bkt, size_t bkt_len, uint8_t *name, size_t name_len, uint64_t offset, uint64_t *value, size_t value_len);

void cache_free(mcache_t* c);
ERL_NIF_TERM serialize_reply_name(ErlNifEnv* env, mc_reply_t reply);
void remove_bucket(mcache_t* cache, uint8_t *name, size_t name_len);

#endif // MC_IMPL_H_INCLUDED
