#include "mcache.h"
#include "mcache_bucket.h"
#include "mcache_utils.h"

static mc_bucket_t* find_bucket(mcache_t* cache, uint8_t *name, size_t name_len) {
  mc_bucket_t* bucket = cache->bucket;
  uint64_t hash = XXH64(name, name_len, cache->conf.hash_seed);
  while (bucket) {
    if (bucket->name_len == name_len
        && bucket->hash == hash
        && meq(bucket->name, name, name_len)) {
      return bucket;
    }
    bucket = bucket->next;
  }
  return bucket;
}

uint64_t size(mcache_t* cache) {
  uint64_t alloc = 0;
  mc_bucket_t* bucket = cache->bucket;
  while (bucket) {
    alloc += bucket->g0.alloc;
    alloc += bucket->g1.alloc;
    alloc += bucket->g2.alloc;
    bucket = bucket->next;
  };
  return alloc;
}


void age(mcache_t* cache) {
  mc_bucket_t* bucket = cache->bucket;
  while (bucket) {
    bucket_age(bucket, cache->conf);
    bucket = bucket->next;
  }
}

uint8_t is_empty(mcache_t* cache) {
  mc_bucket_t* bucket = cache->bucket;
  while (bucket) {
    if (! bucket_is_empty(bucket)) {
      return 0;
    };
    bucket = bucket->next;
  }
  return 1;
}


static mc_reply_t check_limit(mcache_t* cache, uint64_t max_alloc, uint64_t slot) {
  mc_reply_t reply = {NULL, NULL};
  reply.bucket = cache->bucket;
  if (size(cache) < max_alloc || !reply.bucket) {
    return reply;
  }
  uint64_t bucket_quota = max_alloc / cache->bucket_count;
  while (reply.bucket) {
    reply.metric = bucket_check_limit(reply.bucket, cache->conf, bucket_quota, slot);
    if (reply.metric) {
      return reply;
    }
    reply.bucket = reply.bucket->next;
  }
  return reply;
}

mc_reply_t insert(mcache_t* cache, uint8_t *bkt, size_t bkt_len, uint8_t *name, size_t name_len, uint64_t offset, uint64_t *value, size_t value_len) {
  mc_reply_t reply = {NULL, NULL};
  reply.bucket = find_bucket(cache, bkt, bkt_len);
  if (!reply.bucket) {
    reply.bucket = bucket_init(cache->conf, bkt, bkt_len);
    reply.bucket->next = cache->bucket;
    cache->bucket_count++;
    cache->bucket = reply.bucket;
  }
  reply.metric = bucket_insert(reply.bucket, cache->conf, name, name_len, offset, value, value_len);
  return reply;
}

mc_reply_t take(mcache_t* cache, uint8_t *bkt, size_t bkt_len, uint8_t *name, size_t name_len) {
  mc_reply_t reply = {NULL, NULL};
  reply.bucket = find_bucket(cache, bkt, bkt_len);
  if (!reply.bucket) {
    return reply;
  }

  uint64_t hash = XXH64(name, name_len, cache->conf.hash_seed);
  reply.metric = bucket_find_metric_and_remove(reply.bucket, cache->conf, hash, name_len, name);
  return reply;

}

mc_reply_t get(mcache_t* cache, uint8_t *bkt, size_t bkt_len, uint8_t *name, size_t name_len) {
  mc_reply_t reply = {NULL, NULL};
  reply.bucket = find_bucket(cache, bkt, bkt_len);
  if (!reply.bucket) {
    return reply;
  }

  uint64_t hash = XXH64(name, name_len, cache->conf.hash_seed);
  reply.metric = bucket_find_metric(reply.bucket, cache->conf, hash, name_len, name);
  return reply;
}

mc_reply_t pop(mcache_t* cache) {
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
  cache->bucket = NULL;
  cache->bucket_count = 0;
  return cache;
}

void cache_free(mcache_t *cache) {
  dprint("cache_free\r\n");
  mc_bucket_t* bucket = cache->bucket;

  while (bucket) {
    dprint("freeing bucket\r\n");
    bucket = bucket_free(bucket, cache->conf);
  }
  //mc_free(cache);
}

ERL_NIF_TERM serialize_reply_name(ErlNifEnv* env, mc_reply_t reply) {
  ERL_NIF_TERM bucket;
  unsigned char *bucketp;
  ERL_NIF_TERM name;
  unsigned char *namep;
  namep = enif_make_new_binary(env, reply.metric->name_len, &name);
  memcpy(namep, reply.metric->name, reply.metric->name_len);

  bucketp = enif_make_new_binary(env, reply.bucket->name_len, &bucket);
  memcpy(bucketp, reply.bucket->name,reply.bucket->name_len);

  return  enif_make_tuple2(env, bucket, name);
}


/*
  f().
  H = mcache:new(257, [{max_gap,0}, {buckets,1}, {age_cycle,1}, {initial_data_size,12}, {initial_entries,1}, {hash_seed,1}]).
  mcache:insert(H, <<"a">>, <<>>,0, <<0,0,0,0,0,0,0,0>>).
  mcache:insert(H, <<"a">>, <<>>,0, <<0,0,0,0,0,0,0,0>>).
 */
