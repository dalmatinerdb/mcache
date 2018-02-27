#include "mcache.h"
#include "mcache_bucket.h"
#include "mcache_utils.h"

static mc_bucket_t* find_bucket(mcache_t* cache, uint8_t *name, size_t name_len) {
  uint64_t hash = XXH64(name, name_len, cache->conf.hash_seed);
  for (uint32_t b = 0; b < cache->bucket_count; b++) {
    mc_bucket_t* bucket = cache->buckets[b];
    if (bucket->name_len == name_len
        && bucket->hash == hash
        && meq(bucket->name, name, name_len)) {
      // When we found our bucket we see if it is accessed more frequetly
      // then the bucket before it, if it is we let it tickle forward so it
      // is found quicker in the foture. This of cause only makes sense if we
      // are not at the first bucket anyway.
      if (b > 0) {
        mc_bucket_t* last = cache->buckets[b - 1];
        // if this bucket has more writes then the previous one we flip it
        if (
            // If this bucket has aged more
            last->age < bucket->age ||
            // or if they have aged the saame but it has more inserts with a margin
            // of 42, because there had to be a number to be picked this is to prevent
            // too much flapping
            (last->age  == bucket->age && (bucket->inserts - last->inserts) > 42)) {
          // We switch them around
          cache->buckets[b] = last;
          cache->buckets[b - 1] = bucket;
        }
      }
      return bucket;
    }
  }
  return NULL;
}

void remove_bucket(mcache_t* cache, uint8_t *name, size_t name_len) {
  uint64_t hash = XXH64(name, name_len, cache->conf.hash_seed);
  for (uint32_t b = 0; b < cache->bucket_count; b++) {
    mc_bucket_t* bucket = cache->buckets[b];
    if (bucket->name_len == name_len
        && bucket->hash == hash
        && meq(bucket->name, name, name_len)) {
      // free the bucket
      bucket_free(bucket, cache->conf);
      // replace it with the last one
      cache->bucket_count--;
      cache->buckets[b] = cache->buckets[cache->bucket_count];
      return;
    }
  }
}

uint64_t size(mcache_t* cache) {
  uint64_t alloc = 0;
  for (uint32_t b = 0; b < cache->bucket_count; b++) {
    mc_bucket_t* bucket = cache->buckets[b];
    alloc += bucket->g0.alloc;
    alloc += bucket->g1.alloc;
    alloc += bucket->g2.alloc;
  };
  return alloc;
}

void age(mcache_t* cache) {
  for (uint32_t b = 0; b < cache->bucket_count; b++) {
    mc_bucket_t* bucket = cache->buckets[b];
    bucket_age(bucket, cache->conf);
  }
}

uint8_t is_empty(mcache_t* cache) {
  for (uint32_t b = 0; b < cache->bucket_count; b++) {
    mc_bucket_t* bucket = cache->buckets[b];
    if (! bucket_is_empty(bucket)) {
      return 0;
    };
  }
  return 1;
}

uint64_t cache_count(mcache_t* cache) {
  uint64_t count = 0;
  for (uint32_t b = 0; b < cache->bucket_count; b++) {
    count += bucket_count(cache->buckets[b]);
  };
  return count;
}

uint64_t cache_alloc(mcache_t* cache) {
  uint64_t alloc = 0;
  for (uint32_t b = 0; b < cache->bucket_count; b++) {
    alloc += bucket_alloc(cache->buckets[b]);
  };
  return alloc;
}


static mc_reply_t check_limit(mcache_t* cache, uint64_t max_alloc) {
  dprint("[start] check limit\r\n");

  mc_reply_t reply = {NULL, NULL};
  if (size(cache) <= max_alloc || !cache->bucket_count) {
    dprint("check limit: we're good no need to check anything\r\n");
    return reply;
  }


  // We want to free something that's above the averagte size so
  // we aim for that
  uint64_t count = cache_count(cache);
  dprint("check limit: count: %u\r\n", count);
  uint64_t alloc = cache_alloc(cache);
  dprint("check limit: alloc: %u\r\n", alloc);
  uint64_t min_size = alloc / count;

  dprint("check limit: min_size: %u\r\n", min_size);

  // It cound be that due to overhead we can't find anything so we loop here.
  while (!reply.metric) {
    dprint("check limit: 2 %u\r\n", min_size);
    dprint("check limit: 3 [cache->bucket_count] %u\r\n", cache->bucket_count);
    // we traverse bacwards as the least accessed item is supposed to be
    // at the end
    for (int64_t b = cache->bucket_count - 1; b >= 0; b--) {
      dprint("check limit: 4 [b]: %lld\r\n", b);
      reply.bucket = cache->buckets[b];
      // if we are the last bucket and are emtpy we free ourselfs and continue
      // down the road.
      // This way unused buckes, over time, will be freed.
      if (b == cache->bucket_count - 1 && bucket_is_empty(reply.bucket)) {
        dprint("This is the last bucket and it's empty, bye.\r\n");
        bucket_free(reply.bucket, cache->conf);
        reply.bucket = NULL;
        cache->buckets[b] = NULL;
        cache->bucket_count--;
        continue;
      }
      reply.metric = bucket_check_limit(reply.bucket, cache->conf, min_size);
      if (reply.metric) {
        dprint("[end] check limit: found a metric, returning\r\n");
        return reply;
      }
    }
    dprint("check limit: found nothing\r\n");
    // If we screwed up we half our search parameter unless we found zero 
    if (min_size == 0) {
      return reply;
    } else if (min_size == 1) {
        min_size = 0;
    } else {
      min_size /= 2;
    }
  }
  return reply;
}

mc_reply_t insert(mcache_t* cache, uint8_t *bkt, size_t bkt_len, uint8_t *name, size_t name_len, uint64_t offset, uint64_t *value, size_t value_len) {
  mc_reply_t reply = {NULL, NULL};
  reply.bucket = find_bucket(cache, bkt, bkt_len);
  if (!reply.bucket) {
    // grow buckets if needed
    if (cache->bucket_size == cache->bucket_count) {
      cache->bucket_size *= 2;
      mc_bucket_t **new_buckets = mc_alloc(sizeof(mc_bucket_t *) * cache->bucket_size);
      memcpy(new_buckets, cache->buckets, cache->bucket_count * sizeof(mc_bucket_t *));
      mc_free(cache->buckets);
      cache->buckets = new_buckets;
    }
    reply.bucket = bucket_init(cache->conf, bkt, bkt_len);
    cache->buckets[cache->bucket_count] = reply.bucket;
    cache->bucket_count++;
  }
  bucket_insert(reply.bucket, cache->conf, name, name_len, offset, value, value_len);
  return check_limit(cache, cache->conf.max_alloc);


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
  return check_limit(cache, 0);
}

mcache_t* cache_init(mc_conf_t config) {
  dprint("ctor\r\n");

  mcache_t *cache;
  cache = (mcache_t *) enif_alloc_resource(mcache_t_handle, sizeof(mcache_t));
#ifdef TAGGED
  cache->tag = TAG_CACHE;
#endif
  cache->conf = config;
  cache->bucket_size = BKT_GROWTH;
  cache->bucket_count = 0;
  cache->buckets = mc_alloc(sizeof(mc_bucket_t*) * cache->bucket_size);
  return cache;
}

void cache_free(mcache_t *cache) {
  dprint("cache_free\r\n");
  for (uint32_t b = 0; b < cache->bucket_count; b++) {
    mc_bucket_t* bucket = cache->buckets[b];
    dprint("freeing bucket\r\n");
    bucket_free(bucket, cache->conf);
  }
  mc_free(cache->buckets);
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
  H = mcache:new(200, [{max_gap,0}, {buckets,1}, {age_cycle,1}, {initial_data_size,1}, {initial_entries,1}, {hash_seed,1}]).
  mcache:insert(H, <<"a">>, <<>>, 0, <<0,0,0,0,0,0,0,0>>).
  mcache:insert(H, <<"b">>, <<>>, 0, <<0,0,0,0,0,0,0,0>>).
  mcache:pop(H).
  mcache:pop(H).
*/
