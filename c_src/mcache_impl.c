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


/*
 * This is the code used for eviction, it is probably the most tricky
 * part of mcache so it warrents some explenation.
 *
 * Eviction is something we basically don't want to happen however
 * since we're bounded on space it will eventually have to happen.
 *
 * We have a few things to keep in mind:
 *
 * - We want to evict large caches over small caches, as they lead
 *   to fewer evictions over time and help with reducing the number
 *   of IO opperations (which are the devil).
 *
 * - We also want to avoid searching over the entire cache as we could
 *   potentially have a large number of metrics to look at.
 *
 * - We also want to evict rarely used data more then we want to evict
 *   recently accessed data - that way stall metrics get tossed out
 *   to make space for active ones.
 *
 * With that goals we do a few things to optmize eviction. First of
 * all we know the allocation size and the number of elements for each
 * bucket.
 *
 * With that knowledge we start with two things:
 *
 * 1) We calculate the average allocation / metric (allocation/count)
 *    and will try to evict only metrics that are above average.
 * 2) We start with the bucket that has the worst allocation / count
 *    ratio as it is most likely to find a large metric in that.
 * 3) In addition buckets are sorted based on activity, with the
 *    most active bucket being at slot 0 and the least active on
 *    at slot N. Knowing this we're moving backwards through them
 *    to rather evict inactive metrics thena active ones.
 *
 * There is a chance that given the above constraints we do not find
 * a metric matching the requirements, in that case we'll look for
 * something that is allocated at half the average so instead if 50%
 * 25%, then 12.5% and so on.
 *
 * In addition to that we keep a eviciton mutliplyer that is applied
 * to the first limit. The multiplyer is a percentage of how the
 * average is adjusted on the first computations.
 * This is done to reflect different usages better, the multiplyer
 * will be raised by 1% for every successful eviction, and lowered
 * by 1% for every re-try.
 * Ideally this way it'll adopt to the current state of the cahce
 * and result in only reqyiner a re-try every second eviction at
 * max.
 */
static mc_reply_t check_limit(mcache_t* cache, uint64_t max_alloc) {
  dprint("[start] check limit\r\n");

  mc_reply_t reply = {NULL, NULL};


  // We want to free something that's above the averagte size so
  // we aim for that
  uint64_t count = 0;
  uint64_t alloc = cache_alloc(cache);

  // find the bucket with the worst alloc vs count ratio as it will
  // have most likely the most 'tasty' content. We also use this
  // to callculate alloc and count so we don't have to itterate over
  // the buckets multiple times.
  uint64_t bkt_offset = cache->bucket_count;
  double best_ratio = 0;

  for (uint64_t b = 0; b < cache->bucket_count; b++) {
    uint64_t c = bucket_count(cache->buckets[b]);
    uint64_t a = bucket_alloc(cache->buckets[b]);
    if (c > 0 && (best_ratio == 0 || best_ratio < a / c)) {
      bkt_offset = b;
      best_ratio = a / c;
    }
    count += c;
    alloc += a;
  };

  if (alloc <= max_alloc || !cache->bucket_count) {
    dprint("check limit: we're good no need to check anything\r\n");
    return reply;
  }


  uint64_t min_size = (alloc / count) * cache->evict_multiplyer;
  dprint("check limit: count: %llu\r\n", count);
  dprint("check limit: min_size: %llu\r\n", min_size);
  dprint("check limit: alloc: %llu\r\n", alloc);

  // It cound be that due to overhead we can't find anything so we loop here.
  while (!reply.metric) {
    uint64_t tests = 0; // Number of metrics wehad to check
    dprint("check limit: 2 %llu\r\n", min_size);
    dprint("check limit: 3 [cache->bucket_count] %u\r\n", cache->bucket_count);
    // we traverse bacwards as the least accessed item is supposed to be
    // at the end
    for (int64_t bkt_i = 0; bkt_i < cache->bucket_count; bkt_i++) {
      uint64_t b = (bkt_offset - bkt_i) % cache->bucket_count;
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
      reply.metric = bucket_check_limit(reply.bucket, cache->conf, min_size, &tests);
      if (reply.metric) {
        dprint("[end] check limit: found a metric, returning\r\n");
        // If we found something we add one percent to the multiplyer to
        // increase the max on the next runl;

        if (max_alloc) {
          // don't increment for max_alloc == 0 (aka pop)
          if (count > (tests * 3)) {
            // and we found something while looking to the first third of metrics
            cache->evict_multiplyer += 0.01;
          } else {
            // we didn't
            cache->evict_multiplyer -= 0.01;
          }
        }
        return reply;
      }
    }
    dprint("check limit: found nothing\r\n");

    // If we screwed up we half our search parameter unless we found zero,
    // we also reduce the evict multiplyer by 10% to start off lower next time
    cache->evict_multiplyer -= 0.10;
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
  cache->evict_multiplyer = 1.0;
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
