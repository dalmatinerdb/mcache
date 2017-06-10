#include "erl_nif.h"
#include "mcache.h"
#include <string.h>
#include "stdio.h"

#include "xxhash.h"

ErlNifResourceType* mcache_t_handle;

void print_entry(mc_entry_t *entry) {
  uint8_t i;
  if (!entry) {
    printf("\r\n");
    return;
  }
  printf("    @%ld:", entry->start);

  for (i=0; i < entry->count; i++) {
    printf(" %ld", entry->data[i]);
  };
  printf("\r\n");
  if (entry->next) {
    print_entry(entry->next);
  };

}

void print_metric(mc_metric_t *metric) {
  uint16_t i;
  printf("  ");
  for(i = 0; i < metric->name_len; i++)
    printf("%c", (int) metric->name[i]);
  printf(":\r\n");
  print_entry(metric->head);
};

int init_buckets(mc_conf_t conf,/*@out@*/ mc_gen_t *gen) {
  int i;
  gen->buckets = (mc_bucket_t *) enif_alloc(conf.buckets * sizeof(mc_bucket_t));
  for (i = 0; i < conf.buckets; i++) {
    gen->buckets[i].size = conf.initial_entries;
    gen->buckets[i].count = 0;
    gen->buckets[i].metrics = (mc_metric_t **) enif_alloc(gen->buckets[i].size * sizeof(mc_metric_t *));
    if (!gen->buckets[i].metrics) {
      return 0;
    }
  }
  return 1;
}

void age(mcache_t *cache) {
  int i;
  for (i = 0; i < cache->conf.buckets; i++) {
    // G1 -> G2

    mc_metric_t **new_metrics = (mc_metric_t **) enif_alloc((cache->g2.buckets[i].count + cache->g1.buckets[i].count) * sizeof(mc_metric_t *));
    memcpy(new_metrics, cache->g2.buckets[i].metrics, cache->g2.buckets[i].count * sizeof(mc_metric_t *));
    enif_free(cache->g2.buckets[i].metrics);

    cache->g2.buckets[i].metrics = new_metrics;

    memcpy(
           // copy to the end of the current array
           &(cache->g2.buckets[i].metrics[cache->g2.buckets[i].count]),
           // copy the start ofn the lower genj array
           cache->g1.buckets[i].metrics,
           // Copy based on the siuze of the old array
           cache->g1.buckets[i].count * sizeof(mc_metric_t *)
           );
    /*for (int j = 0; j < cache->g1.buckets[i].count; j++) {
      cache->g2.buckets[i].metrics[cache->g2.buckets[i].count + j] = cache->g1.buckets[i].metrics[j];
      };*/
    cache->g2.buckets[i].count += cache->g1.buckets[i].count;
    cache->g2.buckets[i].size = cache->g2.buckets[i].count;
    // Free the G1 metric  list of this bucket as we copied it all out
    enif_free(cache->g1.buckets[i].metrics);
  }
  // free g1 buckets (we copied the content to g2)
  enif_free(cache->g1.buckets);
  // move g0 buckets to g1
  cache->g1.buckets = cache->g0.buckets;

  cache->g2.alloc += cache->g1.alloc;
  cache->g1.alloc = cache->g0.alloc;
  cache->g0.alloc = 0;
  // reinitialize g0
  init_buckets(cache->conf, &(cache->g0));

}

static ERL_NIF_TERM serialize_entry(ErlNifEnv* env, mc_entry_t *entry) {
  ERL_NIF_TERM data;
  size_t to_copy = entry->count * sizeof(ErlNifSInt64);
  unsigned char * datap = enif_make_new_binary(env, to_copy, &data);
  memcpy(datap, entry->data, to_copy);
  return  enif_make_tuple2(env,
                           enif_make_uint64(env, entry->start),
                           data);

}

static ERL_NIF_TERM serialize_metric(ErlNifEnv* env, mc_metric_t *metric) {
  if (! metric) {
    return enif_make_atom(env, "undefined");
  }
  ERL_NIF_TERM result = enif_make_list(env, 0);
  ERL_NIF_TERM reverse;
  mc_entry_t *entry = metric->head;
  while (entry) {
    result = enif_make_list_cell(env, serialize_entry(env, entry), result);
    entry = entry->next;
  }
  enif_make_reverse_list(env, result, &reverse);
  return reverse;
}

static void free_entry(mc_entry_t *e) {
  if (e->next) {
    free_entry(e->next);
  }
  enif_free(e->data);
  enif_free(e);
}


static void free_metric(mc_metric_t *m) {
  if (m->head) {
    free_entry(m->head);
  }
  enif_free(m->name);
  enif_free(m);
}

static void free_gen(mc_conf_t conf, mc_gen_t gen) {
  for (int i = 0; i < conf.buckets; i++) {
    for (int j = 0; j < gen.buckets[i].count; j++) {
      free_metric(gen.buckets[i].metrics[j]);
    };
    enif_free(gen.buckets[i].metrics);
  }
  enif_free(gen.buckets);
};

static void cache_dtor(ErlNifEnv* env, void* handle) {
  mcache_t * c = (mcache_t*) handle;
  free_gen(c->conf, c->g0);
  free_gen(c->conf, c->g1);
  free_gen(c->conf, c->g2);
};


static int
load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
  ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
  mcache_t_handle = enif_open_resource_type(env,
                                            "mcache_t",
                                            "cache",
                                            &cache_dtor,
                                            flags,
                                            0);
  return 0;
}

static int
upgrade(ErlNifEnv* env, void** priv, void** old_priv, ERL_NIF_TERM load_info)
{
  return 0;
}

static ERL_NIF_TERM
new_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {

  mcache_t *cache;
  ErlNifUInt64 max_alloc;
  ErlNifUInt64 buckets;
  ErlNifUInt64 age_cycle;
  ErlNifUInt64 initial_data_size;
  ErlNifUInt64 initial_entries;
  ErlNifUInt64 hash_seed;
  if (argc != 6) {
    return enif_make_badarg(env);
  };
  if (!enif_get_uint64(env, argv[0], &max_alloc)) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[1], &buckets)) return enif_make_badarg(env);
  if (buckets <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[2], &age_cycle)) return enif_make_badarg(env);
  if (age_cycle <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[3], &initial_data_size)) return enif_make_badarg(env);
  if (initial_data_size <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[4], &initial_entries)) return enif_make_badarg(env);
  if (initial_entries <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[5], &hash_seed)) return enif_make_badarg(env);
  if (hash_seed <= 0) return enif_make_badarg(env);

  cache = (mcache_t *) enif_alloc_resource(mcache_t_handle, sizeof(mcache_t));

  // Set up the config
  cache->conf.max_alloc = max_alloc;
  cache->conf.buckets = buckets;
  cache->conf.age_cycle = age_cycle;

  cache->conf.initial_data_size = initial_data_size;
  cache->conf.initial_entries = initial_entries;
  cache->conf.hash_seed = hash_seed;

  // some cache wqide counters
  cache->inserts = 0;
  cache->age = 0;

  //now set up the tree genreations
  cache->g0.v = 0;
  cache->g0.alloc = 0;
  init_buckets(cache->conf, &(cache->g0));
  cache->g1.v = 1;
  cache->g1.alloc = 0;
  init_buckets(cache->conf, &(cache->g1));
  cache->g2.v = 2;
  cache->g2.alloc = 0;
  init_buckets(cache->conf, &(cache->g2));
  ERL_NIF_TERM term = enif_make_resource(env, cache);
  enif_release_resource(cache);
  return  enif_make_tuple2(env,
                           enif_make_atom(env, "ok"),
                           term);
};


mc_metric_t *find_metric_g(mc_conf_t conf, mc_gen_t gen, uint64_t hash, uint16_t name_len, uint8_t *name) {
  // Itterate over the existig metrics and see if we have already
  // seen this one.
  uint64_t bucket = hash % conf.buckets;
  for (int i = 0; i < gen.buckets[bucket].count; i++) {
    mc_metric_t *m = gen.buckets[bucket].metrics[i];
    if (m->name_len == name_len
        && m->hash == hash
        && memcmp(m->name, name, name_len) == 0) {
      return m;
    }
  }
  return NULL;
};

mc_metric_t *find_metric(mcache_t *cache, uint64_t hash, uint16_t name_len, uint8_t *name) {
  mc_metric_t *res = find_metric_g(cache->conf, cache->g0, hash, name_len, name);
  if(!res) {
    res = find_metric_g(cache->conf, cache->g1, hash, name_len, name);
    if(!res) {
      res = find_metric_g(cache->conf, cache->g2, hash, name_len, name);
    }
  }
  return res;
}


mc_metric_t *find_metric_and_remove_g(mc_conf_t conf, mc_gen_t *gen, uint64_t hash, uint16_t name_len, uint8_t *name) {
  int i = 0;
  uint64_t bucket = hash % conf.buckets;
  // Itterate over the existig metrics and see if we have already
  // seen this one.

  for (i = 0; i < gen->buckets[bucket].count; i++) {
    mc_metric_t *m = gen->buckets[bucket].metrics[i];
    if (m->name_len == name_len
        && m->hash == hash
        && memcmp(m->name, name, name_len) == 0) {
      if (i != gen->buckets[bucket].count - 1) {
        gen->buckets[bucket].metrics[i] = gen->buckets[bucket].metrics[gen->buckets[bucket].count - 1];
      }
      gen->buckets[bucket].count--;
      gen->alloc -= m->alloc;
      return m;
    }
  }
  return NULL;
};

mc_metric_t *find_metric_and_remove(mcache_t *cache, uint64_t hash, uint16_t name_len, uint8_t *name) {
  mc_metric_t *res = find_metric_and_remove_g(cache->conf, &(cache->g0), hash, name_len, name);
  if(!res) {
    res = find_metric_and_remove_g(cache->conf, &(cache->g1), hash, name_len, name);
    if(!res) {
      res = find_metric_and_remove_g(cache->conf, &(cache->g2), hash, name_len, name);
    };
  };
  return res;
}


mc_metric_t *get_metric(mcache_t *cache, uint64_t hash, uint16_t name_len, uint8_t *name) {

  uint64_t bucket = hash % cache->conf.buckets;
  // Itterate over the existig metrics and see if we have already
  // seen this one.
  mc_metric_t *metric = find_metric_g(cache->conf, cache->g0, hash, name_len, name);
  if (metric) {
    return metric;
  }
  mc_bucket_t *b = &(cache->g0.buckets[bucket]);
  // We start with a small cache and grow it as required, so we need to
  // make sure the new index doesn't exceet the count. If it does
  // double the size of the cache.
  if (b->count >= b->size) {
    mc_metric_t **new_metrics = enif_alloc(b->size * 2 * sizeof(mc_metric_t *));
    memcpy(new_metrics, b->metrics, b->size * sizeof(mc_metric_t *));
    enif_free(b->metrics);
    b->metrics = new_metrics;
    b->size = b->size * 2;
  }
  // we havn't seen the metric so we'll create a new one.
  metric = find_metric_and_remove_g(cache->conf, &(cache->g1), hash, name_len, name);
  if (!metric) {
    metric = find_metric_and_remove_g(cache->conf, &(cache->g2), hash, name_len, name);
  }
  if (!metric) {
    metric = (mc_metric_t *) enif_alloc(sizeof(mc_metric_t));
    metric->alloc = name_len + sizeof(mc_metric_t);
    metric->name = enif_alloc(name_len * sizeof(uint8_t));
    metric->hash = hash;
    metric->name_len = name_len;
    memcpy(metric->name, name, name_len);
    metric->head = NULL;
    metric->tail = NULL;
  }
  cache->g0.alloc += metric->alloc;
  b->metrics[b->count] = metric;
  b->count++;
  return metric;
};

void add_point(mc_conf_t conf, mc_gen_t *gen, mc_metric_t *metric, ErlNifSInt64 offset, size_t count, ErlNifSInt64* values) {
  // If eitehr we have no data yet or the current data is larger then
  // the offset we generate a new metric.
  // In both cases next will be the current head given that next might
  // be empty and it's needed to set next to empty for the first element.
  mc_entry_t *entry = NULL;
  if((!metric->head) || metric->head->start > offset) {
    size_t alloc = sizeof(ErlNifSInt64) * MAX(conf.initial_data_size, count * 2);
    entry = enif_alloc(sizeof(mc_entry_t));
    entry->start = offset;
    entry->size = conf.initial_data_size;
    entry->data = (ErlNifSInt64 *) enif_alloc(alloc);
    entry->count = 0;
    entry->next = metric->head;
    metric->head = entry;
    if (!metric->tail) {
      metric->tail = entry;
    }
    metric->alloc += alloc + sizeof(mc_entry_t);
    gen->alloc += alloc + sizeof(mc_entry_t);
  }
  if (offset > metric->tail->start) {
    entry = metric->tail;
  } else {
    entry = metric->head;
  }
  do {
    // if the offset is beyond the next chunks start
    // just go ahead and go for this chunk
    if(entry->next && (offset >= entry->next->start)) {
      entry = entry->next;
      continue;
    }

    //We know that we either have no next chunk or the data is before
    //the start of the next chunk

    // we allocate a new chunk behind this and continue with that.
    size_t internal_offset = offset - entry->start;

        // or we'd have gaps
    if (internal_offset > entry->count) {
      mc_entry_t *next = enif_alloc(sizeof(mc_entry_t));
      uint64_t alloc = conf.initial_data_size * sizeof(ErlNifSInt64);
      next->start = offset;
      next->size = conf.initial_data_size;
      next->data = (ErlNifSInt64 *) enif_alloc(alloc);
      next->count = 0;
      next->next = entry->next;
      entry->next = next;
      entry = next;
      if (!entry->next) {
        metric->tail = entry;
      }
      metric->alloc += alloc + sizeof(mc_entry_t);
      gen->alloc += alloc + sizeof(mc_entry_t);
      continue;
    }

    // if we would overlap with the next chunk we combine the two
    if (entry->next &&
        offset + count >= entry->next->start) {
      // the new size is the delta between our start and the total end of the next chunk
      mc_entry_t *next = entry->next;
      uint64_t new_size = next->start + next->size - entry->start;
      uint64_t new_count = next->start - entry->start + next->count;

      /* printf("Asked to write %d -> %d\r\n", offset, offset + count); */
      /* printf("combining %d->%d(%d) and %d->%d(%d)\r\n", */
      /*        entry->start, entry->start + entry->count, entry->start + entry->size, */
      /*        next->start, next->start + next->count, next->start + next->size); */
      /* printf("new range %d->%d(%d)\r\n", entry->start, entry->start + new_count, entry->start + new_size); */
      /* fflush(stdout); */

      ErlNifSInt64 *new_data = (ErlNifSInt64 *) enif_alloc(new_size * sizeof(ErlNifSInt64));

      // recalculate the allocation
      metric->alloc -= (entry->size * sizeof(ErlNifSInt64));
      gen->alloc -= (entry->size * sizeof(ErlNifSInt64));
      metric->alloc -= (next->size * sizeof(ErlNifSInt64));
      gen->alloc -= (next->size * sizeof(ErlNifSInt64));
      metric->alloc += new_size;
      gen->alloc += new_size;


      entry->next = next->next;
      // copy, free and reassign old data
      memcpy(new_data, entry->data, entry->count* sizeof(ErlNifSInt64));
      enif_free(entry->data);
      entry->data = new_data;
      // set new size and count
      entry->size = new_size;
      entry->count = new_count;

      // now we calculate the delta of old and new start to get the offset in the
      // new array to copy data to then free it
      /* printf("2nd chunk offset: %d\r\n", next->start - entry->start); */
      /* printf("copying points: %d\r\n", next->start - entry->start); */
      /* fflush(stdout); */
      memcpy(entry->data + next->start - entry->start, next->data, next->count* sizeof(ErlNifSInt64));
      enif_free(next->data);
      enif_free(next);
      // if we don't have a next we are now the tail!
      if (!entry->next) {
        metric->tail = entry;
      }
      // we go back to the start of the loop in case we'd overlap multiple ones (oh my!)
      continue;
    };

    // the offset in our data array

    if (internal_offset + count >= entry->size) {
      uint64_t new_size = entry->size * 2;
      // keep growing untill we're sure we have the right size
      while (internal_offset + count >= new_size) {
        new_size *= 2;
      }

      // prevent oddmath we just  removethe oldsizeand then add
      // the new size
      metric->alloc -= (entry->size * sizeof(ErlNifSInt64));
      gen->alloc -= (entry->size * sizeof(ErlNifSInt64));

      ErlNifSInt64 *new_data = (ErlNifSInt64 *) enif_alloc(new_size * sizeof(ErlNifSInt64));
      memcpy(new_data, entry->data, entry->size * sizeof(ErlNifSInt64));
      enif_free(entry->data);
      entry->data = new_data;
      entry->size = new_size;
      metric->alloc += (entry->size * sizeof(ErlNifSInt64));
      gen->alloc += (entry->size * sizeof(ErlNifSInt64));
    }

    memcpy(entry->data + internal_offset, values, count * sizeof(ErlNifSInt64));

    entry->count = MAX(offset - entry->start + count, entry->count);

    if (!entry->next) {
      metric->tail = entry;
    }
    return;
  } while (entry);
}

mc_metric_t *
check_limit(ErlNifEnv* env, mcache_t *cache, uint64_t max_alloc, uint64_t bucket) {
  mc_metric_t *metric = NULL;
  if (max_alloc > cache->g0.alloc +
      cache->g1.alloc +
      cache->g2.alloc) {
    return NULL;
  }
  // If we don't have g2 entries we age so g1 becomes
  // g2
  mc_gen_t *gen = &(cache->g2);

  if (gen->alloc == 0) {
    gen = &(cache->g1);
  }

  // If we still have no g2 entries we aga again,
  // this way g0 effectively becomes g0
  if (gen->alloc == 0) {
    gen = &(cache->g0);
  }

  // if we still have no g2 entries we know it's
  // all for nothing and just give up
  if (gen->alloc == 0) {
    return NULL;
  }

  //we know we have at least 1 entrie so we grab the alst one
  // and reduce the count and reduce the alloc;

  //TODO: This is not good!
  for (int i = 0; i < cache->conf.buckets; i++) {
    // We itterate through all buckets starting after the bucket we just edited
    // that way we avoid always changing the same buket over and over
    int b = (i + bucket + 1) % cache->conf.buckets;
    if (gen->buckets[b].count > 0) {
      // If we found a non empty bucket we find the largest metric in there to
      // evict, that way we can can free up the 'most sensible' thing;
      int largest_idx = 0;
      mc_bucket_t *bkt = &(gen->buckets[b]);
      metric = bkt->metrics[0];
      for (int j = 1; j < bkt->count; j++) {
        if (bkt->metrics[j]->alloc >= metric->alloc) {
          largest_idx = j;
          metric = bkt->metrics[j];
        }
      }
      if (largest_idx != bkt->count - 1) {
        bkt->metrics[largest_idx] = bkt->metrics[gen->buckets[b].count - 1];
      }
      bkt->count--;
      gen->alloc -= metric->alloc;
      return metric;
    }
  }
  return NULL;
  // now we work on exporting the metric
}

static ERL_NIF_TERM
insert_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  mc_metric_t *metric;
  ErlNifSInt64 offset;
  ErlNifBinary value;
  ErlNifBinary name_bin;

  ERL_NIF_TERM data;
  ERL_NIF_TERM name;
  unsigned char *namep;

  uint64_t bucket;
  if (argc != 4) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };
  if (!enif_inspect_binary(env, argv[1], &name_bin)) {
    return enif_make_badarg(env);
  };
  if (!enif_get_int64(env, argv[2], &offset)) {
    return enif_make_badarg(env);
  };
  if (!enif_inspect_binary(env, argv[3], &value)) {
    return enif_make_badarg(env);
  };
  if (value.size % sizeof(ErlNifSInt64) && value.size >= 8) {
    return enif_make_badarg(env);
  }
  uint64_t hash = XXH64(name_bin.data, name_bin.size, cache->conf.hash_seed) ;
  bucket = hash % cache->conf.buckets;
  metric = get_metric(cache, hash, name_bin.size, name_bin.data);
  add_point(cache->conf, &(cache->g0), metric, offset, value.size / 8, (ErlNifSInt64 *) value.data);

  cache->inserts++;
  if (cache->inserts > cache->conf.age_cycle) {
    age(cache);
    cache->age++;
    cache->inserts = 0;
  }
  // We now check for overflow note that metric is re-used here!
  metric = check_limit(env, cache, cache->conf.max_alloc, bucket);
  if (metric) {
    data = serialize_metric(env, metric);
    namep = enif_make_new_binary(env, metric->name_len, &name);
    memcpy(namep, metric->name, metric->name_len);
    free_metric(metric);
    return  enif_make_tuple3(env,
                             enif_make_atom(env, "overflow"),
                             name,
                             data);
  }
  return enif_make_atom(env, "ok");
};

static ERL_NIF_TERM
pop_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  mc_metric_t *metric = NULL;
  ERL_NIF_TERM data;
  ERL_NIF_TERM name;
  unsigned char *namep;

  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };

  // We cheat here, we can create a 'pop' by just checkig for a 0 limit.
  metric = check_limit(env, cache, 0, 0);
  if (metric) {
    data = serialize_metric(env, metric);
    namep = enif_make_new_binary(env, metric->name_len, &name);
    memcpy(namep, metric->name, metric->name_len);
    free_metric(metric);
    return  enif_make_tuple3(env,
                             enif_make_atom(env, "ok"),
                             name,
                             data);
  }
  return enif_make_atom(env, "undefined");

};

static ERL_NIF_TERM
get_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  mc_metric_t *metric;
  ErlNifBinary name_bin;
  if (argc != 2) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };

  if (!enif_inspect_binary(env, argv[1], &name_bin)) {
    return enif_make_badarg(env);
  };

  uint64_t hash = XXH64(name_bin.data, name_bin.size, cache->conf.hash_seed) ;
  metric = find_metric(cache, hash, name_bin.size, name_bin.data);
  return  enif_make_tuple2(env,
                           enif_make_atom(env, "ok"),
                           serialize_metric(env, metric));
};

void print_gen(mc_conf_t conf, mc_gen_t gen) {
  int size = 0;
  int count = 0;
  for (int i = 0; i < conf.buckets; i++) {
    size += gen.buckets[i].size;
    count += gen.buckets[i].count;
  };
  printf("Cache: [c: %d |s: %d|a: %zu]:\r\n",  count, size, gen.alloc);
  for (int i = 0; i < conf.buckets; i++) {
    for(int j = 0; j < gen.buckets[i].count; j++) {
      print_metric(gen.buckets[i].metrics[j]);
    };
  }
}

static ERL_NIF_TERM
print_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };
  print_gen(cache->conf, cache->g0);
  print_gen(cache->conf, cache->g1);
  print_gen(cache->conf, cache->g2);
  return  enif_make_atom(env, "ok");
};
static ERL_NIF_TERM
age_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };
  age(cache);
  return  enif_make_atom(env, "ok");
};

static ERL_NIF_TERM
gen_stats(ErlNifEnv* env, mc_conf_t conf, mc_gen_t gen) {
  int size = 0;
  int count = 0;
  for (int i = 0; i < conf.buckets; i++) {
    size += gen.buckets[i].size;
    count += gen.buckets[i].count;
  };

  return enif_make_list3(env,
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "alloc"),
                                          enif_make_uint64(env, gen.alloc)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "count"),
                                          enif_make_uint64(env, count)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "size"),
                                          enif_make_uint64(env, size)));

}

static ERL_NIF_TERM
conf_info(ErlNifEnv* env, mc_conf_t conf) {
  return enif_make_list5(env,
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "buckets"),
                                          enif_make_uint64(env, conf.buckets)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "age_cycle"),
                                          enif_make_uint64(env, conf.age_cycle)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "initial_data_size"),
                                          enif_make_uint64(env, conf.initial_data_size)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "initial_entries"),
                                          enif_make_uint64(env, conf.initial_entries)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "max_alloc"),
                                          enif_make_uint64(env, conf.max_alloc)));

}


static ERL_NIF_TERM
stats_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };

  return enif_make_list7(env,
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "age"),
                                          enif_make_uint64(env, cache->age)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "inserts"),
                                          enif_make_uint64(env, cache->inserts)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "conf"),
                                          conf_info(env, cache->conf)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "total_alloc"),
                                          enif_make_uint64(env,
                                                           cache->g0.alloc +
                                                           cache->g1.alloc +
                                                           cache->g2.alloc)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "gen0"),
                                          gen_stats(env, cache->conf, cache->g0)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "gen1"),
                                          gen_stats(env, cache->conf, cache->g1)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "gen2"),
                                          gen_stats(env, cache->conf, cache->g2)));
};

static ErlNifFunc nif_funcs[] = {
  {"new", 6, new_nif},
  {"print", 1, print_nif},
  {"stats", 1, stats_nif},
  {"age", 1, age_nif},
  {"pop", 1, pop_nif},
  {"get", 2, get_nif},
  {"insert", 4, insert_nif},
};

// Initialize this NIF library.
//
// Args: (MODULE, ErlNifFunc funcs[], load, reload, upgrade, unload)
// Docs: http://erlang.org/doc/man/erl_nif.html#ERL_NIF_INIT

ERL_NIF_INIT(mcache, nif_funcs, &load, NULL, &upgrade, NULL);

/*
  {ok, H} = mcache:new(50*10*8).
  mcache:insert(H, <<"1">>,  1, <<1:64>>).
  mcache:insert(H, <<"2">>,  1, <<1:64>>).
  mcache:insert(H, <<"3">>,  1, <<1:64>>).
  mcache:insert(H, <<"4">>,  1, <<1:64>>).
  mcache:insert(H, <<"5">>,  1, <<1:64>>).
  mcache:insert(H, <<"6">>,  1, <<1:64>>).
  mcache:insert(H, <<"7">>,  1, <<1:64>>).
  mcache:insert(H, <<"8">>,  1, <<1:64>>).
  mcache:insert(H, <<"9">>,  1, <<1:64>>).
  mcache:age(H).

  mcache:insert(H, <<"test2">>, 1, <<2:64>>).
  mcache:age(H).
  mcache:insert(H, <<"test">>,  6, <<3:64>>).
  mcache:print(H).

  mcache:get(H, <<"test">>).

  mcache:insert(H, <<"test1">>, 1, <<2:64>>).
  mcache:insert(H, <<"test2">>, 1, <<2:64>>).
  mcache:insert(H, <<"test3">>, 1, <<2:64>>).
  mcache:insert(H, <<"test4">>, 1, <<2:64>>).
  mcache:insert(H, <<"test5">>, 1, <<2:64>>).
  mcache:insert(H, <<"test6">>, 1, <<2:64>>).
  mcache:insert(H, <<"test7">>, 1, <<2:64>>).
  mcache:insert(H, <<"test8">>, 1, <<2:64>>).
  mcache:insert(H, <<"test9">>, 1, <<2:64>>).
  mcache:insert(H, <<"testa">>, 1, <<2:64>>).
  mcache:insert(H, <<"testb">>, 1, <<2:64>>).
  mcache:insert(H, <<"testc">>, 1, <<2:64>>).
  mcache:get(H, <<"test">>).
*/


/*
  {ok, H} = mcache:new(10000).
  mcache:insert(H, <<"test">>,  1, <<2:64>>).
  mcache:age(H).
  mcache:age(H).
  mcache:insert(H, <<"test1">>,  2, <<2:64>>).
  mcache:print(H).
  mcache:age(H).
*/

/*
  {ok, H} =  mcache:new(726).
  mcache:insert(H, <<>>, 0, <<0,0,0,0,0,0,0,0>>).
  mcache:print(H).
  mcache:age(H).
  mcache:print(H).
  mcache:insert(H, <<>>, 0, <<0,0,0,0,0,0,0,0>>).
  mcache:print(H).
*/
