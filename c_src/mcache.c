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

int init_buckets(/*@out@*/ mc_gen_t *gen) {
  int i;
  for (i = 0; i < BUCKETS; i++) {
    gen->buckets[i].size = INITIAL_ENTRIES;
    gen->buckets[i].count = 0;
    gen->buckets[i].metrics = (mc_metric_t **) malloc(INITIAL_ENTRIES * sizeof(mc_metric_t *));
    if (!gen->buckets[i].metrics) {
      return 0;
    }
  }
  return 1;
}

uint64_t get_bucket(/*@unused@*/ ErlNifBinary bin) {
  return 1; //XXH64(bin.data, bin.size, HASH_SEED) % BUCKETS;
};

void age(mcache_t *cache) {
  int i;
  for (i = 0; i < BUCKETS; i++) {
    // G1 -> G2
    cache->g2.buckets[i].metrics = (mc_metric_t **) realloc(cache->g2.buckets[i].metrics, (cache->g2.buckets[i].count + cache->g1.buckets[i].count) * sizeof(mc_metric_t *));
    for (int j = 0; j < cache->g1.buckets[i].count; j++) {
      cache->g2.buckets[i].metrics[cache->g2.buckets[i].count + j] = cache->g1.buckets[i].metrics[j];
    };
    cache->g2.buckets[i].count += cache->g1.buckets[i].count;
    cache->g2.buckets[i].size = cache->g2.buckets[i].count;
    // G0 -> G1
    cache->g1.buckets[i].size = cache->g0.buckets[i].size;
    cache->g1.buckets[i].count = cache->g0.buckets[i].count;
    cache->g1.buckets[i].metrics = cache->g0.buckets[i].metrics;
  }

  cache->g2.alloc += cache->g1.alloc;
  cache->g1.alloc = cache->g0.alloc;
  cache->g0.alloc = 0;
  init_buckets(&(cache->g0));

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
  free(e->data);
  free(e);
}


static void free_metric(mc_metric_t *m) {
  free_entry(m->head);
  free(m->name);
  free(m);
}

static void free_gen(mc_gen_t gen) {
  for (int i = 0; i < BUCKETS; i++) {
    for (int j = 0; j < gen.buckets[i].count; j++) {
      free_metric(gen.buckets[i].metrics[j]);
    };
    free(gen.buckets[i].metrics);
  }
};

static void cache_dtor(ErlNifEnv* env, void* handle) {
  mcache_t * c = (mcache_t*) handle;
  free_gen(c->g0);
  free_gen(c->g1);
  free_gen(c->g2);
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
  ErlNifSInt64 max_alloc;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_int64(env, argv[0], &max_alloc)) {
    return enif_make_badarg(env);
  };
  if (max_alloc < 0) {
    return enif_make_badarg(env);
  };
  cache = (mcache_t *) enif_alloc_resource(mcache_t_handle, sizeof(mcache_t));
  cache->max_alloc = max_alloc;
  cache->g0.v = 0;
  cache->g0.alloc = 0;
  init_buckets(&(cache->g0));
  cache->g1.v = 1;
  cache->g1.alloc = 0;
  init_buckets(&(cache->g1));
  cache->g2.v = 2;
  cache->g2.alloc = 0;
  init_buckets(&(cache->g2));
  ERL_NIF_TERM term = enif_make_resource(env, cache);
  enif_release_resource(cache);
  return  enif_make_tuple2(env,
                           enif_make_atom(env, "ok"),
                           term);
};


mc_metric_t *find_metric_g(mc_gen_t gen, uint64_t bucket, uint16_t name_len, uint8_t *name) {
  // Itterate over the existig metrics and see if we have already
  // seen this one.
  for (int i = 0; i < gen.buckets[bucket].count; i++) {
    mc_metric_t *m = gen.buckets[bucket].metrics[i];
    if (m->name_len == name_len
        && memcmp(m->name, name, name_len) == 0) {
      return m;
    }
  }
  return NULL;
};

mc_metric_t *find_metric(mcache_t *cache, uint64_t bucket, uint16_t name_len, uint8_t *name) {
  mc_metric_t *res = find_metric_g(cache->g0, bucket, name_len, name);
  if(!res) {
    res = find_metric_g(cache->g1, bucket, name_len, name);
    if(!res) {
      res = find_metric_g(cache->g2, bucket, name_len, name);
    }
  }
  return res;
}


mc_metric_t *find_metric_and_remove_g(mc_gen_t *gen, uint64_t bucket, uint16_t name_len, uint8_t *name) {
  int i = 0;
  // Itterate over the existig metrics and see if we have already
  // seen this one.

  for (i = 0; i < gen->buckets[bucket].count; i++) {
    mc_metric_t *m = gen->buckets[bucket].metrics[i];
    if (m->name_len == name_len
        && memcmp(m->name, name, name_len) == 0) {
      if (i != gen->buckets[bucket].count - 1) {
        gen->buckets[bucket].metrics[i] = gen->buckets[bucket].metrics[gen->buckets[bucket].count - 1];
      }
      gen->buckets[bucket].count--;
      gen->alloc -= (m->alloc + sizeof(mc_metric_t));
      return m;
    }
  }
  return NULL;
};

mc_metric_t *find_metric_and_remove(mcache_t *cache, uint64_t bucket, uint16_t name_len, uint8_t *name) {
  mc_metric_t *res = find_metric_and_remove_g(&(cache->g0), bucket, name_len, name);
  if(!res) {
    res = find_metric_and_remove_g(&(cache->g1), bucket, name_len, name);
    if(!res) {
      res = find_metric_and_remove_g(&(cache->g2), bucket, name_len, name);
    };
  };
  return res;
}


mc_metric_t *get_metric(mcache_t *cache, uint64_t bucket, uint16_t name_len, uint8_t *name) {

  // Itterate over the existig metrics and see if we have already
  // seen this one.
  mc_metric_t *metric = find_metric_g(cache->g0, bucket, name_len, name);
  if (metric) {
    return metric;
  }
  // We start with a small cache and grow it as required, so we need to
  // make sure the new index doesn't exceet the count. If it does
  // double the size of the cache.
  if (cache->g0.buckets[bucket].count >= cache->g0.buckets[bucket].size) {
    cache->g0.buckets[bucket].size = cache->g0.buckets[bucket].size * 2;
    printf("growing g0 buckets\r\n");
    cache->g0.buckets[bucket].metrics = (mc_metric_t **) realloc(cache->g0.buckets[bucket].metrics, cache->g0.buckets[bucket].size * sizeof(mc_metric_t *));
  }
  // we havn't seen the metric so we'll create a new one.
  metric = find_metric_and_remove_g(&(cache->g1), bucket, name_len, name);
  if (!metric) {
    metric = find_metric_and_remove_g(&(cache->g2), bucket, name_len, name);
  }
  if (!metric) {
    metric = (mc_metric_t *) malloc(sizeof(mc_metric_t));
    metric->alloc = sizeof(uint8_t) * name_len;
    metric->name = enif_alloc(metric->alloc);
    memcpy(metric->name, name, name_len);
    metric->name_len = name_len;
    metric->head = NULL;
  }
  cache->g0.alloc += metric->alloc + sizeof(mc_metric_t);
  cache->g0.buckets[bucket].metrics[cache->g0.buckets[bucket].count] = metric;
  cache->g0.buckets[bucket].count++;
  return metric;
};

void add_point(mc_gen_t *gen, mc_metric_t *metric, ErlNifSInt64 offset, ErlNifSInt64* value) {
  // If eitehr we have no data yet or the current data is larger then
  // the offset we generate a new metric.
  // In both cases next will be the current head given that next might
  // be empty and it's needed to set next to empty for the first element.
  mc_entry_t *entry;
  ErlNifSInt64 v = value[0];
  if(!metric->head || metric->head->start > offset) {
    size_t alloc = sizeof(ErlNifSInt64) * INITIAL_DATA_SIZE;
    entry = malloc(sizeof(mc_entry_t));
    entry->count = 1;
    entry->start = offset;
    entry->size = INITIAL_DATA_SIZE;
    entry->data = (ErlNifSInt64 *) malloc(alloc);
    entry->data[0] = v;
    entry->next = metric->head;
    metric->head = entry;

    metric->alloc += alloc + sizeof(mc_entry_t);
    gen->alloc += alloc;
    return;
  }
  entry = metric->head;
  do {
    // if the offset is beyond the next chunks start
    // just go ahead and go for this chunk
    if(entry->next && offset >= entry->next->start) {
      entry = entry->next;
      continue;
    }
    // We either are in this chunk or there is no next chunk

    // There would be a gap!
    if(offset > entry->start + entry->count || entry->count == MAX_CHUNK) {
      // So we create a next node and insert it into our chain
      mc_entry_t *next;
      size_t alloc = sizeof(ErlNifSInt64) * INITIAL_DATA_SIZE;
      //allocate a new entry as next
      next = malloc(sizeof(mc_entry_t));
      // setthe current entry to next
      next->count = 1;
      next->start = offset;
      next->size = INITIAL_DATA_SIZE;
      next->data = (ErlNifSInt64 *) malloc(alloc);
      next->data[0] = v;

      next->next = entry->next;
      entry->next = next;

      metric->alloc += alloc + sizeof(mc_entry_t);
      gen->alloc += alloc;

      return;
    }
    //
    if (entry->count > entry->size) {
      // prevent oddmath we just  removethe oldsizeand then add
      // the new size
      metric->alloc -= (entry->size * sizeof(ErlNifSInt64));
      gen->alloc -= (entry->size * sizeof(ErlNifSInt64));

      entry->size = MAX(entry->size * 2, 255);

      metric->alloc += (entry->size * sizeof(ErlNifSInt64));
      gen->alloc += (entry->size * sizeof(ErlNifSInt64));

      entry->data = (ErlNifSInt64 *) realloc(entry->data, entry->size * sizeof(ErlNifSInt64)) ;
    }
    entry->data[entry->count] = v;
    entry->count++;

    return;
  } while (entry);
}

static ERL_NIF_TERM
check_limit(ErlNifEnv* env, mcache_t *cache) {
  mc_metric_t *metric = NULL;
  ERL_NIF_TERM data;
  ERL_NIF_TERM name;
  unsigned char *namep;
  if (cache->max_alloc > cache->g0.alloc +
      cache->g1.alloc +
      cache->g2.alloc) {
    return enif_make_atom(env, "ok");
  }
  fflush(stdout); 
  // If we don't have g2 entries we age so g1 becomes
  // g2
  if (cache->g2.alloc == 0) {
    age(cache);
  }

  // If we still have no g2 entries we aga again,
  // this way g0 effectively becomes g0
  if (cache->g2.alloc == 0) {
    age(cache);
  }

  // if we still have no g2 entries we know it's
  // all for nothing and just give up
  if (cache->g2.alloc == 0) {
    return enif_make_atom(env, "ok");
  }

  //we know we have at least 1 entrie so we grab the alst one
  // and reduce the count and reduce the alloc;

  for (int i = 0; i < BUCKETS; i++) {
    if (cache->g2.buckets[i].count > 0){
      metric = cache->g2.buckets[i].metrics[cache->g2.buckets[i].count - 1];
      cache->g2.buckets[i].count--;
      cache->g2.alloc -= metric->alloc;
      break;
    }
  }
  if (! metric) {
    return enif_make_atom(env, "ok");
  }
  // now we work on exporting the metric
  data = serialize_metric(env, metric);
  namep = enif_make_new_binary(env, metric->name_len, &name);
  memcpy(namep, metric->name, metric->name_len);
  free_metric(metric);
  return  enif_make_tuple3(env,
                           enif_make_atom(env, "overflow"),
                           name,
                           data);
}

static ERL_NIF_TERM
insert_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  mc_metric_t *metric;
  ErlNifSInt64 offset;
  ErlNifBinary value;
  ErlNifBinary name_bin;
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

  bucket = get_bucket(name_bin);
  metric = get_metric(cache, bucket, name_bin.size, name_bin.data);
  print_metric(metric);
  add_point(&(cache->g0), metric, offset, (ErlNifSInt64 *) value.data);

  return check_limit(env, cache);
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

  // if we still have no g2 entries we know it's
  // all for nothing and just give up

  //we know we have at least 1 entrie so we grab the alst one
  // and reduce the count and reduce the alloc;
  for (int i = 0; i < BUCKETS; i++) {
    if (cache->g2.buckets[i].count > 0){
      metric = cache->g2.buckets[i].metrics[cache->g2.buckets[i].count - 1];
      cache->g2.buckets[i].count--;
      cache->g2.alloc -= metric->alloc;
      break;
    }
  }
  if (! metric) {
    return enif_make_atom(env, "undefined");
  }
  // now we work on exporting the metric
  data = serialize_metric(env, metric);
  namep = enif_make_new_binary(env, metric->name_len, &name);
  memcpy(namep, metric->name, metric->name_len);
  free_metric(metric);
  return  enif_make_tuple3(env,
                           enif_make_atom(env, "ok"),
                           name,
                           data);
};

static ERL_NIF_TERM
get_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  mc_metric_t *metric;
  ErlNifBinary name_bin;
  uint64_t bucket;
  if (argc != 2) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };

  if (!enif_inspect_binary(env, argv[1], &name_bin)) {
    return enif_make_badarg(env);
  };

  bucket = get_bucket(name_bin);
  metric = find_metric(cache, bucket, name_bin.size, name_bin.data);
  return  enif_make_tuple2(env,
                           enif_make_atom(env, "ok"),
                           serialize_metric(env, metric));
};

void print_gen(mc_gen_t gen) {
  int size = 0;
  int count = 0;
  for (int i = 0; i < BUCKETS; i++) {
    size += gen.buckets[i].size;
    count += gen.buckets[i].count;
  };
  printf("Cache [%d/%d]:\r\n",  count, size);
  for (int i = 0; i < BUCKETS; i++) {
    for(int j = 0; i < gen.buckets[i].count; i++) {
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
  print_gen(cache->g0);
  print_gen(cache->g1);
  print_gen(cache->g2);
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
gen_stats(ErlNifEnv* env, mc_gen_t gen) {
  int size = 0;
  int count = 0;
  for (int i = 0; i < BUCKETS; i++) {
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
stats_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };

  return enif_make_list5(env,
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "max_alloc"),
                                          enif_make_uint64(env, cache->max_alloc)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "total_alloc"),
                                          enif_make_uint64(env,
                                                           cache->g0.alloc +
                                                           cache->g1.alloc +
                                                           cache->g2.alloc)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "gen0"),
                                          gen_stats(env, cache->g0)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "gen1"),
                                          gen_stats(env, cache->g1)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "gen2"),
                                          gen_stats(env, cache->g2)));
};

static ErlNifFunc nif_funcs[] = {
  {"new", 1, new_nif},
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