#include "mcache.h"

static ERL_NIF_TERM conf_info(ErlNifEnv* env, mc_conf_t conf) {
  dprint("conf_info\r\n");
  return enif_make_list5(env,
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "slots"),
                                          enif_make_uint64(env, conf.slots)),
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
gen_stats(ErlNifEnv* env, mc_conf_t conf, mc_gen_t gen) {
  dprint("gen_stats\r\n");
  int size = 0;
  int count = 0;
  for (int i = 0; i < conf.slots; i++) {
    for (int sub = 0; sub < SUBS; sub++) {
      size += gen.slots[i].subs[sub].size;
      count += gen.slots[i].subs[sub].count;
    }
  };

  return enif_make_list4(env,
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "alloc"),
                                          enif_make_uint64(env, gen.alloc)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "count"),
                                          enif_make_uint64(env, count)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "g_count"),
                                          enif_make_uint64(env, gen.count)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "size"),
                                          enif_make_uint64(env, size)));
}

static ERL_NIF_TERM bucket_info(ErlNifEnv* env, mc_bucket_t *bucket, mc_conf_t conf) {
  dprint("bucket info\r\n");
  ERL_NIF_TERM name;
  unsigned char *namep;
  namep = enif_make_new_binary(env, bucket->name_len, &name);
  memcpy(namep, bucket->name, bucket->name_len);

  return enif_make_tuple2(env,
                          name,
                          enif_make_list7(env,
                                          enif_make_tuple2(env,
                                                           enif_make_atom(env, "age"),
                                                           enif_make_uint64(env, bucket->age)),
                                          enif_make_tuple2(env,
                                                           enif_make_atom(env, "inserts"),
                                                           enif_make_uint64(env, bucket->inserts)),
                                          enif_make_tuple2(env,
                                                           enif_make_atom(env, "alloc"),
                                                           enif_make_uint64(env, bucket_alloc(bucket))),
                                          enif_make_tuple2(env,
                                                           enif_make_atom(env, "count"),
                                                           enif_make_uint64(env, bucket_count(bucket))),
                                          enif_make_tuple2(env,
                                                           enif_make_atom(env, "gen0"),
                                                           gen_stats(env, conf, bucket->g0)),
                                          enif_make_tuple2(env,
                                                           enif_make_atom(env, "gen1"),
                                                           gen_stats(env, conf, bucket->g1)),
                                          enif_make_tuple2(env,
                                                           enif_make_atom(env, "gen2"),
                                                           gen_stats(env, conf, bucket->g2))));
}

ERL_NIF_TERM cache_info(ErlNifEnv* env, mcache_t *cache) {
  dprint("cache info\r\n");
  ERL_NIF_TERM conf = enif_make_tuple2(env,
                                       enif_make_atom(env, "conf"),
                                       conf_info(env, cache->conf));

  ERL_NIF_TERM buckets = enif_make_list(env, 0);

  for (uint32_t b = 0; b < cache->bucket_count; b++) {
    mc_bucket_t* bucket = cache->buckets[b];
    buckets = enif_make_list_cell(env, bucket_info(env, bucket, cache->conf), buckets);
  };
  //bucket_info(env, cache->bucket, cache->conf)
  return enif_make_list4(env,
                         conf,
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "alloc"),
                                          enif_make_uint64(env, cache_alloc(cache))),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "count"),
                                          enif_make_uint64(env, cache_count(cache))),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "buckets"),
                                          buckets));
}
