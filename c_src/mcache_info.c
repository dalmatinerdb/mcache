#include "mcache.h"

static ERL_NIF_TERM conf_info(ErlNifEnv* env, mc_conf_t conf) {
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
  int size = 0;
  int count = 0;
  for (int i = 0; i < conf.slots; i++) {
    for (int sub = 0; sub < SUBS; sub++) {
      size += gen.slots[i].subs[sub].size;
      count += gen.slots[i].subs[sub].count;
    }
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

ERL_NIF_TERM bucket_info(ErlNifEnv* env, mc_bucket_t *bucket) {
  return enif_make_list7(env,
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "age"),
                                          enif_make_uint64(env, bucket->age)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "inserts"),
                                          enif_make_uint64(env, bucket->inserts)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "conf"),
                                          conf_info(env, bucket->conf)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "total_alloc"),
                                          enif_make_uint64(env,
                                                           bucket->g0.alloc +
                                                           bucket->g1.alloc +
                                                           bucket->g2.alloc)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "gen0"),
                                          gen_stats(env, bucket->conf, bucket->g0)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "gen1"),
                                          gen_stats(env, bucket->conf, bucket->g1)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "gen2"),
                                          gen_stats(env, bucket->conf, bucket->g2)));
}
