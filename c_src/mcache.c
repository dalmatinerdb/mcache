#include "mcache.h"
#include "mcache_impl.h"
#include "mcache_metric.h"

static ERL_NIF_TERM
new_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  dprint("new\r\n");
  mcache_t *cache;
  mc_conf_t conf;
  ErlNifUInt64 val;

  if (argc != 7) {
    return enif_make_badarg(env);
  };
  if (!enif_get_uint64(env, argv[0], &val)) return enif_make_badarg(env);
  conf.max_alloc = val;

  if (!enif_get_uint64(env, argv[1], &val)) return enif_make_badarg(env);
  conf.slots = val;
  if (conf.slots <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[2], &val)) return enif_make_badarg(env);
  conf.age_cycle = val;
  if (conf.age_cycle <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[3], &val)) return enif_make_badarg(env);
  conf.initial_data_size = val;
  if (conf.initial_data_size <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[4], &val)) return enif_make_badarg(env);
  conf.initial_entries = val;
  if (conf.initial_entries <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[5], &val)) return enif_make_badarg(env);
  conf.hash_seed = val;
  if (conf.hash_seed <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[6], &val)) return enif_make_badarg(env);
  conf.max_gap = val;

  cache = cache_init(conf);

  ERL_NIF_TERM term = enif_make_resource(env, cache);
  enif_release_resource(cache);
  dprint("new done\r\n");
  return term;
};

static ERL_NIF_TERM
insert_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  dprint("insert\r\n");
  mcache_t *cache;
  ErlNifUInt64 offset;
  ErlNifBinary value;
  ErlNifBinary name_bin;
  ErlNifBinary bucket_bin;

  if (argc != 5) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };
  if (!enif_inspect_binary(env, argv[1], &bucket_bin)) {
    return enif_make_badarg(env);
  };
  if (!enif_inspect_binary(env, argv[2], &name_bin)) {
    return enif_make_badarg(env);
  };
  if (!enif_get_uint64(env, argv[3], &offset)) {
    return enif_make_badarg(env);
  };
  if (!enif_inspect_binary(env, argv[4], &value)) {
    return enif_make_badarg(env);
  };
  if (value.size % sizeof(uint64_t)) {
    return enif_make_badarg(env);
  }
  mc_reply_t reply = insert(cache, bucket_bin.data, bucket_bin.size, name_bin.data, name_bin.size, offset,
                            (uint64_t *) value.data, value.size / 8);
  if (reply.metric) {
    ERL_NIF_TERM name = serialize_reply_name(env, reply);
    ERL_NIF_TERM data = metric_serialize(env, reply.metric);
    metric_free(reply.metric);
    return  enif_make_tuple3(env,
                             atom_overflow,
                             name,
                             data);
  }
  return atom_ok;
};

static ERL_NIF_TERM
get_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  dprint("get\r\n");
  mcache_t *cache;
  ErlNifBinary bucket_bin;
  ErlNifBinary name_bin;
  if (argc != 3) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };

  if (!enif_inspect_binary(env, argv[1], &bucket_bin)) {
    return enif_make_badarg(env);
  };

  if (!enif_inspect_binary(env, argv[2], &name_bin)) {
    return enif_make_badarg(env);
  };
  mc_reply_t reply = get(cache, bucket_bin.data, bucket_bin.size, name_bin.data, name_bin.size);
  if (reply.metric) {
    return  enif_make_tuple2(env,
                             atom_ok,
                             metric_serialize(env, reply.metric));
  } else {
    return atom_undefined;
  }
};

static ERL_NIF_TERM
take_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  dprint("take\r\n");
  mcache_t *cache;
  ErlNifBinary bucket_bin;
  ErlNifBinary name_bin;
  if (argc != 3) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };

  if (!enif_inspect_binary(env, argv[1], &bucket_bin)) {
    return enif_make_badarg(env);
  };

  if (!enif_inspect_binary(env, argv[2], &name_bin)) {
    return enif_make_badarg(env);
  };
  mc_reply_t reply = take(cache, bucket_bin.data, bucket_bin.size, name_bin.data, name_bin.size);
  if (reply.metric) {
    ERL_NIF_TERM res = enif_make_tuple2(env,
                                        atom_ok,
                                        metric_serialize(env, reply.metric));
    metric_free(reply.metric);
    return res;
  } else {
    return atom_undefined;
  }
};

static ERL_NIF_TERM
pop_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  dprint("pop\r\n");
  mcache_t *cache;

  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };

  // We cheat here, we can create a 'pop' by just checkig for a 0 limit.
  mc_reply_t reply = pop(cache);
  if (reply.metric) {
    ERL_NIF_TERM data = metric_serialize(env, reply.metric);
    ERL_NIF_TERM name = serialize_reply_name(env, reply);
    metric_free(reply.metric);
    return  enif_make_tuple3(env,
                             atom_ok,
                             name,
                             data);
  }
  return atom_undefined;

};

static ERL_NIF_TERM
remove_bucket_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  ErlNifBinary bucket_bin;
  if (argc != 2) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };

  if (!enif_inspect_binary(env, argv[1], &bucket_bin)) {
    return enif_make_badarg(env);
  };

  remove_bucket(cache, bucket_bin.data, bucket_bin.size);
  return  atom_ok;
};

static ERL_NIF_TERM
print_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  dprint("print\r\n");
  mcache_t *cache;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };
  print_cache(cache);
  fflush(stdout);
  return  atom_ok;
};
static ERL_NIF_TERM
age_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  dprint("age\r\n");
  mcache_t *cache;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };
  age(cache);
  return  atom_ok;
};


static ERL_NIF_TERM
stats_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  dprint("stats\r\n");
  mcache_t *cache;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };

  return cache_info(env, cache);
};


static ERL_NIF_TERM
is_empty_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  dprint("is_empty\r\n");
  mcache_t *cache;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };
  if (is_empty(cache)) {
    return enif_make_atom(env, "true");
  } else {
    return enif_make_atom(env, "false");
  }
};

static void cache_dtor(ErlNifEnv* env, void* handle) {
  dprint("dtor\r\n");
  mcache_t * cache = (mcache_t*) handle;
  cache_free(cache);
};

static int
load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
  dprint("load\r\n");
  ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
  mcache_t_handle = enif_open_resource_type(env,
                                            "mcache_t",
                                            "cache",
                                            &cache_dtor,
                                            flags,
                                            0);
  atom_undefined = enif_make_atom(env, "undefined");
  atom_ok = enif_make_atom(env, "ok");
  atom_overflow = enif_make_atom(env, "overflow");
  return 0;
}

static int
upgrade(ErlNifEnv* env, void** priv, void** old_priv, ERL_NIF_TERM load_info)
{
  return 0;
}

static ErlNifFunc nif_funcs[] = {
  {"new", 7, new_nif},
  {"print", 1, print_nif},
  {"stats", 1, stats_nif},
  {"is_empty", 1, is_empty_nif},
  {"age", 1, age_nif},
  {"pop", 1, pop_nif},
  {"remove_bucket", 2, remove_bucket_nif},
  {"get", 3, get_nif},
  {"take", 3, take_nif},
  {"insert", 5, insert_nif},
};

// Initialize this NIF library.
//
// Args: (MODULE, ErlNifFunc funcs[], load, reload, upgrade, unload)
// Docs: http://erlang.org/doc/man/erl_nif.html#ERL_NIF_INIT

ERL_NIF_INIT(mcache, nif_funcs, &load, NULL, &upgrade, NULL);
