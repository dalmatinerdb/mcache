#include "mcache.h"


static ERL_NIF_TERM
new_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {

  mc_bucket_t *bucket;
  mc_conf_t conf;
  if (argc != 7) {
    return enif_make_badarg(env);
  };
  if (!enif_get_uint64(env, argv[0], &conf.max_alloc)) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[1], &conf.slots)) return enif_make_badarg(env);
  if (conf.slots <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[2], &conf.age_cycle)) return enif_make_badarg(env);
  if (conf.age_cycle <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[3], &conf.initial_data_size)) return enif_make_badarg(env);
  if (conf.initial_data_size <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[4], &conf.initial_entries)) return enif_make_badarg(env);
  if (conf.initial_entries <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[5], &conf.hash_seed)) return enif_make_badarg(env);
  if (conf.hash_seed <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[6], &conf.max_gap)) return enif_make_badarg(env);

  bucket = init_bucket(conf);

  ERL_NIF_TERM term = enif_make_resource(env, bucket);
  enif_release_resource(bucket);
  return term;
};

static ERL_NIF_TERM
insert_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mc_bucket_t *bucket;
  mc_metric_t *metric;
  ErlNifUInt64 offset;
  ErlNifBinary value;
  ErlNifBinary name_bin;


  if (argc != 4) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mc_bucket_t_handle, (void **)&bucket)) {
    return enif_make_badarg(env);
  };
  if (!enif_inspect_binary(env, argv[1], &name_bin)) {
    return enif_make_badarg(env);
  };
  if (!enif_get_uint64(env, argv[2], &offset)) {
    return enif_make_badarg(env);
  };
  if (!enif_inspect_binary(env, argv[3], &value)) {
    return enif_make_badarg(env);
  };
  if (value.size % sizeof(ErlNifUInt64) && value.size >= 8) {
    return enif_make_badarg(env);
  }

  if (metric = insert(bucket, name_bin, offset, value)) {
    ERL_NIF_TERM data;
    ERL_NIF_TERM name;
    unsigned char *namep;

    data = serialize_metric(env, metric);
    namep = enif_make_new_binary(env, metric->name_len, &name);
    memcpy(namep, metric->name, metric->name_len);
    free_metric(metric);
    return  enif_make_tuple3(env,
                             atom_overflow,
                             name,
                             data);
  }
  return atom_ok;
};

static ERL_NIF_TERM
pop_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mc_bucket_t *bucket;
  mc_metric_t *metric = NULL;
  ERL_NIF_TERM data;
  ERL_NIF_TERM name;
  unsigned char *namep;

  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mc_bucket_t_handle, (void **)&bucket)) {
    return enif_make_badarg(env);
  };

  // We cheat here, we can create a 'pop' by just checkig for a 0 limit.
  if (metric = pop(bucket)) {
    data = serialize_metric(env, metric);
    namep = enif_make_new_binary(env, metric->name_len, &name);
    memcpy(namep, metric->name, metric->name_len);
    free_metric(metric);
    return  enif_make_tuple3(env,
                             atom_ok,
                             name,
                             data);
  }
  return atom_undefined;

};

static ERL_NIF_TERM
get_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mc_bucket_t *bucket;
  mc_metric_t *metric;
  ErlNifBinary name_bin;
  if (argc != 2) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mc_bucket_t_handle, (void **)&bucket)) {
    return enif_make_badarg(env);
  };

  if (!enif_inspect_binary(env, argv[1], &name_bin)) {
    return enif_make_badarg(env);
  };

  if (metric = get(bucket, name_bin)) {
    return  enif_make_tuple2(env,
                             atom_ok,
                             serialize_metric(env, metric));
  } else {
    return atom_undefined;
  }
};

static ERL_NIF_TERM
take_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mc_bucket_t *bucket;
  mc_metric_t *metric;
  ErlNifBinary name_bin;
  if (argc != 2) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mc_bucket_t_handle, (void **)&bucket)) {
    return enif_make_badarg(env);
  };

  if (!enif_inspect_binary(env, argv[1], &name_bin)) {
    return enif_make_badarg(env);
  };

  if (metric = take(bucket, name_bin)) {
    ERL_NIF_TERM res = enif_make_tuple2(env,
                                        atom_ok,
                                        serialize_metric(env, metric));
    free_metric(metric);
    return res;
  } else {
    return atom_undefined;
  }
};

static ERL_NIF_TERM
remove_prefix_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mc_bucket_t *bucket;
  ErlNifBinary pfx_bin;
  if (argc != 2) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mc_bucket_t_handle, (void **)&bucket)) {
    return enif_make_badarg(env);
  };

  if (!enif_inspect_binary(env, argv[1], &pfx_bin)) {
    return enif_make_badarg(env);
  };

  uint64_t count = remove_prefix(bucket, pfx_bin.size, pfx_bin.data);
  return  enif_make_tuple2(env,
                           atom_ok,
                           enif_make_uint64(env, count));
};

static ERL_NIF_TERM
print_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mc_bucket_t *bucket;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mc_bucket_t_handle, (void **)&bucket)) {
    return enif_make_badarg(env);
  };
  print_bucket(bucket);
  fflush(stdout);
  return  atom_ok;
};
static ERL_NIF_TERM
age_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mc_bucket_t *bucket;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mc_bucket_t_handle, (void **)&bucket)) {
    return enif_make_badarg(env);
  };
  age(bucket);
  return  atom_ok;
};


static ERL_NIF_TERM
stats_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mc_bucket_t *bucket;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mc_bucket_t_handle, (void **)&bucket)) {
    return enif_make_badarg(env);
  };

  return bucket_info(env, bucket);
};


static ERL_NIF_TERM
is_empty_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mc_bucket_t *bucket;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mc_bucket_t_handle, (void **)&bucket)) {
    return enif_make_badarg(env);
  };
  if (is_empty(bucket)) {
    return enif_make_atom(env, "true");
  } else {
    return enif_make_atom(env, "false");
  }
};

static void bucket_dtor(ErlNifEnv* env, void* handle) {
  mc_bucket_t * bucket = (mc_bucket_t*) handle;
  free_bucket(bucket);
};

static int
load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
  ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
  mc_bucket_t_handle = enif_open_resource_type(env,
                                            "mc_bucket_t",
                                            "bucket",
                                            &bucket_dtor,
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
  {"remove_prefix", 2, remove_prefix_nif},
  {"get", 2, get_nif},
  {"take", 2, take_nif},
  {"insert", 4, insert_nif},
};

// Initialize this NIF library.
//
// Args: (MODULE, ErlNifFunc funcs[], load, reload, upgrade, unload)
// Docs: http://erlang.org/doc/man/erl_nif.html#ERL_NIF_INIT

ERL_NIF_INIT(mcache, nif_funcs, &load, NULL, &upgrade, NULL);
