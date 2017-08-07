#ifndef MC_METRIC_H_INCLUDED
#define MC_METRIC_H_INCLUDED

ERL_NIF_TERM metric_serialize(ErlNifEnv* env, mc_metric_t *metric);
void metric_add_point(mc_conf_t conf, mc_gen_t *gen, mc_metric_t *metric, ErlNifUInt64 offset, size_t count, ErlNifUInt64* values);
void metric_free(mc_metric_t *m);

#endif // MC_IMPL_H_INCLUDED
