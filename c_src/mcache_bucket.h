#ifndef MC_BUCKET_H_INCLUDED
#define MC_BUCKET_H_INCLUDED
void         bucket_free(mc_bucket_t *bucket, mc_conf_t conf);
uint8_t      bucket_is_empty(mc_bucket_t *bucket);
void         bucket_age(mc_bucket_t *bucket, mc_conf_t conf);
mc_metric_t* bucket_find_metric(mc_bucket_t *bucket, mc_conf_t conf, uint64_t hash, uint16_t name_len, uint8_t *name);
mc_metric_t* bucket_find_metric_and_remove(mc_bucket_t *bucket, mc_conf_t conf, uint64_t hash, uint16_t name_len, uint8_t *name);
mc_metric_t* bucket_get_metric(mc_bucket_t *bucket, mc_conf_t conf, uint64_t hash, uint16_t name_len, uint8_t *name);
mc_metric_t* bucket_check_limit(mc_bucket_t *bucket, mc_conf_t conf, uint64_t max_alloc, uint64_t slot);
mc_metric_t* bucket_insert(mc_bucket_t *bucket, mc_conf_t conf, ErlNifBinary name, uint64_t offset, ErlNifBinary value);

#endif // MC_BUCKET_H_INCLUDED
