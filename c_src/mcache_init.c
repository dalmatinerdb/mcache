#include "mcache.h"
void init_slots(mc_conf_t conf,/*@out@*/ mc_gen_t *gen) {
  dprint("init_slots\r\n");
  gen->slots = (mc_slot_t *) mc_alloc(conf.slots * sizeof(mc_slot_t));
  for (int b = 0; b < conf.slots; b++) {
    mc_slot_t *bkt = &(gen->slots[b]);

    for (int i = 0; i < LCOUNT; i++) {
      bkt->largest[i] = NULL;
    }

#ifdef TAGGED
    bkt->tag = TAG_BKT;
#endif

    for (int s = 0; s < SUBS; s++) {
      mc_sub_slot_t *sub = &(bkt->subs[s]);
      sub->size = conf.initial_entries;
      sub->count = 0;
      sub->metrics = (mc_metric_t **) mc_alloc(sub->size * sizeof(mc_metric_t *));
#ifdef TAGGED
      sub->tag = TAG_SUB;
      for (int i = 0; i < sub->size; i++) {
        sub->metrics[i] = TAG_METRIC_L;
      }
#endif
    };
  }
}

mc_bucket_t* init_bucket(mc_conf_t config) {
  mc_bucket_t *bucket;
  bucket = mc_alloc(sizeof(mc_bucket_t));
#ifdef TAGGED
  bucket->tag = TAG_BUCKET;
#endif

  bucket->conf = config;
  // some bucket wqide counters
  bucket->inserts = 0;
  bucket->age = 0;

  //now set up the tree genreations
  bucket->g0.v = 0;
  bucket->g0.alloc = 0;
#ifdef TAGGED
  bucket->g0.tag = TAG_GEN;
#endif

  init_slots(bucket->conf, &(bucket->g0));
  bucket->g1.v = 1;
  bucket->g1.alloc = 0;
#ifdef TAGGED
  bucket->g1.tag = TAG_GEN;
#endif
  init_slots(bucket->conf, &(bucket->g1));
  bucket->g2.v = 2;
  bucket->g2.alloc = 0;
#ifdef TAGGED
  bucket->g2.tag = TAG_GEN;
#endif
  init_slots(bucket->conf, &(bucket->g2));

  return bucket;
}

mcache_t* init_cache(mc_conf_t config) {
  dprint("ctor\r\n");

  mcache_t *cache;
  cache = (mcache_t *) enif_alloc_resource(mcache_t_handle, sizeof(mcache_t));
  cache->conf = config;
  cache->bucket = init_bucket(config);
  return cache;
}
