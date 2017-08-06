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
