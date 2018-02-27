#include "mcache.h"
#include "mcache_metric.h"
#include "mcache_utils.h"

static void init_slots(mc_conf_t conf,/*@out@*/ mc_gen_t *gen) {
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

static void free_gen(mc_conf_t conf, mc_gen_t gen) {
  dprint("free_gen\r\n");
  for (int b = 0; b < conf.slots; b++) {
    for (int s = 0; s < SUBS; s++) {
      for (int m = 0; m < gen.slots[b].subs[s].count; m++) {
        metric_free(gen.slots[b].subs[s].metrics[m]);
      };
      mc_free(gen.slots[b].subs[s].metrics);
    }
  }
  mc_free(gen.slots);
};

static mc_metric_t *find_metric_g(mc_conf_t conf, mc_gen_t gen, uint64_t hash, uint16_t name_len, uint8_t *name) {
  // Itterate over the existig metrics and see if we have already
  // seen this one.
  uint64_t slot = hash % conf.slots;
  uint8_t sub = subid(hash);
  for (int i = 0; i < gen.slots[slot].subs[sub].count; i++) {
    mc_metric_t *m = gen.slots[slot].subs[sub].metrics[i];
    if (m->name_len == name_len
        && m->hash == hash
        && meq(m->name, name, name_len)) {
      return m;
    }
  }
  return NULL;
};

static void remove_largest(mc_slot_t *slot, mc_metric_t *metric) {
  int i = 0;
  dprint("[%p] remove largest: %llu\r\n",  slot, metric->hash);
#ifdef DEBUG
  dprint("- >>");
  for(i = 0; i < LCOUNT; i++){
    if (! slot->largest[i]) {
      break;
    };
    dprint(" %llu(%zu)", slot->largest[i]->hash, slot->largest[i]->alloc);
  };
  dprint("\r\n");
#endif

  i = 0;
  // short circuite when we have a last element
  if (slot->largest[LCOUNT - 1]) {
    // if we are the last lement we can set it to null and return
    if (slot->largest[LCOUNT - 1] == metric) {
      slot->largest[LCOUNT - 1] = NULL;
      return;
    }
  }
  //skip all the ones we don't have
  while (i < LCOUNT && slot->largest[i] != metric) {
    // if we found a null we can end our search
    if (!slot->largest[i]) {
      dprint("return %d\r\n", i);
      return;
    }
    i++;
  };
  dprint("found %d\r\n", i);
  uint8_t moved = 0;
  while (i < LCOUNT - 1) {
    dprint("%d <- %d\r\n", i, i + 1);
    // found a null we don't need to go on.
    if (!slot->largest[i]) {
      return;
    }
    moved = 1;
    slot->largest[i] = slot->largest[i+1];
    i++;
  }
  if (moved) {
    dprint("%d <- NULL\r\n", LCOUNT - 1);
    slot->largest[LCOUNT - 1] = NULL;
  }
}

static void insert_largest(mc_slot_t *slot, mc_metric_t *metric) {
  remove_largest(slot, metric);
  dprint("[%p] insert largest: %llu\r\n", slot, metric->hash);
#ifdef DEBUG
  dprint(">>");
  for(int j = 0; j < LCOUNT; j++){
    if (! slot->largest[j]) {
      break;
    };
    dprint(" %llu(%zu)", slot->largest[j]->hash, slot->largest[j]->alloc);
  };
  dprint("\r\n");
#endif
  for (int i = 0; i < LCOUNT; i++) {
    if (!slot->largest[i]) {
      dprint("[%d] <- %llu; %zu!\r\n", i, metric->hash, metric->alloc);
      slot->largest[i] = metric;
      break;
    }
    if (metric == slot->largest[i]) {
      break;
    }
    if (metric->alloc > slot->largest[i]->alloc) {
      dprint("[%d] <- %llu: %zu\r\n", i, metric->hash, metric->alloc);
      mc_metric_t *t = slot->largest[i];
      slot->largest[i] = metric;
      metric = t;
    } else {
      dprint("[%d] | %llu: %zu\r\n", i, slot->largest[i]->hash, slot->largest[i]->alloc);
    }
  }
#ifdef DEBUG
  dprint("<<");
  for(int j = 0; j < LCOUNT; j++){
    if (! slot->largest[j]) {
      break;
    };
    dprint(" %llu(%zu)", slot->largest[j]->hash, slot->largest[j]->alloc);
  };
  dprint("\r\n");
#endif
}

static mc_metric_t *find_metric_and_remove_g(mc_conf_t conf, mc_gen_t *gen, uint64_t hash, uint16_t name_len, uint8_t *name) {
  int i = 0;
  uint64_t slot = hash % conf.slots;
  uint8_t sub = subid(hash);

  // Itterate over the existig metrics and see if we have already
  // seen this one.

  for (i = 0; i < gen->slots[slot].subs[sub].count; i++) {
    mc_metric_t *m = gen->slots[slot].subs[sub].metrics[i];
    if (m->name_len == name_len
        && m->hash == hash
        && meq(m->name, name, name_len)) {
      if (i != gen->slots[slot].subs[sub].count - 1) {
        gen->slots[slot].subs[sub].metrics[i] = gen->slots[slot].subs[sub].metrics[gen->slots[slot].subs[sub].count - 1];
      }
      remove_largest(&(gen->slots[slot]), m);
      gen->slots[slot].subs[sub].count--;
      gen->count--;
      gen->alloc -= m->alloc;
      return m;
    }
  }
  return NULL;
};

mc_metric_t *bucket_get_metric(mc_bucket_t *bucket, mc_conf_t conf, uint64_t hash, uint16_t name_len, uint8_t *name) {
  uint64_t slot = hash % conf.slots;
  uint8_t sub = subid(hash);
  // Itterate over the existig metrics and see if we have already
  // seen this one.
  mc_metric_t *metric = find_metric_g(conf, bucket->g0, hash, name_len, name);
  if (metric) {
    return metric;
  }
  mc_slot_t *b = &(bucket->g0.slots[slot]);
  mc_sub_slot_t *s = &(b->subs[sub]);
  // We start with a small bucket and grow it as required, so we need to
  // make sure the new index doesn't exceet the count. If it does
  // double the size of the bucket.
  if (s->count >= s->size) {
    mc_metric_t **new_metrics = mc_alloc(s->size * 2 * sizeof(mc_metric_t *));
#ifdef TAGGED
    for (int i = 0; i < s->size * 2; i++) {
      new_metrics[i] = TAG_METRIC_L;
    }
#endif
    memcpy(new_metrics, s->metrics, s->count * sizeof(mc_metric_t *));
    mc_free(s->metrics);
    s->metrics = new_metrics;
    s->size = s->size * 2;
  }
  // we havn't seen the metric so we'll create a new one.
  metric = find_metric_and_remove_g(conf, &(bucket->g1), hash, name_len, name);
  if (!metric) {
    metric = find_metric_and_remove_g(conf, &(bucket->g2), hash, name_len, name);
  }
  if (!metric) {
    metric = (mc_metric_t *) mc_alloc(sizeof(mc_metric_t));
#ifdef TAGGED
    metric->tag = TAG_METRIC;
#endif

    metric->alloc = name_len + sizeof(mc_metric_t);
    metric->name = mc_alloc(name_len * sizeof(uint8_t));
    metric->hash = hash;
    metric->name_len = name_len;
    memcpy(metric->name, name, name_len);
    metric->head = NULL;
    metric->tail = NULL;
  }
  bucket->g0.alloc += metric->alloc;
  s->metrics[s->count] = metric;
  s->count++;
  bucket->g0.count++;
  insert_largest(b, metric);
  return metric;
};

mc_metric_t * bucket_check_limit(mc_bucket_t *bucket, mc_conf_t conf, uint64_t min_size) {
  // Start with cehcking g2
  mc_gen_t *gen = &(bucket->g2);

  // If we don't have g2 entires check g1
  if (gen->alloc == 0) {
    gen = &(bucket->g1);
  }

  // If we still have no g1 check g0
  if (gen->alloc == 0) {
    gen = &(bucket->g0);
  }

  // if we have no g0 entries we have no entries at all
  if (gen->alloc == 0) {
    return NULL;
  }

  // We ignore min_size if we're in the 3rd generation (v=2) as we always want to evict
  // those first.
  if (gen->v == 2) {
    min_size = 0;
  }

  //we know we have at least 1 entrie so we grab the alst one
  // and reduce the count and reduce the alloc;

  //TODO: This is not good!
  mc_metric_t *metric = NULL;
  mc_sub_slot_t *largest_sub = NULL;
  mc_slot_t *largest_slot = NULL;
  int metric_idx = 0;
  // try to find a metric using the largest bucket first
  for (int b = 0; b < conf.slots; b++) {
    if (gen->slots[b].largest[0] &&
        // Only check if we are above average allocation
        gen->slots[b].largest[0]->alloc >= min_size &&
        // if we already have a metdic check if we are larger
        (!metric || (metric->alloc < gen->slots[b].largest[0]->alloc))) {
      largest_slot = &(gen->slots[b]);
      metric = largest_slot->largest[0];
      largest_sub = &(largest_slot->subs[subid(metric->hash)]);
    }
  }
  if (metric) {
    metric_idx = 0;
    remove_largest(largest_slot, metric);
    // find the index of the metric
    while (metric_idx < largest_sub->count && largest_sub->metrics[metric_idx] != metric) {
      metric_idx++;
    }
    // We didn't found anything that is problematic so we pretend it never
    // happend, the metric is already removed from the largest index
    if (metric_idx == largest_sub->count) {
      dprint("Oh my\r\n");
      metric_idx = 0;
      metric = NULL;
      largest_slot = NULL;
      largest_sub = NULL;
    }
  }
  for (int slot_id = 0; slot_id < conf.slots; slot_id++) {
    // we break if we found a sutiable metric either in this loop or the loop before
    if (metric) {
      break;
    }

    // We itterate through all slots starting after the slot we just edited
    // that way we avoid always changing the same buket over and over
    mc_slot_t *slot = &(gen->slots[slot_id]);
    for (int sub = 0; sub < SUBS; sub++) {
      // If we found a non empty slot we find the largest metric in there to
      // evict, that way we can can free up the 'most sensible' thing;
      for (int j = 0; j < slot->subs[sub].count; j++) {
        // only set the metric if we're over the minimum size
        if (slot->subs[sub].metrics[j]->alloc >= min_size &&
            (!metric ||
             slot->subs[sub].metrics[j]->alloc >= metric->alloc
             )) {
          metric_idx = j;
          largest_sub = &(slot->subs[sub]);
          metric = largest_sub->metrics[j];
        }
      }
    }
  }
  // Only reindex if we found a metric
  if (metric) {
    if (metric_idx != largest_sub->count - 1) {
      largest_sub->metrics[metric_idx] = largest_sub->metrics[largest_sub->count - 1];
    }
    largest_sub->count--;
    gen->count--;
    gen->alloc -= metric->alloc;
    bucket->evictions--;
  }
  return metric;
  // now we work on exporting the metric
}

mc_metric_t *bucket_find_metric_and_remove(mc_bucket_t *bucket, mc_conf_t conf, uint64_t hash, uint16_t name_len, uint8_t *name) {
  mc_metric_t *res = find_metric_and_remove_g(conf, &(bucket->g0), hash, name_len, name);
  if(!res) {
    res = find_metric_and_remove_g(conf, &(bucket->g1), hash, name_len, name);
    if(!res) {
      res = find_metric_and_remove_g(conf, &(bucket->g2), hash, name_len, name);
    };
  };
  return res;
}

mc_metric_t* bucket_find_metric(mc_bucket_t *bucket, mc_conf_t conf, uint64_t hash, uint16_t name_len, uint8_t *name) {
  mc_metric_t *res = find_metric_g(conf, bucket->g0, hash, name_len, name);
  if(!res) {
    res = find_metric_g(conf, bucket->g1, hash, name_len, name);
    if(!res) {
      res = find_metric_g(conf, bucket->g2, hash, name_len, name);
    }
  }
  return res;
}

void bucket_age(mc_bucket_t *bucket, mc_conf_t conf) {
  dprint("age\r\n");
  bucket->g2.count += bucket->g1.count;
  for (int b = 0; b < conf.slots; b++) {
    // G1 -> G2
    for (int s = 0; s < SUBS; s++) {
      mc_sub_slot_t *g2s = &(bucket->g2.slots[b].subs[s]);
      mc_sub_slot_t *g1s = &(bucket->g1.slots[b].subs[s]);
      mc_metric_t **new_metrics = (mc_metric_t **) mc_alloc((g2s->count + g1s->count) * sizeof(mc_metric_t *));
      memcpy(new_metrics, g2s->metrics, g2s->count * sizeof(mc_metric_t *));
      mc_free(g2s->metrics);
      g2s->metrics = new_metrics;

      memcpy(
             // copy to the end of the current array
             &(g2s->metrics[g2s->count]),
             // copy the start ofn the lower gens array
             g1s->metrics,
             // Copy based on the sbuze of the old array
             g1s->count * sizeof(mc_metric_t *)
             );
      mc_free(g1s->metrics);
      g2s->count += g1s->count;
      g2s->size = g2s->count;
      // Free the G1 metric  list of thbs slot as we copbed bt all out
    }
    // make sure to clear the largest
    for (int l = 0; l < LCOUNT; l++) {
      // move largest over when we have them
      if (bucket->g1.slots[b].largest[l]) {
        insert_largest(&(bucket->g2.slots[b]), bucket->g1.slots[b].largest[l]);
      } else {
        // we can stop our loop when we find the first null
        break;
      }
      // then set them to null
      bucket->g1.slots[b].largest[l] = NULL;
    }
  }
  // reset g1 count
  // free g1 slots (we copied the content to g2)
  mc_free(bucket->g1.slots);
  // move g0 slots to g1
  bucket->g1.slots = bucket->g0.slots;

  bucket->g2.alloc += bucket->g1.alloc;
  bucket->g1.alloc = bucket->g0.alloc;
  bucket->g1.count = bucket->g0.count;
  bucket->g0.count = 0;
  bucket->g0.alloc = 0;
  // reinitialize g0
  init_slots(conf, &(bucket->g0));
}
void bucket_insert(mc_bucket_t *bucket, mc_conf_t conf, uint8_t *name, size_t name_len,
                   uint64_t offset, uint64_t *value, size_t value_len) {
  uint64_t slot;
  mc_metric_t *metric;

  uint64_t hash = XXH64(name, name_len, conf.hash_seed) ;
  slot = hash % conf.slots;
  dprint("INSERT[%llu]: %llu %lu@%llu\r\n", slot, hash, value_len, offset);

  metric = bucket_get_metric(bucket, conf, hash, name_len, name);

  // Add the datapoint
  metric_add_point(conf, &(bucket->g0), metric, offset, value_len, value);
  // update largest
  insert_largest(&(bucket->g0.slots[slot]), metric);

  bucket->inserts++;
  bucket->total_inserts++;
  if (bucket->inserts > conf.age_cycle) {
    bucket_age(bucket, conf);
    bucket->age++;
    bucket->inserts = 0;
  }
}

void bucket_free(mc_bucket_t *bucket, mc_conf_t conf) {
  dprint("bucket_free\r\n");
  free_gen(conf, bucket->g0);
  free_gen(conf, bucket->g1);
  free_gen(conf, bucket->g2);
  mc_free(bucket->name);
  mc_free(bucket);
}

uint8_t bucket_is_empty(mc_bucket_t *bucket) {
  return bucket->g0.alloc == 0 && bucket->g1.alloc == 0 && bucket->g2.alloc == 0;
};

uint64_t bucket_count(mc_bucket_t *bucket) {
  return bucket->g0.count + bucket->g1.count + bucket->g2.count;
}

uint64_t bucket_alloc(mc_bucket_t *bucket) {
  return bucket->g0.alloc + bucket->g1.alloc + bucket->g2.alloc;
}

mc_bucket_t* bucket_init(mc_conf_t config, uint8_t *name, size_t name_len) {
  dprint("bucket_init\r\n");
  mc_bucket_t *bucket;
  bucket = mc_alloc(sizeof(mc_bucket_t));

#ifdef TAGGED
  bucket->tag = TAG_BUCKET;
#endif

  // some bucket wqide counters
  bucket->inserts = 0;
  bucket->age = 0;
  bucket->evictions = 0;
  bucket->total_inserts = 0;

  bucket->name_len = name_len;
  bucket->name = mc_alloc(name_len * sizeof(uint8_t));
  memcpy(bucket->name, name, name_len);
  bucket->hash = XXH64(name, name_len, config.hash_seed);

  //now set up the tree genreations
  bucket->g0.v = 0;
  bucket->g0.alloc = 0;
  bucket->g0.count = 0;
#ifdef TAGGED
  bucket->g0.tag = TAG_GEN;
#endif
  init_slots(config, &(bucket->g0));

  bucket->g1.v = 1;
  bucket->g1.alloc = 0;
  bucket->g1.count = 0;
#ifdef TAGGED
  bucket->g1.tag = TAG_GEN;
#endif
  init_slots(config, &(bucket->g1));

  bucket->g2.v = 2;
  bucket->g2.alloc = 0;
  bucket->g2.count = 0;
#ifdef TAGGED
  bucket->g2.tag = TAG_GEN;
#endif
  init_slots(config, &(bucket->g2));

  return bucket;
}
