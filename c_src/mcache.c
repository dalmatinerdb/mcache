#include "mcache.h"

ErlNifResourceType* mcache_t_handle;
static ERL_NIF_TERM atom_ok;
static ERL_NIF_TERM atom_error;
static ERL_NIF_TERM error_not_found;
static ERL_NIF_TERM atom_undefined;
static ERL_NIF_TERM atom_overflow;

void init_slots(mc_conf_t conf,/*@out@*/ mc_gen_t *gen) {
  dprint("init_slots\r\n");
  gen->slots = (mc_slot_t *) mc_alloc(conf.slots * sizeof(mc_slot_t));
  for (int s = 0; s < conf.slots; s++) {
    mc_slot_t *slot = &(gen->slots[s]);

    for (int i = 0; i < LCOUNT; i++) {
      slot->largest[i] = NULL;
    }

#ifdef TAGGED
    slot->tag = TAG_SLOT;
#endif

    for (int su = 0; su < SUBS; su++) {
      mc_sub_slot_t *sub = &(slot->subs[su]);
      sub->size = conf.initial_entries;
      sub->count = 0;
      sub->metrics = (mc_metric_t **) mc_alloc(sub->size * sizeof(mc_metric_t *));
#ifdef TAGGED
      sub->tag = TAG_SUB;
      for (int i = 0; i < sub->size; i++) {
        // This is for debugging only we set them to fill the data with non random 'bad' pointers
        sub->metrics[i] = (mc_metric_t *) TAG_METRIC_L;
      }
#endif
    };
  }
}

void init_bucket(mc_conf_t conf, mc_bucket_t *bkt) {

  //now set up the tree genreations
#ifdef TAGGED
  bkt->tag = TAG_BKT;
#endif

  bkt->g0.v = 0;
  bkt->g0.alloc = 0;
#ifdef TAGGED
  bkt->g0.tag = TAG_GEN;
#endif
  init_slots(conf, &(bkt->g0));

  bkt->g1.v = 1;
  bkt->g1.alloc = 0;
#ifdef TAGGED
  bkt->g1.tag = TAG_GEN;
#endif
  init_slots(conf, &(bkt->g1));

  bkt->g2.v = 2;
  bkt->g2.alloc = 0;
#ifdef TAGGED
  bkt->g2.tag = TAG_GEN;
#endif
  init_slots(conf, &(bkt->g2));

}

void remove_largest(mc_slot_t *slot, mc_metric_t *metric) {
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

void insert_largest(mc_slot_t *slot, mc_metric_t *metric) {
  dprint("[%p] insert largest: %llu\r\n", slot, metric->hash);
  remove_largest(slot, metric);
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

void age(mc_conf_t conf, mc_bucket_t *bkt) {
  dprint("age\r\n");
  for (int sl = 0; sl < conf.slots; sl++) {
    // G1 -> G2
    for (int su = 0; su < SUBS; su++) {
      mc_sub_slot_t *g2s = &(bkt->g2.slots[sl].subs[su]);
      mc_sub_slot_t *g1s = &(bkt->g1.slots[sl].subs[su]);
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
      // Free the G1 metric  list of thbs lot as we copbed bt all out
    }
    // make sure to clear the largest
    for (int l = 0; l < LCOUNT; l++) {
      // move largest over when we have them
      if (bkt->g1.slots[sl].largest[l]) {
        insert_largest(&(bkt->g2.slots[sl]), bkt->g1.slots[sl].largest[l]);
      } else {
        // we can stop our loop when we find the first null
        break;
      }
      // then set them to null
      bkt->g1.slots[sl].largest[l] = NULL;
    }
  }
  // free g1 buckets (we copied the content to g2)
  mc_free(bkt->g1.slots);
  // move g0 slots to g1
  bkt->g1.slots = bkt->g0.slots;

  bkt->g2.alloc += bkt->g1.alloc;
  bkt->g1.alloc = bkt->g0.alloc;
  bkt->g0.alloc = 0;
  // reinitialize g0
  init_slots(conf, &(bkt->g0));

}

static ERL_NIF_TERM serialize_entry(ErlNifEnv* env, mc_entry_t *entry) {
  ERL_NIF_TERM data;
  size_t to_copy = entry->count * sizeof(ErlNifUInt64);
  unsigned char *datap = enif_make_new_binary(env, to_copy, &data);
  memcpy(datap, entry->data, to_copy);
  return  enif_make_tuple2(env,
                           enif_make_uint64(env, entry->start),
                           data);
}

static ERL_NIF_TERM serialize_metric(ErlNifEnv* env, mc_metric_t *metric) {
  if (! metric) {
    return atom_undefined;
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
  if (e->next) {
    free_entry(e->next);
  }
  mc_free(e->data);
  mc_free(e);
}


static void free_metric(mc_metric_t *m) {
  if (m->head) {
    free_entry(m->head);
  }
  mc_free(m->name);
  mc_free(m);
}

static void free_gen(mc_conf_t conf, mc_gen_t gen) {
  for (int b = 0; b < conf.slots; b++) {
    for (int s = 0; s < SUBS; s++) {
      for (int m = 0; m < gen.slots[b].subs[s].count; m++) {
        free_metric(gen.slots[b].subs[s].metrics[m]);
      };
      mc_free(gen.slots[b].subs[s].metrics);
    }
  }
  mc_free(gen.slots);
};

static void free_bucket(mc_conf_t conf, mc_bucket_t bkt) {
  free_gen(conf, bkt.g0);
  free_gen(conf, bkt.g1);
  free_gen(conf, bkt.g2);
}

static void cache_dtor(ErlNifEnv* env, void* handle) {
  dprint("cache_dtor\r\n");
  mcache_t * c = (mcache_t*) handle;
  for (int b = 0; b < c->bkt_count; b++) {
    mc_bucket_t bkt = c->buckets[b];
    free_bucket(c->conf, bkt);
    mc_free(bkt.name);
  }
  mc_free(c->buckets);
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
  atom_error = enif_make_atom(env, "error");
  atom_overflow = enif_make_atom(env, "overflow");
  error_not_found = enif_make_tuple2(env, atom_error,
                                     enif_make_atom(env, "not_found"));
  return 0;
}

static int
upgrade(ErlNifEnv* env, void** priv, void** old_priv, ERL_NIF_TERM load_info)
{
  return 0;
}

static ERL_NIF_TERM
new_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  dprint("new\r\n");
  mcache_t *cache;
  ErlNifUInt64 max_alloc;
  ErlNifUInt64 slots;
  ErlNifUInt64 age_cycle;
  ErlNifUInt64 initial_data_size;
  ErlNifUInt64 initial_entries;
  ErlNifUInt64 hash_seed;
  ErlNifUInt64 max_gap;
  if (argc != 7) {
    return enif_make_badarg(env);
  };
  if (!enif_get_uint64(env, argv[0], &max_alloc)) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[1], &slots)) return enif_make_badarg(env);
  if (slots <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[2], &age_cycle)) return enif_make_badarg(env);
  if (age_cycle <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[3], &initial_data_size)) return enif_make_badarg(env);
  if (initial_data_size <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[4], &initial_entries)) return enif_make_badarg(env);
  if (initial_entries <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[5], &hash_seed)) return enif_make_badarg(env);
  if (hash_seed <= 0) return enif_make_badarg(env);

  if (!enif_get_uint64(env, argv[6], &max_gap)) return enif_make_badarg(env);

  cache = (mcache_t *) enif_alloc_resource(mcache_t_handle, sizeof(mcache_t));

#ifdef TAGGED
  cache->tag = TAG_CACHE;
#endif

  // Set up the config
  cache->conf.max_alloc = max_alloc;
  cache->conf.slots = slots;
  cache->conf.age_cycle = age_cycle;

  cache->conf.initial_data_size = initial_data_size;
  cache->conf.initial_entries = initial_entries;
  cache->conf.hash_seed = hash_seed;
  cache->conf.max_gap = max_gap;

  // some cache wqide counters
  cache->inserts = 0;
  cache->age = 0;
  cache->bkt_count = 0;

  cache->bkt_size = BKT_GROWTH;
  cache->buckets = mc_alloc(cache->bkt_size * sizeof(mc_bucket_t));

  ERL_NIF_TERM term = enif_make_resource(env, cache);
  enif_release_resource(cache);
  return term;
};


mc_metric_t *find_metric_g(mc_conf_t conf, mc_gen_t gen, uint64_t hash, uint16_t name_len, uint8_t *name) {
  // Itterate over the existig metrics and see if we have already
  // seen this one.
  uint64_t slot = hash % conf.slots;
  uint8_t sub = subid(hash);
  for (int i = 0; i < gen.slots[slot].subs[sub].count; i++) {
    mc_metric_t *m = gen.slots[slot].subs[sub].metrics[i];
    if (m->name_len == name_len
        && m->hash == hash
        && memcmp(m->name, name, name_len) == 0) {
      return m;
    }
  }
  return NULL;
};

mc_metric_t *find_metric(mc_conf_t conf, mc_bucket_t *bkt, uint64_t hash, uint16_t name_len, uint8_t *name) {
  mc_metric_t *res = find_metric_g(conf, bkt->g0, hash, name_len, name);
  if(!res) {
    res = find_metric_g(conf, bkt->g1, hash, name_len, name);
    if(!res) {
      res = find_metric_g(conf, bkt->g2, hash, name_len, name);
    }
  }
  return res;
}


mc_metric_t *find_metric_and_remove_g(mc_conf_t conf, mc_gen_t *gen, uint64_t hash, uint16_t name_len, uint8_t *name) {
  int i = 0;
  uint64_t slot = hash % conf.slots;
  uint8_t sub = subid(hash);

  // Itterate over the existig metrics and see if we have already
  // seen this one.

  for (i = 0; i < gen->slots[slot].subs[sub].count; i++) {
    mc_metric_t *m = gen->slots[slot].subs[sub].metrics[i];
    if (m->name_len == name_len
        && m->hash == hash
        && memcmp(m->name, name, name_len) == 0) {
      if (i != gen->slots[slot].subs[sub].count - 1) {
        gen->slots[slot].subs[sub].metrics[i] = gen->slots[slot].subs[sub].metrics[gen->slots[slot].subs[sub].count - 1];
      }
      remove_largest(&(gen->slots[slot]), m);
      gen->slots[slot].subs[sub].count--;
      gen->alloc -= m->alloc;
      return m;
    }
  }
  return NULL;
};

mc_metric_t *find_metric_and_remove(mc_conf_t conf, mc_bucket_t *bkt, uint64_t hash, uint16_t name_len, uint8_t *name) {
  mc_metric_t *res = find_metric_and_remove_g(conf, &(bkt->g0), hash, name_len, name);
  if(!res) {
    res = find_metric_and_remove_g(conf, &(bkt->g1), hash, name_len, name);
    if(!res) {
      res = find_metric_and_remove_g(conf, &(bkt->g2), hash, name_len, name);
    };
  };
  return res;
}


mc_metric_t *get_metric(mc_conf_t conf, mc_bucket_t *bkt, uint64_t hash, uint16_t name_len, uint8_t *name) {
  uint64_t slot_id = hash % conf.slots;
  uint8_t sub_id = subid(hash);
  // Itterate over the existig metrics and see if we have already
  // seen this one.
  mc_metric_t *metric = find_metric_g(conf, bkt->g0, hash, name_len, name);
  if (metric) {
    return metric;
  }
  mc_slot_t *slot = &(bkt->g0.slots[slot_id]);
  mc_sub_slot_t *sub = &(slot->subs[sub_id]);
  // We start with a small cache and grow it as required, so we need to
  // make sure the new index doesn't exceet the count. If it does
  // double the size of the cache.
  if (sub->count >= sub->size) {
    mc_metric_t **new_metrics = mc_alloc(sub->size * 2 * sizeof(mc_metric_t *));
#ifdef TAGGED
    for (int i = 0; i < sub->size * 2; i++) {
      // We do this to fill the data it's not right but the same as
      new_metrics[i] = (mc_metric_t *) TAG_METRIC_L;
    }
#endif
    memcpy(new_metrics, sub->metrics, sub->count * sizeof(mc_metric_t *));
    mc_free(sub->metrics);
    sub->metrics = new_metrics;
    sub->size = sub->size * 2;
  }
  // we havn't seen the metric so we'll create a new one.
  metric = find_metric_and_remove_g(conf, &(bkt->g1), hash, name_len, name);
  if (!metric) {
    metric = find_metric_and_remove_g(conf, &(bkt->g2), hash, name_len, name);
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
  bkt->g0.alloc += metric->alloc;
  sub->metrics[sub->count] = metric;
  sub->count++;
  insert_largest(slot, metric);
  return metric;
};

void add_point(mc_conf_t conf, mc_gen_t *gen, mc_metric_t *metric, ErlNifUInt64 offset, size_t count, ErlNifUInt64* values) {
  // If eitehr we have no data yet or the current data is larger then
  // the offset we generate a new metric.
  // In both cases next will be the current head given that next might
  // be empty and it's needed to set next to empty for the first element.
  mc_entry_t *entry = NULL;
  if((!metric->head) || offset < metric->head->start) {
    size_t alloc = sizeof(ErlNifUInt64) * MAX(conf.initial_data_size, count * 2);
    entry = mc_alloc(sizeof(mc_entry_t));
    entry->start = offset;
    entry->size = conf.initial_data_size;
    entry->data = (ErlNifUInt64 *) mc_alloc(alloc);

#ifdef TAGGED
    entry->tag = TAG_ENTRY;
    for (int i = 0; i < entry->size; i++) {
      entry->data[i] = TAG_DATA_L;
    }
#endif

    entry->count = 0;
    entry->next = metric->head;
    metric->head = entry;
    metric->tail = entry;
    metric->alloc += alloc + sizeof(mc_entry_t);
    gen->alloc += alloc + sizeof(mc_entry_t);
  }
  if (offset > metric->tail->start) {
    entry = metric->tail;
  } else {
    entry = metric->head;
  }
  do {
    // if the offset is beyond the next chunks start
    // just go ahead and go for this chunk
    if(entry->next && (offset >= entry->next->start)) {
      entry = entry->next;
      continue;
    }

    //We know that we either have no next chunk or the data is before
    //the start of the next chunk

    // we allocate a new chunk behind this and continue with that.
    uint64_t internal_offset = offset - entry->start;

    // or we'd have gaps
    dprint("CHECK: %llu > (%u + %llu)!\r\n", internal_offset, entry->count, conf.max_gap);
    if (internal_offset > (entry->count + conf.max_gap)) {
      dprint("we got a gap!\r\n");
      // TODO: we could combine that with merge and dertermin if we can
      // just pull the start back but that remains for another day
      mc_entry_t *next = mc_alloc(sizeof(mc_entry_t));
      // we allocate at least as much memory as we need for our data
      // so we don't need to allocate twice for bigger blocks
      next->size = MAX(conf.initial_data_size, count);
      uint64_t alloc = next->size * sizeof(ErlNifUInt64);
      // create the new entry with the given offset
      next->start = offset;
      // reserve the data
      next->data = (ErlNifUInt64 *) mc_alloc(alloc);

#ifdef TAGGED
      next->tag = TAG_ENTRY;
      for (int i = 0; i < next->size; i++) {
        next->data[i] = TAG_DATA_L;
      }
#endif

      // we have not put in data yet
      next->count = 0;

      // insert this inbetween this and the next element
      next->next = entry->next;
      entry->next = next;
      // if next->next is null then we know our next is now the tail
      if (!next->next) {
        metric->tail = next;
      }
      // we continue with this element as entry

      entry = next;


      metric->alloc += alloc + sizeof(mc_entry_t);
      gen->alloc += alloc + sizeof(mc_entry_t);

      continue;
    }

    // if we would overlap with the next chunk we combine the two
    if (entry->next &&
        offset + count + conf.max_gap >= entry->next->start) {
      // the new size is the delta between our start and the total end of the next chunk
      mc_entry_t *next = entry->next;
      uint64_t new_size = next->start + next->size - entry->start;
      uint64_t new_count = next->start - entry->start + next->count;

      dprint("Asked to write %lu -> %lu\r\n", offset, offset + count);
      dprint("combining %ld->%ld(%ld) and %ld->%ld(%ld)\r\n",
             entry->start, entry->start + entry->count, entry->start + entry->size,
             next->start, next->start + next->count, next->start + next->size);
      dprint("new range %ld->%llu(%lld)\r\n", entry->start, entry->start + new_count, entry->start + new_size);


      ErlNifUInt64 *new_data = (ErlNifUInt64 *) mc_alloc(new_size * sizeof(ErlNifUInt64));
#ifdef TAGGED
      for (int i = 0; i < new_size; i++) {
        new_data[i] = TAG_DATA_L;
      }
#endif


      // recalculate the allocation
      metric->alloc -= (entry->size * sizeof(ErlNifUInt64));
      gen->alloc -= (entry->size * sizeof(ErlNifUInt64));

      metric->alloc -= (next->size * sizeof(ErlNifUInt64));
      gen->alloc -= (next->size * sizeof(ErlNifUInt64));

      metric->alloc += new_size * sizeof(ErlNifUInt64);
      gen->alloc += new_size * sizeof(ErlNifUInt64);


      entry->next = next->next;
      // copy, free and reassign old data
      memcpy(new_data, entry->data, entry->count * sizeof(ErlNifUInt64));
      mc_free(entry->data);
      entry->data = new_data;
      // set new size and count
      entry->size = new_size;

      // now we calculate the delta of old and new start to get the offset in the
      // new array to copy data to then free it
      dprint("2nd chunk offset: %ld\r\n", next->start - entry->start);
      dprint("copying points: %ld\r\n", next->start - entry->start);

      dprint("Filling between %u and %ld\r\n", entry->count, next->start - entry->start);
      for (int i = entry->count; i < next->start - entry->start; i++) {
        entry->data[i] = 0;
      }

      entry->count = new_count;

      memcpy(entry->data + next->start - entry->start, next->data, next->count* sizeof(ErlNifUInt64));
      mc_free(next->data);
      mc_free(next);
      // if we don't have a next we are now the tail!
      if (!entry->next) {
        metric->tail = entry;
      }
      // we go back to the start of the loop in case we'd overlap multiple ones (oh my!)
      continue;
    };

    // the offset in our data array

    if (internal_offset + count >= entry->size) {
      uint64_t new_size = entry->size * 2;
      // keep growing untill we're sure we have the right size
      while (internal_offset + count >= new_size) {
        new_size *= 2;
      }

      // prevent oddmath we just  removethe oldsizeand then add
      // the new size
      metric->alloc -= (entry->size * sizeof(ErlNifUInt64));
      gen->alloc -= (entry->size * sizeof(ErlNifUInt64));

      ErlNifUInt64 *new_data = (ErlNifUInt64 *) mc_alloc(new_size * sizeof(ErlNifUInt64));
#ifdef TAGGED
      for (int i = 0; i < new_size; i++) {
        new_data[i] = TAG_DATA_L;
      }
#endif

      memcpy(new_data, entry->data, entry->size * sizeof(ErlNifUInt64));
      mc_free(entry->data);
      entry->data = new_data;
      entry->size = new_size;
      metric->alloc += (entry->size * sizeof(ErlNifUInt64));
      gen->alloc += (entry->size * sizeof(ErlNifUInt64));
    }

    // fill gap with zeros
    for (int i = entry->count; i < internal_offset; i++) {
      entry->data[i] = 0;
    }

    memcpy(entry->data + internal_offset, values, count * sizeof(ErlNifUInt64));

    entry->count = MAX(offset - entry->start + count, entry->count);

    if (!entry->next) {
      metric->tail = entry;
    }
    return;
  } while (entry);
}


bucket_metric_t find_largest(mc_conf_t conf, mc_bucket_t *bkt, uint64_t slot) {
  bucket_metric_t bm = {bkt, NULL};
  // If we don't have g2 entries we age so g1 becomes
  // g2
  mc_gen_t *gen = &(bkt->g2);

  if (gen->alloc == 0) {
    gen = &(bkt->g1);
  }

  // If we still have no g2 entries we aga again,
  // this way g0 effectively becomes g0
  if (gen->alloc == 0) {
    gen = &(bkt->g0);
  }

  // if we still have no g2 entries we know it's
  // all for nothing and just give up
  if (gen->alloc == 0) {
    bm.bucket = NULL;
    bm.metric = NULL;
    return bm;
  }

  //we know we have at least 1 entrie so we grab the alst one
  // and reduce the count and reduce the alloc;

  //TODO: This is not good!
  mc_sub_slot_t *largest_sub =  NULL;
  mc_slot_t *largest_slot =  NULL;
  // We go thoguht the largest cache and and see if we find a metric
  for (int slot = 0; slot < conf.slots; slot++) {
    if (gen->slots[slot].largest[0] &&
        (!bm.metric || bm.metric->alloc < gen->slots[slot].largest[0]->alloc)) {
      largest_slot = &(gen->slots[slot]);
      bm.metric = largest_slot->largest[0];
      largest_sub = &(largest_slot->subs[subid(bm.metric->hash)]);
    }
  }


  int largest_idx = 0;
  if (bm.metric) {
    largest_idx = 0;
    remove_largest(largest_slot, bm.metric);
    // find the index of the metric
    while (largest_idx < largest_sub->count && largest_sub->metrics[largest_idx] != bm.metric) {
      largest_idx++;
    }
    // We didn't found anything that is problematic so we pretend it never
    // happend, the metric is already removed from the largest index
    if (largest_idx == largest_sub->count) {
      dprint("Oh my\r\n");
      largest_idx = 0;
      bm.metric = NULL;
      largest_slot = NULL;
      largest_sub = NULL;
    }
  }
  for (int i = 0; i < conf.slots; i++) {
    // we break if we found a sutiable metric either in this loop or the loop before
    if (bm.metric) {
      break;
    }

    // We itterate through all slots starting after the slot we just edited
    // that way we avoid always changing the same buket over and over
    int s = (i + slot + 1) % conf.slots;
    mc_slot_t *slot = &(gen->slots[s]);
    for (int sub = 0; sub < SUBS; sub++) {
      // If we found a non empty slot we find the largest metric in there to
      // evict, that way we can can free up the 'most sensible' thing;
      for (int j = 0; j < slot->subs[sub].count; j++) {
        if (!bm.metric || slot->subs[sub].metrics[j]->alloc >= bm.metric->alloc) {
          largest_idx = j;
          largest_sub = &(slot->subs[sub]);
          bm.metric = largest_sub->metrics[j];
        }
      }
    }
  }
  if (bm.metric) {
    if (largest_idx != largest_sub->count - 1) {
      largest_sub->metrics[largest_idx] = largest_sub->metrics[largest_sub->count - 1];
    }
    largest_sub->count--;
    gen->alloc -= bm.metric->alloc;
  } else {
    bm.bucket = NULL;
    bm.metric = NULL;
  }
  return bm;
}


bucket_metric_t
check_limit(ErlNifEnv* env, mcache_t *cache, uint64_t max_alloc, uint64_t slot) {
  uint64_t current_alloc = 0;
  uint32_t largest_bkt = 0;
  uint64_t largest_bkt_size = 0;
  bucket_metric_t bm = {NULL, NULL};

  for (uint32_t b = 0; b < cache->bkt_count; b++) {
    mc_bucket_t bkt = cache->buckets[b];
    uint64_t bkt_size = bkt.g0.alloc +
      bkt.g1.alloc +
      bkt.g2.alloc;
    current_alloc += bkt_size;
    if (bkt_size >= largest_bkt_size) {
      largest_bkt_size = bkt_size;
      largest_bkt = 0;
    }
  }

  if (max_alloc > current_alloc) {
    return bm;
  }

  for (uint32_t b0 = 0; b0 < cache->bkt_count && !bm.metric && !bm.bucket; b0++) {
    uint32_t b = (b0 + largest_bkt) % cache->bkt_count;
    mc_bucket_t *bkt = &(cache->buckets[b]);
    bm = find_largest(cache->conf, bkt, slot);
  }
  return bm;
  // now we work on exporting the metric
}

static ERL_NIF_TERM
insert_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  mc_metric_t *metric;
  bucket_metric_t bm;
  ErlNifUInt64 offset;
  ErlNifBinary value;
  ErlNifBinary name_bin;
  ErlNifBinary bkt_bin;

  ERL_NIF_TERM data;
  ERL_NIF_TERM name;
  unsigned char *namep;
  ERL_NIF_TERM resp_bkt;
  unsigned char *resp_bktp;

  uint64_t slot;
  if (argc != 5) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };
  if (!enif_inspect_binary(env, argv[1], &bkt_bin)) {
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
  if (value.size % sizeof(ErlNifUInt64) && value.size >= 8) {
    return enif_make_badarg(env);
  }
  int64_t b = find_bkt(cache, bkt_bin.size, bkt_bin.data);

  // This is a new bucket, oh my!
  if (b == -1) {
    // if our buckets are full.
    if (cache->bkt_size == cache->bkt_count) {
      cache->bkt_size += BKT_GROWTH;
      mc_bucket_t *new_bkts = mc_alloc(cache->bkt_size * sizeof(mc_bucket_t));
      memcpy(new_bkts, cache->buckets, cache->bkt_count * sizeof(mc_bucket_t));
      mc_free(cache->buckets);
      cache->buckets = new_bkts;
    }
    // we now have a new bucket
    b = cache->bkt_count;
    dprint("Creating new bucket #%lld\r\n", b);
    cache->bkt_count++;
    init_bucket(cache->conf, &(cache->buckets[b]));
    cache->buckets[b].hash = XXH64(bkt_bin.data, bkt_bin.size, cache->conf.hash_seed);
    cache->buckets[b].name_len = bkt_bin.size;
    cache->buckets[b].name = mc_alloc(bkt_bin.size);
    memcpy(cache->buckets[b].name, bkt_bin.data, bkt_bin.size);
  }

  uint64_t hash = XXH64(name_bin.data, name_bin.size, cache->conf.hash_seed);
  slot = hash % cache->conf.slots;
  dprint("INSERT[%llu]: %llu %lu@%lu\r\n", slot, hash, value.size / 8, offset);

  metric = get_metric(cache->conf, &(cache->buckets[b]), hash, name_bin.size, name_bin.data);
  // Add the datapoint
  mc_gen_t *g0 = &(cache->buckets[b].g0);
  add_point(cache->conf, g0, metric, offset, value.size / 8, (ErlNifUInt64 *) value.data);
  // update largest
  insert_largest(&(g0->slots[slot]), metric);

  cache->inserts++;
  if (cache->inserts > cache->conf.age_cycle) {
    for (int b = 0; b < cache->bkt_count; b++) {
      age(cache->conf, &(cache->buckets[b]));
    }
    cache->age++;
    cache->inserts = 0;
  }
  // We now check for overflow note that metric is re-used here!
  bm = check_limit(env, cache, cache->conf.max_alloc, slot);
  if (bm.metric && bm.bucket) {
    data = serialize_metric(env, bm.metric);
    namep = enif_make_new_binary(env, bm.metric->name_len, &name);
    memcpy(namep, bm.metric->name, bm.metric->name_len);
    free_metric(metric);

    resp_bktp = enif_make_new_binary(env, bm.bucket->name_len, &resp_bkt);
    memcpy(resp_bktp, bm.bucket->name, bm.bucket->name_len);
    return  enif_make_tuple4(env,
                             atom_overflow,
                             resp_bkt,
                             name,
                             data);
  }
  return atom_ok;
};

static ERL_NIF_TERM
pop_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  bucket_metric_t bm;
  ERL_NIF_TERM data;
  ERL_NIF_TERM name;
  unsigned char *namep;
  ERL_NIF_TERM resp_bkt;
  unsigned char *resp_bktp;

  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };

  // We cheat here, we can create a 'pop' by just checkig for a 0 limit.
  bm = check_limit(env, cache, 0, 0);
  if (bm.metric && bm.bucket) {
    data = serialize_metric(env, bm.metric);
    namep = enif_make_new_binary(env, bm.metric->name_len, &name);
    memcpy(namep, bm.metric->name, bm.metric->name_len);
    free_metric(bm.metric);
    resp_bktp = enif_make_new_binary(env, bm.bucket->name_len, &resp_bkt);
    memcpy(resp_bktp, bm.bucket->name, bm.bucket->name_len);
    return  enif_make_tuple4(env,
                             atom_ok,
                             resp_bkt,
                             name,
                             data);
  }
  return atom_undefined;

};

static ERL_NIF_TERM
get_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  mc_metric_t *metric;
  ErlNifBinary name_bin;
  ErlNifBinary bkt_bin;
  if (argc != 3) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };

  if (!enif_inspect_binary(env, argv[1], &bkt_bin)) {
    return enif_make_badarg(env);
  };

  int64_t b = find_bkt(cache, bkt_bin.size, bkt_bin.data);

  if (b == -1) {
    return atom_undefined;
  };

  if (!enif_inspect_binary(env, argv[2], &name_bin)) {
    return enif_make_badarg(env);
  };

  uint64_t hash = XXH64(name_bin.data, name_bin.size, cache->conf.hash_seed) ;
  metric = find_metric(cache->conf, &(cache->buckets[b]), hash, name_bin.size, name_bin.data);
  if (metric) {
    return  enif_make_tuple2(env,
                             atom_ok,
                             serialize_metric(env, metric));
  } else {
    return atom_undefined;
  }
};

static ERL_NIF_TERM
take_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  mc_metric_t *metric;
  ErlNifBinary name_bin;
  ErlNifBinary bkt_bin;
  if (argc != 3) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };

  if (!enif_inspect_binary(env, argv[1], &bkt_bin)) {
    return enif_make_badarg(env);
  };

  int64_t b = find_bkt(cache, bkt_bin.size, bkt_bin.data);

  if (b == -1) {
    return atom_undefined;
  };

  if (!enif_inspect_binary(env, argv[2], &name_bin)) {
    return enif_make_badarg(env);
  };

  uint64_t hash = XXH64(name_bin.data, name_bin.size, cache->conf.hash_seed);
  metric = find_metric_and_remove(cache->conf, &(cache->buckets[b]), hash, name_bin.size, name_bin.data);
  if (metric) {
    ERL_NIF_TERM res =
      enif_make_tuple2(env,
                       atom_ok,
                       serialize_metric(env, metric));
    free_metric(metric);
    return res;
  } else {
    return atom_undefined;
  }
};



static ERL_NIF_TERM
remove_bucket_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  ErlNifBinary bkt_bin;
  if (argc != 2) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };

  if (!enif_inspect_binary(env, argv[1], &bkt_bin)) {
    return enif_make_badarg(env);
  };

  int64_t b = find_bkt(cache, bkt_bin.size, bkt_bin.data);
  if (b != -1) {
    uint32_t last_bucket = cache->bkt_count - 1;
    // free the bucket we found
    free_bucket(cache->conf, cache->buckets[b]);
    // if this isn't the last bucket we replace it with the last one
    if (b < last_bucket) {
      cache->buckets[b] = cache->buckets[last_bucket];
    }
    // decrement the count
    cache->bkt_count--;
    return atom_ok;
  } else {
    return error_not_found;
  };
};


static ERL_NIF_TERM
print_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };
  for (int bkt = 0; bkt < cache->bkt_count; bkt++) {
    print_bkt(cache->conf, cache->buckets[bkt]);
  }
  fflush(stdout);
  return  atom_ok;
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
  for (int b = 0; b < cache->bkt_count; b++) {
    age(cache->conf, &(cache->buckets[b]));
  }
  return  atom_ok;
};

static ERL_NIF_TERM
conf_info(ErlNifEnv* env, mc_conf_t conf) {
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
stats_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };


  uint64_t g0_alloc = 0;
  uint64_t g1_alloc = 0;
  uint64_t g2_alloc = 0;

  uint64_t g0_count = 0;
  uint64_t g1_count = 0;
  uint64_t g2_count = 0;

  uint64_t g0_size = 0;
  uint64_t g1_size = 0;
  uint64_t g2_size = 0;

  for (int b = 0; b < cache->bkt_count; b++) {
    g0_alloc += cache->buckets[b].g0.alloc;
    for (int i = 0; i < cache->conf.slots; i++) {
      for (int sub = 0; sub < SUBS; sub++) {
        g0_size += cache->buckets[b].g0.slots[i].subs[sub].size;
        g0_count += cache->buckets[b].g0.slots[i].subs[sub].count;
      }
    };

    g1_alloc += cache->buckets[b].g1.alloc;
    for (int i = 0; i < cache->conf.slots; i++) {
      for (int sub = 0; sub < SUBS; sub++) {
        g1_size += cache->buckets[b].g1.slots[i].subs[sub].size;
        g1_count += cache->buckets[b].g1.slots[i].subs[sub].count;
      }
    };

    g2_alloc += cache->buckets[b].g2.alloc;
    for (int i = 0; i < cache->conf.slots; i++) {
      for (int sub = 0; sub < SUBS; sub++) {
        g2_size += cache->buckets[b].g2.slots[i].subs[sub].size;
        g2_alloc += cache->buckets[b].g2.slots[i].subs[sub].count;
      }
    };

  }

  ERL_NIF_TERM g0_stats =
    enif_make_list3(env,
                    enif_make_tuple2(env,
                                     enif_make_atom(env, "alloc"),
                                     enif_make_uint64(env, g0_alloc)),
                    enif_make_tuple2(env,
                                     enif_make_atom(env, "count"),
                                     enif_make_uint64(env, g0_count)),
                    enif_make_tuple2(env,
                                     enif_make_atom(env, "size"),
                                     enif_make_uint64(env, g0_size)));
  ERL_NIF_TERM g1_stats =
    enif_make_list3(env,
                    enif_make_tuple2(env,
                                     enif_make_atom(env, "alloc"),
                                     enif_make_uint64(env, g1_alloc)),
                    enif_make_tuple2(env,
                                     enif_make_atom(env, "count"),
                                     enif_make_uint64(env, g1_count)),
                    enif_make_tuple2(env,
                                     enif_make_atom(env, "size"),
                                     enif_make_uint64(env, g1_size)));
  ERL_NIF_TERM g2_stats =
    enif_make_list3(env,
                    enif_make_tuple2(env,
                                     enif_make_atom(env, "alloc"),
                                     enif_make_uint64(env, g2_alloc)),
                    enif_make_tuple2(env,
                                     enif_make_atom(env, "count"),
                                     enif_make_uint64(env, g2_count)),
                    enif_make_tuple2(env,
                                     enif_make_atom(env, "size"),
                                     enif_make_uint64(env, g2_size)));

  return enif_make_list9(env,
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "age"),
                                          enif_make_uint64(env, cache->age)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "inserts"),
                                          enif_make_uint64(env, cache->inserts)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "conf"),
                                          conf_info(env, cache->conf)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "total_alloc"),
                                          enif_make_uint64(env,
                                                           g0_alloc +
                                                           g1_alloc +
                                                           g2_alloc)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "total_count"),
                                          enif_make_uint64(env,
                                                           g0_count +
                                                           g1_count +
                                                           g2_count)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "total_size"),
                                          enif_make_uint64(env,
                                                           g0_size +
                                                           g1_size +
                                                           g2_size)),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "gen0"),
                                          g0_stats),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "gen1"),
                                          g1_stats),
                         enif_make_tuple2(env,
                                          enif_make_atom(env, "gen2"),
                                          g2_stats));
};
static ERL_NIF_TERM
is_empty_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  mcache_t *cache;
  if (argc != 1) {
    return enif_make_badarg(env);
  };
  if (!enif_get_resource(env, argv[0], mcache_t_handle, (void **)&cache)) {
    return enif_make_badarg(env);
  };
  for (int b = 0; b < cache->bkt_count; b++) {
    mc_bucket_t bkt = cache->buckets[b];
    if (bkt.g0.alloc > 0 || bkt.g1.alloc > 0 || bkt.g2.alloc > 0) {
      return enif_make_atom(env, "false");
    }
  }
  return enif_make_atom(env, "true");

};

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

/*
  H = mcache:new(0, [{buckets,1}, {age_cycle,1}, {initial_data_size,1}, {initial_entries,1}]).
  mcache:insert(H, <<>>, <<>>, 0, <<0,0,0,0,0,0,0,0>>).
  mcache:insert(H, <<>>, <<>>, 0, <<0,0,0,0,0,0,0,0>>).
  mcache:pop(H).
  mcache:is_empty(H).
*/
