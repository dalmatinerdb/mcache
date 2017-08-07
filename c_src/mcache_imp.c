#include "mcache.h"

uint8_t is_empty(mc_bucket_t *bucket) {
  return bucket->g0.alloc == 0 && bucket->g1.alloc == 0 && bucket->g2.alloc == 0;
};


static void free_entry(mc_entry_t *e) {
  if (e->next) {
    free_entry(e->next);
  }
  mc_free(e->data);
  mc_free(e);
}

void free_metric(mc_metric_t *m) {
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

void free_bucket(mc_bucket_t *bucket) {
  free_gen(bucket->conf, bucket->g0);
  free_gen(bucket->conf, bucket->g1);
  free_gen(bucket->conf, bucket->g2);
}

void age(mc_bucket_t *bucket) {
  dprint("age\r\n");
  for (int b = 0; b < bucket->conf.slots; b++) {
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
  // free g1 slots (we copied the content to g2)
  mc_free(bucket->g1.slots);
  // move g0 slots to g1
  bucket->g1.slots = bucket->g0.slots;

  bucket->g2.alloc += bucket->g1.alloc;
  bucket->g1.alloc = bucket->g0.alloc;
  bucket->g0.alloc = 0;
  // reinitialize g0
  init_slots(bucket->conf, &(bucket->g0));

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

ERL_NIF_TERM serialize_metric(ErlNifEnv* env, mc_metric_t *metric) {
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

void insert_largest(mc_slot_t *slot, mc_metric_t *metric) {
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


static mc_metric_t *find_metric_g(mc_conf_t conf, mc_gen_t gen, uint64_t hash, uint16_t name_len, uint8_t *name) {
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

mc_metric_t *find_metric(mc_bucket_t *bucket, uint64_t hash, uint16_t name_len, uint8_t *name) {
  mc_metric_t *res = find_metric_g(bucket->conf, bucket->g0, hash, name_len, name);
  if(!res) {
    res = find_metric_g(bucket->conf, bucket->g1, hash, name_len, name);
    if(!res) {
      res = find_metric_g(bucket->conf, bucket->g2, hash, name_len, name);
    }
  }
  return res;
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

mc_metric_t *find_metric_and_remove(mc_bucket_t *bucket, uint64_t hash, uint16_t name_len, uint8_t *name) {
  mc_metric_t *res = find_metric_and_remove_g(bucket->conf, &(bucket->g0), hash, name_len, name);
  if(!res) {
    res = find_metric_and_remove_g(bucket->conf, &(bucket->g1), hash, name_len, name);
    if(!res) {
      res = find_metric_and_remove_g(bucket->conf, &(bucket->g2), hash, name_len, name);
    };
  };
  return res;
}

static uint64_t remove_prefix_g(mc_conf_t conf, mc_gen_t *gen, uint16_t pfx_len, uint8_t *pfx) {
  int i = 0;
  uint64_t counter = 0;
  // Itterate over the existig metrics and see if we have already
  // seen this one.
  for (int slot = 0; slot < conf.slots; slot++) {
    for (int sub = 0 ; sub < SUBS; sub++) {
      for (i = 0; i < gen->slots[slot].subs[sub].count; i++) {
        mc_metric_t *m = gen->slots[slot].subs[sub].metrics[i];
        if (m->name_len >= pfx_len
            && memcmp(m->name, pfx, pfx_len) == 0) {
          if (i != gen->slots[slot].subs[sub].count - 1) {
            gen->slots[slot].subs[sub].metrics[i] = gen->slots[slot].subs[sub].metrics[gen->slots[slot].subs[sub].count - 1];
            // we chear we move the last element in the current position and then go a step
            // back so we re-do it
            i--;
          }
          gen->slots[slot].subs[sub].count--;
          gen->alloc -= m->alloc;
          remove_largest(&(gen->slots[slot]), m);
          free_metric(m);
          counter++;
        }
      }
    }
  }
  return counter;
};

uint64_t remove_prefix(mc_bucket_t *bucket, uint16_t pfx_len, uint8_t *pfx) {
  return remove_prefix_g(bucket->conf, &(bucket->g0), pfx_len, pfx) +
    remove_prefix_g(bucket->conf, &(bucket->g1), pfx_len, pfx) +
    remove_prefix_g(bucket->conf, &(bucket->g2), pfx_len, pfx);
}

mc_metric_t *get_metric(mc_bucket_t *bucket, uint64_t hash, uint16_t name_len, uint8_t *name) {

  uint64_t slot = hash % bucket->conf.slots;
  uint8_t sub = subid(hash);
  // Itterate over the existig metrics and see if we have already
  // seen this one.
  mc_metric_t *metric = find_metric_g(bucket->conf, bucket->g0, hash, name_len, name);
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
  metric = find_metric_and_remove_g(bucket->conf, &(bucket->g1), hash, name_len, name);
  if (!metric) {
    metric = find_metric_and_remove_g(bucket->conf, &(bucket->g2), hash, name_len, name);
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
  insert_largest(b, metric);
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

mc_metric_t * check_limit(mc_bucket_t *bucket, uint64_t max_alloc, uint64_t slot) {
  if (max_alloc > bucket->g0.alloc +
      bucket->g1.alloc +
      bucket->g2.alloc) {
    return NULL;
  }
  // If we don't have g2 entries we age so g1 becomes
  // g2
  mc_gen_t *gen = &(bucket->g2);

  if (gen->alloc == 0) {
    gen = &(bucket->g1);
  }

  // If we still have no g2 entries we aga again,
  // this way g0 effectively becomes g0
  if (gen->alloc == 0) {
    gen = &(bucket->g0);
  }

  // if we still have no g2 entries we know it's
  // all for nothing and just give up
  if (gen->alloc == 0) {
    return NULL;
  }

  //we know we have at least 1 entrie so we grab the alst one
  // and reduce the count and reduce the alloc;

  //TODO: This is not good!
  mc_metric_t *metric = NULL;
  mc_sub_slot_t *largest_sub =  NULL;
  mc_slot_t *largest_slot =  NULL;
  int largest_idx = 0;
  // try to find a metric using the largest bucket first
  for (int b = 0; b < bucket->conf.slots; b++) {
    if (gen->slots[b].largest[0] &&
        (!metric || metric->alloc < gen->slots[b].largest[0]->alloc)) {
      largest_slot = &(gen->slots[b]);
      metric = largest_slot->largest[0];
      largest_sub = &(largest_slot->subs[subid(metric->hash)]);
    }
  }
  if (metric) {
    largest_idx = 0;
    remove_largest(largest_slot, metric);
    // find the index of the metric
    while (largest_idx < SUBS && largest_sub->metrics[largest_idx] != metric) {
      largest_idx++;
    }
    // We didn't found anything that is problematic so we pretend it never
    // happend, the metric is already removed from the largest index
    if (largest_idx == SUBS) {
      dprint("Oh my\r\n");
      largest_idx = 0;
      metric = NULL;
      largest_slot = NULL;
      largest_sub = NULL;
    }
  }
  for (int i = 0; i < bucket->conf.slots; i++) {
    // we break if we found a sutiable metric either in this loop or the loop before
    if (metric) {
      break;
    }

    // We itterate through all slots starting after the slot we just edited
    // that way we avoid always changing the same buket over and over
    int b = (i + slot + 1) % bucket->conf.slots;
    mc_slot_t *slot = &(gen->slots[b]);
    for (int sub = 0; sub < SUBS; sub++) {
      // If we found a non empty slot we find the largest metric in there to
      // evict, that way we can can free up the 'most sensible' thing;
      for (int j = 0; j < slot->subs[sub].count; j++) {
        if (!metric || slot->subs[sub].metrics[j]->alloc >= metric->alloc) {
          largest_idx = j;
          largest_sub = &(slot->subs[sub]);
          metric = largest_sub->metrics[j];
        }
      }
    }

  }
  if (metric) {
    if (largest_idx != largest_sub->count - 1) {
      largest_sub->metrics[largest_idx] = largest_sub->metrics[largest_sub->count - 1];
    }
    largest_sub->count--;
    gen->alloc -= metric->alloc;
    return metric;
  }
  return NULL;
  // now we work on exporting the metric
}

mc_metric_t* take(mc_bucket_t *bucket, ErlNifBinary name) {
  uint64_t hash = XXH64(name.data, name.size, bucket->conf.hash_seed);
  return find_metric_and_remove(bucket, hash, name.size, name.data);
}

mc_metric_t* get(mc_bucket_t *bucket, ErlNifBinary name) {
  uint64_t hash = XXH64(name.data, name.size, bucket->conf.hash_seed);
  return find_metric(bucket, hash, name.size, name.data);
}

mc_metric_t* pop(mc_bucket_t *bucket) {
  return check_limit(bucket, 0, 0);
}

mc_metric_t* insert(mc_bucket_t *bucket, ErlNifBinary name, uint64_t offset, ErlNifBinary value) {
  uint64_t slot;
  mc_metric_t *metric;

  uint64_t hash = XXH64(name.data, name.size, bucket->conf.hash_seed) ;
  slot = hash % bucket->conf.slots;
  dprint("INSERT[%llu]: %llu %lu@%lu\r\n", slot, hash, value.size / 8, offset);

  metric = get_metric(bucket, hash, name.size, name.data);

  // Add the datapoint
  add_point(bucket->conf, &(bucket->g0), metric, offset, value.size / 8, (ErlNifUInt64 *) value.data);
  // update largest
  insert_largest(&(bucket->g0.slots[slot]), metric);

  bucket->inserts++;
  if (bucket->inserts > bucket->conf.age_cycle) {
    //age(bucket);
    bucket->age++;
    bucket->inserts = 0;
  }
  // We now check for overflow note that metric is re-used here!
  return check_limit(bucket, bucket->conf.max_alloc, slot);

}
