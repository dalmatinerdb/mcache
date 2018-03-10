#include "mcache.h"

static void free_entry(mc_entry_t *e) {
  if (e->next) {
    free_entry(e->next);
  }
  mc_free(e->data);
  mc_free(e);
}

void metric_free(mc_metric_t *m) {
  if (m->head) {
    free_entry(m->head);
  }
  mc_free(m->name);
  mc_free(m);
}

static ERL_NIF_TERM serialize_entry(ErlNifEnv* env, mc_entry_t *entry, size_t data_size) {
  ERL_NIF_TERM data;
  size_t to_copy = entry->count * sizeof(ErlNifUInt64) * data_size;
  unsigned char *datap = enif_make_new_binary(env, to_copy, &data);
  memcpy(datap, entry->data, to_copy);
  return  enif_make_tuple2(env,
                           enif_make_uint64(env, entry->start),
                           data);

}

ERL_NIF_TERM metric_serialize(ErlNifEnv* env, mc_metric_t *metric, size_t data_size) {
  if (! metric) {
    return atom_undefined;
  }
  ERL_NIF_TERM result = enif_make_list(env, 0);
  ERL_NIF_TERM reverse;
  mc_entry_t *entry = metric->head;
  while (entry) {
    result = enif_make_list_cell(env, serialize_entry(env, entry, data_size), result);
    entry = entry->next;
  }
  enif_make_reverse_list(env, result, &reverse);
  return reverse;
}

void metric_add_point(mc_conf_t conf, mc_gen_t *gen, mc_metric_t *metric, size_t data_size, uint64_t offset, size_t count, uint64_t* values) {
  // If eitehr we have no data yet or the current data is larger then
  // the offset we generate a new metric.
  // In both cases next will be the current head given that next might
  // be empty and it's needed to set next to empty for the first element.
  mc_entry_t *entry = NULL;
  if((!metric->head) || offset < metric->head->start) {
    size_t alloc = sizeof(ErlNifUInt64) * data_size * MAX(conf.initial_data_size, count * 2);
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
      uint64_t alloc = next->size * sizeof(ErlNifUInt64) * data_size;
      // create the new entry with the given offset
      next->start = offset;
      // reserve the data
      next->data = (ErlNifUInt64 *) mc_alloc(alloc);

#ifdef TAGGED
      next->tag = TAG_ENTRY;
      for (int i = 0; i < next->size  * data_size; i++) {
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

      dprint("Asked to write %llu -> %llu\r\n", offset, offset + count);
      dprint("combining %ld->%ld(%ld) and %ld->%ld(%ld)\r\n",
             entry->start, entry->start + entry->count, entry->start + entry->size,
             next->start, next->start + next->count, next->start + next->size);
      dprint("new range %ld->%llu(%lld)\r\n", entry->start, entry->start + new_count, entry->start + new_size);


      ErlNifUInt64 *new_data = (ErlNifUInt64 *) mc_alloc(new_size * sizeof(ErlNifUInt64) * data_size);
#ifdef TAGGED
      for (int i = 0; i < new_size * data_size; i++) {
        new_data[i] = TAG_DATA_L;
      }
#endif

      // recalculate the allocation
      metric->alloc -= (entry->size * sizeof(ErlNifUInt64) * data_size);
      gen->alloc -= (entry->size * sizeof(ErlNifUInt64) * data_size);

      metric->alloc -= (next->size * sizeof(ErlNifUInt64) * data_size);
      gen->alloc -= (next->size * sizeof(ErlNifUInt64) * data_size);

      metric->alloc += new_size * sizeof(ErlNifUInt64) * data_size;
      gen->alloc += new_size * sizeof(ErlNifUInt64) * data_size;

      entry->next = next->next;
      // copy, free and reassign old data
      memcpy(new_data, entry->data, entry->count * sizeof(ErlNifUInt64) * data_size);
      mc_free(entry->data);
      entry->data = new_data;
      // set new size and count
      entry->size = new_size;

      // now we calculate the delta of old and new start to get the offset in the
      // new array to copy data to then free it
      dprint("2nd chunk offset: %ld\r\n", next->start - entry->start);
      dprint("copying points: %ld\r\n", next->start - entry->start);

      dprint("Filling between %u and %ld\r\n", entry->count, next->start - entry->start);
      for (int i = entry->count * data_size; i < (next->start - entry->start) * data_size; i++) {
        entry->data[i] = 0;
      }

      entry->count = new_count;

      memcpy(entry->data + (next->start - entry->start) * data_size, next->data, next->count * sizeof(ErlNifUInt64) * data_size);
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
      metric->alloc -= (entry->size * sizeof(ErlNifUInt64) * data_size);
      gen->alloc -= (entry->size * sizeof(ErlNifUInt64) * data_size);

      ErlNifUInt64 *new_data = (ErlNifUInt64 *) mc_alloc(new_size * sizeof(ErlNifUInt64) * data_size);
#ifdef TAGGED
      for (int i = 0; i < new_size * data_size; i++) {
        new_data[i] = TAG_DATA_L;
      }
#endif

      memcpy(new_data, entry->data, entry->size * sizeof(ErlNifUInt64) * data_size);
      mc_free(entry->data);
      entry->data = new_data;
      entry->size = new_size;
      metric->alloc += (entry->size * sizeof(ErlNifUInt64) * data_size);
      gen->alloc += (entry->size * sizeof(ErlNifUInt64) * data_size);
    }

    // fill gap with zeros
    for (int i = entry->count * data_size; i < internal_offset * data_size; i++) {
      entry->data[i] = 0;
    }
    
    memcpy(entry->data + internal_offset * data_size, values, count * sizeof(ErlNifUInt64) * data_size);

    entry->count = MAX(offset - entry->start + count, entry->count);

    if (!entry->next) {
      metric->tail = entry;
    }
    return;
  } while (entry);
}
