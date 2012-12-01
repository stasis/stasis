/*
 * logStructured.c
 *
 *  Created on: Jun 28, 2009
 *      Author: sears
 */
#include<stasis/transactional.h>
#include<stasis/util/lhtable.h>
#include<stasis/experimental/group.h>
#include<string.h>
typedef struct {
  struct LH_ENTRY(table) * table;
  recordid runs;
  size_t runlen;
  size_t curlen;
} stasis_log_structured_group_t;

typedef struct {
  byte ** val;
  size_t * vallen;
  size_t count;
} stasis_log_structured_group_entry_t;

typedef struct {
  struct LH_ENTRY(list) entries;
  const struct LH_ENTRY(pair_t)* entry;
  size_t offset;
} stasis_log_structured_group_iterator;

static int stasis_log_structured_group_put(struct stasis_group_t* impl,
                                                                    byte* key, size_t keylen, byte* val, size_t vallen) {
  stasis_log_structured_group_t * g = impl->impl;
  byte * k = malloc(keylen);
  memcpy(k, key, keylen);
  byte * v = malloc(vallen);
  memcpy(v, val, vallen);

  g->curlen += (keylen + vallen);
  if(g->curlen < g->runlen) {
    stasis_log_structured_group_entry_t * entry;
    if((entry = LH_ENTRY(find)(g->table, k,keylen))) {
      entry->count++;
      entry->val = stasis_realloc(entry->val, entry->count, byte*);
      entry->vallen = stasis_realloc(entry->vallen, entry->count, size_t);
    } else {
      entry = malloc(sizeof(*entry));
      entry->val = malloc(sizeof(entry->val[0]));
      entry->vallen = malloc(sizeof(entry->vallen[0]));
      LH_ENTRY(insert)(g->table, k, keylen, entry);
      entry->count = 1;
    }
    entry->vallen[entry->count-1] = vallen;
    entry->val[entry->count-1] = v;
  } else {
    abort();
  }
  return 0;
}

static lladdIterator_t* stasis_log_structured_group_done(stasis_group_t* impl) {
  stasis_log_structured_group_t * g = impl->impl;
  stasis_log_structured_group_iterator * it = malloc(sizeof(*it));
  LH_ENTRY(openlist)(g->table, &it->entries);
  it->entry = 0;
  it->offset = -1;
  lladdIterator_t * ret = malloc(sizeof(*it));
  ret->type = STASIS_LOG_STRUCTURED_GROUP_ITERATOR;
  ret->impl = it;
  return ret;
}


stasis_group_t * TlogStructuredGroup(int xid, size_t runlen) {
  stasis_group_t * ret = malloc(sizeof(*ret));
  ret->put = stasis_log_structured_group_put;
  ret->done = stasis_log_structured_group_done;
  stasis_log_structured_group_t * g;
  g = malloc(sizeof(*g));
  g->curlen = 0;
  g->runlen = runlen;
  g->runs = NULLRID;
  g->table = LH_ENTRY(create)(100);
  ret->impl = g;
  return ret;
}
static void stasis_log_structured_group_it_close(int xid, void* impl) {
  stasis_log_structured_group_iterator * it = impl;
  LH_ENTRY(closelist)(&it->entries);
  free(it);
}
static int stasis_log_structured_group_it_next(int xid, void* impl) {
  stasis_log_structured_group_iterator * it = impl;
  stasis_log_structured_group_entry_t* entry = it->entry ? it->entry->value : 0;
  if((!entry) || it->offset ==  (entry->count-1)) {
    it->entry = lhreadlist(&it->entries);
    it->offset =0 ;
  } else {
    it->offset++;
  }
  return 0 != it->entry;
}
static int stasis_log_structured_group_it_key(int xid, void* impl, byte** key) {
  stasis_log_structured_group_iterator * it = impl;
  *key = (byte*) it->entry->key; // TODO cast strips const
  return it->entry->keyLength;
}
static int stasis_log_structured_group_it_value(int xid, void* impl, byte** val) {
  stasis_log_structured_group_iterator * it = impl;
  stasis_log_structured_group_entry_t* entry = it->entry->value;
  *val = entry->val[it->offset];
  return entry->vallen[it->offset];
}
static void stasis_log_structured_group_it_tupleDone(int xid, void* impl) {}

void stasis_log_structured_group_init() {
  static lladdIterator_def_t def = {
    stasis_log_structured_group_it_close,
    stasis_log_structured_group_it_next,
    stasis_log_structured_group_it_next,
    stasis_log_structured_group_it_key,
    stasis_log_structured_group_it_value,
    stasis_log_structured_group_it_tupleDone
  };
  lladdIterator_register(STASIS_LOG_STRUCTURED_GROUP_ITERATOR, def);
}
