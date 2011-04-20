#include <stasis/common.h>

#undef STLSEARCH  // XXX

#include <stasis/lhtable.h>
#include <stasis/redblack.h>
#include <stasis/replacementPolicy.h>

#include <assert.h>

typedef struct stasis_replacement_policy_lru_entry {
  void * value;
  uint64_t clock;
} stasis_replacement_policy_lru_entry;

typedef struct stasis_replacement_policy_lru_t {
  uint64_t now;
  struct LH_ENTRY(table) * hash;
  struct RB_ENTRY(tree)  * lru;
  void * (*getNode)(void * page, void * conf);
  void   (*setNode)(void * page, void * n, void * conf);
  void * conf;
} stasis_replacement_policy_lru_t;

static int stasis_replacement_policy_lru_entry_cmp(const void * ap, const void * bp, const void * ignored) {
  const stasis_replacement_policy_lru_entry * a = ap;
  const stasis_replacement_policy_lru_entry * b = bp;

  return a->clock < b->clock ? -1
    : a->clock == b->clock ? 0
    : 1;
}

static void stasis_replacement_policy_lru_deinit(replacementPolicy *r) {
  //XXX free other stuff
  stasis_replacement_policy_lru_t * l = r->impl;
  LH_ENTRY(destroy)(l->hash);
  RB_ENTRY(destroy)(l->lru);

  free(r->impl);
  free(r);
}

/** @todo handle clock wraps properly! */

static void stasis_replacement_policy_lru_hit(replacementPolicy *r, Page *p) {
  stasis_replacement_policy_lru_t * l = r->impl;
  stasis_replacement_policy_lru_entry * e = l->getNode(p, l->conf);
  assert(e);
  stasis_replacement_policy_lru_entry * old = (stasis_replacement_policy_lru_entry * ) RB_ENTRY(delete)(e, l->lru);
  assert(e == old);
  e->clock = l->now;
  l->now++;
  old = (stasis_replacement_policy_lru_entry *)RB_ENTRY(search)(e, l->lru);
  assert(e == old);
}
static Page* stasis_replacement_policy_lru_get_stale(replacementPolicy* r) {
  stasis_replacement_policy_lru_t * l = r->impl;
  stasis_replacement_policy_lru_entry * e = (stasis_replacement_policy_lru_entry * ) rbmin(l->lru);
  return  e ? e->value : 0;
}
static Page* stasis_replacement_policy_lru_remove(replacementPolicy* r, Page *p) {
  stasis_replacement_policy_lru_t * l = r->impl;
  stasis_replacement_policy_lru_entry * e = l->getNode(p, l->conf);
  assert(e);
  stasis_replacement_policy_lru_entry * old = (stasis_replacement_policy_lru_entry *) RB_ENTRY(delete)(e, l->lru);
  assert(old == e);
  void * ret = e->value;
  free(e);
  return ret;
}
static Page* stasis_replacement_policy_lru_get_stale_and_remove(replacementPolicy* r) {
  void* ret = stasis_replacement_policy_lru_get_stale(r);
  stasis_replacement_policy_lru_remove(r, ret);
  return ret;
}

static void stasis_replacement_policy_lru_insert(replacementPolicy* r, Page* p) {
  stasis_replacement_policy_lru_t * l = r->impl;
  stasis_replacement_policy_lru_entry * e = malloc(sizeof(stasis_replacement_policy_lru_entry));
  e->value = p;
  e->clock = l->now;
  l->now++;
  l->setNode(p, l->conf, e);
  stasis_replacement_policy_lru_entry * old = (stasis_replacement_policy_lru_entry *) RB_ENTRY(search)(e, l->lru);
  assert(e == old);
}

replacementPolicy * stasis_replacement_policy_lru_init() {
  replacementPolicy * ret = malloc(sizeof(replacementPolicy));
  stasis_replacement_policy_lru_t * l = malloc(sizeof(stasis_replacement_policy_lru_t));
  l->now = 0;
  l->hash = LH_ENTRY(create)(10);
  l->lru = RB_ENTRY(init)(stasis_replacement_policy_lru_entry_cmp, 0);
  ret->init = stasis_replacement_policy_lru_init;
  ret->deinit = stasis_replacement_policy_lru_deinit;
  ret->hit = stasis_replacement_policy_lru_hit;
  ret->getStale = stasis_replacement_policy_lru_get_stale;
  ret->remove = stasis_replacement_policy_lru_remove;
  ret->getStaleAndRemove = stasis_replacement_policy_lru_get_stale_and_remove;
  ret->insert = stasis_replacement_policy_lru_insert;
  ret->impl = l;
  return ret;
}
