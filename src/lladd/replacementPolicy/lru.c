#include <config.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <lladd/pageCache.h>
#include <lladd/lhtable.h>
#include <lladd/redblack.h>
#include <lladd/replacementPolicy.h>
#include <pthread.h>

typedef struct entry { 
  void * value;
  uint64_t clock;
} entry;

pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;

typedef struct lru { 
  uint64_t now;
  struct LH_ENTRY(table) * hash;
  struct RB_ENTRY(tree)  * lru;
} lru;

int cmp(const void * ap, const void * bp, const void * ignored) {
  const entry * a = ap;
  const entry * b = bp;

  return a->clock < b->clock ? -1 
    : a->clock == b->clock ? 0 
    : 1;
}

static void lruDeinit(replacementPolicy* r) { 
  //XXX free other stuff
  lru * l = r->impl;
  LH_ENTRY(destroy)(l->hash);
  RB_ENTRY(destroy)(l->lru);

  free(r->impl);
  free(r);
}

/** @todo handle clock wraps properly! */

static void lruHit(replacementPolicy* r, int id) { 
  pthread_mutex_lock(&mut);
  lru * l = r->impl;
  entry * e = LH_ENTRY(find)(l->hash, &id, sizeof(int));
  assert(e);
  entry * old = (entry * ) RB_ENTRY(delete)(e, l->lru);
  assert(e == old);
  e->clock = l->now;
  l->now++; 
  old = (entry *)RB_ENTRY(search)(e, l->lru);
  assert(e == old);
  pthread_mutex_unlock(&mut);
}
static void * lruGetStale(replacementPolicy* r) { 
  pthread_mutex_lock(&mut);
  lru * l = r->impl;
  entry * e = (entry * ) RB_ENTRY(min)(l->lru);
  pthread_mutex_unlock(&mut);
  return  e ? e->value : 0;
}
static void* lruRemove(replacementPolicy* r, int id) { 
  pthread_mutex_lock(&mut);
  lru * l = r->impl;
  entry * e = LH_ENTRY(remove)(l->hash, &id, sizeof(int));
  assert(e);
  entry * old = (entry *) RB_ENTRY(delete)(e, l->lru);
  assert(old == e);
  void * ret = e->value;
  free(e);
  pthread_mutex_unlock(&mut);
  return ret;
}
static void lruInsert(replacementPolicy* r, int id, void * p) { 
  pthread_mutex_lock(&mut);
  lru * l = r->impl;
  entry * e = LH_ENTRY(find)(l->hash, &id, sizeof(int));
  assert(!e);
  e = malloc(sizeof(entry));
  e->value = p;
  e->clock = l->now;
  l->now++;
  LH_ENTRY(insert)(l->hash, &id, sizeof(int), e);
  entry * old = (entry *) RB_ENTRY(search)(e, l->lru);
  assert(e == old);
  pthread_mutex_unlock(&mut);

}

replacementPolicy * lruInit() {
  replacementPolicy * ret = malloc(sizeof(replacementPolicy));
  lru * l = malloc(sizeof(lru));
  l->now = 0;
  l->hash = LH_ENTRY(create)(10);
  //  l->lru = RB_ENTRY(init)((int(*)(const void*,const void*,const void*))cmp, 0);
  l->lru = RB_ENTRY(init)(cmp, 0);
  ret->init = lruInit;
  ret->deinit = lruDeinit;
  ret->hit = lruHit;
  ret->getStale = lruGetStale;
  ret->remove = lruRemove;
  ret->insert = lruInsert;
  ret->impl = l;
  return ret;
}

