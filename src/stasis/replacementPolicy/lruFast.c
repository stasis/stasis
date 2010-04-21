#include <stasis/common.h>
#include <stasis/replacementPolicy.h>
#include <stasis/doubleLinkedList.h>
#include <assert.h>

typedef LL_ENTRY(value_t) value_t;
typedef struct LL_ENTRY(node_t) node_t;
typedef struct LL_ENTRY(list) list;

typedef struct lruFast {
  struct LL_ENTRY(list)  * lru;
  node_t * (*getNode)(void * page, void * conf);
  void     (*setNode)(void * page, node_t * n, void * conf);
  intptr_t*(*derefCount)(void* page);
  void * conf;
} lruFast;

static void  hit(struct replacementPolicy * r, void * p) {
  lruFast * l = r->impl;
  node_t * n = l->getNode(p, l->conf);
  if(!n) { return; } // ignore attempts to hit pages not in lru
  assert(n);
  LL_ENTRY(removeNoFree)(l->lru, n);
  LL_ENTRY(pushNode)(l->lru, n);
}
static void* getStale(struct replacementPolicy * r) {
  lruFast * l = r->impl;
  return LL_ENTRY(head)(l->lru);
}
static void* remove(struct replacementPolicy* r, void * p) {
  lruFast * l = r->impl;
  void *ret = NULL;

  if(!*l->derefCount(p)) {
    node_t * n = l->getNode(p, l->conf);
    assert(n);
    value_t * v = n->v;
    LL_ENTRY(remove)(l->lru, n);
    l->setNode(p, 0, l->conf);
    ret = v;
  }
  (*l->derefCount(p))++;
  return ret;
}
static void* getStaleAndRemove(struct replacementPolicy* r) {
  lruFast * l = r->impl;
  void * ret = LL_ENTRY(shift)(l->lru);
  if(ret) {
    l->setNode(ret, 0, l->conf);
    assert(!(*l->derefCount(ret)));
    (*l->derefCount(ret))++;
  }
  return ret;
}
static void  insert(struct replacementPolicy* r, void * p) {
  lruFast * l = r->impl;
  (*l->derefCount(p))--;
  assert(*l->derefCount(p) >= 0);
  if(!*l->derefCount(p)) {
    assert(0 == l->getNode(p, l->conf));
    node_t * n = LL_ENTRY(push)(l->lru, p);
    l->setNode(p, n, l->conf);
  }
}
static void deinit(struct replacementPolicy * r) {
  lruFast * l = r->impl;
  // the node_t's get freed by LL_ENTRY.  It's the caller's
  // responsibility to free the void *'s passed into us.
  LL_ENTRY(destroy)(l->lru);
  free(l);
  free(r);
}
replacementPolicy * lruFastInit(
   struct LL_ENTRY(node_t) * (*getNode)(void * page, void * conf),
   void (*setNode)(void * page,
                   struct LL_ENTRY(node_t) * n,
                   void * conf),
   intptr_t* (*derefCount)(void *page),
   void * conf) {
  struct replacementPolicy * ret = malloc(sizeof(struct replacementPolicy));
  ret->deinit = deinit;
  ret->hit = hit;
  ret->getStale = getStale;
  ret->remove = remove;
  ret->getStaleAndRemove = getStaleAndRemove;
  ret->insert = insert;
  lruFast * l = malloc(sizeof(lruFast));
  l->lru = LL_ENTRY(create)();
  l->getNode = getNode;
  l->setNode = setNode;
  l->derefCount = derefCount;
  ret->impl = l;
  return ret;
}
