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
  if(ENOENT == LL_ENTRY(remove)(l->lru, p)) {
    // ignore attempts to hit pages not in lru
    return;
  }
  LL_ENTRY(push)(l->lru, p);
}
static void* getStale(struct replacementPolicy * r) {
  lruFast * l = r->impl;
  return LL_ENTRY(head)(l->lru);
}
static void* remove(struct replacementPolicy* r, void * p) {
  lruFast * l = r->impl;
  void *ret = NULL;

  if(!*l->derefCount(p)) {
    int err = LL_ENTRY(remove)(l->lru, p);
    assert(!err);
    ret = p;
  }
  (*l->derefCount(p))++;
  return ret;
}
static void* getStaleAndRemove(struct replacementPolicy* r) {
  lruFast * l = r->impl;
  void * ret = LL_ENTRY(shift)(l->lru);
  if(ret) {
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
    int err = LL_ENTRY(push)(l->lru, p);
    assert(!err);
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
  l->lru = LL_ENTRY(create)(getNode, setNode, conf);
  l->getNode = getNode;
  l->setNode = setNode;
  l->derefCount = derefCount;
  ret->impl = l;
  return ret;
}
