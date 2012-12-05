#include <stasis/common.h>
#include <stasis/flags.h>
#include <stasis/replacementPolicy.h>
#include <assert.h>
#include <stasis/page.h>

typedef Page List;

typedef struct lruFast {
  List list;
} lruFast;
static inline void llInit( List *list ) {
   bzero( list, sizeof( *list ) );
   list->id = 0xdeadbeef;
   list->next = list->prev = list;
}
static inline void llPush( List *list, Page *p ) {
  p->next = list;
  p->prev = list->prev;
  p->next->prev = p;
  p->prev->next = p;
}
static inline void llRemoveNoReinit( Page *p ) {
  p->prev->next = p->next;
  p->next->prev = p->prev;
}
static inline void llRemove( Page *p ) {
  llRemoveNoReinit( p );
  p->prev = NULL;
  p->next = NULL;
}
static inline int llIsEmpty( List *list ) {
  return list->next == list;
}
static inline Page* llHead( List *list ) {
  return ( llIsEmpty( list ) ) ? NULL : list->next;
}
static inline Page* llShift( List *list ) {
  if(llIsEmpty(list)) return NULL;

  Page * ret = list->next;
  llRemove(ret);

  return ret;
}
static void  stasis_lru_fast_hit(struct replacementPolicy * r, Page *p) {
  lruFast *l = r->impl;
  if( p->prev == NULL ) {
    // ignore attempts to hit pages not in lru
    return;
  }
  llRemoveNoReinit(p);
  llPush(&l->list, p);
}
static Page* stasis_lru_fast_getStale(struct replacementPolicy *r) {
  lruFast *l = r->impl;
  return llHead(&l->list);
}
static Page* stasis_lru_fast_remove(struct replacementPolicy* r, Page *p) {
  Page *ret = NULL;

  if(!p->pinCount) {
    if(p->next) {
      llRemove(p);
    } else {
      assert(p->dirty);
    }
    ret = p;
  }
  p->pinCount++;

  return ret;
}
static Page* stasis_lru_fast_getStaleAndRemove(struct replacementPolicy *r) {
  lruFast * l = r->impl;
  Page *ret = llShift(&l->list);
  if(ret) {
    assert(!ret->pinCount);
    ret->pinCount++;
  }
  return ret;
}
static void  stasis_lru_fast_insert(struct replacementPolicy *r, Page *p) {
  lruFast * l = r->impl;
  p->pinCount--;
  assert(p->pinCount >= 0);
  if(stasis_buffer_manager_hint_writes_are_sequential &&
     !stasis_buffer_manager_debug_stress_latching) {
    // We are in sequential mode, and only want to evict pages from
    // the writeback thread.  Therefore, it would be a waste of time
    // to put this dirty page in the LRU.  (Also, we know that, when
    // the page is evicted, it will be taken out of LRU, and put back in.

    // If we're *trying* to stress the buffer manager latches, etc, then
    // insert the dirty page.  This will cause the buffer manager to perform
    // all sorts of useless (and otherwise rare) latching operations.

    if(!p->pinCount && !p->dirty) {
      llPush(&l->list, p);
    }
  } else {
    if(!p->pinCount) {
      llPush(&l->list, p);
    }
  }
}
static void stasis_lru_fast_deinit(struct replacementPolicy * r) {
  lruFast * l = r->impl;
  free(l);
  free(r);
}
replacementPolicy * lruFastInit(void) {
  struct replacementPolicy * ret = stasis_alloc(struct replacementPolicy);
  ret->deinit = stasis_lru_fast_deinit;
  ret->hit = stasis_lru_fast_hit;
  ret->getStale = stasis_lru_fast_getStale;
  ret->remove = stasis_lru_fast_remove;
  ret->getStaleAndRemove = stasis_lru_fast_getStaleAndRemove;
  ret->insert = stasis_lru_fast_insert;
  lruFast * l = stasis_alloc(lruFast);
  llInit(&l->list);
  ret->impl = l;
  return ret;
}
