/**
 * clock.c
 *
 * @file Implementation of the CLOCK buffer replacement algorithm.
 *
 * This replacement policy keeps a pointer "ptr" to the array of pages
 * that back the buffer manager, which it treats as a circular buffer.
 * It sweeps ptr over the array of pages, marking "hot" pages as "cold",
 * and returning "cold" pages for eviction.  It increments ptr one slot
 * each time getStaleAndRemove is called.  When pages are "hit", it marks
 * them "hot", regardless of their prior state.
 *
 * This module also maintains p->pinCount in the obvious way so that it does
 * not evict pages that have been pinned by the calling thread.
 *
 * This module does not use mutexes, and instead uses atomic instructions
 * such as test and set.
 *
 * States (Stored in p->queue):
 *
 * Page pinning:  [-1, 0, 1] -> remove -> insert -> 1
 * Get stale and remove:  [1] -> [0].  [0] -> [-1] -> insert -> [1].  [-1] -> [-1]
 *
 *  Created on: Aug 22, 2011
 *      Author: sears
 */
#include <config.h>
#include <stasis/common.h>
#include <stasis/util/latches.h>
#include <stasis/replacementPolicy.h>
#include <stasis/page.h>
typedef struct {
  uint64_t ptr;
  Page * pages;
  uint64_t page_count;
} stasis_replacement_policy_clock_t;

static void  clockDeinit  (struct replacementPolicy* impl) {
  stasis_replacement_policy_clock_t * clock = impl->impl;
  free(clock);
  free(impl);
}
static void  clockHit     (struct replacementPolicy* impl, Page* page) {
  page->next = (void*)1;
}
static Page* clockGetStale(struct replacementPolicy* impl) {
  stasis_replacement_policy_clock_t * clock = impl->impl;

  // NOTE: This just exists for legacy purposes, and is fundamantally not
  // threadsafe.  So, this doesn't mess with the __sync* stuff, or worry
  // about order of operations like getStaleAndRemove does.

  for(uint64_t spin = 0; spin < 2*clock->page_count; spin++) {
    uint64_t ptr = clock->ptr % clock->page_count;
    clock->ptr++;
    if(clock->pages[ptr].next == (void*)0 &&
       clock->pages[ptr].pinCount == 0) {

      // Don't set state to -1, since that would cause trouble if the caller
      // doesn't call remove.  The -1 state is only there to protect us
      // against concurrent interleavings of getStale() and remove(), which
      // this (deprecated part of the) API does not support.

      //clock->pages[ptr].next = (void*)-1;

      return &clock->pages[ptr];
    } else if(clock->pages[ptr].next == (void*)1){
      clock->pages[ptr].next = (void*)0;
    }
  }
  return NULL;
}
static Page* clockRemove  (struct replacementPolicy* impl, Page* page) {
  int ret = __sync_fetch_and_add(&page->pinCount,1);
  if(ret == 0) {
    return page;
  } else {
    return NULL;
  }
}
static Page* clockGetStaleAndRemove  (struct replacementPolicy* impl) {
  stasis_replacement_policy_clock_t * clock = impl->impl;

  for(uint64_t spin = 0; spin < 2*clock->page_count; spin++) {
    uint64_t ptr = __sync_fetch_and_add(&clock->ptr,1) % clock->page_count;
    if(__sync_bool_compare_and_swap(&clock->pages[ptr].next, 0, -1)) {
      // evict this page, but not if it is pinned (this protects the caller
      // from evicting pages that it has pinned, not pages that were pinned
      // in race by other threads.)
      if(clock->pages[ptr].pinCount == 0) {
        clock->pages[ptr].pinCount++;
        return &clock->pages[ptr];
      } else {
        // Reset the queue flag to 0, unless someone has changed it to 0
        // or 1 in race.
        __sync_bool_compare_and_swap(&clock->pages[ptr].next,-1,0);
      }
    } else if(__sync_bool_compare_and_swap(&clock->pages[ptr].next, 1, 0)) {
      // page was hot.  now it's cold.  nothing else to see here; move along.
    }
/*
    else if(__sync_bool_compare_and_swap(&clock->pages[ptr]->queue, -1, -1)) {  // extraneous no-op
      // page was being evicted in race with us.  unlikely, but move on.
    } else {                                                                      // ditto.
      // page state changed concurrently with us.  who knows what happened, so move on.
    }
 */
  }
  return NULL;
}
static void  clockInsert  (struct replacementPolicy* impl, Page* page) {
  __sync_fetch_and_sub(&page->pinCount,1);  // don't care about ordering of this line and next.  pinCount is just a "message to ourselves"
  page->next = (void*)1;
  __sync_synchronize();
}

replacementPolicy* replacementPolicyClockInit(Page * pageArray, int page_count) {
  replacementPolicy *ret = stasis_alloc(replacementPolicy);
  stasis_replacement_policy_clock_t * clock = stasis_alloc(stasis_replacement_policy_clock_t);
  clock->pages = pageArray;
  clock->page_count = page_count;
  clock->ptr = 0;
  ret->init = NULL;
  ret->deinit = clockDeinit;
  ret->hit = clockHit;
  ret->getStale = clockGetStale;
  ret->getStaleAndRemove = clockGetStaleAndRemove;
  ret->remove = clockRemove;
  ret->insert = clockInsert;
  ret->impl = clock;
  return ret;
}

