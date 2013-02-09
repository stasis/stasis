/*
 * dirtyPageTable.c
 *
 *  Created on: May 18, 2009
 *      Author: sears
 */
#include <config.h>
#include <stasis/common.h>
#include <stasis/util/redblack.h>
#include <stasis/util/multiset.h>
#include <stasis/util/latches.h>
#include <stasis/util/time.h>
#include <stasis/flags.h>
#include <stasis/dirtyPageTable.h>
#include <stasis/page.h>
#include <stdio.h>

typedef struct {
  pageid_t p;
  lsn_t lsn;
} dpt_entry;


static int dpt_cmp_page(const void *ap, const void * bp, const void * ignored) {
  const dpt_entry * a = (const dpt_entry *)ap;
  const dpt_entry * b = (const dpt_entry *)bp;
  return (a->p < b->p) ? -1 : ((a->p == b->p) ? 0 : 1);
}
static int dpt_cmp_lsn_and_page(const void *ap, const void * bp, const void * ignored) {
  const dpt_entry * a = (const dpt_entry *)ap;
  const dpt_entry * b = (const dpt_entry *)bp;

  return (a->lsn < b->lsn) ? -1 : ((a->lsn == b->lsn) ? dpt_cmp_page(ap, bp, 0) : 1);
}

struct stasis_dirty_page_table_t {
  struct rbtree * tableByPage;
  struct rbtree * tableByLsnAndPage;
  stasis_buffer_manager_t * bufferManager;
  uint32_t count; // NOTE: this is 32 bit so that it is cheap to atomically manipulate it on 32 bit intels.
  pthread_mutex_t mutex;
  pthread_cond_t flushDone;
  int flushing;
  stasis_util_multiset_t * outstanding_flush_lsns;
  pthread_cond_t writebackCond;
};

void stasis_dirty_page_table_set_dirty(stasis_dirty_page_table_t * dirtyPages, Page * p) {
  if(!p->dirty) {
    while(stasis_dirty_page_table_dirty_count(dirtyPages)
          > stasis_dirty_page_count_hard_limit) {
      struct timespec ts = stasis_double_to_timespec(0.01);
      nanosleep(&ts,0);
    }
    pthread_mutex_lock(&dirtyPages->mutex);
    if(!p->dirty) {
      p->dirty = 1;
      dpt_entry * e = stasis_alloc(dpt_entry);
      e->p = p->id;
      e->lsn = p->LSN;
      const void * ret = rbsearch(e, dirtyPages->tableByPage);
      assert(ret == e); // otherwise, the entry was already in the table.

      e = stasis_alloc(dpt_entry);
      e->p = p->id;
      e->lsn = p->LSN;
      ret = rbsearch(e, dirtyPages->tableByLsnAndPage);
      assert(ret == e); // otherwise, the entry was already in the table.
      FETCH_AND_ADD(&dirtyPages->count,1);
    }
    pthread_mutex_unlock(&dirtyPages->mutex);
#ifdef SANITY_CHECKS
  } else {
    pthread_mutex_lock(&dirtyPages->mutex);
    dpt_entry e = { p->id, 0};
    assert(rbfind(&e, dirtyPages->tableByPage));
    pthread_mutex_unlock(&dirtyPages->mutex);
#endif //SANITY_CHECKS
  }
}

void stasis_dirty_page_table_set_clean(stasis_dirty_page_table_t * dirtyPages, Page * p) {
  if(p->dirty) {
    pthread_mutex_lock(&dirtyPages->mutex);
    if(p->dirty) {
      dpt_entry dummy = {p->id, 0};

      const dpt_entry * e = (const dpt_entry *)rbdelete(&dummy, dirtyPages->tableByPage);
      assert(e);
      assert(e->p == p->id);
      dummy.lsn = e->lsn;
      free((void*)e);

      e = (const dpt_entry *)rbdelete(&dummy, dirtyPages->tableByLsnAndPage);
      assert(e);
      assert(e->p == p->id);
      assert(e->lsn == dummy.lsn);
      free((void*)e);
      assert(p->dirty);
      p->dirty = 0;

      lsn_t min_waiting = stasis_util_multiset_min(dirtyPages->outstanding_flush_lsns);
      e = (const dpt_entry *)rbmin(dirtyPages->tableByLsnAndPage);
      if(dummy.lsn >= min_waiting &&
         (!e || e->lsn >= min_waiting)) {
        pthread_cond_broadcast( &dirtyPages->writebackCond );
      }

      //dirtyPages->count--;
      FETCH_AND_ADD(&dirtyPages->count, -1);
    }
    pthread_mutex_unlock(&dirtyPages->mutex);
  }
}

int stasis_dirty_page_table_is_dirty(stasis_dirty_page_table_t * dirtyPages, Page * p) {
  int ret;

  ret = p->dirty;
#ifdef SANITY_CHECKS
  pthread_mutex_lock(&dirtyPages->mutex);
  dpt_entry e = { p->id, 0};
  const void* found = rbfind(&e, dirtyPages->tableByPage);
  assert((found && ret) || !(found||ret));
  pthread_mutex_unlock(&dirtyPages->mutex);
#endif
  return ret;
}

lsn_t stasis_dirty_page_table_minRecLSN(stasis_dirty_page_table_t * dirtyPages) {
  pthread_mutex_lock(&dirtyPages->mutex);
  const dpt_entry * e = (const dpt_entry *)rbmin(dirtyPages->tableByLsnAndPage);
  lsn_t lsn =  e ? e->lsn : LSN_T_MAX;
  pthread_mutex_unlock(&dirtyPages->mutex);
  return lsn;
}

pageid_t stasis_dirty_page_table_dirty_count(stasis_dirty_page_table_t * dirtyPages) {
  return ATOMIC_READ_32(&dirtyPages->mutex, &dirtyPages->count);
}

int stasis_dirty_page_table_flush_with_target(stasis_dirty_page_table_t * dirtyPages, lsn_t targetLsn) {
  DEBUG("stasis_dirty_page_table_flush_with_target called");
  const long stride = stasis_dirty_page_table_flush_quantum;
  int all_flushed;
  pthread_mutex_lock(&dirtyPages->mutex);
  if (targetLsn == LSN_T_MAX) {
    if(dirtyPages->flushing) {
      pthread_cond_wait(&dirtyPages->flushDone, &dirtyPages->mutex);
      pthread_mutex_unlock(&dirtyPages->mutex);
      // We return EAGAIN here because the other flush may have begun
      // before some page that this flush is interested in was
      // written.
      return EAGAIN;
    }
    dirtyPages->flushing = 1;
  }

  // Normally, we will be called by a background thread that wants to maximize
  // write back throughput, and sets targetLsn to LSN_T_MAX.

  // Sometimes, we are called by log truncation, which wants to prioritize writeback
  // of pages that are blocking log truncation.

  // If we are writing back for the buffer manager, sort writebacks by page number.
  // Otherwise, sort them by the LSN that first dirtied the page.
  // TODO: Re-sort LSN ordered pages before passing them to the OS?
  struct rbtree * tree = targetLsn == LSN_T_MAX ? dirtyPages->tableByPage
                                                : dirtyPages->tableByLsnAndPage;

  long buffered = 0;
  do {
    dpt_entry dummy = { 0, 0 };
    pageid_t vals[stride];
    int off = 0;
    int strides = 0;
    all_flushed = 1;
    for(const dpt_entry * e = (const dpt_entry *)rblookup(RB_LUGTEQ, &dummy, tree);
        e && e->lsn < targetLsn;
        e = (const dpt_entry *)rblookup(RB_LUGREAT, &dummy, tree)) {
      dummy = *e;
      vals[off] = dummy.p;
      off++;
      if(off == stride) {
        pthread_mutex_unlock(&dirtyPages->mutex);
        for(pageid_t i = 0; i < off; i++) {
          if (dirtyPages->bufferManager->tryToWriteBackPage(dirtyPages->bufferManager, vals[i]) == EBUSY) {
            all_flushed = 0;
          } else {
            buffered++;
          }
          if(buffered == stride) {
            DEBUG("Forcing %lld pages A\n", buffered);
            buffered = 0;
            dirtyPages->bufferManager->asyncForcePages(dirtyPages->bufferManager, 0);
          }
        }
        off = 0;
        strides++;
        pthread_mutex_lock(&dirtyPages->mutex);
      }
    }
    pthread_mutex_unlock(&dirtyPages->mutex);
    for(pageid_t i = 0; i < off; i++) {
      if (dirtyPages->bufferManager->tryToWriteBackPage(dirtyPages->bufferManager, vals[i]) == EBUSY) {
        all_flushed = 0;
      } else {
        buffered++;
      }
      DEBUG("Forcing %lld pages B\n", buffered);
      buffered = 0;
    }
    dirtyPages->bufferManager->asyncForcePages(dirtyPages->bufferManager, 0);
    pthread_mutex_lock(&dirtyPages->mutex);
    dpt_entry * e = ((dpt_entry*)rbmin(tree));

    DEBUG("Finished elevator sweep.\n");

    if (!all_flushed &&
        targetLsn < LSN_T_MAX &&
        ATOMIC_READ_32(0, &dirtyPages->count) > 0 &&
        e && targetLsn > e->lsn ) {
      struct timespec ts;
      struct timeval tv;

      all_flushed = 1;

      int res = gettimeofday(&tv, 0);
      assert(res == 0);

      printf("Warning; going into slow fallback path in dirtyPageTable\n");

      // We expect previously pinned pages to be unpinned and flushed within
      // 100 milliseconds. If there aren't then  we had race condition and the
      // pinning thread sampled p->needFlush before we set it to 1. This
      // should be very rare.

      tv.tv_usec += 100000;
      if (tv.tv_usec >= 1000000 ) {
        ++tv.tv_sec;
        tv.tv_usec -= 1000000;
      }

      ts.tv_sec = tv.tv_sec;
      ts.tv_nsec = 1000*tv.tv_usec;

      dpt_entry * e = ((dpt_entry*)rbmin(tree));

      if(targetLsn != LSN_T_MAX) { stasis_util_multiset_insert(dirtyPages->outstanding_flush_lsns, targetLsn); }

      while( e && targetLsn > e->lsn ) {
        if (pthread_cond_timedwait(&dirtyPages->writebackCond, &dirtyPages->mutex, &ts) == ETIMEDOUT) {
          all_flushed = 0;
          break;
        }
        e = ((dpt_entry*)rbmin(tree));
      }

      if(targetLsn != LSN_T_MAX) {
        int found = stasis_util_multiset_remove(dirtyPages->outstanding_flush_lsns, targetLsn);
        assert(found);
      }

    }

  } while(targetLsn != LSN_T_MAX && !all_flushed);
  if (targetLsn == LSN_T_MAX) {
    pthread_cond_broadcast(&dirtyPages->flushDone);
    dirtyPages->flushing = 0;
  }

  pthread_mutex_unlock(&dirtyPages->mutex);

  return 0;
}

int stasis_dirty_page_table_flush(stasis_dirty_page_table_t * dirtyPages) {
    DEBUG("stasis_dirty_page_table_flush called");
    return stasis_dirty_page_table_flush_with_target(dirtyPages, LSN_T_MAX);
}

int stasis_dirty_page_table_get_flush_candidates(stasis_dirty_page_table_t * dirtyPages, pageid_t start, pageid_t stop, int count, pageid_t* range_starts, pageid_t* range_ends) {
  pthread_mutex_lock(&dirtyPages->mutex);
  int n = 0;
  int b = -1;
  dpt_entry dummy;
  dummy.lsn = -1;
  dummy.p = start;

  for(const dpt_entry *e = (const dpt_entry *)rblookup(RB_LUGTEQ, &dummy, dirtyPages->tableByPage);
      e && (stop == 0 || e->p < stop) && n < ATOMIC_READ_32(0, &count);
      e = (const dpt_entry *)rblookup(RB_LUGREAT, e, dirtyPages->tableByPage)) {
    if(n == 0 || range_ends[b] != e->p) {
      b++;
      range_starts[b] = e->p;
      range_ends[b] = e->p+1;
    } else {
      range_ends[b]++;
    }
    n++;
  }
  pthread_mutex_unlock(&dirtyPages->mutex);
  return b+1;
}
void stasis_dirty_page_table_flush_range(stasis_dirty_page_table_t * dirtyPages, pageid_t start, pageid_t stop) {

  pthread_mutex_lock(&dirtyPages->mutex);
  int waitCount = 0;
  while(dirtyPages->flushing) {
    pthread_cond_wait(&dirtyPages->flushDone, &dirtyPages->mutex);
    waitCount++;
    if(waitCount == 2) {
      // a call to stasis_dirty_page_table_flush was initiated and completed since we were called.
      pthread_mutex_unlock(&dirtyPages->mutex);
      return;
    } // else, a call to flush returned, but that call could have been initiated before we were called...
  }

  pageid_t * staleDirtyPages = 0;
  pageid_t n = 0;
  dpt_entry dummy = { start, 0 };
  for(const dpt_entry * e = (const dpt_entry *)rblookup(RB_LUGTEQ, &dummy, dirtyPages->tableByPage);
         e && (stop == 0 || e->p < stop);
         e = (const dpt_entry *)rblookup(RB_LUGREAT, e, dirtyPages->tableByPage)) {
    n++;
    staleDirtyPages = stasis_realloc(staleDirtyPages, n, pageid_t);
    staleDirtyPages[n-1] = e->p;
  }
  pthread_mutex_unlock(&dirtyPages->mutex);

  for(pageid_t i = 0; i < n; i++) {
    if(stop) {
      int err = dirtyPages->bufferManager->writeBackPage(dirtyPages->bufferManager, staleDirtyPages[i]);
      if(err == EBUSY) { abort(); /*api violation!*/ }
    } else {
      dirtyPages->bufferManager->tryToWriteBackPage(dirtyPages->bufferManager, staleDirtyPages[i]);
    }
  }
  free(staleDirtyPages);
}

void stasis_dirty_page_table_set_buffer_manager(stasis_dirty_page_table_t * dpt, stasis_buffer_manager_t *bufferManager) {
  dpt->bufferManager = bufferManager;
}

stasis_dirty_page_table_t * stasis_dirty_page_table_init(void) {
  stasis_dirty_page_table_t * ret = stasis_alloc(stasis_dirty_page_table_t);
  ret->outstanding_flush_lsns = stasis_util_multiset_create();

  ret->tableByPage = rbinit(dpt_cmp_page, 0);
  ret->tableByLsnAndPage = rbinit(dpt_cmp_lsn_and_page, 0);
  ret->count = 0;
  pthread_mutex_init(&ret->mutex, 0);
  pthread_cond_init(&ret->flushDone, 0);
  ret->flushing = 0;
  pthread_cond_init(&ret->writebackCond, 0);
  return ret;
}

void stasis_dirty_page_table_deinit(stasis_dirty_page_table_t * dirtyPages) {
  int areDirty = 0;
  dpt_entry dummy = {0, 0};
  for(const dpt_entry * e = (const dpt_entry *)rblookup(RB_LUGTEQ, &dummy, dirtyPages->tableByPage);
         e;
         e = (const dpt_entry *)rblookup(RB_LUGREAT, &dummy, dirtyPages->tableByPage)) {

    if((!areDirty) &&
       (!stasis_suppress_unclean_shutdown_warnings)) {
      printf("Warning:  dirtyPagesDeinit detected dirty, unwritten pages.  "
         "Updates lost?\n");
      areDirty = 1;
    }
    dummy = *e;
    rbdelete(e, dirtyPages->tableByPage);
    free((void*)e);
  }

  dpt_entry dummy2 = {0, 0};
  for(const dpt_entry * e = (const dpt_entry *)rblookup(RB_LUGTEQ, &dummy2, dirtyPages->tableByLsnAndPage);
         e;
         e = (const dpt_entry *)rblookup(RB_LUGREAT, &dummy2, dirtyPages->tableByLsnAndPage)) {
    dummy2 = *e;
    rbdelete(e, dirtyPages->tableByLsnAndPage);
    free((void*)e);
  }

  rbdestroy(dirtyPages->tableByPage);
  rbdestroy(dirtyPages->tableByLsnAndPage);
  pthread_mutex_destroy(&dirtyPages->mutex);
  stasis_util_multiset_destroy(dirtyPages->outstanding_flush_lsns);
  pthread_cond_destroy(&dirtyPages->flushDone);
  pthread_cond_destroy(&dirtyPages->writebackCond);
  free(dirtyPages);
}
