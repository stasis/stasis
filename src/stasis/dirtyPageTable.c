/*
 * dirtyPageTable.c
 *
 *  Created on: May 18, 2009
 *      Author: sears
 */

#include <stasis/common.h>
#include <stasis/redblack.h>
#include <stasis/flags.h>
#include <stasis/dirtyPageTable.h>
#include <stasis/page.h>

#include <stdio.h>

typedef struct {
  pageid_t p;
  lsn_t lsn;
} dpt_entry;


static int dpt_cmp_page(const void *ap, const void * bp, const void * ignored) {
  const dpt_entry * a = ap;
  const dpt_entry * b = bp;
  return (a->p < b->p) ? -1 : ((a->p == b->p) ? 0 : 1);
}
static int dpt_cmp_lsn_and_page(const void *ap, const void * bp, const void * ignored) {
  const dpt_entry * a = ap;
  const dpt_entry * b = bp;

  return (a->lsn < b->lsn) ? -1 : ((a->lsn == b->lsn) ? dpt_cmp_page(ap, bp, 0) : 1);
}

struct stasis_dirty_page_table_t {
  struct rbtree * tableByPage;
  struct rbtree * tableByLsnAndPage;
  stasis_buffer_manager_t * bufferManager;
  pageid_t count;
  pthread_mutex_t mutex;
  pthread_cond_t flushDone;
  int flushing;

  pthread_cond_t writebackCond;
};

void stasis_dirty_page_table_set_dirty(stasis_dirty_page_table_t * dirtyPages, Page * p) {
  if(!p->dirty) {
    pthread_mutex_lock(&dirtyPages->mutex);
    if(!p->dirty) {
      p->dirty = 1;
      dpt_entry * e = malloc(sizeof(*e));
      e->p = p->id;
      e->lsn = p->LSN;
      const void * ret = rbsearch(e, dirtyPages->tableByPage);
      assert(ret == e); // otherwise, the entry was already in the table.

      e = malloc(sizeof(*e));
      e->p = p->id;
      e->lsn = p->LSN;
      ret = rbsearch(e, dirtyPages->tableByLsnAndPage);
      assert(ret == e); // otherwise, the entry was already in the table.
      dirtyPages->count++;
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

      const dpt_entry * e = rbdelete(&dummy, dirtyPages->tableByPage);
      assert(e);
      assert(e->p == p->id);
      dummy.lsn = e->lsn;
      free((void*)e);

      e = rbdelete(&dummy, dirtyPages->tableByLsnAndPage);
      assert(e);
      assert(e->p == p->id);
      assert(e->lsn == dummy.lsn);

      assert(p->dirty);
      p->dirty = 0;

      pthread_cond_broadcast( &dirtyPages->writebackCond );

      dirtyPages->count--;
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
  const dpt_entry * e = rbmin(dirtyPages->tableByLsnAndPage);
  lsn_t lsn =  e ? e->lsn : LSN_T_MAX;
  pthread_mutex_unlock(&dirtyPages->mutex);
  return lsn;
}

pageid_t stasis_dirty_page_table_dirty_count(stasis_dirty_page_table_t * dirtyPages) {
  pthread_mutex_lock(&dirtyPages->mutex);
  pageid_t ret = dirtyPages->count;
  assert(dirtyPages->count >= 0);
  pthread_mutex_unlock(&dirtyPages->mutex);
  return ret;
}

int stasis_dirty_page_table_flush_with_target(stasis_dirty_page_table_t * dirtyPages, lsn_t targetLsn ) {
  const int stride = 200;
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

  do {
    dpt_entry dummy = { 0, 0 };
    pageid_t vals[stride];
    int off = 0;
    int strides = 0;
    all_flushed = 1;
    for(const dpt_entry * e = rblookup(RB_LUGTEQ, &dummy, dirtyPages->tableByLsnAndPage) ;
        e && e->lsn < targetLsn;
        e = rblookup(RB_LUGREAT, &dummy, dirtyPages->tableByLsnAndPage)) {
      dummy = *e;
      vals[off] = dummy.p;
      off++;
      if(off == stride) {
        pthread_mutex_unlock(&dirtyPages->mutex);
        for(pageid_t i = 0; i < off; i++) {
          if (dirtyPages->bufferManager->tryToWriteBackPage(dirtyPages->bufferManager, vals[i]) == EBUSY) {
              all_flushed = 0;
          }
        }
        off = 0;
        strides++;
        pthread_mutex_lock(&dirtyPages->mutex);
      }
    }
    pthread_mutex_unlock(&dirtyPages->mutex);
    for(int i = 0; i < off; i++) {
      if (dirtyPages->bufferManager->tryToWriteBackPage(dirtyPages->bufferManager, vals[i]) == EBUSY) {
        all_flushed = 0;
      };
    }
    pthread_mutex_lock(&dirtyPages->mutex);

    dpt_entry * e = ((dpt_entry*)rbmin(dirtyPages->tableByLsnAndPage));

    if (!all_flushed &&
        targetLsn < LSN_T_MAX &&
        dirtyPages->count > 0 &&
        e && targetLsn > e->lsn ) {
      struct timespec ts;
      struct timeval tv;

      all_flushed = 1;

      int res = gettimeofday(&tv, 0);
      assert(res == 0);

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

      dpt_entry * e = ((dpt_entry*)rbmin(dirtyPages->tableByLsnAndPage));

      while( e && targetLsn > e->lsn ) {
        if (pthread_cond_timedwait(&dirtyPages->writebackCond, &dirtyPages->mutex, &ts) == ETIMEDOUT) {
          all_flushed = 0;
          break;
        }
        e = ((dpt_entry*)rbmin(dirtyPages->tableByLsnAndPage));
      }
    }
  } while(!all_flushed);
  if (targetLsn == LSN_T_MAX) {
    pthread_cond_broadcast(&dirtyPages->flushDone);
    dirtyPages->flushing = 0;
  }

  pthread_mutex_unlock(&dirtyPages->mutex);

  return 0;
}

int stasis_dirty_page_table_flush(stasis_dirty_page_table_t * dirtyPages) {
    return stasis_dirty_page_table_flush_with_target(dirtyPages, LSN_T_MAX);
}

int stasis_dirty_page_table_get_flush_candidates(stasis_dirty_page_table_t * dirtyPages, pageid_t start, pageid_t stop, int count, pageid_t* range_starts, pageid_t* range_ends) {
  pthread_mutex_lock(&dirtyPages->mutex);
  int n = 0;
  int b = -1;
  dpt_entry dummy;
  dummy.lsn = -1;
  dummy.p = start;

  for(const dpt_entry *e = rblookup(RB_LUGTEQ, &dummy, dirtyPages->tableByPage);
      e && (stop == 0 || e->p < stop) && n < count;
      e = rblookup(RB_LUGREAT, e, dirtyPages->tableByPage)) {
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
  for(const dpt_entry * e = rblookup(RB_LUGTEQ, &dummy, dirtyPages->tableByPage);
         e && (stop == 0 || e->p < stop);
         e = rblookup(RB_LUGREAT, e, dirtyPages->tableByPage)) {
    n++;
    staleDirtyPages = realloc(staleDirtyPages, sizeof(pageid_t) * n);
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

stasis_dirty_page_table_t * stasis_dirty_page_table_init() {
  stasis_dirty_page_table_t * ret = malloc(sizeof(*ret));
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
  for(const dpt_entry * e = rblookup(RB_LUGTEQ, &dummy, dirtyPages->tableByPage);
         e;
         e = rblookup(RB_LUGREAT, &dummy, dirtyPages->tableByPage)) {

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
  for(const dpt_entry * e = rblookup(RB_LUGTEQ, &dummy2, dirtyPages->tableByLsnAndPage);
         e;
         e = rblookup(RB_LUGREAT, &dummy2, dirtyPages->tableByLsnAndPage)) {
    dummy2 = *e;
    rbdelete(e, dirtyPages->tableByLsnAndPage);
    free((void*)e);
  }

  rbdestroy(dirtyPages->tableByPage);
  rbdestroy(dirtyPages->tableByLsnAndPage);
  pthread_mutex_destroy(&dirtyPages->mutex);
  free(dirtyPages);
}
