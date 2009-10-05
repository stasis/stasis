
// Multiple include trick.
#define HASH_ENTRY(x) bh_hash##x
#define HASH_FCN(val,y,z) (*(pageid_t*)val)
#define LH_ENTRY(x) bh##x
#define LH_HASH(val,len,x,y) bh_hash(val,len,x,y)
#undef PBL_COMPAT
#define PBL_COMPAT 0
#include <stasis/hash.h>
#include "../lhtable.c"
// End multiple include trick.

#include <stasis/bufferManager/bufferHash.h>

#include <stasis/bufferPool.h>
#include <stasis/doubleLinkedList.h>

#include <stasis/dirtyPageTable.h>
#include <stasis/transactional.h>

#include <stasis/replacementPolicy.h>
#include <stasis/page.h>
#include <assert.h>
#include <stdio.h>

//#define LATCH_SANITY_CHECKING

typedef struct {
  struct LH_ENTRY(table) * cachedPages;
  pthread_t worker;
  pthread_mutex_t mut;
  pthread_cond_t readComplete;
  pthread_cond_t needFree;
  pageid_t pageCount;
  replacementPolicy *lru;
  stasis_buffer_pool_t *buffer_pool;
  stasis_page_handle_t *page_handle;
  stasis_dirty_page_table_t *dpt;
  stasis_log_t *log;
  int flushing;
  int running;
} stasis_buffer_hash_t;

typedef struct LL_ENTRY(node_t) node_t;

static node_t * pageGetNode(void * page, void * ignore) {
  Page * p = page;
  return (node_t*)p->prev;
}
static void pageSetNode(void * page, node_t * n, void * ignore) {
  Page * p = page;
  p->prev = (Page *) n;
}

static inline struct Page_s ** pagePendingPtr(Page * p) {
  return ((struct Page_s **)(&((p)->next)));
}
static inline intptr_t* pagePinCountPtr(Page * p) {
  return ((intptr_t*)(&((p)->queue)));
}
static inline int needFlush(stasis_buffer_manager_t * bm) {
  stasis_buffer_hash_t *bh = bm->impl;
  pageid_t count = stasis_dirty_page_table_dirty_count(bh->dpt);
  const pageid_t needed = 1000; //MAX_BUFFER_SIZE / 5;
  if(count > needed) {
    DEBUG("Need flush?  Dirty: %lld Total: %lld ret = %d\n", count, needed, count > needed);
  }
  return  count > needed;
}
#ifdef LONG_RUN

inline static void checkPageState(Page * p) {
  Page * check = LH_ENTRY(find)(cachedPages, &(p->id), sizeof(p->id));
  if(check) {
    int pending = *pagePendingPtr(p);
    int pinned  = *pagePinCountPtr(p);
    if((!pinned) && (!pending)) {
      assert(pageGetNode(p, 0));
    } else {
      assert(!pageGetNode(p,0));
    }
    int notfound = 1;
    for(pageid_t i = 0; i < freeCount; i++) {
      if(freeList[i] == p) { notfound = 0; }
    }
    assert(notfound);
  } else {
    assert(!pageGetNode(p,0));
    assert(!*pagePendingPtr(p));
    assert(!*pagePinCountPtr(p));
    int found = 0;
    for(pageid_t i = 0; i < freeCount; i++) {
      if(freeList[i] == p) { found = 1; }
    }
    assert(found);
  }
}

#else

inline static void checkPageState(Page * p) { }

#endif

inline static int tryToWriteBackPage(stasis_buffer_manager_t *bm, pageid_t page) {
  stasis_buffer_hash_t * bh = bm->impl;
  Page * p = LH_ENTRY(find)(bh->cachedPages, &page, sizeof(page));

  if(!p) { return ENOENT; }

  assert(p->id == page);

  if(*pagePendingPtr(p) || *pagePinCountPtr(p)) {
    return EBUSY;
  }
  DEBUG("Write(%ld)\n", (long)victim->id);
  bh->page_handle->write(bh->page_handle, p);  /// XXX pageCleanup and pageFlushed might be heavyweight.

  return 0;
}

/** Returns a free page.  The page will not be in freeList,
    cachedPages or lru. */
inline static Page * getFreePage(stasis_buffer_manager_t *bm) {
  stasis_buffer_hash_t * bh = bm->impl;
  Page * ret;
  if(bh->pageCount < MAX_BUFFER_SIZE) {
    ret = stasis_buffer_pool_malloc_page(bh->buffer_pool);
    stasis_buffer_pool_free_page(bh->buffer_pool, ret,-1);
    (*pagePinCountPtr(ret)) = 0;
    (*pagePendingPtr(ret)) = 0;
    pageSetNode(ret,0,0);
    bh->pageCount++;
  } else {
    while((ret = bh->lru->getStale(bh->lru))) {
      // Make sure we have an exclusive lock on victim.
      if(!ret) {
        printf("bufferHash.c: Cannot find free page for application request.\nbufferHash.c: This should not happen unless all pages have been pinned.\nbufferHash.c: Crashing.");
        abort();
      }
      assert(!*pagePinCountPtr(ret));
      assert(!*pagePendingPtr(ret));
      if(ret->dirty) {
        pthread_mutex_unlock(&bh->mut);
        DEBUG("Blocking app thread");
        // We don't really care if this flush happens, so long as *something* is being written back, so ignore the EAGAIN it could return.
        // (Besides, once this returns EAGAIN twice, we know that some other flush concurrently was initiated + returned, so we're good to go...)
        stasis_dirty_page_table_flush(bh->dpt);
        pthread_mutex_lock(&bh->mut);
      } else {
        break;
      }
    }

    bh->lru->remove(bh->lru, ret);
    Page * check = LH_ENTRY(remove)(bh->cachedPages, &ret->id, sizeof(ret->id));
    assert(check == ret);
  }
  assert(!*pagePinCountPtr(ret));
  assert(!*pagePendingPtr(ret));
  assert(!pageGetNode(ret,0));
  assert(!ret->dirty);
  return ret;
}

static void * writeBackWorker(void * bmp) {
  stasis_buffer_manager_t* bm = bmp;
  stasis_buffer_hash_t * bh = bm->impl;
  pthread_mutex_lock(&bh->mut);
  while(1) {
    while(bh->running && (!needFlush(bm))) {
      bh->flushing = 0;
      DEBUG("Sleeping in write back worker (count = %lld)\n", stasis_dirty_page_table_dirty_count(bh->dpt));
      pthread_cond_wait(&bh->needFree, &bh->mut);
      DEBUG("Woke write back worker (count = %lld)\n", stasis_dirty_page_table_dirty_count(bh->dpt));
      bh->flushing = 1;
    }
    if(!bh->running) { break; }
    pthread_mutex_unlock(&bh->mut);
    DEBUG("Calling flush\n");
    // ignore ret val; this flush is for performance, not correctness.
    stasis_dirty_page_table_flush(bh->dpt);
    pthread_mutex_lock(&bh->mut);
  }
  pthread_mutex_unlock(&bh->mut);
  return 0;
}

static Page * bhGetCachedPage(stasis_buffer_manager_t* bm, int xid, const pageid_t pageid) {
  stasis_buffer_hash_t * bh = bm->impl;
  pthread_mutex_lock(&bh->mut);
  // Is the page in cache?
  Page * ret = LH_ENTRY(find)(bh->cachedPages, &pageid, sizeof(pageid));
  if(ret) {
    checkPageState(ret);
#ifdef LATCH_SANITY_CHECKING
    int locked = tryreadlock(ret->loadlatch,0);
    assert(locked);
#endif
    if(!*pagePendingPtr(ret)) {
      if(!*pagePinCountPtr(ret) ) {
        // Then ret is in lru (otherwise it would be pending, or not cached); remove it.
        bh->lru->remove(bh->lru, ret);
      }
      (*pagePinCountPtr(ret))++;
      checkPageState(ret);
      assert(ret->id == pageid);
    } else {
      ret = 0;
    }
  }
  pthread_mutex_unlock(&bh->mut);
  return ret;
}

static Page * bhLoadPageImpl_helper(stasis_buffer_manager_t* bm, int xid, const pageid_t pageid, int uninitialized, pagetype_t type) {
  stasis_buffer_hash_t * bh = bm->impl;

  // Note:  Calls to loadlatch in this function violate lock order, but
  // should be safe, since we make sure no one can have a writelock
  // before we grab the readlock.

  void* check;

  pthread_mutex_lock(&bh->mut);

  // Is the page in cache?
  Page * ret = LH_ENTRY(find)(bh->cachedPages, &pageid,sizeof(pageid));

  do {

    // Is the page already in memory or being read from disk?
    // (If ret == 0, then no...)
    while(ret) {
      checkPageState(ret);
      if(*pagePendingPtr(ret)) {
        pthread_cond_wait(&bh->readComplete, &bh->mut);
        if(ret->id != pageid) {
          ret = LH_ENTRY(find)(bh->cachedPages, &pageid, sizeof(pageid));
        }
      } else {
#ifdef LATCH_SANITY_CHECKING
        int locked = tryreadlock(ret->loadlatch,0);
        assert(locked);
#endif
        if(! *pagePinCountPtr(ret) ) {
          // Then ret is in lru (otherwise it would be pending, or not cached); remove it.
          bh->lru->remove(bh->lru, ret);
        }
        (*pagePinCountPtr(ret))++;
        checkPageState(ret);
        pthread_mutex_unlock(&bh->mut);
        assert(ret->id == pageid);
        return ret;
      }
    }

    // The page is not in cache, and is not (no longer is) pending.
    assert(!ret);

    // Remove a page from the freelist.  This may cause writeback, and release our latch.
    Page * ret2 = getFreePage(bm);

    // Did some other thread put the page in cache for us?
    ret = LH_ENTRY(find)(bh->cachedPages, &pageid,sizeof(pageid));

    if(!ret) {

      stasis_page_cleanup(ret2);
      // Make sure that no one mistakenly thinks this is still a live copy.
      ret2->id = -1;

      // No, so we're ready to add it.
      ret = ret2;
      // Esacpe from this loop.
      break;
    } else {
      // Put the page we were about to evict back in cached pages
      LH_ENTRY(insert)(bh->cachedPages, &ret2->id, sizeof(ret2->id), ret2);
      bh->lru->insert(bh->lru, ret2);
      // On the next loop iteration, we'll probably return the page the other thread inserted for us.
    }
    // try again.
  } while(1);

  // Add a pending entry to cachedPages to block like-minded threads and writeback
  (*pagePendingPtr(ret)) = (void*)1;

  check = LH_ENTRY(insert)(bh->cachedPages,&pageid,sizeof(pageid), ret);
  assert(!check);

  ret->id = pageid;

  if(!uninitialized) {

    // Now, it is safe to release the mutex; other threads won't
    // try to read this page from disk.
    pthread_mutex_unlock(&bh->mut);

    bh->page_handle->read(bh->page_handle, ret, type);

    pthread_mutex_lock(&bh->mut);

  } else {
    memset(ret->memAddr,0,PAGE_SIZE);
    *stasis_page_lsn_ptr(ret) = ret->LSN;
    assert(!ret->dirty);
//    ret->dirty = 0;
    stasis_page_loaded(ret, type);
  }
  *pagePendingPtr(ret) = 0;
  // Would remove from lru, but getFreePage() guarantees that it isn't
  // there.
  assert(!pageGetNode(ret, 0));

  assert(!(*pagePinCountPtr(ret)));
  (*pagePinCountPtr(ret))++;

#ifdef LATCH_SANITY_CHECKING
  int locked = tryreadlock(ret->loadlatch, 0);
  assert(locked);
#endif

  pthread_mutex_unlock(&bh->mut);
  pthread_cond_broadcast(&bh->readComplete);

  // TODO Improve writeback policy
  if((!bh->flushing) && needFlush(bm)) {
    pthread_cond_signal(&bh->needFree);
  }
  assert(ret->id == pageid);
  checkPageState (ret);
  return ret;
}

static Page * bhLoadPageImpl(stasis_buffer_manager_t *bm, int xid, const pageid_t pageid, pagetype_t type) {
  return bhLoadPageImpl_helper(bm, xid, pageid, 0, type);
}
static Page * bhLoadUninitPageImpl(stasis_buffer_manager_t *bm, int xid, const pageid_t pageid) {
  return bhLoadPageImpl_helper(bm, xid,pageid,1,UNKNOWN_TYPE_PAGE); // 1 means dont care about preimage of page.
}


static void bhReleasePage(stasis_buffer_manager_t * bm, Page * p) {
  stasis_buffer_hash_t * bh = bm->impl;
  pthread_mutex_lock(&bh->mut);
  checkPageState(p);
  (*pagePinCountPtr(p))--;
  if(!(*pagePinCountPtr(p))) {
    assert(!pageGetNode(p, 0));
    bh->lru->insert(bh->lru,p);
  }
#ifdef LATCH_SANITY_CHECKING
  unlock(p->loadlatch);
#endif
  pthread_mutex_unlock(&bh->mut);
}
static int bhWriteBackPage(stasis_buffer_manager_t* bm, pageid_t pageid) {
  stasis_buffer_hash_t * bh = bm->impl;
  pthread_mutex_lock(&bh->mut);
  int ret = tryToWriteBackPage(bm, pageid);
  pthread_mutex_unlock(&bh->mut);
  return ret;
}
static void bhForcePages(stasis_buffer_manager_t* bm) {
  stasis_buffer_hash_t * bh = bm->impl;
  bh->page_handle->force_file(bh->page_handle);
}
static void bhForcePageRange(stasis_buffer_manager_t *bm, pageid_t start, pageid_t stop) {
  stasis_buffer_hash_t * bh = bm->impl;
  bh->page_handle->force_range(bh->page_handle, start, stop);
}
static void bhBufDeinit(stasis_buffer_manager_t * bm) {
  stasis_buffer_hash_t * bh = bm->impl;
  pthread_mutex_lock(&bh->mut);
  bh->running = 0;
  pthread_mutex_unlock(&bh->mut);

  pthread_cond_signal(&bh->needFree); // Wake up the writeback thread so it will exit.
  pthread_join(bh->worker, 0);

  // XXX flush range should return an error number, which we would check.  (Right now, it aborts...)
  int ret = stasis_dirty_page_table_flush(bh->dpt);
  assert(!ret); // currently the only return value that we'll see is EAGAIN, which means a concurrent thread is in writeback... That should never be the case!

  struct LH_ENTRY(list) iter;
  const struct LH_ENTRY(pair_t) * next;
  LH_ENTRY(openlist)(bh->cachedPages, &iter);
  while((next = LH_ENTRY(readlist)(&iter))) {
    Page * p = next->value;
    assertunlocked(p->rwlatch);
    assert(0 == *pagePinCountPtr(p));
    readlock(p->rwlatch,0);
    assert(!stasis_dirty_page_table_is_dirty(bh->dpt, p));
    unlock(p->rwlatch);
    stasis_page_cleanup(p); // normally called by writeBackOnePage()
  }
  LH_ENTRY(closelist)(&iter);
  LH_ENTRY(destroy)(bh->cachedPages);

  bh->lru->deinit(bh->lru);
  stasis_buffer_pool_deinit(bh->buffer_pool);
  bh->page_handle->close(bh->page_handle);

  pthread_mutex_destroy(&bh->mut);
  pthread_cond_destroy(&bh->needFree);
  pthread_cond_destroy(&bh->readComplete);
  free(bh);
}
static void bhSimulateBufferManagerCrash(stasis_buffer_manager_t *bm) {
  stasis_buffer_hash_t * bh = bm->impl;
  pthread_mutex_lock(&bh->mut);
  bh->running = 0;
  pthread_mutex_unlock(&bh->mut);

  pthread_cond_signal(&bh->needFree);
  pthread_join(bh->worker, 0);

  struct LH_ENTRY(list) iter;
  const struct LH_ENTRY(pair_t) * next;
  LH_ENTRY(openlist)(bh->cachedPages, &iter);
  while((next = LH_ENTRY(readlist)(&iter))) {
    Page * p = next->value;
    writelock(p->rwlatch,0);
    stasis_page_flushed(p); // normally, pageWrite() would call this...
    stasis_page_cleanup(p); // normally called by writeBackOnePage()
    unlock(p->rwlatch);
  }
  LH_ENTRY(closelist)(&iter);
  LH_ENTRY(destroy)(bh->cachedPages);

  bh->lru->deinit(bh->lru);
  stasis_buffer_pool_deinit(bh->buffer_pool);
  bh->page_handle->close(bh->page_handle);

  pthread_mutex_destroy(&bh->mut);
  pthread_cond_destroy(&bh->needFree);
  pthread_cond_destroy(&bh->readComplete);
  free(bh);
}

stasis_buffer_manager_t* stasis_buffer_manager_hash_open(stasis_page_handle_t * h, stasis_log_t * log, stasis_dirty_page_table_t * dpt) {
  stasis_buffer_manager_t *bm = malloc(sizeof(*bm));
  stasis_buffer_hash_t *bh = malloc(sizeof(*bh));

  bm->loadPageImpl = bhLoadPageImpl;
  bm->loadUninitPageImpl = bhLoadUninitPageImpl;
  bm->getCachedPageImpl = bhGetCachedPage;
  bm->releasePageImpl = bhReleasePage;
  bm->writeBackPage = bhWriteBackPage;
  bm->forcePages = bhForcePages;
  bm->forcePageRange = bhForcePageRange;
  bm->stasis_buffer_manager_close = bhBufDeinit;
  bm->stasis_buffer_manager_simulate_crash = bhSimulateBufferManagerCrash;

  bm->impl = bh;

  bh->page_handle = h;
  bh->log = log;
  bh->dpt = dpt;
  bh->running = 0;

#ifdef LONG_RUN
  printf("Using expensive bufferHash sanity checking.\n");
#endif

  bh->flushing = 0;

  bh->buffer_pool = stasis_buffer_pool_init();

  bh->lru = lruFastInit(pageGetNode, pageSetNode, 0);

  bh->cachedPages = LH_ENTRY(create)(MAX_BUFFER_SIZE);

  bh->pageCount = 0;

  bh->running = 1;

  pthread_mutex_init(&bh->mut,0);

  pthread_cond_init(&bh->needFree,0);

  pthread_cond_init(&bh->readComplete,0);

  pthread_create(&bh->worker, 0, writeBackWorker, bm);

  return bm;
}

stasis_buffer_manager_t* stasis_buffer_manager_hash_factory(stasis_log_t *log, stasis_dirty_page_table_t *dpt) {
  stasis_page_handle_t *ph = stasis_page_handle_default_factory(log, dpt);
  return stasis_buffer_manager_hash_open(ph, log, dpt);
}
