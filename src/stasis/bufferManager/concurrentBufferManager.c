/*
 * concurrentBufferManager.c
 *
 *  Created on: Oct 19, 2009
 *      Author: sears
 */
#include <stasis/concurrentHash.h>
#include <stasis/doubleLinkedList.h>
#include <stasis/replacementPolicy.h>
#include <stasis/bufferPool.h>
#include <stasis/pageHandle.h>

#ifndef NO_CONCURRENT_LRU
#ifndef CONCURRENT_LRU
#define CONCURRENT_LRU
#endif // CONCURRENT_LRU
#endif // NO_CONCURRENT_LRU

typedef struct {
  Page *p;
  stasis_buffer_manager_t *bm;
} stasis_buffer_concurrent_hash_tls_t;

typedef struct {
  hashtable_t *ht;
  pthread_t worker;
  pageid_t pageCount;
  replacementPolicy *lru;
  stasis_buffer_pool_t *buffer_pool;
  stasis_page_handle_t *page_handle;
  stasis_dirty_page_table_t *dpt;
  stasis_log_t *log;
  int running;
  stasis_buffer_concurrent_hash_tls_t * tls;
  pthread_key_t key;
  pthread_cond_t needFree;
} stasis_buffer_concurrent_hash_t;

typedef struct LL_ENTRY(node_t) node_t;

static node_t * pageGetNode(void * page, void * ignore) {
  Page * p = page;
  return (node_t*)p->prev;
}
static void pageSetNode(void * page, node_t * n, void * ignore) {
  Page * p = page;
  p->prev = (Page *) n;
}
static inline intptr_t* pagePinCountPtr(void * page) {
  Page * p = page;
  return ((intptr_t*)(&((p)->queue)));
}

static inline int needFlush(stasis_buffer_manager_t * bm) {
  stasis_buffer_concurrent_hash_t *bh = bm->impl;
  pageid_t count = stasis_dirty_page_table_dirty_count(bh->dpt);
  const pageid_t needed = 1000; //MAX_BUFFER_SIZE / 5;
  if(count > needed) {
    DEBUG("Need flush?  Dirty: %lld Total: %lld ret = %d\n", count, needed, count > needed);
  }
  return  count > needed;
}

static int chWriteBackPage(stasis_buffer_manager_t* bm, pageid_t pageid) {
  stasis_buffer_concurrent_hash_t *ch = bm->impl;
  hashtable_bucket_handle_t h;
  Page * p = hashtable_lookup_lock(ch->ht, pageid, &h);
  int ret = 0;
  if(!p) {
    ret = ENOENT;
  } else if(!trywritelock(p->loadlatch,0)) {
    ret = EBUSY;
  }
  hashtable_unlock(&h);
  if(ret) { return ret; }
  // write calls stasis_page_flushed(p);
  ch->page_handle->write(ch->page_handle, p);
  unlock(p->loadlatch);
  return 0;
}
static void * writeBackWorker(void * bmp) {
  stasis_buffer_manager_t* bm = bmp;
  stasis_buffer_concurrent_hash_t * ch = bm->impl;
  pthread_mutex_t mut;
  pthread_mutex_init(&mut,0);
  while(1) {
    while(ch->running && (!needFlush(bm))) {
      DEBUG("Sleeping in write back worker (count = %lld)\n", stasis_dirty_page_table_dirty_count(bh->dpt));
      pthread_mutex_lock(&mut);
      pthread_cond_wait(&ch->needFree, &mut); // XXX Make sure it's OK to have many different mutexes waiting on the same cond.
      pthread_mutex_unlock(&mut);
      DEBUG("Woke write back worker (count = %lld)\n", stasis_dirty_page_table_dirty_count(bh->dpt));
    }
    if(!ch->running) { break; }
    DEBUG("Calling flush\n");
    // ignore ret val; this flush is for performance, not correctness.
    stasis_dirty_page_table_flush(ch->dpt);
  }
  pthread_mutex_destroy(&mut);
  return 0;

}
static Page * chGetCachedPage(stasis_buffer_manager_t* bm, int xid, const pageid_t pageid) {
  stasis_buffer_concurrent_hash_t * ch = bm->impl;
  hashtable_bucket_handle_t h;
  Page * p = hashtable_lookup_lock(ch->ht, pageid, &h);
  if(p) {
    int succ = tryreadlock(p->loadlatch, 0);
    if(!succ) {
      // weird corner case: if we're writing back the page during the call, this returns NULL instead of blocking.
      // Could go either way on what the correct behavior is.
      p = NULL;
    } else {
      ch->lru->remove(ch->lru, p);
    }
  }
  hashtable_unlock(&h);
  return p;
}
static void deinitTLS(void *tlsp) {
  stasis_buffer_concurrent_hash_tls_t * tls = tlsp;
  stasis_buffer_concurrent_hash_t *ch = tls->bm->impl;

  Page * p = tls->p;
  p->id = -1;
  while(hashtable_test_and_set(ch->ht,p->id, p)) {
    p->id --;
  }
  ch->lru->insert(ch->lru, tls->p); // TODO: put it into the LRU end instead of the MRU end, so the memory is treated as stale.
}
static inline stasis_buffer_concurrent_hash_tls_t * populateTLS(stasis_buffer_manager_t* bm) {
  stasis_buffer_concurrent_hash_t *ch = bm->impl;
  stasis_buffer_concurrent_hash_tls_t *tls = pthread_getspecific(ch->key);
  if(tls == NULL) {
    tls = malloc(sizeof(*tls));
    tls->p = NULL;
    tls->bm = bm;
    pthread_setspecific(ch->key, tls);
  }
  int count = 0;
  while(tls->p == NULL) {
    Page * tmp = ch->lru->getStaleAndRemove(ch->lru);
    hashtable_bucket_handle_t h;
    tls->p = hashtable_remove_begin(ch->ht, tmp->id, &h);
    if(tls->p) {
      // It used to be the case that we could get in trouble because page->id could change concurrently with us.  However, this is no longer a problem,
      // since getStaleAndRemove is atomic, and the only code that changes page->id does so with pages that are in TLS (and therefore went through getStaleAndRemove)
      int succ =
	trywritelock(tls->p->loadlatch,0);  // if this blocks, it is because someone else has pinned the page (it can't be due to eviction because the lru is atomic)

      if(succ) {
	// The getStaleAndRemove was not atomic with the hashtable remove, which is OK (but we can't trust tmp anymore...)
	tmp = 0;
	if(tls->p->id >= 0) {
	  ch->page_handle->write(ch->page_handle, tls->p);
	}
	hashtable_remove_finish(ch->ht, &h);  // need to hold bucket lock until page is flushed.  Otherwise, another thread could read stale data from the filehandle.
	tls->p->id = INVALID_PAGE; // in case loadPage has a pointer to it, and we did this in race with it; when loadPage reacquires loadlatch, it will notice the discrepency
	assert(!tls->p->dirty);
	unlock(tls->p->loadlatch);
	break;
      } else {
	// put back in LRU before making it accessible (again) via the hash.
	// otherwise, someone could try to pin it.
	ch->lru->insert(ch->lru, tmp);  // OK because lru now does refcounting, and we know that tmp->id can't change (because we're the ones that got it from LRU)
	hashtable_remove_cancel(ch->ht, &h);
	tls->p = NULL; // This iteration of the loop failed, set this so the loop runs again.
      }
    } else {
      // page is not in hashtable, but it is in LRU.  getStale and hashtable remove are not atomic.
      // However, we cannot observe this; the lru remove happens before the hashtable remove,
      // and the hashtable insert happens before the lru insert.
      abort();
    }
    count ++;
    if(count == 100) {
      printf("Hashtable is spinning attempting to evict a page");
    }
  }
  return tls;
}

static void chReleasePage(stasis_buffer_manager_t * bm, Page * p);

static Page * chLoadPageImpl_helper(stasis_buffer_manager_t* bm, int xid, const pageid_t pageid, int uninitialized, pagetype_t type) {
  stasis_buffer_concurrent_hash_t *ch = bm->impl;
  stasis_buffer_concurrent_hash_tls_t *tls = populateTLS(bm);
  hashtable_bucket_handle_t h;
  Page * p = 0;
  int first = 1;

  do {
    if(p) { // the last time around pinned the wrong page!
      chReleasePage(bm, p);
      // ch->lru->insert(ch->lru, p);
      // unlock(p->loadlatch);
    }
    while(NULL == (p = hashtable_test_and_set_lock(ch->ht, pageid, tls->p, &h))) {
      first = 0;
      // The page was not in the hash.  Now it is up to us.
      p = tls->p;
      tls->p = NULL;

      // Need to acquire lock because some loadPage (in race with us) could have a
      // pointer to the page.  However, we must be the only thread
      // that has the page in its TLS, or something is seriously wrong.
      writelock(p->loadlatch, 0);

      // this has to happen before the page enters LRU; concurrentWrapper (and perhaps future implementations) hash on pageid.
      p->id = pageid;

      // this has to happen after the hashtable insertion succeeds, otherwise above, we could get a page from lru that isn't in the cache.
      ch->lru->insert(ch->lru, p);

      hashtable_unlock(&h);

      if(uninitialized) {
	type = UNINITIALIZED_PAGE;
	stasis_page_loaded(p, UNINITIALIZED_PAGE);
      } else {
	ch->page_handle->read(ch->page_handle, p, type);
      }
      unlock(p->loadlatch);

      tls = populateTLS(bm);
      if(needFlush(bm)) { pthread_cond_signal(&ch->needFree); }
    }
    ch->lru->remove(ch->lru, p);  // this can happen before or after the the hashable unlock, since the invariant is that in lru -> in hashtable, and it's in the hash
    hashtable_unlock(&h);
    // spooky.  It's not in LRU, and it's not been load latched.  This
    // is OK, but it's possible that popluateTLS has a pointer to the page
    // already, regardless of which hash bucket it's in.
    readlock(p->loadlatch, 0);
    // Now, we know that populateTLS won't evict the page, since it gets a writelock before doing so.
  } while(p->id != pageid); // On the off chance that the page got evicted, we'll need to try again.
  return p;
}
static Page * chLoadPageImpl(stasis_buffer_manager_t *bm, stasis_buffer_manager_handle_t *h, int xid, const pageid_t pageid, pagetype_t type) {
  return chLoadPageImpl_helper(bm, xid, pageid, 0, type);
}
static Page * chLoadUninitPageImpl(stasis_buffer_manager_t *bm, int xid, const pageid_t pageid) {
  return chLoadPageImpl_helper(bm, xid,pageid,1,UNKNOWN_TYPE_PAGE); // 1 means dont care about preimage of page.
}
static void chReleasePage(stasis_buffer_manager_t * bm, Page * p) {
  stasis_buffer_concurrent_hash_t * ch = bm->impl;
  ch->lru->insert(ch->lru, p);
  unlock(p->loadlatch);
}
static void chForcePages(stasis_buffer_manager_t* bm, stasis_buffer_manager_handle_t *h) {
  stasis_buffer_concurrent_hash_t * ch = bm->impl;
  ch->page_handle->force_file(ch->page_handle);
}
static void chForcePageRange(stasis_buffer_manager_t *bm, stasis_buffer_manager_handle_t *h, pageid_t start, pageid_t stop) {
  stasis_buffer_concurrent_hash_t * ch = bm->impl;
  ch->page_handle->force_range(ch->page_handle, start, stop);
}
static void chBufDeinitHelper(stasis_buffer_manager_t * bm, int crash) {
  stasis_buffer_concurrent_hash_t *ch = bm->impl;
  ch->running = 0;
  pthread_key_delete(ch->key);
  pthread_cond_signal(&ch->needFree);
  pthread_join(ch->worker, NULL);
  pthread_cond_destroy(&ch->needFree);
  if(!crash) {
    stasis_dirty_page_table_flush(ch->dpt);
    ch->page_handle->force_file(ch->page_handle);
  }
  hashtable_deinit(ch->ht);
  ch->lru->deinit(ch->lru);
  stasis_buffer_pool_deinit(ch->buffer_pool);
  free(ch);
}
static void chSimulateBufferManagerCrash(stasis_buffer_manager_t *bm) {
  chBufDeinitHelper(bm, 1);
}
static void chBufDeinit(stasis_buffer_manager_t * bm) {
  chBufDeinitHelper(bm, 0);
}
static stasis_buffer_manager_handle_t * chOpenHandle(stasis_buffer_manager_t *bm, int is_sequential) {
  // no-op
  return (void*)1;
}
static int chCloseHandle(stasis_buffer_manager_t *bm, stasis_buffer_manager_handle_t* h) {
  return 0; // no error
}

stasis_buffer_manager_t* stasis_buffer_manager_concurrent_hash_open(stasis_page_handle_t * h, stasis_log_t * log, stasis_dirty_page_table_t * dpt) {
  stasis_buffer_manager_t *bm = malloc(sizeof(*bm));
  stasis_buffer_concurrent_hash_t *ch = malloc(sizeof(*ch));
  bm->openHandleImpl = chOpenHandle;
  bm->closeHandleImpl = chCloseHandle;
  bm->loadPageImpl = chLoadPageImpl;
  bm->loadUninitPageImpl = chLoadUninitPageImpl;
  bm->prefetchPages = NULL;
  bm->getCachedPageImpl = chGetCachedPage;
  bm->releasePageImpl = chReleasePage;
  bm->writeBackPage = chWriteBackPage;
  bm->forcePages = chForcePages;
  bm->forcePageRange = chForcePageRange;
  bm->stasis_buffer_manager_close = chBufDeinit;
  bm->stasis_buffer_manager_simulate_crash = chSimulateBufferManagerCrash;

  bm->impl = ch;

  ch->page_handle = h;
  ch->log = log;
  ch->dpt = dpt;
  ch->running = 1;

#ifdef LONG_RUN
  printf("Using expensive bufferHash sanity checking.\n");
#endif
  ch->buffer_pool = stasis_buffer_pool_init();
#ifdef CONCURRENT_LRU
  replacementPolicy ** lrus = malloc(sizeof(lrus[0]) * 37);
  for(int i = 0; i < 37; i++) {
    lrus[i] = lruFastInit(pageGetNode, pageSetNode, pagePinCountPtr, 0);
  }
  ch->lru = replacementPolicyConcurrentWrapperInit(lrus, 37);
  free(lrus);
#else
  ch->lru = replacementPolicyThreadsafeWrapperInit(lruFastInit(pageGetNode, pageSetNode, pagePinCountPtr, 0));
#endif
  ch->ht = hashtable_init(MAX_BUFFER_SIZE * 4);

  for(int i = 0; i < MAX_BUFFER_SIZE; i++) {
    Page *p = stasis_buffer_pool_malloc_page(ch->buffer_pool);
    stasis_buffer_pool_free_page(ch->buffer_pool, p,-1*i);
    pageSetNode(p,0,0);
    (*pagePinCountPtr(p)) = 1;
    ch->lru->insert(ch->lru, p);  // decrements pin count ptr (setting it to zero)
    hashtable_insert(ch->ht, p->id, p);
  }

  ch->pageCount = 0;

  ch->running = 1;

  pthread_key_create(&ch->key, deinitTLS);
  pthread_cond_init(&ch->needFree, 0);
  pthread_create(&ch->worker, 0, writeBackWorker, bm);

  return bm;
}

stasis_buffer_manager_t* stasis_buffer_manager_concurrent_hash_factory(stasis_log_t *log, stasis_dirty_page_table_t *dpt) {
  stasis_page_handle_t *ph = stasis_page_handle_default_factory(log, dpt);
  return stasis_buffer_manager_concurrent_hash_open(ph, log, dpt);
}
