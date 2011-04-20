/*
 * concurrentBufferManager.c
 *
 *  Created on: Oct 19, 2009
 *      Author: sears
 */
#include <stasis/concurrentHash.h>
#include <stasis/replacementPolicy.h>
#include <stasis/bufferPool.h>
#include <stasis/pageHandle.h>
#include <stasis/flags.h>

#ifndef NO_CONCURRENT_LRU
#ifndef CONCURRENT_LRU
#define CONCURRENT_LRU
#endif // CONCURRENT_LRU
#endif // NO_CONCURRENT_LRU

//#define STRESS_TEST_WRITEBACK 1 // if defined, writeback as much as possible, as fast as possible.

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

static inline int needFlush(stasis_buffer_manager_t * bm) {
  stasis_buffer_concurrent_hash_t *bh = bm->impl;
  pageid_t count = stasis_dirty_page_table_dirty_count(bh->dpt);
  const pageid_t needed = 1000; //MAX_BUFFER_SIZE / 5;
  if(count > needed) {
    DEBUG("Need flush?  Dirty: %lld Total: %lld ret = %d\n", count, needed, count > needed);
  }
#ifdef STRESS_TEST_WRITEBACK
  return count > 0;
#else
  return  count > needed;
#endif
}

static int chWriteBackPage_helper(stasis_buffer_manager_t* bm, pageid_t pageid, int is_hint) {
  stasis_buffer_concurrent_hash_t *ch = bm->impl;
  Page * p = hashtable_lookup(ch->ht, pageid/*, &h*/);
  int ret = 0;
  if(!p) {
    ret = ENOENT;
  } else {
    if(is_hint) {
      if(!trywritelock(p->loadlatch,0)) {
        ret = EBUSY;
        p->needsFlush = 1; // Not atomic.  Oh well.
        if(p->id != pageid) {
          fprintf(stderr, "BUG FIX: %s:%d would have corrupted a latch's state, but did not\n", __FILE__, __LINE__);
        }
      } else {
        if(p->id != pageid) {  // it must have been written back...
          unlock(p->loadlatch);
          return 0;
        }
      }
    } else {
      // Uggh.  With the current design, it's possible that the trywritelock will block on the writeback thread.
      // That leaves us with few options, so we expose two sets of semantics up to the caller.

      // This used to call writelock while holding a hashtable bucket lock, risking deadlock if called when the page was pinned.

      // We could assume that the caller knows what it's doing, and writeback regardless of whether the page is
      // pinned.  This could allow torn pages to reach disk, and would risk calling page compaction, etc while the
      // page is concurrently read.  However, it would prevent us from blocking on application pins.  Unfortunately,
      // compacting under during application reads is a deal breaker.

      // Instead, we release the hashtable lock, get the write latch, then double check the pageid.
      // This is safe, since we know that page pointers are never freed, only reused.  However, it causes writeback
      // to block waiting for application threads to unpin their pages.
      writelock(p->loadlatch,0);
      if(p->id != pageid) {
        // someone else wrote it back.  woohoo.
        unlock(p->loadlatch);
        return 0;
      }
    }
  }
  if(ret) { return ret; }

  // When we optimize for sequential writes, we try to make sure that
  // write back only happens in a single thread.  Therefore, there is
  // no reason to put dirty pages in the LRU, and lruFast will ignore
  // dirty pages that are inserted into it.  Since we may be making a dirty
  // page become clean here, we remove the page from LRU, and put it
  // back in.  (There is no need to do this if the sequential
  // optimizations are turned off...)
  if(stasis_buffer_manager_hint_writes_are_sequential)
    ch->lru->remove(ch->lru, p);

  // write calls stasis_page_flushed(p);
  ch->page_handle->write(ch->page_handle, p);

  // Put the page back in LRU iff we just took it out.
  if(stasis_buffer_manager_hint_writes_are_sequential)
    ch->lru->insert(ch->lru, p);

  p->needsFlush = 0;
  unlock(p->loadlatch);
  return 0;
}
static int chWriteBackPage(stasis_buffer_manager_t* bm, pageid_t pageid) {
  return chWriteBackPage_helper(bm,pageid,0); // not hint; for correctness.  Block (deadlock?) on contention.
}
static int chTryToWriteBackPage(stasis_buffer_manager_t* bm, pageid_t pageid) {
  return chWriteBackPage_helper(bm,pageid,1); // just a hint.  Return EBUSY on contention.
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
  p->id = -2;
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
    Page * tmp;
    int spin_count = 0;
    while(!(tmp = ch->lru->getStaleAndRemove(ch->lru))) {
      spin_count++;
      if(needFlush(bm)) {
        // exponential backoff -- don't test exponential backoff flag
        // here.  LRU should only return null if we're in big trouble,
        // or if the flag is set to true.

        // wake writeback thread
        pthread_cond_signal(&ch->needFree);

        // sleep
        struct timespec ts = { 0, (1024 * 1024) << (spin_count > 3 ? 3 : spin_count) };
        nanosleep(&ts, 0);
        if(spin_count > 9) {
          static int warned = 0;
          if(!warned) {
            fprintf(stderr, "Warning: lots of spinning attempting to get page from LRU\n");
            warned = 1;
          }
        }
      }
    }
    hashtable_bucket_handle_t h;
    tls->p = hashtable_remove_begin(ch->ht, tmp->id, &h);
    if(tls->p) {
      // It used to be the case that we could get in trouble because page->id could change concurrently with us.  However, this is no longer a problem,
      // since getStaleAndRemove is atomic, and the only code that changes page->id does so with pages that are in TLS (and therefore went through getStaleAndRemove)
      int succ =
      trywritelock(tls->p->loadlatch,0);  // if this blocks, it is because someone else has pinned the page (it can't be due to eviction because the lru is atomic)

      if(succ && (
          // Work-stealing heuristic: If we don't know that writes are sequential, then write back the page we just encountered.
          (!stasis_buffer_manager_hint_writes_are_sequential)
          // Otherwise, if writes are sequential, then we never want to steal work from the writeback thread,
          // so, pass over pages that are dirty.
          || (!tls->p->dirty)
        )) {
        // The getStaleAndRemove was not atomic with the hashtable remove, which is OK (but we can't trust tmp anymore...)
        if(tmp != tls->p) {
          int copy_count = hashtable_debug_number_of_key_copies(ch->ht, tmp->id);
          assert(copy_count == 1);
          assert(tmp == tls->p);
          abort();
        }
        // note that we'd like to assert that the page is unpinned here.  However, we can't simply look at p->queue, since another thread could be inside the "spooky" quote below.
        tmp = 0;
        if(tls->p->id >= 0) {
	  // Page is not in LRU, so we don't have to worry about the case where we
	  // are in sequential mode, and have to remove/add the page from/to the LRU.
          ch->page_handle->write(ch->page_handle, tls->p);
        }
        hashtable_remove_finish(ch->ht, &h);  // need to hold bucket lock until page is flushed.  Otherwise, another thread could read stale data from the filehandle.
        tls->p->id = INVALID_PAGE; // in case loadPage has a pointer to it, and we did this in race with it; when loadPage reacquires loadlatch, it will notice the discrepancy
        assert(!tls->p->dirty);
        unlock(tls->p->loadlatch);
        break;
      } else {
        if(succ) {
          assert(tmp == tls->p);
          // can only reach this if writes are sequential, and the page is dirty.
          unlock(tls->p->loadlatch);
        }
        // put back in LRU before making it accessible (again) via the hash.
        // otherwise, someone could try to pin it.
        ch->lru->insert(ch->lru, tmp);  // OK because lru now does refcounting, and we know that tmp->id can't change (because we're the ones that got it from LRU)
        hashtable_remove_cancel(ch->ht, &h);
        tls->p = NULL; // This iteration of the loop failed, set this so the loop runs again.
        if(succ) {
          // writes are sequential, p is dirty, and there is a big backlog.  Go to sleep for 1 msec to let things calm down.
          if(count > 10) {
            struct timespec ts = { 0, 1000000 };
            nanosleep(&ts, 0);
          }
        }
      }
    } else {
      // page is not in hashtable, but it is in LRU.  getStale and hashtable remove are not atomic.
      // However, we cannot observe this; the lru remove happens before the hashtable remove,
      // and the lru insert happens while we hold a latch on the hashtable bucket.
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

static Page * chLoadPageImpl_helper(stasis_buffer_manager_t* bm, int xid, stasis_page_handle_t *ph, const pageid_t pageid, int uninitialized, pagetype_t type) {
  if(uninitialized) assert(!bm->in_redo);

  stasis_buffer_concurrent_hash_t *ch = bm->impl;
  stasis_buffer_concurrent_hash_tls_t *tls = populateTLS(bm);
  hashtable_bucket_handle_t h;
  Page * p = 0;

  ph = ph ? ph : ch->page_handle;

  do {
    if(p) { // the last time around pinned the wrong page!
      chReleasePage(bm, p);
      // ch->lru->insert(ch->lru, p);
      // unlock(p->loadlatch);
    }
    while(NULL == (p = hashtable_test_and_set_lock(ch->ht, pageid, tls->p, &h))) {

      // The page was not in the hash.  Now it is up to us.
      p = tls->p;
      tls->p = NULL;

      // Need to acquire lock because some loadPage (in race with us) could have a
      // pointer to the page.  However, we must be the only thread
      // that has the page in its TLS, or something is seriously wrong.

      // This cannot deadlock because the load page will look at p->id, and see
      // that it is -1.  Then it will immediately release loadlatch, allowing
      // us to make progress.
      writelock(p->loadlatch, 0);

      // this has to happen before the page enters LRU; concurrentWrapper (and perhaps future implementations) hash on pageid.
      p->id = pageid;

      // this has to happen after the hashtable insertion succeeds, otherwise above, we could get a page from lru that isn't in the cache.
      ch->lru->insert(ch->lru, p);

      hashtable_unlock(&h);

      if(uninitialized) {
        type = UNINITIALIZED_PAGE;
        assert(!p->dirty);
        stasis_uninitialized_page_loaded(xid, p);
      } else {
        ph->read(ph, p, type);
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
  return chLoadPageImpl_helper(bm, xid, (stasis_page_handle_t*)h, pageid, 0, type);
}
static Page * chLoadUninitPageImpl(stasis_buffer_manager_t *bm, int xid, const pageid_t pageid) {
  assert(!bm->in_redo);
  return chLoadPageImpl_helper(bm, xid, 0, pageid,1,UNKNOWN_TYPE_PAGE); // 1 means dont care about preimage of page.
}
static void chReleasePage(stasis_buffer_manager_t * bm, Page * p) {
  stasis_buffer_concurrent_hash_t * ch = bm->impl;
  ch->lru->insert(ch->lru, p);
  int doFlush = p->needsFlush;
  pageid_t pid = p->id;
  unlock(p->loadlatch);
  if(doFlush) {
    DEBUG(__FILE__ "releasePage: Page writeback is hungry.  Pageid = %lld Do flush = %d\n", (long long)pid, doFlush);
    bm->tryToWriteBackPage(bm, pid);
  }
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
  stasis_buffer_concurrent_hash_t * bh = bm->impl;
  return (stasis_buffer_manager_handle_t*)bh->page_handle->dup(bh->page_handle, is_sequential);
}
static int chCloseHandle(stasis_buffer_manager_t *bm, stasis_buffer_manager_handle_t* h) {
  ((stasis_page_handle_t*)h)->close((stasis_page_handle_t*)h);
  return 0;
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
  bm->tryToWriteBackPage = chTryToWriteBackPage;
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
    lrus[i] = lruFastInit();
  }
  ch->lru = replacementPolicyConcurrentWrapperInit(lrus, 37);
  free(lrus);
#else
  ch->lru = replacementPolicyThreadsafeWrapperInit(lruFastInit(pageGetNode, pageSetNode, pagePinCountPtr, 0));
#endif
  ch->ht = hashtable_init(stasis_buffer_manager_size * 4);

  for(pageid_t i = 0; i < stasis_buffer_manager_size; i++) {
    Page *p = stasis_buffer_pool_malloc_page(ch->buffer_pool);
    stasis_buffer_pool_free_page(ch->buffer_pool, p,(-1*i)-2);
    p->prev = p->next = NULL;
    p->pinCount = 1;
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
  stasis_page_handle_t *ph = stasis_page_handle_factory(log, dpt);
  return stasis_buffer_manager_concurrent_hash_open(ph, log, dpt);
}
