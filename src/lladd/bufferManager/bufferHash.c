#include <pthread.h>
#include <config.h>
#include "bufferManager/bufferHash.h"
//#include <lladd/transactional.h>
#include <lladd/bufferPool.h>
#include <lladd/redblack.h>
#include <lladd/lhtable.h>
#include "latches.h"


#include <lladd/bufferPool.h>
#include "pageFile.h"

#include <lladd/replacementPolicy.h>
#include <lladd/bufferManager.h>

#include <assert.h>
static struct LH_ENTRY(table) * cachedPages;
static struct LH_ENTRY(table) * pendingPages;

static pthread_t worker;
static pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t readComplete = PTHREAD_COND_INITIALIZER;
static pthread_cond_t haveFree     = PTHREAD_COND_INITIALIZER;
static pthread_cond_t needFree     = PTHREAD_COND_INITIALIZER;

static int freeLowWater;
static int freeListLength;
static int freeCount;
static int pageCount;

static Page ** freeList;

static replacementPolicy * lru;

static int running;


static Page * getFreePage() { 
  Page * ret;
  if(pageCount < MAX_BUFFER_SIZE) { 
    ret = pageMalloc();
    pageCount++;
  } else { 
    while(!freeCount) { 
      pthread_cond_signal(&needFree);
      pthread_cond_wait(&haveFree, &mut);
    }
    ret = freeList[freeCount-1];
    freeList[freeCount-1] = 0;
    freeCount--;
    assert(ret);
    if(freeCount < freeLowWater) { 
      pthread_cond_signal(&needFree);
    }
  } 
  return ret;
}

static void * writeBackWorker(void * ignored) { 
  pthread_mutex_lock(&mut);
  while(1) { 
    while(running && (freeCount == freeListLength || pageCount < MAX_BUFFER_SIZE)) { 
      pthread_cond_wait(&needFree, &mut);
    }
    if(!running) { break; } 
    assert(freeCount < freeListLength);

    Page * victim = lru->getStale(lru);
    assert(victim);
    
    int i = 0;
    while(!trywritelock(victim->loadlatch,0)) { 
      // someone's pinned this page...
      lru->hit(lru, victim->id);
      victim = lru->getStale(lru);
      i++;
      if(i > MAX_BUFFER_SIZE) { 
	printf("Couldn't find a non-pinned page to write back.  Aborting.\n");
	abort();
      }
      if(! i % 100 ) { 
	printf("Severe thrashing detected. :(\n");
      }
    }

    if(victim) { 
      // We have a write lock on victim. 
      lru->remove(lru, victim->id);
      Page * old = LH_ENTRY(remove)(cachedPages, &(victim->id), sizeof(int));
      assert(old == victim);
       
      pageWrite(victim);

      freeList[freeCount] = victim;
      freeCount++;

      unlock(victim->loadlatch);
      pthread_mutex_unlock(&mut);
      pthread_cond_signal(&haveFree);
      pthread_mutex_lock(&mut);
    }
  }
  pthread_mutex_unlock(&mut);
  return 0;
}

static Page * bhLoadPageImpl(int xid, int pageid) {
  
  // Note:  Calls to loadlatch in this function violate lock order, but
  // should be safe, since we make sure no one can have a writelock
  // before we grab the readlock.

  void* check;
 
  pthread_mutex_lock(&mut);

  // Is the page in cache?

  Page * ret = LH_ENTRY(find)(cachedPages, &pageid,sizeof(int));

  if(ret) { 
    int locked = tryreadlock(ret->loadlatch, 0);
    assert(locked);
    pthread_mutex_unlock(&mut);
    assert(ret->id == pageid);
    return ret;
  }

  // Is the page already being read from disk?

  intptr_t * pending = (intptr_t*) LH_ENTRY(find)(pendingPages,&pageid,sizeof(int));

  while(pending) {
    pthread_cond_wait(&readComplete, &mut);
    ret = LH_ENTRY(find)(cachedPages, &pageid, sizeof(int));
    pending = LH_ENTRY(find)(pendingPages,&pageid,sizeof(int));
    if(ret) {
      int locked = tryreadlock(ret->loadlatch,0);
      assert(locked);
      pthread_mutex_unlock(&mut);
      assert(ret->id == pageid);
      return ret;
    }
  }

  assert(!pending &&  ! ret);

  // Either:

  // The page is not in cache, and was not pending.

  // -or-

  // The page was read then evicted since this function was
  // called.  It is now this thread's responsibility to read 
  // the page from disk.

  // Add an entry to pending to block like-minded threads.

  check = LH_ENTRY(insert)(pendingPages,&pageid,sizeof(int), (void*)1);
  assert(!check);

  // Now, it is safe to release the mutex; other threads won't 
  // try to read this page from disk.

  // Remove a page from the freelist.
  ret = getFreePage();

  pthread_mutex_unlock(&mut);

  ret->id = pageid;
  pageRead(ret);

  pthread_mutex_lock(&mut);

  check = LH_ENTRY(remove)(pendingPages, &pageid,sizeof(int));
  assert(check);
  check = LH_ENTRY(insert)(cachedPages, &pageid, sizeof(int), ret);
  assert(!check);
  pthread_cond_broadcast(&readComplete);
  lru->insert(lru, ret->id, ret);
  int locked = tryreadlock(ret->loadlatch, 0);
  assert(locked);
  pthread_mutex_unlock(&mut);

  assert(ret->id == pageid);
  return ret;
}
static void bhReleasePage(Page * p) { 
  pthread_mutex_lock(&mut);
  lru->hit(lru, p->id);
  unlock(p->loadlatch);
  pthread_mutex_unlock(&mut);
}
static void bhWriteBackPage(Page * p) {
  pageWrite(p); //XXX Correct?!?
}
static void bhForcePages() { 
  forcePageFile(); // XXX Correct?!?
}
static void bhBufDeinit() { 
  running = 0;

  pthread_cond_signal(&needFree); // Wake up the writeback thread so it will exit.
  pthread_join(worker, 0);

  struct LH_ENTRY(list) iter;
  const struct LH_ENTRY(pair_t) * next;
  LH_ENTRY(openlist)(cachedPages, &iter);

  while((next = LH_ENTRY(readlist)(&iter))) { 
    pageWrite((next->value));
  }

  LH_ENTRY(closelist)(&iter);
  LH_ENTRY(destroy)(cachedPages);
  
  LH_ENTRY(openlist(pendingPages, &iter));
  if((next = LH_ENTRY(readlist)(&iter))) { 
    abort(); // Pending loadPage during Tdeinit()!
  }
  LH_ENTRY(closelist)(&iter);
  LH_ENTRY(destroy)(pendingPages);

  free(freeList);

  closePageFile();
  lru->deinit(lru);
  bufferPoolDeInit();
}
void bhBufInit() { 

  assert(!running);

  loadPageImpl = bhLoadPageImpl;
  releasePage = bhReleasePage;
  writeBackPage = bhWriteBackPage;
  forcePages = bhForcePages;
  bufDeinit = bhBufDeinit; 
  simulateBufferManagerCrash = bhBufDeinit; 

  bufferPoolInit();

  openPageFile();
  lru = lruInit();

  cachedPages = LH_ENTRY(create)(MAX_BUFFER_SIZE);
  pendingPages = LH_ENTRY(create)(MAX_BUFFER_SIZE/10);

  freeListLength = MAX_BUFFER_SIZE / 10;
  freeLowWater  = freeListLength / 2;
  freeCount = 0;
  pageCount = 0;

  freeList = calloc(freeListLength, sizeof(Page*));

  running = 1;

  pthread_create(&worker, 0, writeBackWorker, 0);
  
}
