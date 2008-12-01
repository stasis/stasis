#include <string.h>

#include <stasis/common.h>

#include <stasis/bufferManager.h>
#include <stasis/pageHandle.h>
#include <stasis/bufferManager/legacy/pageFile.h>
#include <stasis/bufferManager/legacy/pageCache.h>

#include <stasis/page.h>

#include <stasis/lhtable.h>

static struct LH_ENTRY(table) *activePages; /* page lookup */
static pthread_mutex_t loadPagePtr_mutex;
static Page * dummy_page;
static pthread_key_t lastPage;

#define RO 0
#define RW 1

static void bufManBufDeinit();
static compensated_function Page *bufManLoadPage(int xid, pageid_t pageid);
static compensated_function Page *bufManLoadUninitPage(int xid, pageid_t pageid);
static void bufManReleasePage (Page * p);
static void bufManSimulateBufferManagerCrash();

int bufManBufInit() {

    releasePageImpl = bufManReleasePage;
    loadPageImpl = bufManLoadPage;
    loadUninitPageImpl = bufManLoadUninitPage;
    writeBackPage = pageWrite;
    forcePages = forcePageFile;
    forcePageRange = forceRangePageFile;
    bufDeinit = bufManBufDeinit; 
    simulateBufferManagerCrash = bufManSimulateBufferManagerCrash;

        bufferPoolInit();

	pthread_mutex_init(&loadPagePtr_mutex, NULL);

	activePages = LH_ENTRY(create)(16);

	dummy_page = pageMalloc();
	pageFree(dummy_page, -1);
	Page *first;
	first = pageMalloc();
	pageFree(first, 0);
	LH_ENTRY(insert)(activePages, &first->id, sizeof(first->id), first);
        pageRead(first);
	pageCacheInit(first);

	int err = pthread_key_create(&lastPage, 0);
	assert(!err);

	assert(activePages);
#ifdef PROFILE_LATCHES_WRITE_ONLY
    profile_load_hash = LH_ENTRY(create)(10);
    profile_load_pins_hash = LH_ENTRY(create)(10);
#endif
	return 0;
}

static void bufManBufDeinit() {

	DEBUG("pageCacheDeinit()");

	struct LH_ENTRY(list) iter;
	const struct LH_ENTRY(pair_t) * next;
	LH_ENTRY(openlist(activePages, &iter));

	while((next = LH_ENTRY(readlist)(&iter))) { 
	  pageWrite((Page*)next->value);
	  DEBUG("+");
	}

	LH_ENTRY(destroy)(activePages);

	pthread_mutex_destroy(&loadPagePtr_mutex);
	
	pageCacheDeinit();

	//closePageFile();

	bufferPoolDeInit();
	
#ifdef PIN_COUNT
	if(pinCount != 0) { 
	  printf("WARNING:  At exit, %d pages were still pinned!\n", pinCount);
	}
#endif
	return;
}
/**
    Just close file descriptors, don't do any other clean up. (For
    testing.)

    @todo buffer manager should never call closePageFile(); it not longer manages pageFile handles
*/
void bufManSimulateBufferManagerCrash() {
  closePageFile();
#ifdef PIN_COUNT
  pinCount = 0;
#endif
}

static void bufManReleasePage (Page * p) {
  unlock(p->loadlatch);
#ifdef PIN_COUNT
  pthread_mutex_lock(&pinCount_mutex);
  pinCount --;
  pthread_mutex_unlock(&pinCount_mutex);
#endif

}

static Page* bufManGetPage(pageid_t pageid, int locktype, int uninitialized) {
  Page * ret;
  int spin  = 0;

  pthread_mutex_lock(&loadPagePtr_mutex);
  ret = LH_ENTRY(find)(activePages, &pageid, sizeof(pageid));

  if(ret) {
#ifdef PROFILE_LATCHES_WRITE_ONLY
    // "holder" will contain a \n delimited list of the sites that
    // called loadPage() on the pinned page since the last time it was
    // completely unpinned.  One such site is responsible for the
    // leak.
    
    char * holder = LH_ENTRY(find)(profile_load_hash, &ret, sizeof(void*));
    int * pins = LH_ENTRY(find)(profile_load_pins_hash, &ret, sizeof(void*));
    char * holderD =0;
    int pinsD = 0;
    if(holder) {
      holderD = strdup(holder);
      pinsD = *pins; 
    }
#endif
    if(locktype == RW) {
      writelock(ret->loadlatch, 217);
    } else {
      readlock(ret->loadlatch, 217);
    }
#ifdef PROFILE_LATCHES_WRITE_ONLY
    if(holderD) 
      free(holderD);
#endif
  }

  while (ret && (ret->id != pageid)) {
    unlock(ret->loadlatch);
    pthread_mutex_unlock(&loadPagePtr_mutex);
    sched_yield();
    pthread_mutex_lock(&loadPagePtr_mutex);
    ret = LH_ENTRY(find)(activePages, &pageid, sizeof(pageid));

    if(ret) {
#ifdef PROFILE_LATCHES_WRITE_ONLY
      // "holder" will contain a \n delimited list of the sites that
      // called loadPage() on the pinned page since the last time it was
      // completely unpinned.  One such site is responsible for the
      // leak.
      
      char * holder = LH_ENTRY(find)(profile_load_hash, &ret, sizeof(void*));
      int * pins = LH_ENTRY(find)(profile_load_pins_hash, &ret, sizeof(void*));
      
      char * holderD = 0;
      int pinsD = 0;
      if(holder) { 
	holderD = strdup(holder);
	pinsD = *pins;
      }
#endif
      if(locktype == RW) {
	writelock(ret->loadlatch, 217);
      } else {
	readlock(ret->loadlatch, 217);
      }
#ifdef PROFILE_LATCHES_WRITE_ONLY
      if(holderD)
	free(holderD);
#endif
    }
    spin++;
    if(spin > 10000 && !(spin % 10000)) {
      printf("GetPage is stuck!");
    }
  } 

  if(ret) { 
    cacheHitOnPage(ret);
    assert(ret->id == pageid);
    pthread_mutex_unlock(&loadPagePtr_mutex);
  } else {

    /* If ret is null, then we know that:

       a) there is no cache entry for pageid
       b) this is the only thread that has gotten this far,
          and that will try to add an entry for pageid
       c) the most recent version of this page has been 
          written to the OS's file cache.                  */
    pageid_t oldid = -1;

    if( cache_state == FULL ) {

      /* Select an item from cache, and remove it atomicly. (So it's
	 only reclaimed once) */

      ret = cacheStalePage();
      cacheRemovePage(ret);

      oldid = ret->id;
    
      assert(oldid != pageid);

    } else {

      ret = pageMalloc();
      ret->id = -1;
      ret->inCache = 0;
    }

    // If you leak a page, and it eventually gets evicted, and reused, the system deadlocks here.
#ifdef PROFILE_LATCHES_WRITE_ONLY
    // "holder" will contain a \n delimited list of the sites that
    // called loadPage() on the pinned page since the last time it was
    // completely unpinned.  One such site is responsible for the
    // leak.

    char * holder = LH_ENTRY(find)(profile_load_hash, &ret, sizeof(void*));
    int * pins = LH_ENTRY(find)(profile_load_pins_hash, &ret, sizeof(void*));

    char * holderD = 0;
    int pinsD = 0;
    if(holder) { 
      holderD = strdup(holder);
      pinsD = *pins;
    }
    
#endif

    writelock(ret->loadlatch, 217); 
#ifdef PROFILE_LATCHES_WRITE_ONLY
    if(holderD)
      free(holderD);
#endif

    /* Inserting this into the cache before releasing the mutex
       ensures that constraint (b) above holds. */
    LH_ENTRY(insert)(activePages, &pageid, sizeof(pageid), ret);
    pthread_mutex_unlock(&loadPagePtr_mutex); 

    /* Could writelock(ret) go here? */

    assert(ret != dummy_page);
    if(ret->id != -1) { 
      pageWrite(ret);
    }

    pageFree(ret, pageid);
    if(!uninitialized) {
      pageRead(ret);
    } else {
      memset(ret->memAddr, 0, PAGE_SIZE);
      ret->dirty = 0;
      *stasis_page_lsn_ptr(ret) = ret->LSN;

      // XXX need mutex for this call?
      stasis_page_loaded(ret);
    }

    writeunlock(ret->loadlatch);
 
    pthread_mutex_lock(&loadPagePtr_mutex);

    LH_ENTRY(remove)(activePages, &(oldid), sizeof(oldid));

    /* @todo Put off putting this back into cache until we're done with
       it. -- This could cause the cache to empty out if the ratio of
       threads to buffer slots is above ~ 1/3, but it decreases the
       liklihood of thrashing. */
    cacheInsertPage(ret);

    pthread_mutex_unlock(&loadPagePtr_mutex);
#ifdef PROFILE_LATCHES_WRITE_ONLY
    // "holder" will contain a \n delimited list of the sites that
    // called loadPage() on the pinned page since the last time it was
    // completely unpinned.  One such site is responsible for the
    // leak.
    
    holder = LH_ENTRY(find)(profile_load_hash, &ret, sizeof(void*));
    pins = LH_ENTRY(find)(profile_load_pins_hash, &ret, sizeof(void*));
    
    if(holder) {
      holderD = strdup(holder);
      pinsD = *pins;
    }
#endif
    if(locktype == RW) {
      writelock(ret->loadlatch, 217);
    } else {
      readlock(ret->loadlatch, 217);
    }
#ifdef PROFILE_LATCHES_WRITE_ONLY
    if(holderD)
      free(holderD);
#endif
    if(ret->id != pageid) {
      unlock(ret->loadlatch);
      printf("pageCache.c: Thrashing detected.  Strongly consider increasing LLADD's buffer pool size!\n"); 
      fflush(NULL);
      return bufManGetPage(pageid, locktype, uninitialized);
    }

  }
  return ret;
}


static compensated_function Page *bufManLoadPage(int xid, pageid_t pageid) {

  Page * ret = pthread_getspecific(lastPage);

  if(ret && ret->id == pageid) { 
    pthread_mutex_lock(&loadPagePtr_mutex);
    readlock(ret->loadlatch, 1);
    if(ret->id != pageid) { 
      unlock(ret->loadlatch);
      ret = 0;
    } else { 
      cacheHitOnPage(ret);
      pthread_mutex_unlock(&loadPagePtr_mutex);
    }
  } else { 
    ret = 0;
  }
  if(!ret) { 
    ret = bufManGetPage(pageid, RO, 0);
    pthread_setspecific(lastPage, ret);
  }

#ifdef PIN_COUNT
  pthread_mutex_lock(&pinCount_mutex);
  pinCount ++;
  pthread_mutex_unlock(&pinCount_mutex);
#endif

  return ret;
}

static compensated_function Page *bufManLoadUninitPage(int xid, pageid_t pageid) {

  Page * ret = pthread_getspecific(lastPage);

  if(ret && ret->id == pageid) { 
    pthread_mutex_lock(&loadPagePtr_mutex);
    readlock(ret->loadlatch, 1);
    if(ret->id != pageid) { 
      unlock(ret->loadlatch);
      ret = 0;
    } else { 
      cacheHitOnPage(ret);
      pthread_mutex_unlock(&loadPagePtr_mutex);
    }
  } else { 
    ret = 0;
  }
  if(!ret) { 
    ret = bufManGetPage(pageid, RO, 1);
    pthread_setspecific(lastPage, ret);
  }

#ifdef PIN_COUNT
  pthread_mutex_lock(&pinCount_mutex);
  pinCount ++;
  pthread_mutex_unlock(&pinCount_mutex);
#endif

  return ret;
}
