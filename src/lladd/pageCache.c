/**
   @file

   pageCache handles the replacement policy for buffer manager.  This
   allows bufferManager's implementation to focus on providing atomic
   writes, and locking.
*/
#include <config.h>
#include <lladd/common.h>
#include "latches.h"
#include "page.h" 
#include <lladd/pageCache.h>
#include <lladd/bufferManager.h>

#include <assert.h>
#include <pbl/pbl.h>

#include <stdio.h>
#include "pageFile.h"
static pblHashTable_t *activePages; /* page lookup */
static unsigned int bufferSize; /* < MAX_BUFFER_SIZE */
static Page *repHead, *repMiddle, *repTail; /* replacement policy */
static pthread_mutex_t loadPagePtr_mutex;

#define INITIAL 0
#define FULL    1

static int state;

/* These three functions are for internal use.  They are not declared
   static so that their test case can compile. */
static void cacheHitOnPage(Page * ret);
static void cacheRemovePage(Page * ret);
static void cacheInsertPage (Page * ret);

static Page * dummy_page;

void pageCacheInit() {

  Page *first;
  bufferSize = 1;
  state  = INITIAL;

  pthread_mutex_init(&loadPagePtr_mutex, NULL);

  activePages = pblHtCreate();
  assert(activePages);
  
  DEBUG("pageCacheInit()");

  first = pageAlloc(0);
  dummy_page = pageAlloc(-1);
  pageRealloc(first, 0);
  pageRealloc(dummy_page, -1);
  first->inCache = 1;
  pblHtInsert(activePages, &first->id, sizeof(int), first);
  
  first->prev = first->next = NULL;
  /*  pageMap(first); */
  pageRead(first);

  repHead = repTail = first;
  repMiddle = NULL;


}

void pageCacheDeinit() {
  Page *p;
  DEBUG("pageCacheDeinit()");

  for( p = (Page*)pblHtFirst( activePages ); p; p = (Page*)pblHtRemove( activePages, 0, 0 )) {
    DEBUG("+");
    /** @todo No one seems to set the dirty flag... */
    /*if(p->dirty && (ret = pageWrite(p)/ *flushPage(*p)* /)) {
      printf("ERROR: flushPage on %s line %d", __FILE__, __LINE__);
      abort();
      / *      exit(ret); * /
      }*/

    pageWrite(p);
  }

  pthread_mutex_destroy(&loadPagePtr_mutex);
  

  pblHtDelete(activePages);
}

static void headInsert(Page *ret) {

	assert(ret != repMiddle);
	assert(ret != repTail);
	assert(ret != repHead);

	repHead->prev = ret;
	ret->next = repHead;
	ret->prev = NULL;
	repHead = ret;
}

static void middleInsert(Page *ret) {
  
  assert(state == FULL);

  /*	assert( bufferSize == MAX_BUFFER_SIZE ); */

	assert(ret != repMiddle);
	assert(ret != repTail);
	assert(ret != repHead);

	ret->prev  = repMiddle->prev;
	ret->next = repMiddle;
	repMiddle->prev = ret;
	ret->prev->next = ret;
	ret->queue = 2;

	repMiddle = ret;
	assert(ret->next != ret && ret->prev != ret);
}

/** @todo Under high contention, the buffer pool can empty.  What should be done about this, other than making sure that # threads > buffer size? */
static void qRemove(Page *ret) {

  assert(state == FULL);

  assert(ret->next != ret && ret->prev != ret);
  
	if( ret->prev )
	  ret->prev->next = ret->next;
	else /* is head */
		repHead = ret->next; /* won't have head == tail because of test in loadPage */
	if( ret->next ) {
		ret->next->prev = ret->prev;
		/* TODO: these if can be better organizeed for speed */
		if( ret == repMiddle ) 
			/* select new middle */
			repMiddle = ret->next;
	}
	else /* is tail */
		repTail = ret->prev;

	assert(ret != repMiddle);
	assert(ret != repTail);
	assert(ret != repHead);
}

Page * getPage(int pageid, int locktype) {
  Page * ret;
  int spin  = 0;
  pthread_mutex_lock(&loadPagePtr_mutex);
  ret = pblHtLookup(activePages, &pageid, sizeof(int));

  if(ret) {
    if(locktype == RW) {
      writelock(ret->loadlatch, 217);
    } else {
      readlock(ret->loadlatch, 217);
    }
    //writelock(ret->loadlatch, 217);
  }

  while (ret && (ret->id != pageid)) {
    unlock(ret->loadlatch);
    pthread_mutex_unlock(&loadPagePtr_mutex);
    sched_yield();
    pthread_mutex_lock(&loadPagePtr_mutex);
    ret = pblHtLookup(activePages, &pageid, sizeof(int));

    if(ret) {
      //      writelock(ret->loadlatch, 217);
      if(locktype == RW) {
	writelock(ret->loadlatch, 217);
      } else {
	readlock(ret->loadlatch, 217);
      }
    }
    spin++;
    if(spin > 10000) {
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
    int oldid = -1;

    if( state == FULL ) {

      /* Select an item from cache, and remove it atomicly. (So it's
	 only reclaimed once) */

      ret = repTail;
      cacheRemovePage(ret);

      oldid = ret->id;
    
      assert(oldid != pageid);

    } else {

      ret = pageAlloc(-1);
      ret->id = -1;
      ret->inCache = 0;
    }

    writelock(ret->loadlatch, 217); 

    /* Inserting this into the cache before releasing the mutex
       ensures that constraint (b) above holds. */
    pblHtInsert(activePages, &pageid, sizeof(int), ret); 

    pthread_mutex_unlock(&loadPagePtr_mutex); 

    /* Could writelock(ret) go here? */

    assert(ret != dummy_page);
    if(ret->id != -1) { 
      pageWrite(ret);
    }

    pageRealloc(ret, pageid);

    pageRead(ret);

    writeunlock(ret->loadlatch);
 
    pthread_mutex_lock(&loadPagePtr_mutex);

    /*    pblHtRemove(activePages, &(ret->id), sizeof(int));  */
    pblHtRemove(activePages, &(oldid), sizeof(int)); 

    /* Put off putting this back into cache until we're done with
       it. -- This could cause the cache to empty out if the ratio of
       threads to buffer slots is above ~ 1/3, but it decreases the
       liklihood of thrashing. */
    cacheInsertPage(ret);

    pthread_mutex_unlock(&loadPagePtr_mutex);

    /*    downgradelock(ret->loadlatch); */

    //    writelock(ret->loadlatch, 217);
    if(locktype == RW) {
      writelock(ret->loadlatch, 217);
    } else {
      readlock(ret->loadlatch, 217);
    }
    if(ret->id != pageid) {
      unlock(ret->loadlatch);
      printf("pageCache.c: Thrashing detected.  Strongly consider increasing LLADD's buffer pool size!\n"); 
      fflush(NULL);
      return getPage(pageid, locktype);
    }

    /*  } else { 

    pthread_mutex_unlock(&loadPagePtr_mutex);
  
    } */
  }

  assert(ret->id == pageid);
    
  return ret;
  
}

static void cacheInsertPage (Page * ret) {
  bufferSize++;
  assert(!ret->inCache);
  ret->inCache ++;
  if(state == FULL) {
    middleInsert(ret);
  } else {
    if(bufferSize == MAX_BUFFER_SIZE/* - 1*/) {  /* Set up page kick mechanism. */
      int i;
      Page *iter;
      
      state = FULL;
      
      headInsert(ret);
      assert(ret->next != ret && ret->prev != ret);
      
      /* split up queue:
       * "in all cases studied ... fixing the primary region to 30% ...
       * resulted in the best performance"
       */
      repMiddle = repHead;
      for( i = 0; i < MAX_BUFFER_SIZE / 3; i++ ) {
	repMiddle->queue = 1;
	repMiddle = repMiddle->next;
      }
      
      for( iter = repMiddle; iter; iter = iter->next ) {
	iter->queue = 2;
      }
    } else { /* Just insert it. */
      headInsert(ret);
      assert(ret->next != ret && ret->prev != ret);
      assert(ret->next != ret && ret->prev != ret);
    }
  }
}

static void cacheRemovePage(Page * ret) {
  assert(ret->inCache);
  qRemove(ret);
  ret->inCache--;
  bufferSize --;
}

static void cacheHitOnPage(Page * ret) {
  /* The page may not be in cache if it is about to be freed. */
  if(ret->inCache && state == FULL) { /* we need to worry about page sorting */
    /* move to head */
    if( ret != repHead ) {
      qRemove(ret);
      headInsert(ret);
      assert(ret->next != ret && ret->prev != ret);
      
      if( ret->queue == 2 ) {
	/* keep first queue same size */
	repMiddle = repMiddle->prev;
	repMiddle->queue = 2;
	
	ret->queue = 1;
      }
    }
  }
}

Page *loadPage(int pageid) {
  Page * ret = getPage(pageid, RW);
  return ret;
}
