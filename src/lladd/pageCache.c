/**
   @file

   pageCache handles the replacement policy for buffer manager.  This
   allows bufferManager's implementation to focus on providing atomic
   writes, and locking.
*/
#include <config.h>
#include <lladd/common.h>
#include "latches.h"
#include <lladd/pageCache.h>
#include <lladd/bufferManager.h>

#include <assert.h>
#include <pbl/pbl.h>

#include <stdio.h>
#include "page.h" 
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

  /* 	assert( bufferSize == MAX_BUFFER_SIZE ); */
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

static Page *getFreePage() {
  Page *ret;
  if( state == FULL ) { /* kick */
    
    ret = repTail;

    /** Make sure no one else will try to reuse this page. */

    cacheRemovePage(ret);

    /** Temporarily drop the mutex while we wait for outstanding
	operations on the page to complete. */
    
    pthread_mutex_unlock(&loadPagePtr_mutex);


    /** @ todo getFreePage (finalize) needs to yield the getPage mutex,
	but also needs to remove a page from the kick list before
	doing so.  If there is a cache hit on the page that's been
	removed from the kick list, then the cache eviction policy
	code needs o know this, and ignore the hit. -- Done. */

    finalize(ret);  /* This cannot deadlock because each thread can
		       only have outstanding pending events on the
		       page that it's accessing, but they can only
		       hold that lock if the page is in cache.  If the
		       page is in cache, then the thread surely isn't
		       here!  Therefore any threads that finalize will
		       block on can not possibly be blocking on this
		       thread's latches. */

    /*    writelock(ret->loadlatch, 181);  */ /* Don't need the lock here--No one else has a pointer to this page! */

    pthread_mutex_lock(&loadPagePtr_mutex); 

    /* Now that finalize returned, pull ret out of the cache's lookup table. */

    /*     pblHtRemove(activePages, &ret->id, sizeof(int)); */


  

  } else {

    ret = pageAlloc(-1);

    ret->id = -1;
    ret->inCache = 0;
    /*    writelock(ret->loadlatch, 166); */

  }

  return ret;
}

#define RO 0
#define RW 1

Page * getPage(int pageid, int locktype) {
  Page * ret;

  assert(locktype == RO);
  
  pthread_mutex_lock(&loadPagePtr_mutex);

  ret = pblHtLookup(activePages, &pageid, sizeof(int));

  /* Unfortunately, this is a heuristic, as a race condition exists.
     (Until we obtain a readlock on ret, we have no way of knowing if
     we've gotten the correct page.) */

  if(ret) { 
    cacheHitOnPage(ret);
    assert(ret->id == -1 || ret->id == pageid);
  }
  
  pthread_mutex_unlock(&loadPagePtr_mutex);

  if(!ret) {
    ret = dummy_page;
  }
  
  readlock(ret->loadlatch, 217);
  
  while(ret->id != pageid) {  /* Either we got a stale mapping from the HT, or no mapping at all. */

    unlock(ret->loadlatch);

    pthread_mutex_lock(&loadPagePtr_mutex);

    ret = getFreePage();

    pblHtRemove(activePages, &(ret->id), sizeof(int));

    pthread_mutex_unlock(&loadPagePtr_mutex);

    writelock(ret->loadlatch, 231); 
   
    if(ret->id != -1) {
      assert(ret != dummy_page);
      pageWrite(ret);
    }

    pageRealloc(ret, pageid); /* Do we need any special lock here? */

    pageRead(ret);

    unlock(ret->loadlatch);

    pthread_mutex_lock(&loadPagePtr_mutex);

    /* By inserting ret into the cache, we give up the implicit write lock. */

    cacheInsertPage(ret);

    pblHtInsert(activePages, &pageid, sizeof(int), ret); 

    pthread_mutex_unlock(&loadPagePtr_mutex);

    readlock(ret->loadlatch, 217);

  }

  assert(ret->id == pageid);

  return ret;
  
}

/* Page * getPageOld(int pageid, int locktype) {
  Page * ret;
  int spin = 0;

  assert(0);
  
  / ** This wonderful bit of code avoids deadlocks.

      Locking works like this:

        a) get a HT mutex, lookup pageid, did we get a pointer?
	       - yes, release the mutex, so we don't deadlock getting a page lock.
	       - no, keep the mutex, move on to the next part of the function.
  	b) lock whatever pointer the HT returned.  (Safe, since the memory + mutex are allocated exactly one time.)
	c) did we get the right page? 
	       - yes, success!
	       - no,  goto (a)

  * /

  pthread_mutex_lock(&loadPagePtr_mutex);

  do {
    
    do {
      
      if(spin) {
	sched_yield();
      }
      spin ++;
      if(spin > 1000 && (spin % 10000 == 0)) {
	DEBUG("Spinning in pageCache's hashtable lookup: %d\n", spin);
      }
      
      
      ret = pblHtLookup(activePages, &pageid, sizeof(int));
      
      if(ret) {
	
	/ *      pthread_mutex_unlock(&loadPagePtr_mutex);   * /
	
	if(locktype == RO) { 
	  readlock(ret->loadlatch, 147);
	} else {
	  writelock(ret->loadlatch, 149);
	}
	
	/ *      pthread_mutex_lock(&loadPagePtr_mutex);    * /
 
      }

      
    } while (ret && (ret->id != pageid));

    if(ret) {
      cacheHitOnPage(ret);
      pthread_mutex_unlock(&loadPagePtr_mutex);
      assert(ret->id == pageid);
      return ret;
    }
    
    / * OK, we need to handle a cache miss.  This is also tricky.  
       
    If getFreePage needs to kick a page, then it will need a
    writeLock on the thing it kicks, so we drop our mutex here.
    
    But, before we do that, we need to make sure that no one else
    tries to load our page.  We do this by inserting a dummy entry in
    the cache.  Since it's pageid won't match the pageid that we're
    inserting, other threads will spin in the do..while loop untile
    we've loaded the page.
    
    * /
     
    pblHtInsert(activePages, &pageid, sizeof(int), dummy_page);
    
    
    ret = getFreePage();
    
    / * ret is now a pointer that no other thread has access to, and we
       hold a write lock on it * /
    
    pblHtRemove(activePages, &pageid, sizeof(int));
    pblHtInsert(activePages, &pageid, sizeof(int), ret); 
    
    / * writes were here... * /
    
    / *  pthread_mutex_unlock(&loadPagePtr_mutex); * /
    
    if(ret->id != -1) {
      pageWrite(ret);
    }
    
    pageRealloc(ret, pageid);
    
    pageRead(ret);
    
    / * pthread_mutex_lock(&loadPagePtr_mutex); * /
    
    
    
    assert(ret->inCache == 0);
    
    cacheInsertPage(ret);
    
    assert(ret->inCache == 1);
    
    pthread_mutex_unlock(&loadPagePtr_mutex);
    
    if(locktype == RO) {
      readlock(ret->loadlatch, 314);
    } else {
      writelock(ret->loadlatch, 316);
    }

    

    if(locktype == RO) {
      downgradelock(ret->loadlatch);
    }
    
  } while (ret->id != pageid);
    
  return ret;
  }*/
/*
static Page *kickPage(int pageid) {

	Page *ret = repTail;


	assert( bufferSize == MAX_BUFFER_SIZE );

	qRemove(ret);
	pblHtRemove(activePages, &ret->id, sizeof(int));

	/ * It's almost safe to release the mutex here.  The LRU-2
	   linked lists are in a consistent (but under-populated)
	   state, and there is no reference to the page that we're
	   holding in the hash table, so the data structures are
	   internally consistent.  

	   The problem is that that loadPagePtr could be called
	   multiple times with the same pageid, so we need to check
	   for that, or we might load the same page into multiple
	   cache slots, which would cause consistency problems.

	   @todo Don't block while holding the loadPagePtr mutex!
	* /
	
	/ *pthread_mutex_unlock(loadPagePtr_mutex);* /

	/ *pthread_mutex_lock(loadPagePtr_mutex);* /

	writelock(ret->rwlatch, 121);

	/ *  pblHtInsert(activePages, &pageid, sizeof(int), ret); * /

	return ret;
} 

int lastPageId = -1;
Page * lastPage = 0;
*/
/*
static void noteRead(Page * ret) {
  if( bufferSize == MAX_BUFFER_SIZE ) { / * we need to worry about page sorting * /
    / * move to head * /
    if( ret != repHead ) {
      qRemove(ret);
      headInsert(ret);
      assert(ret->next != ret && ret->prev != ret);

      if( ret->queue == 2 ) {
	/ * keep first queue same size * /
	repMiddle = repMiddle->prev;
	repMiddle->queue = 2;
	
	ret->queue = 1;
      }
    }
  }
}


void loadPagePtrFoo(int pageid, int readOnly) { 
  Page * ret;

  pthread_mutex_lock(&loadPagePtr_mutex);
  
  ret = pblHtLookup(activePages, &pageid, sizeof(int));

  getPage(
  
  if(ret) {
    if(readOnly) {
      readlock(ret->rwlatch, 178);
    } else {
      writelock(ret->rwlatch, 180);
    }
    noteRead(ret);

    pthread_mutex_unlock(&loadPagePtr_mutex);

  } else if(bufferSize == MAX_BUFFER_SIZE - 1) {
    
  }

}
*/
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

void *loadPagePtr(int pageid) {
  Page * ret = getPage(pageid, RO);
  return ret;
}

/** @todo loadPagePtr needs to aquire the page read/write lock -- if it does, then should page.c do any locking? */
/*void *loadPagePtr(int pageid) {
	/ * lock activePages, bufferSize * /
	Page *ret;

	pthread_mutex_lock(&(loadPagePtr_mutex));

	if(lastPage && lastPageId == pageid) {
	  void * ret = lastPage;
	  pthread_mutex_unlock(&(loadPagePtr_mutex));
	  
	  return ret;
	} else {
	  ret = pblHtLookup(activePages, &pageid, sizeof(int));
	}

	if( ret ) {
	  / ** Don't need write lock for linked list manipulations.  The loadPagePtr_mutex protects those operations. * /

		if( bufferSize == MAX_BUFFER_SIZE ) { / * we need to worry about page sorting * /
			/ * move to head * /
			if( ret != repHead ) {
				qRemove(ret);
				headInsert(ret);
				assert(ret->next != ret && ret->prev != ret);

				if( ret->queue == 2 ) {
					/ * keep first queue same size * /
					repMiddle = repMiddle->prev;
					repMiddle->queue = 2;

					ret->queue = 1;
				}
			}
		}
		
		lastPage = ret;
		lastPageId = pageid;
		
		/ * DEBUG("Loaded page %d => %x\n", pageid, (unsigned int) ret->memAddr); * /
		pthread_mutex_unlock(&(loadPagePtr_mutex));
		
		return ret;
	} else if( bufferSize == MAX_BUFFER_SIZE ) { / * we need to kick * /
	        ret = kickPage(pageid);
		pageWrite(ret);
		pageRealloc(ret, pageid);
		middleInsert(ret);
		
	} else if( bufferSize == MAX_BUFFER_SIZE-1 ) { / * we need to setup kickPage mechanism * /
	        int i;
		Page *iter;
		
		ret = pageAlloc(pageid);
		bufferSize++;

		pageRealloc(ret, pageid);
		writelock(ret->rwlatch, 224);
		
		headInsert(ret);
		assert(ret->next != ret && ret->prev != ret);
		
		/ * split up queue:
		 * "in all cases studied ... fixing the primary region to 30% ...
		 * resulted in the best performance"
		 * /
		repMiddle = repHead;
		for( i = 0; i < MAX_BUFFER_SIZE / 3; i++ ) {
			repMiddle->queue = 1;
			repMiddle = repMiddle->next;
		}

		for( iter = repMiddle; iter; iter = iter->next ) {
			iter->queue = 2;
		}

	} else { / * we are adding to an nonfull queue * /

		bufferSize++;

		ret = pageAlloc(pageid);

		pageRealloc(ret, pageid);
  
		writelock(ret->rwlatch, 224);
		headInsert(ret);
		assert(ret->next != ret && ret->prev != ret);
		assert(ret->next != ret && ret->prev != ret);

	}
	

	/ * we now have a page we can dump info into * /
	
	

	assert( ret->id == pageid );


	pblHtInsert( activePages, &pageid, sizeof(int), ret );

	lastPage = ret;
	lastPageId = pageid;


	/ * release mutex for this function * /

	pthread_mutex_unlock(&(loadPagePtr_mutex));

	pageRead(ret);

	/ * release write lock on the page. * /

	writeunlock(ret->rwlatch);
	
	/ * 	DEBUG("Loaded page %d => %x\n", pageid, (unsigned int) ret->memAddr); * /
	
	return ret;
}
*/
