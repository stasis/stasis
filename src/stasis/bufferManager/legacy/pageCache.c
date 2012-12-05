/**
   @file

   pageCache handles the replacement policy for buffer manager.  This
   allows bufferManager's implementation to focus on providing atomic
   writes, and locking.
*/
#include <stasis/common.h>
#include <stasis/flags.h>

#include <stasis/page.h>
#include <stasis/bufferManager.h>
#include <stasis/bufferManager/legacy/pageCache.h>

#include <assert.h>
#include <stdio.h>

static pageid_t bufferSize;
static Page *repHead, *repMiddle, *repTail; /* replacement policy */

int cache_state;


void pageCacheInit(Page * first) {


  bufferSize = 1;
  cache_state  = INITIAL;

  DEBUG("pageCacheInit()");

  first->inCache = 1;

  first->prev = first->next = NULL;
  repHead = repTail = first;
  repMiddle = NULL;


}

void pageCacheDeinit(void) {

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

  assert(cache_state == FULL);

  /*	assert( bufferSize == MAX_BUFFER_SIZE ); */

	assert(ret != repMiddle);
	assert(ret != repTail);
	assert(ret != repHead);

	ret->prev  = repMiddle->prev;
	ret->next = repMiddle;
	repMiddle->prev = ret;
	ret->prev->next = ret;
	ret->pinCount = 2;

	repMiddle = ret;
	assert(ret->next != ret && ret->prev != ret);
}

/** @todo Under high contention, the buffer pool can empty.  What should be done about this, other than making sure that # threads > buffer size? */
static void qRemove(Page *ret) {

  assert(cache_state == FULL);

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

void cacheInsertPage (Page * ret) {
  bufferSize++;
  assert(!ret->inCache);
  ret->inCache ++;
  if(cache_state == FULL) {
    middleInsert(ret);
  } else {
    if(bufferSize == stasis_buffer_manager_size/* - 1*/) {  /* Set up page kick mechanism. */
      Page *iter;

      cache_state = FULL;

      headInsert(ret);
      assert(ret->next != ret && ret->prev != ret);

      /* split up queue:
       * "in all cases studied ... fixing the primary region to 30% ...
       * resulted in the best performance"
       */
      repMiddle = repHead;
      for(pageid_t i = 0; i < stasis_buffer_manager_size / 3; i++ ) {
        repMiddle->pinCount = 1;
        repMiddle = repMiddle->next;
      }

      for( iter = repMiddle; iter; iter = iter->next ) {
	iter->pinCount = 2;
      }
    } else { /* Just insert it. */
      headInsert(ret);
      assert(ret->next != ret && ret->prev != ret);
      assert(ret->next != ret && ret->prev != ret);
    }
  }
}

void cacheRemovePage(Page * ret) {
  assert(ret->inCache);
  qRemove(ret);
  ret->inCache--;
  bufferSize --;
}

void cacheHitOnPage(Page * ret) {
  /* The page may not be in cache if it is about to be freed. */
  if(ret->inCache && cache_state == FULL) { /* we need to worry about page sorting */
    /* move to head */
    if( ret != repHead ) {
      qRemove(ret);
      headInsert(ret);
      assert(ret->next != ret && ret->prev != ret);

      if( ret->pinCount == 2 ) {
	/* keep first queue same size */
	repMiddle = repMiddle->prev;
	repMiddle->pinCount = 2;

	ret->pinCount = 1;
      }
    }
  }
}

Page * cacheStalePage(void) {
  return repTail;
}
