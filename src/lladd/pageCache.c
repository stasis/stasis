/**
   @file

   pageCache handles the replacement policy for buffer manager.  This
   allows bufferManager's implementation to focus on providing atomic
   writes, and locking.
*/
#include <lladd/pageCache.h>
#include <assert.h>
#include <pbl/pbl.h>
#include <lladd/constants.h>
#include <lladd/page.h>
#include <stdlib.h>

#include <lladd/bufferManager.h>

static pblHashTable_t *activePages; /* page lookup */
static unsigned int bufferSize; /* < MAX_BUFFER_SIZE */
static Page *repHead, *repMiddle, *repTail; /* replacement policy */

void pageCacheInit() {

  Page *first;
  bufferSize = 1;
  activePages = pblHtCreate();
  assert(activePages);
  
  first = pageAlloc(0);
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

	assert( bufferSize == MAX_BUFFER_SIZE );

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

static void qRemove(Page *ret) {

	assert( bufferSize == MAX_BUFFER_SIZE );
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

static Page *kickPage(int pageid) {

	Page *ret = repTail;

	assert( bufferSize == MAX_BUFFER_SIZE );

	qRemove(ret);
	pblHtRemove(activePages, &ret->id, sizeof(int));
	
	/* if( munmap(ret->memAddr, PAGE_SIZE) ) */
	/*if(pageWrite(ret)) / * flushPage(*ret)) * /
		assert( 0 ); */

	pageWrite(ret);

	pageRealloc(ret, pageid);

	middleInsert(ret);
	pblHtInsert(activePages, &pageid, sizeof(int), ret);

	return ret;
}

int lastPageId = -1;
Page * lastPage = 0;

Page *loadPagePtr(int pageid) {
	/* lock activePages, bufferSize */
	Page *ret;

	if(lastPage && lastPageId == pageid) {
	  DEBUG("Loaded page %d => %x\n", pageid, (unsigned int) lastPage->memAddr);
	  return lastPage;
	} else {
	  ret = pblHtLookup(activePages, &pageid, sizeof(int));
	}

	if( ret ) {
		if( bufferSize == MAX_BUFFER_SIZE ) { /* we need to worry about page sorting */
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

		lastPage = ret;
		lastPageId = pageid;

		DEBUG("Loaded page %d => %x\n", pageid, (unsigned int) ret->memAddr);

		return ret;
	} else if( bufferSize == MAX_BUFFER_SIZE ) { /* we need to kick */
		ret = kickPage(pageid);
	} else if( bufferSize == MAX_BUFFER_SIZE-1 ) { /* we need to setup kickPage mechanism */
		int i;
		Page *iter;

		ret = pageAlloc(pageid);
		headInsert(ret);
		assert(ret->next != ret && ret->prev != ret);

		pblHtInsert( activePages, &pageid, sizeof(int), ret );

		bufferSize++;

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

	} else { /* we are adding to an nonfull queue */

		bufferSize++;

		ret = pageAlloc(pageid);
		headInsert(ret);
		assert(ret->next != ret && ret->prev != ret);
		assert(ret->next != ret && ret->prev != ret);
		pblHtInsert( activePages, &pageid, sizeof(int), ret );
	}

	/* we now have a page we can dump info into */
	assert( ret->id == pageid );

	/*pageMap(ret); */
	pageRead(ret);

	lastPage = ret;
	lastPageId = pageid;

	DEBUG("Loaded page %d => %x\n", pageid, (unsigned int) ret->memAddr);
	
	return ret;
}
