/*---
This software is copyrighted by the Regents of the University of
California, and other parties. The following terms apply to all files
associated with the software unless explicitly disclaimed in
individual files.
                                                                                                                                  
The authors hereby grant permission to use, copy, modify, distribute,
and license this software and its documentation for any purpose,
provided that existing copyright notices are retained in all copies
and that this notice is included verbatim in any distributions. No
written agreement, license, or royalty fee is required for any of the
authorized uses. Modifications to this software may be copyrighted by
their authors and need not follow the licensing terms described here,
provided that the new terms are clearly indicated on the first page of
each file where they apply.
                                                                                                                                  
IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY
FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES
ARISING OUT OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY
DERIVATIVES THEREOF, EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
                                                                                                                                  
THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
NON-INFRINGEMENT. THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, AND
THE AUTHORS AND DISTRIBUTORS HAVE NO OBLIGATION TO PROVIDE
MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
                                                                                                                                  
GOVERNMENT USE: If you are acquiring this software on behalf of the
U.S. government, the Government shall have only "Restricted Rights" in
the software and related documentation as defined in the Federal
Acquisition Regulations (FARs) in Clause 52.227.19 (c) (2). If you are
acquiring the software on behalf of the Department of Defense, the
software shall be classified as "Commercial Computer Software" and the
Government shall have only "Restricted Rights" as defined in Clause
252.227-7013 (c) (1) of DFARs. Notwithstanding the foregoing, the
authors grant the U.S. Government and others acting in its behalf
permission to use and distribute the software in accordance with the
terms specified in this license.
---*/
/*******************************
 * $Id$
 *
 * implementation of the page buffer
 * *************************************************/

#include <config.h>
#include <lladd/common.h>
#include <latches.h>
#include <assert.h>

#include <lladd/bufferManager.h>

#include "page.h"
#include "blobManager.h"
#include <lladd/pageCache.h>
#include "pageFile.h"

#include <pbl/pbl.h>


static pblHashTable_t *activePages; /* page lookup */

static pthread_mutex_t loadPagePtr_mutex;

static Page * dummy_page;

int bufInit() {

	pageInit();
	openPageFile();


	pthread_mutex_init(&loadPagePtr_mutex, NULL);

	activePages = pblHtCreate();

	dummy_page = pageAlloc(-1);
	pageRealloc(dummy_page, -1);
	Page *first;
	first = pageAlloc(0);
	pageRealloc(first, 0);
	pblHtInsert(activePages, &first->id, sizeof(int), first);
  
	openBlobStore();

	pageCacheInit(first);

	assert(activePages);

	return 0;
}

void bufDeinit() {

	closeBlobStore();

	Page *p;
	DEBUG("pageCacheDeinit()");
	
	for( p = (Page*)pblHtFirst( activePages ); p; p = (Page*)pblHtNext(activePages)) {

	  pblHtRemove( activePages, 0, 0 )
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
	pageCacheDeinit();
	closePageFile();
	
	pageDeInit();

	return;
}
/**
    Just close file descriptors, don't do any other clean up. (For
    testing.)
*/
void simulateBufferManagerCrash() {
  closeBlobStore();
  closePageFile();
}

/* ** No file I/O below this line. ** */

void releasePage (Page * p) {
  unlock(p->loadlatch);
}

int bufTransCommit(int xid, lsn_t lsn) {

  commitBlobs(xid);
  pageCommit(xid);

  return 0;
}

int bufTransAbort(int xid, lsn_t lsn) {

  abortBlobs(xid);  /* abortBlobs doesn't write any log entries, so it doesn't need the lsn. */
  pageAbort(xid);

  return 0;
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

    if( cache_state == FULL ) {

      /* Select an item from cache, and remove it atomicly. (So it's
	 only reclaimed once) */

      ret = cacheStalePage();
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

  }

  assert(ret->id == pageid);
    
  return ret;
  
}

Page *loadPage(int pageid) {
  Page * ret = getPage(pageid, RO);
  return ret;
}
