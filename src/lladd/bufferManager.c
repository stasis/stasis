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

#ifdef PROFILE_LATCHES_WRITE_ONLY

#define _GNU_SOURCE   
#include <stdio.h>  // Need _GNU_SOURCE for asprintf
#include <lladd/lhtable.h>

#endif

#include <lladd/common.h>
#include <latches.h>
#include <assert.h>

#include <lladd/bufferManager.h>

#include <lladd/bufferPool.h>

#include <lladd/lockManager.h>
#include "page.h"
#include "blobManager.h"
#include <lladd/pageCache.h>
#include "pageFile.h"
#include <pbl/pbl.h>

#include <lladd/truncation.h>

#undef loadPage
#undef releasePage
#undef Page

#ifdef LONG_TEST 
#define PIN_COUNT
#endif

#ifdef PROFILE_LATCHES_WRITE_ONLY

// These should only be defined if PROFILE_LATCHES_WRITE_ONLY is set.

#undef loadPage
#undef releasePage

pthread_mutex_t profile_load_mutex = PTHREAD_MUTEX_INITIALIZER;
struct LH_ENTRY(table) * profile_load_hash = 0;
struct LH_ENTRY(table) * profile_load_pins_hash = 0;

#endif

#ifdef PIN_COUNT
pthread_mutex_t pinCount_mutex = PTHREAD_MUTEX_INITIALIZER;
int pinCount = 0;
#endif

static pblHashTable_t *activePages; /* page lookup */
/*static Page * activePagePtrs[MAX_BUFFER_SIZE];*/


static pthread_mutex_t loadPagePtr_mutex;

static Page * dummy_page;

int bufInit() {

	pageInit();
	openPageFile();


	pthread_mutex_init(&loadPagePtr_mutex, NULL);

	activePages = pblHtCreate(); 

	dummy_page = pageMalloc();
	pageFree(dummy_page, -1);
	Page *first;
	first = pageMalloc();
	pageFree(first, 0);
	pblHtInsert(activePages, &first->id, sizeof(int), first); 

	openBlobStore();

	pageCacheInit(first);

	assert(activePages);
#ifdef PROFILE_LATCHES_WRITE_ONLY
    profile_load_hash = LH_ENTRY(create)(10);
    profile_load_pins_hash = LH_ENTRY(create)(10);
#endif
	return 0;
}

void bufDeinit() {

	closeBlobStore();

	Page *p;
	DEBUG("pageCacheDeinit()");

	for( p = (Page*)pblHtFirst( activePages ); p; p = (Page*)pblHtNext(activePages)) { 

	  pblHtRemove( activePages, 0, 0 );
	  DEBUG("+");
	  pageWrite(p);

	}
	
	pthread_mutex_destroy(&loadPagePtr_mutex);
	
	pblHtDelete(activePages);
	pageCacheDeinit();
	closePageFile();
	
	pageDeInit();
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
*/
void simulateBufferManagerCrash() {
  closeBlobStore();
  closePageFile();
#ifdef PIN_COUNT
  pinCount = 0;
#endif
}

void releasePage (Page * p) {
  unlock(p->loadlatch);
#ifdef PIN_COUNT
  pthread_mutex_lock(&pinCount_mutex);
  pinCount --;
  pthread_mutex_unlock(&pinCount_mutex);
#endif

}

static Page * getPage(int pageid, int locktype) {
  Page * ret;
  int spin  = 0;

  pthread_mutex_lock(&loadPagePtr_mutex);
  ret = pblHtLookup(activePages, &pageid, sizeof(int));

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
    ret = pblHtLookup(activePages, &pageid, sizeof(int));

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
    pblHtInsert(activePages, &pageid, sizeof(int), ret); 

    pthread_mutex_unlock(&loadPagePtr_mutex); 

    /* Could writelock(ret) go here? */

    assert(ret != dummy_page);
    if(ret->id != -1) { 
      pageWrite(ret);
    }

    pageFree(ret, pageid);

    pageRead(ret);

    writeunlock(ret->loadlatch);
 
    pthread_mutex_lock(&loadPagePtr_mutex);

    pblHtRemove(activePages, &(oldid), sizeof(int)); 

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
      return getPage(pageid, locktype);
    }

  }
  return ret;
}

#ifdef PROFILE_LATCHES_WRITE_ONLY

compensated_function Page * __profile_loadPage(int xid, int pageid, char * file, int line) { 

  Page * ret = loadPage(xid, pageid);


  pthread_mutex_lock(&profile_load_mutex);

  char * holder = LH_ENTRY(find)(profile_load_hash, &ret, sizeof(void*));
  int * pins = LH_ENTRY(find)(profile_load_pins_hash, &ret, sizeof(void*));

  if(!pins) { 
    pins = malloc(sizeof(int));
    *pins = 0;
    LH_ENTRY(insert)(profile_load_pins_hash, &ret, sizeof(void*), pins);
  }

  if(*pins) { 
    assert(holder);
    char * newHolder;
    asprintf(&newHolder, "%s\n%s:%d", holder, file, line);
    free(holder);
    holder = newHolder;
  } else { 
    assert(!holder);
    asprintf(&holder, "%s:%d", file, line);
  }
  (*pins)++;
  LH_ENTRY(insert)(profile_load_hash, &ret, sizeof(void*), holder);
  pthread_mutex_unlock(&profile_load_mutex);

  return ret;

}


compensated_function void  __profile_releasePage(Page * p) { 
  pthread_mutex_lock(&profile_load_mutex);

  //  int pageid = p->id;
  int * pins = LH_ENTRY(find)(profile_load_pins_hash, &p, sizeof(void*));

  assert(pins);

  if(*pins == 1) {
    
    char * holder = LH_ENTRY(remove)(profile_load_hash, &p, sizeof(void*));
    assert(holder);
    free(holder); 

  }

  (*pins)--;

  pthread_mutex_unlock(&profile_load_mutex);

  releasePage(p);


}

#endif

compensated_function Page *loadPage(int xid, int pageid) {

  try_ret(NULL) {
    if(globalLockManager.readLockPage) { globalLockManager.readLockPage(xid, pageid); }
  } end_ret(NULL);


  Page * ret = getPage(pageid, RO);

#ifdef PIN_COUNT
  pthread_mutex_lock(&pinCount_mutex);
  pinCount ++;
  pthread_mutex_unlock(&pinCount_mutex);
#endif

  return ret;
}

