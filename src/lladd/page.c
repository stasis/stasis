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

/**

  @file

  Generic page interface.  This file handles updates to the LSN, but
  leaves finer grained concurrency to the implementor of each of the
  page types.  This interface's primary purpose is to wrap common
  functionality together, and to delegate responsibility for page
  handling to other modules.
  
 Latching summary:

   Each page has an associated read/write lock.  This lock only
   protects the internal layout of the page, and the members of the
   page struct.  Here is how it is held in various circumstances:

   Record allocation:  Write lock
   Record read:        Read lock
   Read LSN            Read lock
   Record write       *READ LOCK*
   Write LSN           Write lock
 
 Any circumstance where these locks are held during an I/O operation
 is a bug.
 
*/


/* _XOPEN_SOURCE is needed for posix_memalign */
#define _XOPEN_SOURCE 600
#include <stdlib.h>

#include <config.h>
#include <lladd/common.h>
#include "latches.h"
#include "page.h"

#include <assert.h>
#include <stdio.h>

#include <lladd/constants.h>
#include <assert.h>
#include "blobManager.h"
#include "pageFile.h"

#include "page/slotted.h"

/* TODO:  Combine with buffer size... */
static int nextPage = 0;

/**
   Invariant: This lock should be held while updating lastFreepage, or
   while performing any operation that may decrease the amount of
   freespace in the page that lastFreepage refers to.  

   Since pageCompact and pageDeRalloc may only increase this value,
   they do not need to hold this lock.  Since bufferManager is the
   only place where pageRalloc is called, pageRalloc does not obtain
   this lock.
*/
pthread_mutex_t lastFreepage_mutex;
unsigned int lastFreepage = 0;



/* ------ */

static pthread_mutex_t pageAllocMutex;
/** We need one dummy page for locking purposes, so this array has one extra page in it. */
Page pool[MAX_BUFFER_SIZE+1];

/**
 * pageWriteLSN() assumes that the page is already loaded in memory.  It takes
 * as a parameter a Page.  The Page struct contains the new LSN and the page
 * number to which the new LSN must be written to.
 *
 * @param page You must have a writelock on page before calling this function.
 */
void pageWriteLSN(Page * page, lsn_t lsn) {
  /* unlocked since we're only called by a function that holds the writelock. */
  /*  *(long *)(page->memAddr + START_OF_LSN) = page->LSN; */
  if(page->LSN < lsn) {
    page->LSN = lsn;
    *lsn_ptr(page) = page->LSN;
  } 
}

/**
 * pageReadLSN() assumes that the page is already loaded in memory.  It takes
 * as a parameter a Page and returns the LSN that is currently written on that
 * page in memory.
 */
lsn_t pageReadLSN(const Page * page) {
  lsn_t ret;

  readlock(page->rwlatch, 259); 
  /*  ret = *(long *)(page->memAddr + START_OF_LSN); */
  ret = *lsn_ptr(page);
  readunlock(page->rwlatch); 

  return ret;
}



static void pageReallocNoLock(Page *p, int id) {
  p->id = id;
  p->LSN = 0;
  p->dirty = 0;
  /*  assert(p->pending == 0);
  assert(p->waiting == 1);
  p->waiting = 0;*/
}

/* ----- end static functions ----- */

/* ----- (de)initialization functions.  Do not need to support multithreading. -----*/

/**
 * pageInit() initializes all the important variables needed in
 * all the functions dealing with pages.
 */
void pageInit() {

  nextPage = 0;
	/**
	 * For now, we will assume that slots are 4 bytes long, and that the
	 * first two bytes are the offset, and the second two bytes are the
	 * the length.  There are some functions at the bottom of this file
	 * that may be useful later if we decide to dynamically choose
	 * sizes for offset and length.
	 */

	/**
	 * the largest a slot length can be is the size of the page,
	 * and the greatest offset at which a record could possibly 
	 * start is at the end of the page
	 */
  /*	SLOT_LENGTH_SIZE = SLOT_OFFSET_SIZE = 2; / * in bytes * /
	SLOT_SIZE = SLOT_OFFSET_SIZE + SLOT_LENGTH_SIZE;

	LSN_SIZE = sizeof(long);
	FREE_SPACE_SIZE = NUMSLOTS_SIZE = 2;

	/ * START_OF_LSN is the offset in the page to the lsn * /
	START_OF_LSN = PAGE_SIZE - LSN_SIZE;
	START_OF_FREE_SPACE = START_OF_LSN - FREE_SPACE_SIZE;
	START_OF_NUMSLOTS = START_OF_FREE_SPACE - NUMSLOTS_SIZE;

	MASK_0000FFFF = (1 << (2*BITS_PER_BYTE)) - 1;
	MASK_FFFF0000 = ~MASK_0000FFFF;
*/
	
	pthread_mutex_init(&pageAllocMutex, NULL);
	for(int i = 0; i < MAX_BUFFER_SIZE+1; i++) {
	  pool[i].rwlatch = initlock();
	  pool[i].loadlatch = initlock();
	  assert(!posix_memalign((void*)(&(pool[i].memAddr)), PAGE_SIZE, PAGE_SIZE));
	}

	pthread_mutex_init(&lastFreepage_mutex , NULL);
	lastFreepage = 0;


}

void pageDeInit() {
  for(int i = 0; i < MAX_BUFFER_SIZE+1; i++) {

    deletelock(pool[i].rwlatch);
    deletelock(pool[i].loadlatch);
    free(pool[i].memAddr);
  }
}

void pageCommit(int xid) {
}

void pageAbort(int xid) {
}

/**
   @todo DATA CORRUPTION BUG pageAllocMultiple needs to scan forward in the store file until
   it finds page(s) with type = UNINITIALIZED_PAGE.  Otherwise, after recovery, it will trash the storefile.

   A better way to implement this is probably to reserve the first
   slot of the first page in the storefile for metadata, and to keep
   lastFreepage there, instead of in RAM.
*/
int pageAllocMultiple(int newPageCount) {
  pthread_mutex_lock(&lastFreepage_mutex);  
  int ret = lastFreepage+1;  /* Currently, just discard the current page. */
  lastFreepage += (newPageCount + 1);
  pthread_mutex_unlock(&lastFreepage_mutex);
  return ret;
}

/** @todo ralloc ignores it's xid parameter; change the interface? 
    @todo ralloc doesn't set the page type, and interacts poorly with other methods that allocate pages.  

*/
recordid ralloc(int xid, long size) {
  
  recordid ret;
  Page * p;
  
  /*  DEBUG("Rallocing record of size %ld\n", (long int)size); */
  
  assert(size < BLOB_THRESHOLD_SIZE);
  
  pthread_mutex_lock(&lastFreepage_mutex);  
  p = loadPage(lastFreepage);
  *page_type_ptr(p) = SLOTTED_PAGE;
  while(freespace(p) < size ) { 
    releasePage(p);
    lastFreepage++;
    p = loadPage(lastFreepage);
    *page_type_ptr(p) = SLOTTED_PAGE;
  }
  
  ret = pageRalloc(p, size);
    
  releasePage(p);

  pthread_mutex_unlock(&lastFreepage_mutex);
  
  /*  DEBUG("alloced rid = {%d, %d, %ld}\n", ret.page, ret.slot, ret.size); */

  return ret;
}




/** @todo Does pageRealloc really need to obtain a lock? */
void pageRealloc(Page *p, int id) {
  writelock(p->rwlatch, 10);
  pageReallocNoLock(p,id);
  writeunlock(p->rwlatch);
}


/** 
	Allocate a new page. 
        @param id The id of the new page.
	@return A pointer to the new page.  This memory is part of a pool, 
	        and should never be freed manually.
 */
Page *pageAlloc(int id) {
  Page *page;

  pthread_mutex_lock(&pageAllocMutex);
  
  page = &(pool[nextPage]);
  
  nextPage++;
  /* There's a dummy page that we need to keep around, thus the +1 */
  assert(nextPage <= MAX_BUFFER_SIZE + 1); 

  pthread_mutex_unlock(&pageAllocMutex);

  return page;
}

void writeRecord(int xid, Page * p, lsn_t lsn, recordid rid, const void *dat) {
  

  if(rid.size > BLOB_THRESHOLD_SIZE) {
    /*    DEBUG("Writing blob.\n"); */
    writeBlob(xid, p, lsn, rid, dat);

  } else {
    /*    DEBUG("Writing record.\n"); */

    assert( (p->id == rid.page) && (p->memAddr != NULL) );	

    pageWriteRecord(xid, p, lsn, rid, dat);

    assert( (p->id == rid.page) && (p->memAddr != NULL) );	

  }
  
  writelock(p->rwlatch, 225);  /* Need a writelock so that we can update the lsn. */

  pageWriteLSN(p, lsn);

  unlock(p->rwlatch);    

}

void readRecord(int xid, Page * p, recordid rid, void *buf) {
  if(rid.size > BLOB_THRESHOLD_SIZE) {
    /*    DEBUG("Reading blob. xid = %d rid = { %d %d %ld } buf = %x\n", 
	  xid, rid.page, rid.slot, rid.size, (unsigned int)buf); */
    /* @todo should readblob take a page pointer? */
    readBlob(xid, p, rid, buf);
  } else {
    assert(rid.page == p->id); 
    /*    DEBUG("Reading record xid = %d rid = { %d %d %ld } buf = %x\n", 
	  xid, rid.page, rid.slot, rid.size, (unsigned int)buf); */
    pageReadRecord(xid, p, rid, buf);
    assert(rid.page == p->id); 
  }
}

