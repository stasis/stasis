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
#include <lladd/lockManager.h>
#include "pageFile.h"
#include <lladd/compensations.h>
#include "page/slotted.h"
#include "page/fixed.h"

/* TODO:  Combine with buffer size... */
static int nextPage = 0;

static int lastAllocedPage;
static pthread_mutex_t lastAllocedPage_mutex;



/* ------ */

static pthread_mutex_t pageMallocMutex;
/** We need one dummy page for locking purposes, so this array has one extra page in it. */
Page pool[MAX_BUFFER_SIZE+1];

void pageWriteLSN(int xid, Page * page, lsn_t lsn) {
  /* unlocked since we're only called by a function that holds the writelock. */
  /*  *(long *)(page->memAddr + START_OF_LSN) = page->LSN; */
  
  /*  tr y {
    if(globalLockM anager.writ eLockPage) { 
      globalLock Manager.writeL ockPage(xid, page->id); 
    }
    } en d; */

  if(page->LSN < lsn) {
    page->LSN = lsn;
    *lsn_ptr(page) = page->LSN;
  } 
  page->dirty = 1;
  return;
}

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
}

/* ----- end static functions ----- */

/* ----- (de)initialization functions.  Do not need to support multithreading. -----*/

/**
 * pageInit() initializes all the important variables needed in
 * all the functions dealing with pages.
 */
void pageInit() {

  nextPage = 0;
	
  pthread_mutex_init(&pageMallocMutex, NULL);

  for(int i = 0; i < MAX_BUFFER_SIZE+1; i++) {
    pool[i].rwlatch = initlock();
    pool[i].loadlatch = initlock();
#ifdef HAVE_POSIX_MEMALIGN
    // Note that the backup behavior for posix_memalign breaks O_DIRECT.  Therefore, O_DIRECT should be
    // disabled whenever posix_memalign is not present. 
    int ret = posix_memalign((void*)(&(pool[i].memAddr)), PAGE_SIZE, PAGE_SIZE);
    assert(!ret);
#else
//#warn Not using posix_memalign
    pool[i].memAddr = malloc(PAGE_SIZE);
    assert(pool[i].memAddr);
#endif
  }
  pthread_mutex_init(&lastAllocedPage_mutex , NULL);
	
  lastAllocedPage = 0; 
  slottedPageInit();

}

void pageDeInit() {
  for(int i = 0; i < MAX_BUFFER_SIZE+1; i++) {
    deletelock(pool[i].rwlatch);
    deletelock(pool[i].loadlatch);
    free(pool[i].memAddr); // breaks efence
  }
  pthread_mutex_destroy(&lastAllocedPage_mutex);
}

void pageCommit(int xid) {
}

void pageAbort(int xid) {
}
/*
static int pageAllocUnlocked() {
  int ret = lastAllocedPage;
  Page * p;

  lastAllocedPage += 1;
  
  p = load Page(lastAllocedPage);
  / ** TODO Incorrect, but this kludge tricks the tests (for now) * /
  while(*page_type_ptr(p) != UNINITIALIZED_PAGE) {
    releasePage(p);
    lastAllocedPage++;
    p = load Page(lastAllocedPage);
  }
  releasePage(p);

  return ret;
}*/

/**
   @todo DATA CORRUPTION BUG pageAllocMultiple needs to scan forward in the store file until
   it finds page(s) with type = UNINITIALIZED_PAGE.  Otherwise, after recovery, it will trash the storefile.

   A better way to implement this is probably to reserve the first
   slot of the first page in the storefile for metadata, and to keep
   lastFreepage there, instead of in RAM.
*/
/*int pageAlloc() {
  pthread_mutex_lock(&lastAllocedPage_mutex);  
  int ret = pageAllocUnlocked();
  pthread_mutex_unlock(&lastAllocedPage_mutex);
  return ret;
  }*/




/** @todo Does pageRealloc really need to obtain a lock? */
void pageRealloc(Page *p, int id) {
  writelock(p->rwlatch, 10);
  pageReallocNoLock(p,id);
  writeunlock(p->rwlatch);
}


/** 
	Allocate a new page. 
	@return A pointer to the new page.  This memory is part of a pool, 
	        and should never be freed manually.  Instead, you can 
		reclaim it with pageRealloc()
 */
Page *pageMalloc() {
  Page *page;

  pthread_mutex_lock(&pageMallocMutex);
  
  page = &(pool[nextPage]);
  
  nextPage++;
  /* There's a dummy page that we need to keep around, thus the +1 */
  assert(nextPage <= MAX_BUFFER_SIZE + 1); 

  pthread_mutex_unlock(&pageMallocMutex);

  return page;
}

/*void setRecordType(Page * page, recordid rid, int slot_type) {
  if(*page_type_ptr(page) == SLOTTED_PAGE) {
    slottedSetType(page, rid.slot, slot_type);
  }
  }*/


void writeRecord(int xid, Page * p, lsn_t lsn, recordid rid, const void *dat) {

  assert( (p->id == rid.page) && (p->memAddr != NULL) );	

  
  /*  writelock(p->rwlatch, 225);  // Need a writelock so that we can update the lsn. 
  int lock_ret = pageWriteLSN(xid, p, lsn);
  unlock(p->rwlatch);    
  if(lock_ret) {
    return lock_ret;
    } */

  writelock(p->rwlatch, 225);
  pageWriteLSN(xid, p, lsn);
  unlock(p->rwlatch);
  
  if(rid.size > BLOB_THRESHOLD_SIZE) {
    writeBlob(xid, p, lsn, rid, dat);
  } else if(*page_type_ptr(p) == SLOTTED_PAGE) {
    slottedWrite(xid, p, lsn, rid, dat);
  } else if(*page_type_ptr(p) == FIXED_PAGE  || *page_type_ptr(p)==ARRAY_LIST_PAGE || !*page_type_ptr(p) )  {
    fixedWrite(p, rid, dat);
  } else {
    abort();
  }
  assert( (p->id == rid.page) && (p->memAddr != NULL) );	
  
}

int readRecord(int xid, Page * p, recordid rid, void *buf) {
  assert(rid.page == p->id); 
  
  int page_type = *page_type_ptr(p);

  if(rid.size > BLOB_THRESHOLD_SIZE) {
    readBlob(xid, p, rid, buf);
  } else if(page_type == SLOTTED_PAGE) {
    slottedRead(xid, p, rid, buf);
    /* FIXED_PAGES can function correctly even if they have not been
       initialized. */
  } else if(page_type == FIXED_PAGE || page_type==ARRAY_LIST_PAGE || !page_type) { 
    fixedRead(p, rid, buf);
  } else {
    abort();
  }
  assert(rid.page == p->id); 

  return 0;
}


int readRecordUnlocked(int xid, Page * p, recordid rid, void *buf) {
  assert(rid.page == p->id); 
  
  int page_type = *page_type_ptr(p);

  if(rid.size > BLOB_THRESHOLD_SIZE) {
    abort(); /* Unsupported for now. */
    readBlob(xid, p, rid, buf);
  } else if(page_type == SLOTTED_PAGE) {
    slottedReadUnlocked(xid, p, rid, buf);
    /* FIXED_PAGES can function correctly even if they have not been
       initialized. */
  } else if(page_type == FIXED_PAGE || !page_type) { 
    fixedReadUnlocked(p, rid, buf);
  } else {
    abort();
  }
  assert(rid.page == p->id); 

  return 0;
}

int getRecordTypeUnlocked(int xid, Page * p, recordid rid) {
  assert(rid.page == p->id);
  
  int page_type = *page_type_ptr(p);
  if(page_type == UNINITIALIZED_PAGE) {
    return UNINITIALIZED_RECORD;	
    
  } else if(page_type == SLOTTED_PAGE) {
    if(*numslots_ptr(p) <= rid.slot || *slot_ptr(p, rid.slot) == INVALID_SLOT /*|| *slot_length_ptr(p, rid.slot) == INVALID_SLOT*/) {
      return UNINITIALIZED_PAGE;
      //    } else if(*slot_length_ptr(p, rid.slot) == BLOB_REC_SIZE) {
    } else if (*slot_length_ptr(p, rid.slot) == BLOB_SLOT) {
      return BLOB_RECORD; 
    } else {
      return SLOTTED_RECORD;
    }
  } else if(page_type == FIXED_PAGE || page_type == ARRAY_LIST_PAGE) {
    return  (fixedPageCount(p) > rid.slot) ? 
      FIXED_RECORD : UNINITIALIZED_RECORD;
  } else {
    abort();
    return UNINITIALIZED_RECORD;
  }
}

int getRecordType(int xid, Page * p, recordid rid) {
	readlock(p->rwlatch, 343);
	int ret = getRecordTypeUnlocked(xid, p, rid);
	unlock(p->rwlatch);
	return ret;
}
/** @todo implement getRecordLength for blobs and fixed length pages. */
int getRecordSize(int xid, Page * p, recordid rid) {
  readlock(p->rwlatch, 353);
  int ret = getRecordTypeUnlocked(xid, p, rid);
  if(ret == UNINITIALIZED_RECORD) {
    ret = -1;
  } else if(ret == SLOTTED_RECORD) {
    ret = *slot_length_ptr(p, rid.slot);
  } else { 
    abort(); // unimplemented for fixed length pages and blobs.
  }
  unlock(p->rwlatch);
  return ret;
}

void writeRecordUnlocked(int xid, Page * p, lsn_t lsn, recordid rid, const void *dat) {

  assert( (p->id == rid.page) && (p->memAddr != NULL) );	

  /*  writelock(p->rwlatch, 225);  // Need a writelock so that we can update the lsn. 
  int lock_error = pageWriteLSN(xid, p, lsn);
  if(lock_error) {
    unlock(p->rwlatch);
    return lock_error;
  }
  unlock(p->rwlatch);    */


  // Need a writelock so that we can update the lsn. 

  writelock(p->rwlatch, 225);
  pageWriteLSN(xid, p, lsn);
  unlock(p->rwlatch);


  if(rid.size > BLOB_THRESHOLD_SIZE) {
    abort();
    writeBlob(xid, p, lsn, rid, dat);
  } else if(*page_type_ptr(p) == SLOTTED_PAGE) {
    slottedWriteUnlocked(xid, p, lsn, rid, dat);
  } else if(*page_type_ptr(p) == FIXED_PAGE  || *page_type_ptr(p)==ARRAY_LIST_PAGE || !*page_type_ptr(p) )  {
    fixedWriteUnlocked(p, rid, dat);
  } else {
    abort();
  }
  assert( (p->id == rid.page) && (p->memAddr != NULL) );	
  

}
