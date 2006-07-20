#define _XOPEN_SOURCE 600
#include <stdlib.h>

#include "../page.h"
#include <lladd/operations/pageOperations.h>
#include <assert.h>
/*#include "../page/slotted.h"*/
#include "../page/header.h"
#include "../pageFile.h"
#include "../page/fixed.h"
#include <alloca.h>

static pthread_mutex_t pageAllocMutex;

int __pageSet(int xid, Page * p, lsn_t lsn, recordid r, const void * d) {
  memcpy(p->memAddr, d, PAGE_SIZE);
  pageWriteLSN(xid, p, lsn);
  return 0;
}

compensated_function int TpageGet(int xid, int pageid, byte *memAddr) {
  Page * q = 0;
  try_ret(compensation_error()) {
    q = loadPage(xid, pageid);
    memcpy(memAddr, q->memAddr, PAGE_SIZE);
  } end_ret(compensation_error());
  return 0;
}

compensated_function int TpageSet(int xid, int pageid, byte * memAddr) {
  recordid rid;
  rid.page = pageid;
  rid.slot = 0;
  rid.size = 0;
  try_ret(compensation_error()) { 
    Tupdate(xid,rid,memAddr, OPERATION_PAGE_SET);
  } end_ret(compensation_error());
  return 0;
}


/** 
    This calls loadPage and releasePage directly, and bypasses the
    logger.
*/
compensated_function void pageOperationsInit() {

  regionsInit();

  pthread_mutex_init(&pageAllocMutex, NULL);
}


/** @todo TpageAlloc / TpageDealloc + undo + page reuse is not multi-transaction / threadsafe. 
    
   Example of the problem:

   T1                              T2
   dealloc(100)
  (a)    list ptr 30 -> 100
  (b)    p(100) nil -> 30
                                   alloc() -> 100       <- Can't allow this to happen!
                                   list_ptr 100 -> 30
                                   alloc() -> 30
                                   list_ptr 30 -> 20
  abort();

  // Really just needs to remove 100 from the linked list.  Instead,
  we use physical, value based locking. 

  list ptr 20 <- 30   <- Oops! Page 30 is in use, and we lose the rest 
                         of the freelist, starting at 20!
      
  The partial solution: dealloc() aquires a lock on the freelist until
  commit / abort.  If other transactions need to allocate when the
  lock is held, then they simply do not reuse pages.  Since locking is
  not yet implemented, we require applications to manually serialize
  transactions that call Talloc or Tdealloc

  A better solution: defer the addition of 100 to the freelist until
  commit, and use a 'real' data structure, like a concurrent B-Tree.

*/

compensated_function int TpageDealloc(int xid, int pageid) {
  TregionDealloc(xid, pageid); // @todo inefficient hack!
  return 0;
}

compensated_function int TpageAlloc(int xid /*, int type */) {
  return TregionAlloc(xid, 1, STORAGE_MANAGER_NAIVE_PAGE_ALLOC);
}

int __fixedPageAlloc(int xid, Page * p, lsn_t lsn, recordid r, const void * d) {
  fixedPageInitialize(p, r.size, recordsPerPage(r.size));
  pageWriteLSN(xid, p, lsn);
  return 0;
}


/**
    @return a recordid.  The page field contains the page that was
    allocated, the slot field contains the number of slots on the
    apge, and the size field contains the size of each slot.
*/
recordid TfixedPageAlloc(int xid, int size) {
  int page = TpageAlloc(xid);
  recordid rid = {page, recordsPerPage(size), size};
  Tupdate(xid, rid, 0, OPERATION_FIXED_PAGE_ALLOC);
  return rid;
}

Operation getFixedPageAlloc() {
  Operation o = {
    OPERATION_FIXED_PAGE_ALLOC,
    0,
    OPERATION_NOOP,
    &__fixedPageAlloc
  };
  return o;
}

compensated_function int TpageAllocMany(int xid, int count /*, int type*/) {
  return TregionAlloc(xid, count, STORAGE_MANAGER_NAIVE_PAGE_ALLOC);
  //  return 0;//newpage;
}

/** Safely allocating and freeing pages is suprisingly complex.  Here is a summary of the process:

   Alloc:

     obtain mutex
        choose a free page using in-memory data
	load page to be used, and update in-memory data.  (obtains lock on loaded page)
	T update() the page, zeroing it, and saving the old successor in the log.
	relase the page (avoid deadlock in next step)
	T update() LLADD's header page (the first in the store file) with a new copy of 
	                              the in-memory data, saving old version in the log.
     release mutex

   Free: 

     obtain mutex
        determine the current head of the freelist using in-memory data
        T update() the page, initializing it to be a freepage, and physically logging the old version
	release the page
	T update() LLADD's header page with a new copy of the in-memory data, saving old version in the log
     release mutex

*/

/** frees a page by zeroing it, setting its type to LLADD_FREE_PAGE,
    and setting the successor pointer. This operation physically logs
    a whole page, which makes it expensive.  Doing so is necessary in
    general, but it is possible that application specific logic could
    avoid the physical logging here. 
    
    Instead, we should just record the fact that the page was freed
    somewhere.  That way, we don't need to read the page in, or write
    out information about it.  If we lock the page against
    reallocation until the current transaction commits, then we're
    fine.

*/

Operation getPageSet() {
  Operation o = {
    OPERATION_PAGE_SET,
    PAGE_SIZE,              /* This is the type of the old page, for undo purposes */
    /*OPERATION_PAGE_SET, */  NO_INVERSE_WHOLE_PAGE, 
    &__pageSet
  };
  return o;
}
