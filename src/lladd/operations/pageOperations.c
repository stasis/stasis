#include "../page.h"
#include <lladd/operations/pageOperations.h>
#include <assert.h>
#include "../page/slotted.h"
int __pageAlloc(int xid, Page * p, lsn_t lsn, recordid r, const void * d) {
  int type = *(int*)d;

  *page_type_ptr(p) = type;
  /** @todo this sort of thing should be done in a centralized way. */
  if(type == SLOTTED_PAGE) {
    slottedPageInitialize(p);
  }

  return 0;

}

int __pageDealloc(int xid, Page * p, lsn_t lsn, recordid r, const void * d) {
  *page_type_ptr(p) = UNINITIALIZED_PAGE;
  return 0;
}

int __pageSet(int xid, Page * p, lsn_t lsn, recordid r, const void * d) {
  memcpy(p->memAddr, d, PAGE_SIZE);
  pageWriteLSN(p, lsn);
  return 0;
}
int TpageSet(int xid, int pageid, Page* p) {
  recordid rid;
  rid.page = pageid;
  rid.slot = 0;
  rid.size = 0;
  Tupdate(xid,rid,p->memAddr, OPERATION_PAGE_SET);
  return 0;
}

/** @todo Need to re-think TpageDealloc/TpageAlloc's logging
strategies when we implement page re-use. Currently, TpageDealloc can
use logical logging.  Perhaps TpageDealloc should use physical
logging, and wipe the page to zero, while TpageAlloc should continue to
use logical logging.  (Have we ever had operation's whose inverses
took differnt types of log entries?  Do such operations work?) */

int TpageDealloc(int xid, int pageid) {
  recordid rid;
  rid.page = pageid;
  rid.slot = 0;
  rid.size = 0;

  Page * p = loadPage(pageid);
  int type = *page_type_ptr(p);
  releasePage(p);

  Tupdate(xid, rid, &type, OPERATION_PAGE_DEALLOC);
  return 0;
}

int TpageAlloc(int xid, int type) {
  recordid rid;

  int pageid = pageAlloc();

  rid.page = pageid;
  rid.slot = 0;
  rid.size = 0;

  Tupdate(xid, rid, &type, OPERATION_PAGE_ALLOC);
  return pageid;
}
/** Allocs an extent of pages.  @todo CONCURRENCY BUG TpageAllocMany
    can not be concurrent until ralloc uses TpageAlloc to allocate new
    records. (And. concurrency for TpageAllocMany hasn't been
    implemented yet...
*/
int TpageAllocMany(int xid, int count, int type) {
  int firstPage = -1;
  int lastPage = -1;
  for(int i = 0 ; i < count; i++) {
    int thisPage = TpageAlloc(xid, type);
    if(lastPage == -1) {
      firstPage = lastPage = thisPage;
    } else {
      assert((lastPage +1) == thisPage);
      lastPage = thisPage;
    }
  }
  return firstPage;
}

Operation getPageAlloc() {
  Operation o = {
    OPERATION_PAGE_ALLOC,
    sizeof(int),
    OPERATION_PAGE_DEALLOC,
    &__pageAlloc
  };
  return o;
}

Operation getPageDealloc() {
  Operation o = {
    OPERATION_PAGE_DEALLOC,
    sizeof(int),
    OPERATION_PAGE_ALLOC,
    &__pageDealloc
  };
  return o;
}

Operation getPageSet() {
  Operation o = {
    OPERATION_PAGE_SET,
    PAGE_SIZE,              /* This is the type of the old page, for undo purposes */
    /*OPERATION_PAGE_SET, */  NO_INVERSE_WHOLE_PAGE, 
    &__pageSet
  };
  return o;
}


