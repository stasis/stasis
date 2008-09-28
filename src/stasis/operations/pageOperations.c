#define _XOPEN_SOURCE 600
#include <stdlib.h>

#include "config.h"
#include <stasis/page.h>
#include <stasis/operations/pageOperations.h>
#include <assert.h>
#include <alloca.h>

static pthread_mutex_t pageAllocMutex;

static int op_page_set_range(const LogEntry* e, Page* p) {
  assert(e->update.arg_size >= sizeof(int));
  assert(!((e->update.arg_size - sizeof(int)) % 2));

  int off = *(int*)getUpdateArgs(e);
  int len = (e->update.arg_size - sizeof(int)) >> 1;

  assert(off+len <=PAGE_SIZE);

  memcpy(p->memAddr + off, getUpdateArgs(e)+sizeof(int), len);
  return 0;
}
static int op_page_set_range_inverse(const LogEntry* e, Page* p) {
  assert(e->update.arg_size >= sizeof(int));
  assert(!((e->update.arg_size - sizeof(int)) % 2));

  int off = *(int*)getUpdateArgs(e);
  int len = (e->update.arg_size - sizeof(int)) >> 1;

  assert(off+len <=PAGE_SIZE);

  memcpy(p->memAddr + off, getUpdateArgs(e)+sizeof(int)+len, len);
  return 0;
}

compensated_function int TpageGet(int xid, int pageid, void *memAddr) {
  Page * q = 0;
  try_ret(compensation_error()) {
    q = loadPage(xid, pageid);
    memcpy(memAddr, q->memAddr, PAGE_SIZE);
  } end_ret(compensation_error());
  try_ret(compensation_error()) {
    releasePage(q);
  } end_ret(compensation_error());
  return 0;
}

compensated_function int TpageSet(int xid, int pageid, const void * memAddr) {
  return TpageSetRange(xid, pageid, 0, memAddr, PAGE_SIZE);
}

int TpageSetRange(int xid, int pageid, int offset, const void * memAddr, int len) {
  // XXX need to pack offset into front of log entry

  recordid rid;
  rid.page = pageid;
  rid.slot = 0;
  rid.size = 0;
  Page * p = loadPage(xid, rid.page);
  byte * logArg = malloc(sizeof(int) + 2 * len);

  *(int*)logArg = offset;
  memcpy(logArg+sizeof(int),     ((const byte*)memAddr), len);
  memcpy(logArg+sizeof(int)+len, p->memAddr+offset,         len);

  try_ret(compensation_error()) {
    Tupdate(xid,rid,logArg,sizeof(int)+len*2,OPERATION_PAGE_SET_RANGE);
  } end_ret(compensation_error());

  free(logArg);
  return 0;
}

/** @todo region sizes should be dynamic. */
#define TALLOC_PAGE_REGION_SIZE 128 // 512K

/** 
    This calls loadPage and releasePage directly, and bypasses the
    logger.
*/
compensated_function void pageOperationsInit() {

  regionsInit();

  boundary_tag t;
  recordid rid = {0, 0, sizeof(boundary_tag)};
  // Need to find a region with some free pages in it.
  Tread(-1, rid, &t);
  

  pthread_mutex_init(&pageAllocMutex, NULL);
}


compensated_function int TpageDealloc(int xid, int pageid) {
  TregionDealloc(xid, pageid); // @todo inefficient hack!
  return 0;
}

compensated_function int TpageAlloc(int xid /*, int type */) {
  return TregionAlloc(xid, 1, STORAGE_MANAGER_NAIVE_PAGE_ALLOC);
}

int op_fixed_page_alloc(const LogEntry* e, Page* p) {
  writelock(p->rwlatch,0);
  assert(e->update.arg_size == sizeof(int));
  int slot_size = *(const int*)getUpdateArgs(e);
  stasis_fixed_initialize_page(p, slot_size, stasis_fixed_records_per_page(slot_size));
  unlock(p->rwlatch);
  return 0;
}


/**
    @return a recordid.  The page field contains the page that was
    allocated, the slot field contains the number of slots on the
    apge, and the size field contains the size of each slot.
*/
recordid TfixedPageAlloc(int xid, int size) {
  int page = TpageAlloc(xid);

  recordid rid = {page, stasis_fixed_records_per_page(size), size};

  Tupdate(xid, rid, &size, sizeof(int), OPERATION_FIXED_PAGE_ALLOC);

  return rid;
}

Operation getFixedPageAlloc() {
  Operation o = {
    OPERATION_FIXED_PAGE_ALLOC,
    OPERATION_NOOP,
    &op_fixed_page_alloc
  };
  return o;
}

compensated_function int TpageAllocMany(int xid, int count /*, int type*/) {
  return TregionAlloc(xid, count, STORAGE_MANAGER_NAIVE_PAGE_ALLOC);
}

int TpageGetType(int xid, int pageid) { 
  Page * p = loadPage(xid, pageid);
  int ret = *stasis_page_type_ptr(p);
  releasePage(p);
  return ret;
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

Operation getPageSetRange() {
  Operation o = {
    OPERATION_PAGE_SET_RANGE,
    OPERATION_PAGE_SET_RANGE_INVERSE,
    op_page_set_range
  };
  return o;
}

Operation getPageSetRangeInverse() {
  Operation o = {
    OPERATION_PAGE_SET_RANGE_INVERSE,
    OPERATION_PAGE_SET_RANGE,
    &op_page_set_range_inverse
  };
  return o;
}
