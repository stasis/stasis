#include <config.h>
#include <lladd/common.h>

#include <lladd/operations/alloc.h>

#include <lladd/transactional.h>
#include <lladd/page.h>
#include <lladd/bufferManager.h>
#include "../blobManager.h"
/**
   Implementation of Talloc() as an operation

   This is a bit strange compared to other operations, as it happens
   in two phases.  The buffer manager reserves space for a record
   before the log entry is allocated.  Then, the recordid of this
   space is written to the log.  Finally, alloc tells bufferManager
   that it will use the space.

   @todo Currently, if the system crashes during an alloc, (before the
   log is flushed, but after bufferManager returns a rid), then the
   space alloced during the crash is leaked.  This doesn't seem to be
   too big of a deal, but it should be fixed someday.  A more serious
   problem results from crashes during blob allocation.
   
*/

static int operate(int xid, lsn_t lsn, recordid rid, const void * dat) {
  if(rid.size >= BLOB_THRESHOLD_SIZE) {
    allocBlob(xid, lsn, rid);
  } else {
    Page loadedPage = loadPage(rid.page);
    /** Has no effect during normal operation. */
    pageSlotRalloc(loadedPage, lsn, rid);
  }

  return 0;
}

/** @todo Currently, we just leak store space on dealloc. */
static int deoperate(int xid, lsn_t lsn, recordid rid, const void * dat) {
  Page loadedPage = loadPage(rid.page);
  /** Has no effect during normal operation, other than updating the LSN. */
  pageSlotRalloc(loadedPage, lsn, rid);
  return 0;
}

Operation getAlloc() {
  Operation o = {
    OPERATION_ALLOC, /* ID */
    0,
    OPERATION_DEALLOC,
    &operate
  };
  return o;
}


recordid Talloc(int xid, long size) {
  recordid rid;

  if(size >= BLOB_THRESHOLD_SIZE) { 
    rid = preAllocBlob(xid, size);
  } else {

    rid = ralloc(xid, size);

  }

  Tupdate(xid,rid, NULL, OPERATION_ALLOC);

  return rid;
  
}

Operation getDealloc() {
  Operation o = {
    OPERATION_DEALLOC,
    0,
    OPERATION_ALLOC,
    &deoperate
  };
  return o;
}

void Tdealloc(int xid, recordid rid) {
  Tupdate(xid, rid, NULL, OPERATION_DEALLOC);
}
