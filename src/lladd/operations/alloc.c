#include <lladd/operations/alloc.h>
#include <lladd/page.h>
#include <lladd/bufferManager.h>

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
  Page loadedPage = loadPage(rid.page);
  /** Has no effect during normal operation. */
  pageSlotRalloc(loadedPage, rid);
  return 0;
}

/** @todo Currently, we just lead store space on dealloc. */
static int deoperate(int xid, lsn_t lsn, recordid rid, const void * dat) {
  Page loadedPage = loadPage(rid.page);
  /** Has no effect during normal operation. */
  pageSlotRalloc(loadedPage, rid);
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


recordid Talloc(int xid, size_t size) {
  recordid rid;

  /** 

  @todo we pass lsn -1 into ralloc here.  This is a kludge, since we
  need to log ralloc's return value, but can't get that return value
  until after its executed.  When it comes time to perform recovery,
  it is possible that this record will be leaked during the undo
  phase.  We could do a two phase allocation to prevent the leak, but
  then we'd need to lock the page that we're allocating a record in,
  and that's a pain.  Plus, this is a small leak.  (There is a similar
  problem involving blob allocation, which is more serious, as it may
  result in double allocation...)

  @todo If this should be the only call to ralloc in lladd, we may consider
  removing the lsn parameter someday.  (Is there a good reason to
  provide users with direct access to ralloc()?)

  */

  rid = ralloc(xid, -1, size);

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
