#include <config.h>
#include <stasis/common.h>

#include <stasis/operations.h>
#include <stasis/transactional.h>
#include <stasis/bufferManager.h>
#include <stasis/allocationPolicy.h>
#include "../blobManager.h"
#include "../page.h"
#include <assert.h>
//try{
/**
   @file

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

   Here are some requirements for alloc:

   [DONE] Space Reuse: There are many ways to implement this.  One method
   (that I'm not particularly attached to) is to maintain seperate
   linked lists for each type of page, seperated by an estimate of the
   amount of space free (actually 'un-reserved'; see below) on the
   page.  Allocation would move pages between linked lists, and search
   in the appropriate linked list before expanding the page file.
    
   @todo Treserve: Hashtables, linked lists, and other graph-like structures
   can be optimized by exploiting physical locality.  A call such as
   this allows page-level locality to be established / maintained:

   int page = Treserve(int xid, int size)

   This would tell Talloc to treat the page as though 'size' bytes had
   already been reserved.  The 'free space' that Talloc () reasons
   about would be: max(reservedSpace, usedSpace).  A seperate call,
   TallocFromPage (xid, page, size) already exists, and should ignore
   the presence of the 'reserved space' field.

   @todo Track level locality is another problem that Talloc should address, 
   especially for the blob implementation.

   [DONE] Concurrent transaction support.  Consider this sequence of events:

   recordid rid1 = Talloc (xid1, 1);
   recordid rid2 = Talloc (xid2, 1);  // May deadlock if page level  
                                     // locking is used.

   [NOT TO BE DONE] (We don't want allocation to grab locks...

   The lock manager needs a 'try lock' operation that allows
   transactions to attempt to read multiple pages.  When the current
   lock manager returns "LLADD_DEADLOCK", it pretends the lock request
   never happened (ie; it's externally visible state is left unchanged
   by the call), effectively providing 'try lock' by default.  Talloc
   should make use of this by trying to alloc from a different page
   whenever deadlock is encountered.  Better, the system should
   provide a list of 'cold' pages that are in memory, but haven't been
   accessed recently.  This requires integration with the page reuse
   policy.

   @ingroup OPERATIONS

   $Id$
   
*/
//}end

static int operate_helper(int xid, Page * p, recordid rid, const void * dat) {

  if(stasis_record_type_read(xid, p, rid) == INVALID_SLOT) {
    stasis_record_alloc_done(xid, p, rid);
  }

  assert(stasis_record_length_read(xid, p, rid) == stasis_record_type_to_size(rid.size));
  if(rid.size < 0) {
    assert(stasis_record_type_read(xid,p,rid) == rid.size);
  }
  return 0;
}

static int operate(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) {
  writelock(p->rwlatch, 0);
  int ret = operate_helper(xid,p,rid,dat);
  stasis_page_lsn_write(xid,p,lsn);
  unlock(p->rwlatch);
  return ret;
}

static int deoperate(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) {
  writelock(p->rwlatch,0);
  stasis_record_free(xid, p, rid);
  stasis_page_lsn_write(xid,p,lsn);
  assert(stasis_record_type_read(xid, p, rid) == INVALID_SLOT);
  unlock(p->rwlatch);
  return 0;
}

static int reoperate(int xid, Page *p, lsn_t lsn, recordid rid, const void * dat) {
  writelock(p->rwlatch,0);
  assert(stasis_record_type_read(xid, p, rid) == INVALID_SLOT);
  int ret = operate_helper(xid, p, rid, dat);
  byte * buf = stasis_record_write_begin(xid,p,rid);
  memcpy(buf, dat, stasis_record_length_read(xid,p,rid));
  stasis_page_lsn_write(xid,p,lsn);
  unlock(p->rwlatch);

  return ret;
}

static pthread_mutex_t talloc_mutex = PTHREAD_MUTEX_INITIALIZER;

Operation getAlloc() {
  Operation o = {
    OPERATION_ALLOC, /* ID */
    0,
    OPERATION_DEALLOC, /* OPERATION_NOOP, */
    &operate
  };
  return o;
}


Operation getDealloc() {
  Operation o = {
    OPERATION_DEALLOC,
    SIZEOF_RECORD,
    OPERATION_REALLOC, /* OPERATION_NOOP, */
    &deoperate
  };
  return o;
}

/*This is only used to undo deallocs... */
Operation getRealloc() {
  Operation o = {
    OPERATION_REALLOC,
    0,
    OPERATION_NOOP,
    &reoperate
  };
  return o;
}

static uint64_t lastFreepage;
static allocationPolicy * allocPolicy;

void TallocInit() { 
  lastFreepage = UINT64_MAX;
  allocPolicy = allocationPolicyInit();
  //  pthread_mutex_init(&talloc_mutex, NULL);
}
void TallocDeinit() { 
  allocationPolicyDeinit(allocPolicy);
}

static void reserveNewRegion(int xid) {
     int firstPage = TregionAlloc(xid, TALLOC_REGION_SIZE, STORAGE_MANAGER_TALLOC);
     int initialFreespace = -1;

     void* nta = TbeginNestedTopAction(xid, OPERATION_NOOP, 0,0);

     availablePage ** newPages = malloc(sizeof(availablePage**)*(TALLOC_REGION_SIZE+1));

     for(int i = 0; i < TALLOC_REGION_SIZE; i++) {
       availablePage * next = malloc(sizeof(availablePage) * TALLOC_REGION_SIZE);

       TinitializeSlottedPage(xid, firstPage + i);
       if(initialFreespace == -1) {
         Page * p = loadPage(xid, firstPage);
         readlock(p->rwlatch,0);
         initialFreespace = stasis_record_freespace(xid, p);
         unlock(p->rwlatch);
         releasePage(p);
       }
       next->pageid = firstPage + i;
       next->freespace = initialFreespace;
       next->lockCount = 0;
       newPages[i] = next;
     }
     newPages[TALLOC_REGION_SIZE]= 0;
     allocationPolicyAddPages(allocPolicy, newPages);
     free(newPages); // Don't free the structs it points to; they are in use by the allocation policy.

     TendNestedTopAction(xid, nta);

}

compensated_function recordid Talloc(int xid, unsigned long size) { 
  short type;
  if(size >= BLOB_THRESHOLD_SIZE) { 
    type = BLOB_SLOT;
  } else { 
    type = size;
  }
  
  recordid rid;

  begin_action_ret(pthread_mutex_unlock, &talloc_mutex, NULLRID) { 
    pthread_mutex_lock(&talloc_mutex);
    Page * p;

    availablePage * ap = allocationPolicyFindPage(allocPolicy, xid, stasis_record_type_to_size(type));

    if(!ap) {
      reserveNewRegion(xid);
      ap = allocationPolicyFindPage(allocPolicy, xid, stasis_record_type_to_size(type));
    }
    lastFreepage = ap->pageid;

    p = loadPage(xid, lastFreepage);
    writelock(p->rwlatch, 0);
    while(stasis_record_freespace(xid, p) < stasis_record_type_to_size(type)) {
      stasis_record_compact(p);
      int newFreespace = stasis_record_freespace(xid, p);

      if(newFreespace >= stasis_record_type_to_size(type)) {
	break;
      }

      unlock(p->rwlatch);
      allocationPolicyUpdateFreespaceLockedPage(allocPolicy, xid, ap, newFreespace);

      releasePage(p);

      ap = allocationPolicyFindPage(allocPolicy, xid, stasis_record_type_to_size(type));

      if(!ap) {
	reserveNewRegion(xid);
	ap = allocationPolicyFindPage(allocPolicy, xid, stasis_record_type_to_size(type));
      }

      lastFreepage = ap->pageid;

      p = loadPage(xid, lastFreepage);
      writelock(p->rwlatch, 0);
    }

    rid = stasis_record_alloc_begin(xid, p, type);

    assert(rid.size != INVALID_SLOT);

    stasis_record_alloc_done(xid, p, rid);
    int newFreespace = stasis_record_freespace(xid, p);
    allocationPolicyUpdateFreespaceLockedPage(allocPolicy, xid, ap, newFreespace);
    unlock(p->rwlatch);

    Tupdate(xid, rid, NULL, OPERATION_ALLOC);

    if(type == BLOB_SLOT) {
      rid.size = size;
      allocBlob(xid, rid);
    }

    releasePage(p);
  } compensate_ret(NULLRID);
  return rid;
}

void allocTransactionAbort(int xid) { 
  begin_action(pthread_mutex_unlock, &talloc_mutex) { 
    pthread_mutex_lock(&talloc_mutex);
    allocationPolicyTransactionCompleted(allocPolicy, xid);
  } compensate;
}
void allocTransactionCommit(int xid) { 
  begin_action(pthread_mutex_unlock, &talloc_mutex) { 
    pthread_mutex_lock(&talloc_mutex);
    allocationPolicyTransactionCompleted(allocPolicy, xid);
  } compensate;
}

compensated_function recordid TallocFromPage(int xid, long page, unsigned long type) {

  unsigned long size = type;
  if(size > BLOB_THRESHOLD_SIZE) { 
    type = BLOB_SLOT;
  }

  pthread_mutex_lock(&talloc_mutex);
  Page * p = loadPage(xid, page);
  writelock(p->rwlatch,0);
  recordid rid = stasis_record_alloc_begin(xid, p, type);

  if(rid.size != INVALID_SLOT) {
    stasis_record_alloc_done(xid,p,rid);
    allocationPolicyAllocedFromPage(allocPolicy, xid, page);
    unlock(p->rwlatch);

    Tupdate(xid,rid,NULL,OPERATION_ALLOC);

    if(type == BLOB_SLOT) {
      rid.size = size;
      allocBlob(xid,rid);
    }

  } else {
    unlock(p->rwlatch);
  }


  releasePage(p);
  pthread_mutex_unlock(&talloc_mutex);

  return rid;
}

compensated_function void Tdealloc(int xid, recordid rid) {
  
  // @todo this needs to garbage collect empty storage regions.

  void * preimage = malloc(rid.size);
  Page * p;
  pthread_mutex_lock(&talloc_mutex);
  try {
    p = loadPage(xid, rid.page);
  } end;

  
  recordid newrid = stasis_record_dereference(xid, p, rid);
  allocationPolicyLockPage(allocPolicy, xid, newrid.page);
  
  begin_action(releasePage, p) {
    stasis_record_read(xid, p, rid, preimage);
    /** @todo race in Tdealloc; do we care, or is this something that the log manager should cope with? */
    Tupdate(xid, rid, preimage, OPERATION_DEALLOC);
  } compensate;
  pthread_mutex_unlock(&talloc_mutex);

  free(preimage);

}

compensated_function int TrecordType(int xid, recordid rid) {
  Page * p;
  p = loadPage(xid, rid.page);
  readlock(p->rwlatch,0);
  int ret;
  ret = stasis_record_type_read(xid, p, rid);
  unlock(p->rwlatch);
  releasePage(p);
  return ret;
}

compensated_function int TrecordSize(int xid, recordid rid) {
  int ret;
  Page * p;
  p = loadPage(xid, rid.page);
  readlock(p->rwlatch,0);
  ret = stasis_record_length_read(xid, p, rid);
  unlock(p->rwlatch);
  releasePage(p);
  return ret;
}

void TinitializeSlottedPage(int xid, int pageid) {
  recordid rid = { pageid, SLOTTED_PAGE, 0 };
  Tupdate(xid, rid, NULL, OPERATION_INITIALIZE_PAGE);
}
void TinitializeFixedPage(int xid, int pageid, int slotLength) {
  recordid rid = { pageid, FIXED_PAGE, slotLength };
  Tupdate(xid, rid, NULL, OPERATION_INITIALIZE_PAGE);
}

static int operate_initialize_page(int xid, Page *p, lsn_t lsn, recordid rid, const void * dat) {
  writelock(p->rwlatch, 0);
  switch(rid.slot) { 
  case SLOTTED_PAGE:
    stasis_slotted_initialize_page(p);
    break;
  case FIXED_PAGE: 
    stasis_fixed_initialize_page(p, rid.size, stasis_fixed_records_per_page(rid.size));
    break;
  default:
    abort();
  }
  stasis_page_lsn_write(xid, p, lsn);
  unlock(p->rwlatch);
  return 0;
}


Operation getInitializePage() { 
  Operation o = { 
    OPERATION_INITIALIZE_PAGE,
    0,
    OPERATION_NOOP,
    &operate_initialize_page
  };
  return o;
}
