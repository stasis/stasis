#include <config.h>
#include <lladd/common.h>

#include <lladd/operations.h>
#include <lladd/transactional.h>
#include <lladd/bufferManager.h>
#include <lladd/allocationPolicy.h>
#include "../blobManager.h"
#include "../page.h"
#include "../page/slotted.h"
#include "../page/fixed.h"

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
static int operate(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) {
  slottedPostRalloc(xid, p, lsn, rid); 

  return 0;
}

static int deoperate(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) {
  assert(rid.page == p->id);
  slottedDeRalloc(xid, p, lsn, rid);
  return 0;
}

static int reoperate(int xid, Page *p, lsn_t lsn, recordid rid, const void * dat) {

  //  if(rid.size >= BLOB_THRESHOLD_SIZE) { // && rid.size != BLOB_SLOT) {
    //    rid.size = BLOB_REC_SIZE; /* Don't reuse blob space yet... */
  //    rid.size = BLOB_SLOT; //sizeof(blob_record_t); 
    //  } 

  slottedPostRalloc(xid, p, lsn, rid); 
  recordWrite(xid, p, lsn, rid, dat);

  return 0;
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
static int initialFreespace = -1;
static allocationPolicy * allocPolicy;

void TallocInit() { 
  lastFreepage = UINT64_MAX;
  allocPolicy = allocationPolicyInit();
  //  pthread_mutex_init(&talloc_mutex, NULL);
}

static compensated_function recordid TallocFromPageInternal(int xid, Page * p, unsigned long size);

static void reserveNewRegion(int xid) {
     int firstPage = TregionAlloc(xid, TALLOC_REGION_SIZE, STORAGE_MANAGER_TALLOC);
     
     void* nta = TbeginNestedTopAction(xid, OPERATION_NOOP, 0,0);

     availablePage ** newPages = malloc(sizeof(availablePage**)*(TALLOC_REGION_SIZE+1));
     if(initialFreespace == -1) { 
       Page * p = loadPage(xid, firstPage);
       initialFreespace = slottedFreespace(p);
       releasePage(p);
     }
     for(int i = 0; i < TALLOC_REGION_SIZE; i++) { 
       availablePage * next = malloc(sizeof(availablePage) * TALLOC_REGION_SIZE);
       
       next->pageid = firstPage + i;
       next->freespace = initialFreespace;
       next->lockCount = 0;
       newPages[i] = next;
       TinitializeSlottedPage(xid, firstPage + i);
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

    availablePage * ap = allocationPolicyFindPage(allocPolicy, xid, physical_slot_length(type));

    if(!ap) {
      reserveNewRegion(xid);
      ap = allocationPolicyFindPage(allocPolicy, xid, physical_slot_length(type));
    }
    lastFreepage = ap->pageid;

    p = loadPage(xid, lastFreepage);

    while(slottedFreespace(p) < physical_slot_length(type)) { 
      writelock(p->rwlatch,0);
      slottedCompact(p);
      unlock(p->rwlatch);
      int newFreespace = slottedFreespace(p);
      if(newFreespace >= physical_slot_length(type)) { 
	break;
      }
      allocationPolicyUpdateFreespaceLockedPage(allocPolicy, xid, ap, newFreespace);

      releasePage(p);
      
      ap = allocationPolicyFindPage(allocPolicy, xid, physical_slot_length(type));

      if(!ap) { 
	reserveNewRegion(xid);
	ap = allocationPolicyFindPage(allocPolicy, xid, physical_slot_length(type));
      }
      
      lastFreepage = ap->pageid;
      
      p = loadPage(xid, lastFreepage);

    }
    
    rid = TallocFromPageInternal(xid, p, size);
    
    int newFreespace = slottedFreespace(p);
    allocationPolicyUpdateFreespaceLockedPage(allocPolicy, xid, ap, newFreespace);

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

compensated_function recordid TallocFromPage(int xid, long page, unsigned long size) {
  pthread_mutex_lock(&talloc_mutex);
  Page * p = loadPage(xid, page);
  recordid ret = TallocFromPageInternal(xid, p, size);
  if(ret.size != INVALID_SLOT) {
    allocationPolicyAllocedFromPage(allocPolicy, xid, page);
  }
  releasePage(p);
  pthread_mutex_unlock(&talloc_mutex);

  return ret;
}

static compensated_function recordid TallocFromPageInternal(int xid, Page * p, unsigned long size) {
  recordid rid;

  // Does TallocFromPage need to understand blobs?  This function
  // seems to be too complex; all it does it delegate the allocation
  // request to the page type's implementation.  (Does it really need
  // to check for freespace?)

  short type;
  if(size >= BLOB_THRESHOLD_SIZE) { 
    type = BLOB_SLOT;
  } else { 
    type = size;
  }

  unsigned long slotSize = INVALID_SLOT;
  
  slotSize = physical_slot_length(type);
  
  assert(slotSize < PAGE_SIZE && slotSize > 0);
  
  /*  if(slottedFreespace(p) < slotSize) { 
    slottedCompact(p);
    }  */
  if(slottedFreespace(p) < slotSize) {
    rid = NULLRID;
  } else { 
    rid = slottedRawRalloc(p, type);
    assert(rid.size == type);
    rid.size = size;
    Tupdate(xid, rid, NULL, OPERATION_ALLOC); 

    if(type == BLOB_SLOT) { 
      allocBlob(xid, rid);
    }

    rid.size = type;

  }
  
  if(rid.size == type &&  // otherwise TallocFromPage failed
     type == BLOB_SLOT    // only special case blobs (for now)
     ) { 
    rid.size = size;
  }
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

  
  recordid newrid = recordDereference(xid, p, rid);
  allocationPolicyLockPage(allocPolicy, xid, newrid.page);
  
  begin_action(releasePage, p) {
    recordRead(xid, p, rid, preimage);
    /** @todo race in Tdealloc; do we care, or is this something that the log manager should cope with? */
    Tupdate(xid, rid, preimage, OPERATION_DEALLOC);
  } compensate;
  pthread_mutex_unlock(&talloc_mutex);

  free(preimage);

}

compensated_function int TrecordType(int xid, recordid rid) {
  Page * p;
  try_ret(compensation_error()) {
    p = loadPage(xid, rid.page);
  } end_ret(compensation_error());
  int ret;
  ret = recordType(xid, p, rid);
  releasePage(p);
  return ret;
}

compensated_function int TrecordSize(int xid, recordid rid) {
  int ret;
  Page * p;
  try_ret(compensation_error()) { 
    p = loadPage(xid, rid.page);
  } end_ret(compensation_error());
  ret = recordSize(xid, p, rid);
  releasePage(p);
  return ret;
}

compensated_function int TrecordsInPage(int xid, int pageid) {
  Page * p;
  try_ret(compensation_error()) {
    p = loadPage(xid, pageid);
  } end_ret(compensation_error());
  readlock(p->rwlatch, 187);
  int ret = *numslots_ptr(p);
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
  switch(rid.slot) { 
  case SLOTTED_PAGE:
    slottedPageInitialize(p);
    break;
  case FIXED_PAGE: 
    fixedPageInitialize(p, rid.size, recordsPerPage(rid.size));
    break;
  default:
    abort();
  }
  pageWriteLSN(xid, p, lsn);
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
