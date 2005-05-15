#include <config.h>
#include <lladd/common.h>

#include <lladd/operations.h>
#include <lladd/transactional.h>
#include <lladd/bufferManager.h>
#include "../blobManager.h"
#include "../page.h"
#include "../page/slotted.h"

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

   @todo The entire allocaction system needs to be redone.  

   Here are some requirements for the next version of alloc:

   Space Reuse: There are many ways to implement this.  One method
   (that I'm not particularly attached to) is to maintain seperate
   linked lists for each type of page, seperated by an estimate of the
   amount of space free (actually 'un-reserved'; see below) on the
   page.  Allocation would move pages between linked lists, and search
   in the appropriate linked list before expanding the page file.
    
   Treserve: Hashtables, linked lists, and other graph-like structures
   can be optimized by exploiting physical locality.  A call such as
   this allows page-level locality to be established / maintained:

   int page = Treserve(int xid, int size)

   This would tell Talloc to treat the page as though 'size' bytes had
   already been reserved.  The 'free space' that Talloc () reasons
   about would be: max(reservedSpace, usedSpace).  A seperate call,
   TallocFromPage (xid, page, size) already exists, and should ignore
   the presence of the 'reserved space' field.

   Track level locality is another problem that Talloc should address, 
   especially for the blob implementation.

   Better support for locking.  Consider this sequence of events:

   recordid rid1 = Talloc (xid1, 1);
   recordid rid2 = Talloc (xid2, 1);  // May deadlock if page level  
                                     // locking is used.

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
  /* * @ todo Currently, T alloc () needs to clean up the page type (for recovery).  Should this be elsewhere? */

 /* if(*page_type_ptr(p) == UNINITIALIZED_PAGE) {
    *page_type_ptr(p) = SLOTTED_PAGE;
  }

  assert(*page_type_ptr(p) == SLOTTED_PAGE); */
  
  if(rid.size >= BLOB_THRESHOLD_SIZE && rid.size != BLOB_SLOT) {
    allocBlob(xid, p, lsn, rid);
  } else {
    slottedPostRalloc(xid, p, lsn, rid); 
  }

  return 0;
}

/** @todo Currently, we leak empty pages on dealloc. */
static int deoperate(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) {
  assert(rid.page == p->id);
  slottedDeRalloc(xid, p, lsn, rid);
  return 0;
}

static int reoperate(int xid, Page *p, lsn_t lsn, recordid rid, const void * dat) {

  if(rid.size >= BLOB_THRESHOLD_SIZE && rid.size != BLOB_SLOT) {
    //    rid.size = BLOB_REC_SIZE; /* Don't reuse blob space yet... */
    rid.size = sizeof(blob_record_t); 
  } 

  slottedPostRalloc(xid, p, lsn, rid); 
  /** @todo dat should be the pointer to the space in the blob store. */
  writeRecord(xid, p, lsn, rid, dat);

  return 0;
}

static pthread_mutex_t talloc_mutex;

Operation getAlloc() {
  pthread_mutex_init(&talloc_mutex, NULL);
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

compensated_function recordid TallocRaw(int xid, long size) {
  recordid rid;
  Page * p = NULL;
  
  //    begin_action_ret(pthread_mutex_unlock, &talloc_mutex, NULLRID) {
  pthread_mutex_lock(&talloc_mutex);
  rid = slottedPreRalloc(xid, size, &p);
  Tupdate(xid, rid, NULL, OPERATION_ALLOC);
  /** @todo does releasePage do the correct error checking? */
  releasePage(p);
  //    } compensate_ret(NULLRID);
  pthread_mutex_unlock(&talloc_mutex);
  
  return rid;

}

compensated_function recordid Talloc(int xid, long size) {
  recordid rid;

  int isBlob = size >= BLOB_THRESHOLD_SIZE && size != BLOB_SLOT;

  if(isBlob) {
    //    try_ret(NULLRID) {
    rid = preAllocBlob(xid, size);
    //    abort();
      Tupdate(xid,rid, NULL, OPERATION_ALLOC);
      //    } end_ret(NULLRID);
  } else {

    rid = TallocRaw(xid, size);
  }
  return rid;
}

compensated_function recordid TallocFromPage(int xid, long page, long size) {
  recordid rid;

  Page * p = NULL;
  if(size >= BLOB_THRESHOLD_SIZE && size != BLOB_SLOT) { 
    //    try_ret(NULLRID) { 
      rid = preAllocBlobFromPage(xid, page, size);
      Tupdate(xid,rid, NULL, OPERATION_ALLOC);
      //    } end_ret(NULLRID);
  } else {
    //    begin_action_ret(pthread_mutex_unlock, &talloc_mutex, NULLRID) {
      pthread_mutex_lock(&talloc_mutex); 
      rid = slottedPreRallocFromPage(xid, page, size, &p);
      if(rid.size == size) { 
	Tupdate(xid,rid, NULL, OPERATION_ALLOC);
      } else {
	p = loadPage(xid, page);
	slottedCompact(p);
	releasePage(p);
	p = NULL;
	rid = slottedPreRallocFromPage(xid, page, size, &p);
	if(rid.size == size) {
	  Tupdate(xid,rid, NULL, OPERATION_ALLOC);
	} else {
	  assert(rid.size < 0);
	}
      }
      if(p) {
	/* @todo alloc.c pins multiple pages -> Will deadlock with small buffer sizes.. */      
	releasePage(p);
      }
      //    } compensate_ret(NULLRID);
      pthread_mutex_unlock(&talloc_mutex);
  }
  
  return rid;
}

compensated_function void Tdealloc(int xid, recordid rid) {
  void * preimage = malloc(rid.size);
  Page * p = NULL;
  //  try {
  p = loadPage(xid, rid.page);
  //  } end;
  //  begin_action(releasePage, p) {
  readRecord(xid, p, rid, preimage);
  /** @todo race in Tdealloc; do we care, or is this something that the log manager should cope with? */
  Tupdate(xid, rid, preimage, OPERATION_DEALLOC);
  //  } compensate;
  releasePage(p);
  free(preimage);
}

compensated_function int TrecordType(int xid, recordid rid) {
  Page * p = NULL;
  // try_ret(compensation_error()) {
  p = loadPage(xid, rid.page);
  //  } end_ret(compensation_error());
  int ret;
  ret = getRecordType(xid, p, rid);
  releasePage(p);
  return ret;
}

compensated_function int TrecordSize(int xid, recordid rid) {
  int ret;
  Page * p = NULL;
  //  try_ret(compensation_error()) { 
  p = loadPage(xid, rid.page);
  //  } end_ret(compensation_error());
  ret = getRecordSize(xid, p, rid);
  releasePage(p);
  return ret;
}

compensated_function int TrecordsInPage(int xid, int pageid) {
  Page * p = NULL;
  //  try_ret(compensation_error()) {
  p = loadPage(xid, pageid);
  //  } end_ret(compensation_error());
  readlock(p->rwlatch, 187);
  int ret = *numslots_ptr(p);
  unlock(p->rwlatch);
  releasePage(p);
  return ret;
}
