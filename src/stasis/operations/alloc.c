#include <config.h>
#include <stasis/constants.h>
#include <stasis/common.h>

#include <stasis/operations.h>
#include <stasis/transactional.h>
#include <stasis/bufferManager.h>
#include <stasis/allocationPolicy.h>
#include <stasis/blobManager.h>
#include <stasis/page.h>
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

   pageid_t page = Treserve(int xid, int size)

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

static int operate_helper(int xid, Page * p, recordid rid) {

  if(stasis_record_type_read(xid, p, rid) == INVALID_SLOT) {
    stasis_record_alloc_done(xid, p, rid);
  }

  assert(stasis_record_length_read(xid, p, rid) == stasis_record_type_to_size(rid.size));
  if(rid.size < 0) {
    assert(stasis_record_type_read(xid,p,rid) == rid.size);
  }
  return 0;
}

typedef struct {
  slotid_t slot;
  int64_t type;
} alloc_arg;

static int op_alloc(const LogEntry* e, Page* p) {
  assert(e->update.arg_size >= sizeof(alloc_arg));

  const alloc_arg* arg = (const alloc_arg*)getUpdateArgs(e);
  recordid rid = {
    p->id,
    arg->slot,
    arg->type
  };

  int ret = operate_helper(e->xid,p,rid);

  int64_t size = stasis_record_length_read(e->xid,p,rid);

  if(e->update.arg_size == sizeof(alloc_arg) + size) {
    // if we're aborting a dealloc we better have a sane preimage to apply
    rid.size = size;
    stasis_record_write(e->xid,p,e->LSN,rid,(const byte*)(arg+1));
    rid.size = arg->type;
  } else {
    // otherwise, no preimage
    assert(e->update.arg_size == sizeof(alloc_arg));
  }

  return ret;
}

static int op_dealloc(const LogEntry* e, Page* p) {
  assert(e->update.arg_size >= sizeof(alloc_arg));
  const alloc_arg* arg = (const alloc_arg*)getUpdateArgs(e);
  recordid rid = {
    p->id,
    arg->slot,
    arg->type
  };
  // assert that we've got a sane preimage or we're aborting a talloc (no preimage)
  int64_t size = stasis_record_length_read(e->xid,p,rid);
  assert(e->update.arg_size == sizeof(alloc_arg) + size ||
         e->update.arg_size == sizeof(alloc_arg));

  stasis_record_free(e->xid, p, rid);
  assert(stasis_record_type_read(e->xid, p, rid) == INVALID_SLOT);
  return 0;
}

static int op_realloc(const LogEntry* e, Page* p) {
  assert(e->update.arg_size >= sizeof(alloc_arg));
  const alloc_arg* arg = (const alloc_arg*)getUpdateArgs(e);

  recordid rid = {
    p->id,
    arg->slot,
    arg->type
  };
  assert(stasis_record_type_read(e->xid, p, rid) == INVALID_SLOT);
  int ret = operate_helper(e->xid, p, rid);

  int64_t size = stasis_record_length_read(e->xid,p,rid);

  assert(e->update.arg_size == sizeof(alloc_arg)
         + size);
  rid.size = size;
  byte * buf = stasis_record_write_begin(e->xid,p,rid);
  memcpy(buf, arg+1, size);
  stasis_record_write_done(e->xid,p,rid,buf);
  rid.size = arg->type;
  return ret;
}

static pthread_mutex_t talloc_mutex = PTHREAD_MUTEX_INITIALIZER;

Operation getAlloc() {
  Operation o = {
    OPERATION_ALLOC, /* ID */
    OPERATION_DEALLOC, /* OPERATION_NOOP, */
    op_alloc
  };
  return o;
}


Operation getDealloc() {
  Operation o = {
    OPERATION_DEALLOC,
    OPERATION_REALLOC, /* OPERATION_NOOP, */
    op_dealloc
  };
  return o;
}

/*This is only used to undo deallocs... */
Operation getRealloc() {
  Operation o = {
    OPERATION_REALLOC,
    OPERATION_NOOP,
    op_realloc
  };
  return o;
}

static pageid_t lastFreepage;
static allocationPolicy * allocPolicy;
static void registerOldRegions();
void TallocInit() { 
  lastFreepage = PAGEID_T_MAX;
  allocPolicy = allocationPolicyInit();
}
void TallocPostInit() {
  registerOldRegions();
}
void TallocDeinit() { 
  allocationPolicyDeinit(allocPolicy);
}

static void registerOldRegions() {
  pageid_t boundary = REGION_FIRST_TAG;
  boundary_tag t;
  DEBUG("registering old regions\n");
  int succ = TregionReadBoundaryTag(-1, boundary, &t);
  if(succ) {
    do { 
      DEBUG("boundary tag %lld type %d\n", boundary, t.allocation_manager);
      if(t.allocation_manager == STORAGE_MANAGER_TALLOC) {
	availablePage ** newPages = malloc(sizeof(availablePage*)*(t.size+1));
	for(pageid_t i = 0; i < t.size; i++) {
	  Page * p = loadPage(-1, boundary + i);
	  readlock(p->rwlatch,0);
	  if(*stasis_page_type_ptr(p) == SLOTTED_PAGE) {
	    availablePage * next = malloc(sizeof(availablePage));
	    next->pageid = boundary+i;
	    next->freespace = stasis_record_freespace(-1, p);
	    next->lockCount = 0;
	    newPages[i] = next;
	    DEBUG("registered page %lld\n", boundary+i);
	  } else {
	    abort();
	  }
	  unlock(p->rwlatch);
	  releasePage(p);
	}
	newPages[t.size]=0;
	allocationPolicyAddPages(allocPolicy, newPages);
	free(newPages);
      }
    } while(TregionNextBoundaryTag(-1, &boundary, &t, 0));  //STORAGE_MANAGER_TALLOC)) {
  }
}

static void reserveNewRegion(int xid) {
     void* nta = TbeginNestedTopAction(xid, OPERATION_NOOP, 0,0);

     pageid_t firstPage = TregionAlloc(xid, TALLOC_REGION_SIZE, STORAGE_MANAGER_TALLOC);
     int initialFreespace = -1;

     availablePage ** newPages = malloc(sizeof(availablePage*)*(TALLOC_REGION_SIZE+1));

     for(pageid_t i = 0; i < TALLOC_REGION_SIZE; i++) {
       availablePage * next = malloc(sizeof(availablePage)); // * TALLOC_REGION_SIZE);

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
    assert(size >= 0);
    type = size;
  }

  recordid rid;

  begin_action_ret(pthread_mutex_unlock, &talloc_mutex, NULLRID) { 
    pthread_mutex_lock(&talloc_mutex);
    Page * p;

    availablePage * ap =
      allocationPolicyFindPage(allocPolicy, xid,
                               stasis_record_type_to_size(type));

    if(!ap) {
      reserveNewRegion(xid);
      ap = allocationPolicyFindPage(allocPolicy, xid,
                                    stasis_record_type_to_size(type));
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
      if(!ap->lockCount) {
	allocationPolicyUpdateFreespaceUnlockedPage(allocPolicy, ap,
                                                    newFreespace);
      } else {
        allocationPolicyUpdateFreespaceLockedPage(allocPolicy, xid, ap,
                                                  newFreespace);
      }
      releasePage(p);

      ap = allocationPolicyFindPage(allocPolicy, xid,
                                    stasis_record_type_to_size(type));

      if(!ap) {
	reserveNewRegion(xid);
	ap = allocationPolicyFindPage(allocPolicy, xid,
                                      stasis_record_type_to_size(type));
      }

      lastFreepage = ap->pageid;

      p = loadPage(xid, lastFreepage);
      writelock(p->rwlatch, 0);
    }

    rid = stasis_record_alloc_begin(xid, p, type);

    assert(rid.size != INVALID_SLOT);

    stasis_record_alloc_done(xid, p, rid);
    int newFreespace = stasis_record_freespace(xid, p);
    allocationPolicyAllocedFromPage(allocPolicy, xid, ap->pageid);
    allocationPolicyUpdateFreespaceLockedPage(allocPolicy, xid, ap, newFreespace);
    unlock(p->rwlatch);

    alloc_arg a = { rid.slot, type };

    Tupdate(xid, rid.page, &a, sizeof(a), OPERATION_ALLOC);
 
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

compensated_function recordid TallocFromPage(int xid, pageid_t page, unsigned long size) {
  short type;
  if(size >= BLOB_THRESHOLD_SIZE) { 
    type = BLOB_SLOT;
  } else {
    assert(size > 0);
    type = size;
  }

  pthread_mutex_lock(&talloc_mutex);
  if(!allocationPolicyCanXidAllocFromPage(allocPolicy, xid, page)) {
    pthread_mutex_unlock(&talloc_mutex);
    return NULLRID;
  }
  Page * p = loadPage(xid, page);
  writelock(p->rwlatch,0);
  recordid rid = stasis_record_alloc_begin(xid, p, type);


  if(rid.size != INVALID_SLOT) {
    stasis_record_alloc_done(xid,p,rid);
    allocationPolicyAllocedFromPage(allocPolicy, xid, page);
    unlock(p->rwlatch);

    alloc_arg a = { rid.slot, type };

    Tupdate(xid, rid.page, &a, sizeof(a), OPERATION_ALLOC);

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

  pthread_mutex_lock(&talloc_mutex);
  Page * p = loadPage(xid, rid.page);

  readlock(p->rwlatch,0);

  recordid newrid = stasis_record_dereference(xid, p, rid);
  allocationPolicyLockPage(allocPolicy, xid, newrid.page);

  int64_t size = stasis_record_length_read(xid,p,rid);
  int64_t type = stasis_record_type_read(xid,p,rid);

  if(type == NORMAL_SLOT) { type = size; }

  byte * preimage = malloc(sizeof(alloc_arg)+size);

  ((alloc_arg*)preimage)->slot = rid.slot;
  ((alloc_arg*)preimage)->type = type;

  // stasis_record_read() wants rid to have its raw size to prevent
  // code that doesn't know about record types from introducing memory
  // bugs.
  rid.size = size;
  stasis_record_read(xid, p, rid, preimage+sizeof(alloc_arg));
  // restore rid to valid state.
  rid.size = type;

  // Ok to release latch; page is still pinned (so no WAL problems).
  // allocationPolicy protects us from running out of space due to concurrent
  // xacts.

  // Also, there can be no reordering of allocations / deallocations ,
  // since we're holding talloc_mutex.  However, we might reorder a Tset()
  // to and a Tdealloc() or Talloc() on the same page.  If this happens,
  // it's an unsafe race in the application, and not technically our problem.

  // @todo  Tupdate forces allocation to release a latch, leading to potentially nasty application bugs.  Perhaps this is the wrong API!

  // @todo application-level allocation races can lead to unrecoverable logs.
  unlock(p->rwlatch);

  Tupdate(xid, rid.page, preimage,
          sizeof(alloc_arg)+size, OPERATION_DEALLOC);

  releasePage(p);

  pthread_mutex_unlock(&talloc_mutex);

  if(type==BLOB_SLOT) {
    deallocBlob(xid,(blob_record_t*)(preimage+sizeof(alloc_arg)));
  }

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
  rid.size = stasis_record_length_read(xid, p, rid);
  if(stasis_record_type_read(xid,p,rid) == BLOB_SLOT) {
    blob_record_t r;
    stasis_record_read(xid,p,rid,(byte*)&r);
    ret = r.size;
  } else {
    ret = rid.size;
  }
  unlock(p->rwlatch);
  releasePage(p);
  return ret;
}

void TinitializeSlottedPage(int xid, pageid_t page) {
  alloc_arg a = { SLOTTED_PAGE, 0 };
  Tupdate(xid, page, &a, sizeof(a), OPERATION_INITIALIZE_PAGE);
}
void TinitializeFixedPage(int xid, pageid_t page, int slotLength) {
  alloc_arg a = { FIXED_PAGE, slotLength };
  Tupdate(xid, page, &a, sizeof(a), OPERATION_INITIALIZE_PAGE);
}

static int op_initialize_page(const LogEntry* e, Page* p) {
  assert(e->update.arg_size == sizeof(alloc_arg));
  const alloc_arg* arg = (const alloc_arg*)getUpdateArgs(e);

  switch(arg->slot) {
  case SLOTTED_PAGE:
    stasis_slotted_initialize_page(p);
    break;
  case FIXED_PAGE:
    stasis_fixed_initialize_page(p, arg->type,
                                 stasis_fixed_records_per_page
                                 (
                                  stasis_record_type_to_size(arg->type)));
    break;
  default:
    abort();
  }
  return 0;
}


Operation getInitializePage() { 
  Operation o = { 
    OPERATION_INITIALIZE_PAGE,
    OPERATION_NOOP,
    op_initialize_page
  };
  return o;
}
