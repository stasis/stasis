#include <config.h>
#include <lladd/common.h>

#include <lladd/operations.h>
#include <lladd/transactional.h>
#include <lladd/bufferManager.h>
#include "../blobManager.h"
#include "../page.h"
#include "../page/slotted.h"

#include <assert.h>
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

   @ingroup OPERATIONS

   $Id$
   
*/

static int operate(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) {
  /* * @ todo Currently, Talloc() needs to clean up the page type (for recovery).  Should this be elsewhere? */

 /* if(*page_type_ptr(p) == UNINITIALIZED_PAGE) {
    *page_type_ptr(p) = SLOTTED_PAGE;
  }

  assert(*page_type_ptr(p) == SLOTTED_PAGE); */
  
  if(rid.size >= BLOB_THRESHOLD_SIZE) {
    allocBlob(xid, p, lsn, rid);
  } else {
    slottedPostRalloc(p, lsn, rid); 
  }

  return 0;
}

/** @todo Currently, we leak empty pages on dealloc. */
static int deoperate(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) {
  assert(rid.page == p->id);
  slottedDeRalloc(p, lsn, rid);
  return 0;
}

static int reoperate(int xid, Page *p, lsn_t lsn, recordid rid, const void * dat) {

  if(rid.size >= BLOB_THRESHOLD_SIZE) {
    rid.size = BLOB_REC_SIZE; /* Don't reuse blob space yet... */
  } 

  slottedPostRalloc(p, lsn, rid); 
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

recordid Talloc(int xid, long size) {
  recordid rid;
  Page * p = NULL;
  if(size >= BLOB_THRESHOLD_SIZE) { 
    rid = preAllocBlob(xid, size);
  } else {
    pthread_mutex_lock(&talloc_mutex); 
    rid = slottedPreRalloc(xid, size, &p);
    assert(p != NULL);
  }

  Tupdate(xid,rid, NULL, OPERATION_ALLOC);
  
  if(p != NULL) {
    /* release the page that preAllocBlob pinned for us. */

    /* @todo alloc.c pins multiple pages -> Will deadlock with small buffer sizes.. */
    releasePage(p);
    pthread_mutex_unlock(&talloc_mutex);  

    /*pthread_mutex_unlock(&talloc_mutex); */

  }

  return rid;
  
}

void Tdealloc(int xid, recordid rid) {
  void * preimage = malloc(rid.size);
  Page * p = loadPage(rid.page);
  readRecord(xid, p, rid, preimage);
  /** @todo race in Tdealloc; do we care, or is this something that the log manager should cope with? */
  Tupdate(xid, rid, preimage, OPERATION_DEALLOC);
  releasePage(p);  
  free(preimage);
}

int TrecordType(int xid, recordid rid) {
  Page * p = loadPage(rid.page);
  int ret = getRecordType(xid, p, rid);
  releasePage(p);
  return ret;
}
