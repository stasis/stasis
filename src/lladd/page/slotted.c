/** $Id$ */


#include "../page.h"
#include "../blobManager.h"  /** So that we can call sizeof(blob_record_t) */
#include "slotted.h"
#include <assert.h>

static void really_do_ralloc(Page * page, recordid rid) ;

/**
   
Move all of the records to the beginning of the page in order to 
increase the available free space.

The caller of this function must have a writelock on the page.
*/

void slottedCompact(Page * page) {

	short i;
	Page bufPage;
	byte buffer[PAGE_SIZE];

	short numSlots;
	size_t meta_size;

	bufPage.id = -1;
	bufPage.memAddr = buffer;

	/* Can't compact in place, slot numbers can come in different orders than 
	   the physical space allocated to them. */

	memset(buffer, -1, PAGE_SIZE);
	
	meta_size = (((size_t)page->memAddr) + PAGE_SIZE ) - (size_t)end_of_usable_space_ptr(page); 

	memcpy(buffer + PAGE_SIZE - meta_size, page->memAddr + PAGE_SIZE - meta_size, meta_size);

	slottedPageInitialize(&bufPage);

	numSlots = *numslots_ptr(page);

	// Iterate over the slots backwards.  This lets
	// really_do_ralloc() create the entire freelist at once.
	// This is a bit inefficient, since it then must remove things
	// from the freelist, but it ensures that the list is kept in
	// sorted order, and we rely upon that later.

	for(i = numSlots-1; i >= 0; i--) { 

	  if (isValidSlot(page, i)) {
	    /*	    printf("copying %d\n", i); 
		    fflush(NULL); */
	    /*  		  DEBUG("Buffer offset: %d\n", freeSpace); */
	    recordid rid;

	    rid.page = -1;
	    rid.slot = i;
	    rid.size = *slot_length_ptr(page, i);

	    really_do_ralloc(&bufPage, rid);

	    memcpy(record_ptr(&bufPage, rid.slot), record_ptr(page, rid.slot), rid.size);
	    
	  }
	}
	
	/** The freelist could potentially run past the end of the
	    space that is allocated for slots (this would happen if
	    the number of slots needed by this page just decreased.
	    If we let the list run outside of that area, it could
	    cause inadvertant page corruption.  Therefore, we need to
	    truncate the list before continuing. 

	    The list is sorted from lowest to highest numbered slot */

	short next = *freelist_ptr(&bufPage);
	short last = INVALID_SLOT;
	while(next < numSlots && next != INVALID_SLOT) { 
	  assert(*slot_ptr(&bufPage, next) == INVALID_SLOT);
	  last = next;
	  next = *slot_length_ptr(&bufPage, next);
	  // Check to see that the entries are sorted.  (This check
	  // misses entries after the first entry that is greater than
	  // numslots_ptr.)
	  assert(next > last || next == INVALID_SLOT || last == INVALID_SLOT);
	}
	
	if(last != INVALID_SLOT) { 
	  *slot_length_ptr(&bufPage, last) = INVALID_SLOT;
	}

	memcpy(page->memAddr, buffer, PAGE_SIZE);
}

/**
   Invariant: This lock should be held while updating lastFreepage, or
   while performing any operation that may decrease the amount of
   freespace in the page that lastFreepage refers to.  

   Since pageCompact and slottedDeRalloc may only increase this value,
   they do not need to hold this lock.  Since bufferManager is the
   only place where rawPageRallocSlot is called, rawPageRallocSlot does not obtain
   this lock.
   
   If you are calling rawPageRallocSlot on a page that may be the page
   lastFreepage refers to, then you will need to acquire
   lastFreepage_mutex.  (Doing so from outside of slotted.c is almost
   certainly asking for trouble, so lastFreepage_mutex is static.)

*/


/*static pthread_mutex_t lastFreepage_mutex; */
  static uint64_t lastFreepage = -10; 

void slottedPageInit() {
  /*pthread_mutex_init(&lastFreepage_mutex , NULL);  */
  lastFreepage = UINT64_MAX;
}

void slottedPageDeInit() {
  /*  pthread_mutex_destroy(&lastFreepage_mutex); */
}


void slottedPageInitialize(Page * page) {
  /*printf("Initializing page %d\n", page->id);
  fflush(NULL);  */
  memset(page->memAddr, 0, PAGE_SIZE);
  *page_type_ptr(page) = SLOTTED_PAGE;
  *freespace_ptr(page) = 0;
  *numslots_ptr(page)  = 0;
  *freelist_ptr(page)  = INVALID_SLOT;

}
size_t slottedFreespaceUnlocked(Page * page);

/** 
    This is needed to correctly implement really_do_ralloc(), since
    it takes the position of the new slot's header into account.
*/
size_t slottedFreespaceForSlot(Page * page, int slot) { 
  size_t slotOverhead;

  if(slot >= 0 && slot < *numslots_ptr(page)) { 
    slotOverhead = 0;
  } else { 
    slotOverhead = SLOTTED_PAGE_OVERHEAD_PER_RECORD * (*numslots_ptr(page) - slot);
  }
  // end_of_free_space points to the beginning of the slot header at the bottom of the page header.
  byte* end_of_free_space = (byte*)slot_length_ptr(page, (*numslots_ptr(page))-1); 

  // start_of_free_space points to the first unallocated byte in the page
  // (ignoring space that could be reclaimed by compaction)
  byte* start_of_free_space =  (byte*)(page->memAddr + *freespace_ptr(page));

  assert(end_of_free_space >= start_of_free_space);

  if(end_of_free_space < start_of_free_space + slotOverhead) { 
    // The regions would overlap after allocation.  There is no free space.
    return 0;
  } else { 
    // The regions would not overlap.  There might be free space.
    return (size_t) (end_of_free_space - start_of_free_space - slotOverhead);
  }
}

/** @todo Implement a model of slotted pages in the test scripts, and
    then write a randomized test that confirms the model matches the
    implementation's behavior. */
size_t slottedFreespaceUnlocked(Page * page) { 
  return slottedFreespaceForSlot(page, -1);
}

size_t slottedFreespace(Page * page) {
  size_t ret;
  readlock(page->rwlatch, 292);
  ret = slottedFreespaceUnlocked(page);
  readunlock(page->rwlatch);
  return ret;
}


/** @todo slottedPreRalloc ignores it's xid parameter; change the
    interface?  (The xid is there for now, in case it allows some
    optimizations later.  Perhaps it's better to cluster allocations
    from the same xid on the same page, or something...)

    @todo slottedPreRalloc should understand deadlock, and try another page if deadlock occurs.

    @todo need to obtain (transaction-level) write locks _before_ writing log entries.  Otherwise, we can deadlock at recovery.
*/
compensated_function recordid slottedPreRalloc(int xid, unsigned long size, Page ** pp) {
  recordid ret;
  int isBlob = 0;
  if(size == BLOB_SLOT) {
    isBlob = 1;
    size = sizeof(blob_record_t);
  }
  assert(size < BLOB_THRESHOLD_SIZE);


  /** @todo is ((unsigned int) foo) == -1 portable?  Gotta love C.*/

  if(lastFreepage == UINT64_MAX) {
    try_ret(NULLRID) {
      lastFreepage = TpageAlloc(xid);
    } end_ret(NULLRID);
    try_ret(NULLRID) {
      *pp = loadPage(xid, lastFreepage);
    } end_ret(NULLRID);
    assert(*page_type_ptr(*pp) == UNINITIALIZED_PAGE);
    slottedPageInitialize(*pp);
  } else {
    try_ret(NULLRID) {
      *pp = loadPage(xid, lastFreepage);
    } end_ret(NULLRID);
  }


  if(slottedFreespace(*pp) < size ) { 
    releasePage(*pp);
    try_ret(NULLRID) {
      lastFreepage = TpageAlloc(xid);
    } end_ret(NULLRID);
    try_ret(NULLRID) {
      *pp = loadPage(xid, lastFreepage);
    } end_ret(NULLRID);
    slottedPageInitialize(*pp);
  }
  assert(*page_type_ptr(*pp) == SLOTTED_PAGE);
  ret = slottedRawRalloc(*pp, size);
  assert(ret.size == size);
  
  if(isBlob) {
    *slot_length_ptr(*pp, ret.slot) = BLOB_SLOT;
  }

  DEBUG("alloced rid = {%d, %d, %ld}\n", ret.page, ret.slot, ret.size); 

  return ret;
}


recordid slottedRawRalloc(Page * page, int size) {

	writelock(page->rwlatch, 342);

	recordid rid;

	rid.page = page->id;
	rid.slot = *numslots_ptr(page);
	rid.size = size;

	/* The freelist_ptr points to the first free slot number, which 
	   is the head of a linked list of free slot numbers.*/
	if(*freelist_ptr(page) != INVALID_SLOT) {
	  rid.slot = *freelist_ptr(page);
	  *freelist_ptr(page) = *slot_length_ptr(page, rid.slot);
	  *slot_length_ptr(page, rid.slot) = 0;
	}  
	  
	really_do_ralloc(page, rid);

	/*	DEBUG("slot: %d freespace: %d\n", rid.slot, freeSpace); */

	assert(slottedFreespaceUnlocked(page) >= 0);

	writeunlock(page->rwlatch);


	return rid;
}

/** 
  @todo Allocation is scary without locking.  Consider this situation:

   (1) *numslot_ptr(page) is 10
   (2) An aborting transcation calls really_do_ralloc(page) with rid.slot = 12
   (3) *numslot_ptr(page) must be incremented to 12.  Now, what happens to 11?
     - If 11 was also deleted by a transaction that could abort, we should lock it so that it won't be reused.
   (4) This function adds it to the freelist to avoid leaking space.  (Therefore, Talloc() can return recordids that will
       be reused by aborting transactions...)
*/
static void really_do_ralloc(Page * page, recordid rid) {

  short freeSpace;
  
  int isBlob = 0;

  if(rid.size == BLOB_SLOT) {
    isBlob = 1;
    rid.size = sizeof(blob_record_t);
  }

  assert(rid.size > 0);

  // Compact the page if we don't have enough room.
  if(slottedFreespaceForSlot(page, rid.slot) < rid.size) {
    slottedCompact(page);
    
    // Make sure we have enough enough free space for the new record
    assert (slottedFreespaceForSlot(page, rid.slot) >= rid.size);
  }

  freeSpace = *freespace_ptr(page);

  // Remove this entry from the freelist (if necessary) slottedCompact
  // assumes that this does not change the order of items in the list.
  // If it did, then slottedCompact could leaks slot id's (or worse!)
  if(rid.slot < *numslots_ptr(page) && *slot_ptr(page,rid.slot) == INVALID_SLOT) { 
    short next = *freelist_ptr(page);
    short last = INVALID_SLOT;
    // special case:  is the slot physically before us the predecessor? 
    if(rid.slot > 0) { 
      if(*slot_length_ptr(page, rid.slot-1) == rid.slot && *slot_ptr(page, rid.slot-1) == INVALID_SLOT) { 
	next = rid.slot;
	last = rid.slot-1;
      } 
    }
    while(next != INVALID_SLOT && next != rid.slot) { 
      last = next;
      next = *slot_length_ptr(page, next);
    }
    if(next == rid.slot) { 
      if(last == INVALID_SLOT) { 
	*freelist_ptr(page) = *slot_length_ptr(page, rid.slot);
      } else { 
	*slot_length_ptr(page, last) = *slot_length_ptr(page, rid.slot);
      }
    }
  }

  // Insert any slots that come between the previous numslots_ptr()
  // and the slot we're allocating onto the freelist.  In order to 
  // promote the reuse of free slot numbers, we go out of our way to make sure
  // that we put them in the list in increasing order.  (Note:  slottedCompact's 
  // correctness depends on this behavior!)
  short lastFree = INVALID_SLOT;
  while(*numslots_ptr(page) < rid.slot) {
    int slot = *numslots_ptr(page);
    short successor;
    if(lastFree == INVALID_SLOT) { 

      // The first time through, get our successor pointer from the 
      // page's freelist pointer.

      // @todo Grab this from the *end* of the freelist, since we 
      // know that each slot we are about to insert has a higher number
      // than anything in the list.

      successor = *freelist_ptr(page);
      *freelist_ptr(page) = slot;
    } else { 
      // Put this page after the last page we inserted into the list
      successor = *slot_length_ptr(page, lastFree);
      *slot_length_ptr(page, lastFree) = slot;

      // Make sure that we didn't just find an allocated page on the free list.
      assert(*slot_ptr(page, lastFree) == INVALID_SLOT);
    }

    // Update the pointers in the new slot header.
    *slot_length_ptr(page, slot) = successor;
    *slot_ptr(page, slot) = INVALID_SLOT;
    (*numslots_ptr(page))++;
    lastFree = slot;
  }
  // Increment numslots_ptr if necessary.
  if(*numslots_ptr(page) == rid.slot) { 
    (*numslots_ptr(page))++;
  } 

  DEBUG("Num slots %d\trid.slot %d\n", *numslots_ptr(page), rid.slot);
  
  // Reserve space for this record and record the space's offset in
  // the slot header.
  *freespace_ptr(page) = freeSpace + rid.size;
  *slot_ptr(page, rid.slot)  = freeSpace;

  // Remember how long this record is
  if(isBlob) {
    *slot_length_ptr(page, rid.slot = BLOB_SLOT);
  } else {
    *slot_length_ptr(page, rid.slot) = rid.size; 
  }

}

recordid slottedPostRalloc(int xid, Page * page, lsn_t lsn, recordid rid) {
  
	writelock(page->rwlatch, 376);

	if(*page_type_ptr(page) != SLOTTED_PAGE) {
	  /* slottedPreRalloc calls this when necessary.  However, in
	     the case of a crash, it is possible that
	     slottedPreRalloc's updates were lost, so we need to check
	     for that here.  

	     If slottedPreRalloc didn't call slottedPageInitialize,
	     then there would be a race condition:
	
	     Thread 1             Thread 2
	     preAlloc(big record)

	                          preAlloc(big record) // Should check the freespace of the page and fail
	                          postAlloc(big record)

             postAlloc(big record)  // Thread 2 stole my space! => Crash?

	     Note that this _will_ cause trouble if recovery is
	     multi-threaded, and allows the application to begin
	     updating the storefile without first locking any pages
	     that suffer from this problem.

	  */

	  slottedPageInitialize(page);  
	}

	// Make sure the slot is invalid.  If the slot has not been used yet, then 
	// slot_length_ptr will still be zero, so we allow that too.
	if((*slot_length_ptr(page, rid.slot) == 0) || (*slot_ptr(page, rid.slot) == INVALID_SLOT)) {
	  really_do_ralloc(page, rid);
	
       	} else {

	  // Check to see that the slot happens to be the right size,
	  // so we are (hopefully) just overwriting a slot with
	  // itself, or that the slot is a blob slot.  This can happen
	  // under normal operation, since really_do_ralloc() must
	  // be called before and after the log entry is generated.
	  // (See comment above...)  
	  // @todo Check to see that the blob is the right size?
	  
	  assert((rid.size == *slot_length_ptr(page, rid.slot)) ||
		 (*slot_length_ptr(page, rid.slot) >= PAGE_SIZE));

	}

	pageWriteLSN(xid, page, lsn);

	writeunlock(page->rwlatch);

	return rid;
}

void slottedDeRalloc(int xid, Page * page, lsn_t lsn, recordid rid) {
  writelock(page->rwlatch, 443);
  // readlock(page->rwlatch, 443);
  size_t oldFreeLen = slottedFreespaceUnlocked(page);
  *slot_ptr(page, rid.slot) =  INVALID_SLOT;
  *slot_length_ptr(page, rid.slot) = *freelist_ptr(page); 
  *freelist_ptr(page) = rid.slot;  
  /*  *slot_length_ptr(page, rid.slot) = 0; */
  
  pageWriteLSN(xid, page, lsn);
  size_t newFreeLen = slottedFreespaceUnlocked(page);
  assert(oldFreeLen <= newFreeLen);
  unlock(page->rwlatch);
}

void slottedReadUnlocked(int xid, Page * page, recordid rid, byte *buff) {
  int slot_length;

  assert(page->id == rid.page);
  slot_length = *slot_length_ptr(page, rid.slot); 
  assert((rid.size == slot_length) || (rid.size == BLOB_SLOT && slot_length == sizeof(blob_record_t))|| (slot_length >= PAGE_SIZE));

  if(!memcpy(buff, record_ptr(page, rid.slot),  rid.size)) {
    perror("memcpy");
    abort();
  }

}

/*
  This should trust the rid (since the caller needs to
  override the size in special circumstances)

  @todo If the rid size has been overridden, we should check to make
  sure that this really is a special record.
*/
void slottedRead(int xid, Page * page, recordid rid, byte *buff) {

  int slot_length;
  readlock(page->rwlatch, 519);

  assert(page->id == rid.page);

  // DELETE THIS

  //  int free_space = slottedFreespaceUnlocked(page);
  //  int slot_count = *numslots_ptr(page);

  // END DELETE THIS

  slot_length = *slot_length_ptr(page, rid.slot); 
  assert((rid.size == slot_length) || (rid.size == BLOB_SLOT && slot_length == sizeof(blob_record_t))|| (slot_length >= PAGE_SIZE));


  if(!memcpy(buff, record_ptr(page, rid.slot),  rid.size)) {
    perror("memcpy");
    abort();
  }

  unlock(page->rwlatch);
  
}

void slottedWrite(int xid, Page * page, lsn_t lsn, recordid rid, const byte *data) {
  int slot_length;

  readlock(page->rwlatch, 529);  
  

  assert(rid.size < PAGE_SIZE); 
  assert(page->id == rid.page);
  
  slot_length = *slot_length_ptr(page, rid.slot); 
  assert((rid.size == slot_length) || (rid.size == BLOB_SLOT && slot_length == sizeof(blob_record_t))|| (slot_length >= PAGE_SIZE));

  if(!memcpy(record_ptr(page, rid.slot), data, rid.size)) {
    perror("memcpy");
    abort();
  }

  /*page->LSN = lsn;
    *lsn_ptr(page) = lsn * / 
  pageWriteLSN-page); */
  unlock(page->rwlatch); 

}
void slottedWriteUnlocked(int xid, Page * page, lsn_t lsn, recordid rid, const byte *data) {
  int slot_length;

  assert(rid.size < PAGE_SIZE); 
  assert(page->id == rid.page);
  
  slot_length = *slot_length_ptr(page, rid.slot); 
  assert((rid.size == slot_length) ||  (rid.size == BLOB_SLOT && slot_length == sizeof(blob_record_t))|| (slot_length >= PAGE_SIZE));

  if(!memcpy(record_ptr(page, rid.slot), data, rid.size)) {
    perror("memcpy");
    abort();
  }
}

/*void slottedSetType(Page * p, int slot, int type) {
  assert(type > PAGE_SIZE);
  writelock(p->rwlatch, 686);
  *slot_length_ptr(p, slot) = type;
  unlock(p->rwlatch);
}

int slottedGetType(Page *  p, int slot) {
  int ret; 
  readlock(p->rwlatch, 693);
  ret = *slot_length_ptr(p, slot);
  unlock(p->rwlatch);

  / * getSlotType does the locking for us. * /
  return ret > PAGE_SIZE ? ret : NORMAL_SLOT;
  }*/
