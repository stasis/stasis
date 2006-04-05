/** $Id$ */


#include "../page.h"
#include "../blobManager.h"  /** So that we can call sizeof(blob_record_t) */
#include "slotted.h"
#include <assert.h>

/* ------------------ STATIC FUNCTIONS.  NONE OF THESE ACQUIRE LOCKS
                      ON THE MEMORY THAT IS PASSED INTO THEM -------------*/

static void __really_do_ralloc(Page * page, recordid rid) ;

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
                                /* *slot_length_ptr(page, (*numslots_ptr(page))-1);*/

	memcpy(buffer + PAGE_SIZE - meta_size, page->memAddr + PAGE_SIZE - meta_size, meta_size);

	slottedPageInitialize(&bufPage);

	numSlots = *numslots_ptr(page);
	for (i = 0; i < numSlots; i++) {
	  /*	  ("i = %d\n", i);   */
	  if (isValidSlot(page, i)) {
	    /*	    printf("copying %d\n", i); 
		    fflush(NULL); */
	    /*  		  DEBUG("Buffer offset: %d\n", freeSpace); */
	    recordid rid;

	    rid.page = -1;
	    rid.slot = i;
	    rid.size = *slot_length_ptr(page, i);

	    __really_do_ralloc(&bufPage, rid);

	    memcpy(record_ptr(&bufPage, rid.slot), record_ptr(page, rid.slot), rid.size);
	    
	  } else {
	    *slot_ptr(&bufPage, i) = INVALID_SLOT;
	    *slot_length_ptr(&bufPage, i) = *freelist_ptr(page);
	    *freelist_ptr(page) = i;
	  }
	}
	
	/** The freelist could potentially run past the end of the
	    space that is allocated for slots (this would happen if
	    the number of slots needed by this page just decreased.
	    If we let the list run outside of that area, it could
	    cause inadvertant page corruption.  Therefore, we need to
	    truncate the list before continuing. */

	short next = *freelist_ptr(page);
	while(next >= numSlots && next != INVALID_SLOT) {
	  next = *slot_length_ptr(page, next);
	}

	*freelist_ptr(page) = next;
	
	// Rebuild the freelist. 
	
	/*	*freelist_ptr(&bufPage) = 0;
	for (i = 0; i < numSlots; i++) { 
	  if (!isValidSlot(&bufPage, i)) {
	    *slot_length_ptr(&bufPage, i) = *freelist_ptr(&bufPage);
	    *freelist_ptr(&bufPage) = i;
	    break;
	  }
	  } */
	

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
  static unsigned int lastFreepage = -10; 

void slottedPageInit() {
  /*pthread_mutex_init(&lastFreepage_mutex , NULL);  */
  lastFreepage = -1;
}

void slottedPageDeinit() {
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
//@todo Still wrong...handles full pages incorrectly.

/** 
    This is needed to correctly implement __really_do_ralloc(), since
    it takes the position of the new slot's header into account.
*/
size_t slottedFreespaceForSlot(Page * page, int slot) { 
  size_t slotOverhead;

  if(slot >= 0 && slot < *numslots_ptr(page)) { 
    slotOverhead = 0;
  } else { 
    //    assert(*numslots_ptr(page) == slot || slot < 0);
    slotOverhead = SLOTTED_PAGE_OVERHEAD_PER_RECORD * (*numslots_ptr(page) - slot);
  }
  // end_of_free_space points to the beginning of the slot header at the bottom of the page header.
  byte* end_of_free_space = (byte*)slot_length_ptr(page, (*numslots_ptr(page))-1); 
  // start_of_free_space points to the first unallocated byte in the page
  // (ignoring space that could be reclaimed by compaction)
  byte* start_of_free_space =  (byte*)(page->memAddr + *freespace_ptr(page));
  assert(end_of_free_space >= start_of_free_space);
  // We need the "+ SLOTTED_PAGE_OVERHEAD_PER_RECORD" because the regions they cover could overlap.
  //  assert(end_of_free_space + SLOTTED_PAGE_OVERHEAD_PER_RECORD >= start_of_free_space); 

  if(end_of_free_space < start_of_free_space + slotOverhead) { 
    // The regions would overlap after allocation; there is no free space.
    return 0;
  } else { 
    // The regions do not overlap.  There might be free space.
    return (size_t) (end_of_free_space - start_of_free_space - slotOverhead);
  }
}

/** @todo Implement a model of slotted pages in the test scripts, and
    then write a randomized test that confirms the model matches the
    implementation's behavior. */
size_t slottedFreespaceUnlocked(Page * page) { 
  return slottedFreespaceForSlot(page, -1);
}
/*size_t slottedFreespaceUnlocked(Page * page) {
  // end_of_free_space points to the beginning of the slot header the caller is about to allocate.
  byte* end_of_free_space = (byte*)slot_length_ptr(page, *numslots_ptr(page)); 
  // start_of_free_space points to the first unallocated byte in the page
  // (ignoring space that could be reclaimed by compaction)
  byte* start_of_free_space =  (byte*)(page->memAddr + *freespace_ptr(page));
  // We need the "+ SLOTTED_PAGE_OVERHEAD_PER_RECORD" because the regions they cover could overlap.
  assert(end_of_free_space + SLOTTED_PAGE_OVERHEAD_PER_RECORD >= start_of_free_space); 

  if(end_of_free_space < start_of_free_space) { 
    // The regions overlap; there is no free space.
    return 0;
  } else { 
    // The regions do not overlap.  There might be free space.
    return (size_t) (end_of_free_space - start_of_free_space);
  }
  }*/


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
compensated_function recordid slottedPreRalloc(int xid, long size, Page ** pp) {
  
  recordid ret;
  
  int isBlob = 0;

  if(size == BLOB_SLOT) {
    isBlob = 1;
    size = sizeof(blob_record_t);
  }

  assert(size < BLOB_THRESHOLD_SIZE);


  /** @todo is ((unsigned int) foo) == -1 portable?  Gotta love C.*/

  if(lastFreepage == -1) {
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
  
  ret = slottedRawRalloc(*pp, size);
  
  if(isBlob) {
    *slot_length_ptr(*pp, ret.slot) = BLOB_SLOT;
  }

  DEBUG("alloced rid = {%d, %d, %ld}\n", ret.page, ret.slot, ret.size); 

  return ret;
}

compensated_function recordid slottedPreRallocFromPage(int xid, long page, long size, Page **pp) {
  int isBlob = 0;
  if(size == BLOB_SLOT) {
    isBlob = 1;
    size = sizeof(blob_record_t);
  }
  try_ret(NULLRID) {
    *pp = loadPage(xid, page);
  } end_ret(NULLRID);
  if(slottedFreespace(*pp) < size) {
    releasePage(*pp);
    *pp = NULL;
    return NULLRID;
  }
  
  if(*page_type_ptr(*pp) == UNINITIALIZED_PAGE) {
    slottedPageInitialize(*pp);
  }
  assert(*page_type_ptr(*pp) == SLOTTED_PAGE);
  recordid ret = slottedRawRalloc(*pp, size);
  assert(ret.size == size);
  if(isBlob) {
    *slot_length_ptr(*pp, ret.slot) = BLOB_SLOT;
  }
  return ret;
  
}

recordid slottedRawRalloc(Page * page, int size) {

	writelock(page->rwlatch, 342);

	recordid rid;

	rid.page = page->id;
	rid.slot = *numslots_ptr(page);
	rid.size = size;

	/* new way - The freelist_ptr points to the first free slot number, which 
	   is the head of a linked list of free slot numbers.*/
	if(*freelist_ptr(page) != INVALID_SLOT) {
	  rid.slot = *freelist_ptr(page);
	  *freelist_ptr(page) = *slot_length_ptr(page, rid.slot);
	  *slot_length_ptr(page, rid.slot) = 0;
	}  
	  
	__really_do_ralloc(page, rid);

	/*	DEBUG("slot: %d freespace: %d\n", rid.slot, freeSpace); */

	assert(slottedFreespaceUnlocked(page) >= 0);

	writeunlock(page->rwlatch);


	return rid;
}

static void __really_do_ralloc(Page * page, recordid rid) {

  short freeSpace;
  
  int isBlob = 0;

  if(rid.size == BLOB_SLOT) {
    isBlob = 1;
    rid.size = sizeof(blob_record_t);
  }

  assert(rid.size > 0);

  if(slottedFreespaceForSlot(page, rid.slot) < rid.size) {
    slottedCompact(page);
    
    /* Make sure there's enough free space... */
    // DELETE NEXT LINE
    //int size = slottedFreespaceForSlot(page, rid.slot);
    assert (slottedFreespaceForSlot(page, rid.slot) >= rid.size);
  }

  //  assert(*numslots_ptr(page) >= rid.slot);

  freeSpace = *freespace_ptr(page);
  // Totally wrong!
  if(*numslots_ptr(page) <= rid.slot) {
    /*    printf("Incrementing numSlots."); */
    *numslots_ptr(page) = rid.slot + 1;
  } 

  DEBUG("Num slots %d\trid.slot %d\n", *numslots_ptr(page), rid.slot);

  *freespace_ptr(page) = freeSpace + rid.size;

  *slot_ptr(page, rid.slot)  = freeSpace;

  /*  assert(!*slot_length_ptr(page, rid.slot) || (-1 == *slot_length_ptr(page, rid.slot)));*/
  if(isBlob) {
    *slot_length_ptr(page, rid.slot = BLOB_SLOT);
  } else {
    *slot_length_ptr(page, rid.slot) = rid.size; 
  }

  assert(slottedFreespaceUnlocked(page) >= 0);

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

	if((*slot_length_ptr(page, rid.slot) == 0) || (*slot_ptr(page, rid.slot) == INVALID_SLOT)) {
	  /*	if(*slot_ptr(page, rid.slot) == INVALID_SLOT) { */

	  __really_do_ralloc(page, rid);
	
       	} else {

	  /*	  int ijk = rid.size;
		  int lmn = *slot_length_ptr(page, rid.slot); */

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
