/** $Id$ */


#include "../page.h"
#include "slotted.h"
#include <assert.h>

static void really_do_ralloc(Page * page, recordid rid) ;
size_t slottedFreespaceForSlot(Page * page, int slot);
void fsckSlottedPage(const Page const * page) {

#ifdef SLOTTED_PAGE_SANITY_CHECKS
  Page dummy;

  dummy.id = -1;
  dummy.memAddr = 0;

  const short page_type = *page_type_ptr(page);
  const short numslots  = *numslots_ptr(page);
  const short freespace = *freespace_ptr(page);
  const short freelist  = *freelist_ptr(page);

  const long slotListStart = (long)slot_length_ptr(&dummy, numslots-1);
  assert(slotListStart < PAGE_SIZE && slotListStart >= 0);
  assert(page_type == SLOTTED_PAGE || 
	 page_type == BOUNDARY_TAG_PAGE);
  assert(numslots >= 0);
  assert(numslots * SLOTTED_PAGE_OVERHEAD_PER_RECORD < PAGE_SIZE);
  assert(freespace >= 0);
  assert(freespace <= slotListStart);
  assert(freelist >= INVALID_SLOT);
  assert(freelist < numslots);
  
  // Now, check integrity of freelist.  All free slots less than numslots should be on it, in order.  

  short * slot_offsets = alloca(numslots * sizeof(short));
  short * slot_lengths = alloca(numslots * sizeof(short));
  for(int i = 0; i < numslots; i++) { 
    slot_offsets[i] = *slot_ptr(page, i);
    slot_lengths[i] = *slot_length_ptr(page, i);
  }

  short foundEndOfList = 0;

  if(freelist != INVALID_SLOT) {
    assert(slot_offsets[freelist] == INVALID_SLOT);
  } else { 
    foundEndOfList = 1;
  }

  for(short i = 0; i < numslots; i++) { 
    const short slot_length = slot_lengths[i];
    const short slot_offset = slot_offsets[i];
    if(slot_offset == INVALID_SLOT) {
      /*      if(last_freeslot == INVALID_SLOT) { 
	assert(freelist == i);
      } else {
	assert(last_freeslot_length == i);
      }
      last_freeslot = i;
      last_freeslot_length = slot_length; */
      if(slot_length == INVALID_SLOT) { 
	assert(!foundEndOfList);
	foundEndOfList = 1;  
      } else {
	assert (slot_offsets[slot_length] == INVALID_SLOT);
      }
    } else { 
      assert(slot_offset + slot_length <= freespace);
    }
  }

  // Is the free list terminated?
  assert(foundEndOfList);

#ifdef SLOTTED_PAGE_CHECK_FOR_OVERLAP

  const byte UNUSED = 0xFF;
  const byte PAGE_HEADER = 0xFE;
  const byte SLOTTED_HEADER = 0xFD;
  //  const byte SLOT_LIST = 0xFC;
  const byte FREE_SPACE = 0xFB;

  const unsigned short S_SLOT_LIST = 0xFCFC;

  byte image[PAGE_SIZE];
  for(short i = 0; i < PAGE_SIZE; i++) { 
    image[i] = UNUSED;
  }
  for(short i = USABLE_SIZE_OF_PAGE; i < PAGE_SIZE; i++) {
    image[i] = PAGE_HEADER;
  }
  for(short i = USABLE_SIZE_OF_PAGE - SLOTTED_PAGE_HEADER_OVERHEAD; i < USABLE_SIZE_OF_PAGE; i++) { 
    image[i] = SLOTTED_HEADER;
  }
  for(short i = *freespace_ptr(page); i < slotListStart; i++) { 
    image[i] = FREE_SPACE;
  }

  dummy.memAddr = image;

  for(short i = 0; i < *numslots_ptr(page); i++) { 
    *slot_ptr(&dummy, i) = S_SLOT_LIST;
    *slot_length_ptr(&dummy, i) = S_SLOT_LIST;
  }
  for(short i = 0; i < *numslots_ptr(page); i++) { 
    short slot_offset = *slot_ptr(page, i);
    if(slot_offset != INVALID_SLOT) { 
      const unsigned char ci = i % 0xFF;
      short slot_len = physical_slot_length(*slot_length_ptr(page, i));
    
      for(short j = 0; j < slot_len; j++) { 
	assert(image[slot_offset + j] == 0xFF);
	image[slot_offset + j] = ci;
      }
    }
  }
#endif // SLOTTED_PAGE_CHECK_FOR_OVERLAP
#endif // SLOTTED_PAGE_SANITY_CHECKS

}

#ifndef SLOTTED_PAGE_SANITY_CHECKS
#define fsckSlottedPage(x) ((void)0)
#endif

/**
   
Move all of the records to the beginning of the page in order to 
increase the available free space.

The caller of this function must have a writelock on the page.
*/

void slottedCompact(Page * page) { 
  Page bufPage; 
  byte buffer[PAGE_SIZE];
  bufPage.memAddr = buffer;
  
  // Copy external headers into bufPage.

  memcpy(&buffer[USABLE_SIZE_OF_PAGE], &(page->memAddr[USABLE_SIZE_OF_PAGE]), PAGE_SIZE - USABLE_SIZE_OF_PAGE);

  // Now, build new slotted page in the bufPage struct.

  *freespace_ptr(&bufPage) = 0;
  // numslots_ptr will be set later.
  *freelist_ptr(&bufPage) = INVALID_SLOT;

  const short numSlots = *numslots_ptr(page);
  short lastFreeSlot = INVALID_SLOT;
  short lastFreeSlotBeforeUsedSlot = INVALID_SLOT;
  short lastUsedSlot = -1;

  // Rebuild free list.

  for(short i = 0; i < numSlots; i++) { 
    if(*slot_ptr(page, i) == INVALID_SLOT) { 
      if(lastFreeSlot == INVALID_SLOT) { 
	*freelist_ptr(&bufPage) = i;
      } else { 
	*slot_length_ptr(&bufPage, lastFreeSlot) = i;
      }
      *slot_ptr(&bufPage, i) = INVALID_SLOT;
      lastFreeSlot = i;
    } else {
      lastUsedSlot = i; 
      lastFreeSlotBeforeUsedSlot = lastFreeSlot;

      short logicalSize = *slot_length_ptr(page, i);
      short physicalSize = physical_slot_length(logicalSize);

      memcpy(&(buffer[*freespace_ptr(&bufPage)]), record_ptr(page, i), physicalSize);

      *slot_ptr(&bufPage, i) = *freespace_ptr(&bufPage);
      *slot_length_ptr(&bufPage, i) = logicalSize;

      (*freespace_ptr(&bufPage)) += physicalSize;

    }
  }
  
  // Truncate linked list, and update numslots_ptr.
  *slot_length_ptr(&bufPage, lastFreeSlotBeforeUsedSlot) = INVALID_SLOT;
  *numslots_ptr(&bufPage) = lastUsedSlot+1;
  
  memcpy(page->memAddr, buffer, PAGE_SIZE);

  fsckSlottedPage(page);

}

void slottedCompactOld(Page * page) {

  fsckSlottedPage(page);

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
	    recordid rid;

	    rid.page = -1;
	    rid.slot = i;
	    rid.size = *slot_length_ptr(page, i);

	    really_do_ralloc(&bufPage, rid);

	    memcpy(record_ptr(&bufPage, rid.slot), record_ptr(page, rid.slot), physical_slot_length(rid.size));
	    
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
	
  fsckSlottedPage(page);

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
// static uint64_t lastFreepage = -10; 

void slottedPageInit() {
#ifdef SLOTTED_PAGE_CHECK_FOR_OVERLAP
  printf("slotted.c: Using expensive page sanity checking.\n");
#endif  
  /*pthread_mutex_init(&lastFreepage_mutex , NULL);  */
  //lastFreepage = UINT64_MAX;
}

void slottedPageDeInit() {
  /*  pthread_mutex_destroy(&lastFreepage_mutex); */
}


void slottedPageInitialize(Page * page) {
  /*printf("Initializing page %d\n", page->id);
  fflush(NULL);  */
  //  memset(page->memAddr, 0, PAGE_SIZE);
  *page_type_ptr(page) = SLOTTED_PAGE;
  *freespace_ptr(page) = 0;
  *numslots_ptr(page)  = 0;
  *freelist_ptr(page)  = INVALID_SLOT;
  fsckSlottedPage(page);

}
size_t slottedFreespaceUnlocked(Page * page);

/** 
    This is needed to correctly implement really_do_ralloc(), since
    it takes the position of the new slot's header into account.
*/
size_t slottedFreespaceForSlot(Page * page, int slot) { 
  size_t slotOverhead;

  if(slot == INVALID_SLOT) { 
    slotOverhead = (*freelist_ptr(page) == INVALID_SLOT) ? SLOTTED_PAGE_OVERHEAD_PER_RECORD : 0;
  } else if(slot < *numslots_ptr(page)) { 
    slotOverhead = 0;
  } else { 
    //    slotOverhead = SLOTTED_PAGE_OVERHEAD_PER_RECORD * (*numslots_ptr(page) - slot);
    slotOverhead = SLOTTED_PAGE_OVERHEAD_PER_RECORD * ((slot+1) - *numslots_ptr(page));
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
  return slottedFreespaceForSlot(page, INVALID_SLOT);
}

size_t slottedFreespace(Page * page) {
  size_t ret;
  readlock(page->rwlatch, 292);
  ret = slottedFreespaceUnlocked(page);
  readunlock(page->rwlatch);
  return ret;
}



recordid slottedRawRalloc(Page * page, int size) {
  int type = size;
  size = physical_slot_length(type);
  assert(type != INVALID_SLOT);

  writelock(page->rwlatch, 342);
  fsckSlottedPage(page);

  recordid rid;
  
  rid.page = page->id;
  rid.slot = *numslots_ptr(page);
  rid.size = type;  // The rid should reflect the fact that this is a special slot.

  /* The freelist_ptr points to the first free slot number, which 
     is the head of a linked list of free slot numbers.*/
  if(*freelist_ptr(page) != INVALID_SLOT) {
    rid.slot = *freelist_ptr(page);  
    // really_do_ralloc will look this slot up in the freelist (which
    // is O(1), since rid.slot is the head), and then remove it from
    // the list.
  }
  
  really_do_ralloc(page, rid);

  assert(*numslots_ptr(page) > rid.slot);
  assert(type == *slot_length_ptr(page, rid.slot));
  assert(size == physical_slot_length(*slot_length_ptr(page, rid.slot)));

  /*	DEBUG("slot: %d freespace: %d\n", rid.slot, freeSpace); */
  
  fsckSlottedPage(page);

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

  @param rid Recordid with 'internal' size.  The size should have already been translated to a type if necessary.
*/
static void really_do_ralloc(Page * page, recordid rid) {

  short freeSpace;
  
  // Compact the page if we don't have enough room.
  if(slottedFreespaceForSlot(page, rid.slot) < physical_slot_length(rid.size)) {
    slottedCompact(page);
    
    // Make sure we have enough enough free space for the new record
    assert (slottedFreespaceForSlot(page, rid.slot) >= physical_slot_length(rid.size));
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
      assert(next < *numslots_ptr(page));
      short next_slot_ptr = *slot_ptr(page, next);
      assert(next_slot_ptr == INVALID_SLOT);
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

  
  if(rid.slot > *numslots_ptr(page)) { 
    short lastSlot;
    short numSlots = *numslots_ptr(page);
    if(*freelist_ptr(page) == INVALID_SLOT) { 

      *freelist_ptr(page) = numSlots;
      lastSlot = numSlots;

      *slot_ptr(page, lastSlot) = INVALID_SLOT;
      // will set slot_length_ptr on next iteration.


      (*numslots_ptr(page))++;
    } else {
      lastSlot = INVALID_SLOT;
      short next = *freelist_ptr(page);
      while(next != INVALID_SLOT) { 
	lastSlot = next;
	next = *slot_length_ptr(page, lastSlot);
	assert(lastSlot < *numslots_ptr(page));
	assert(*slot_ptr(page, lastSlot) == INVALID_SLOT);
      }
      *slot_ptr(page, lastSlot) = INVALID_SLOT;

    }

    // lastSlot now contains the tail of the free list.  We can start adding slots to the list starting at *numslots_ptr.
    
    while(*numslots_ptr(page) < rid.slot) { 
      *slot_length_ptr(page, lastSlot) = *numslots_ptr(page);
      lastSlot = *numslots_ptr(page);
      *slot_ptr(page, lastSlot) = INVALID_SLOT;
      (*numslots_ptr(page))++;
    }

    // Terminate the end of the list.
    assert(lastSlot < *numslots_ptr(page));
    *slot_length_ptr(page, lastSlot) = INVALID_SLOT;

  }

  if(*numslots_ptr(page) == rid.slot) { 
    *numslots_ptr(page) = rid.slot+1;
  }

  assert(*numslots_ptr(page) > rid.slot);

  DEBUG("Num slots %d\trid.slot %d\n", *numslots_ptr(page), rid.slot);
  
  // Reserve space for this record and record the space's offset in
  // the slot header.

  assert(rid.slot < *numslots_ptr(page));
  *freespace_ptr(page) = freeSpace + physical_slot_length(rid.size);
  *slot_ptr(page, rid.slot)  = freeSpace;

  *slot_length_ptr(page, rid.slot) = rid.size; 

}
/**
   @param rid with user-visible size.
*/
recordid slottedPostRalloc(int xid, Page * page, lsn_t lsn, recordid rid) {
  
	writelock(page->rwlatch, 376);

	if(rid.size >= BLOB_THRESHOLD_SIZE) { 
	  rid.size = BLOB_SLOT;
	}

	if(*page_type_ptr(page) != SLOTTED_PAGE && *page_type_ptr(page) != BOUNDARY_TAG_PAGE) {
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

	     Also, this only works because pages that are of type
	     BOUNDARY_TAG_PAGE are guaranteed to have their page type
	     set before recovery calls this function.

	  */

	  slottedPageInitialize(page);  
	}
	fsckSlottedPage(page);

	if(*slot_ptr(page, rid.slot) == INVALID_SLOT || rid.slot >= *numslots_ptr(page)) {
	  really_do_ralloc(page, rid);
	
       	} else {

	  // Check to see that the slot happens to be the right size,
	  // so we are (hopefully) just overwriting a slot with
	  // itself.  This can happen under normal operation, since
	  // really_do_ralloc() must be called before and after the
	  // log entry is generated.  (See comment above...)

	  assert(rid.size == *slot_length_ptr(page, rid.slot));

	}

	pageWriteLSN(xid, page, lsn);

        fsckSlottedPage(page);
	writeunlock(page->rwlatch);


	return rid;
}

void slottedDeRalloc(int xid, Page * page, lsn_t lsn, recordid rid) {
  writelock(page->rwlatch, 443);
  fsckSlottedPage(page);

  if(*freespace_ptr(page) == *slot_ptr(page, rid.slot) + physical_slot_length(rid.size)) {
    (*freespace_ptr(page)) -= physical_slot_length(rid.size);
  }

  assert(rid.slot < *numslots_ptr(page));
  if(rid.slot == *numslots_ptr(page)-1) { 
    (*numslots_ptr(page))--;
  } else { 
    *slot_ptr(page, rid.slot) =  INVALID_SLOT;
    *slot_length_ptr(page, rid.slot) = *freelist_ptr(page); 
    *freelist_ptr(page) = rid.slot;  
  }
    
  pageWriteLSN(xid, page, lsn);
  fsckSlottedPage(page);
  unlock(page->rwlatch);
}

void slottedReadUnlocked(int xid, Page * page, recordid rid, byte *buff) {
  int slot_length;

  fsckSlottedPage(page);
  assert(page->id == rid.page);
  slot_length = *slot_length_ptr(page, rid.slot); 
  assert((rid.size == slot_length)); // || (rid.size == BLOB_SLOT && slot_length == sizeof(blob_record_t))|| (slot_length >= PAGE_SIZE));

  if(!memcpy(buff, record_ptr(page, rid.slot),  physical_slot_length(rid.size))) {
    perror("memcpy");
    abort();
  }
  fsckSlottedPage(page);

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
  fsckSlottedPage(page);

  //  printf("Reading from rid = {%d,%d,%d (%d)}\n", rid.page, rid.slot, rid.size, physical_slot_length(rid.size));

  assert(page->id == rid.page);

  // DELETE THIS

  //  int free_space = slottedFreespaceUnlocked(page);
  //  int slot_count = *numslots_ptr(page);

  // END DELETE THIS

  slot_length = *slot_length_ptr(page, rid.slot); 
  assert((rid.size == slot_length)); // || (rid.size == BLOB_SLOT && slot_length == sizeof(blob_record_t))|| (slot_length >= PAGE_SIZE));


  if(!memcpy(buff, record_ptr(page, rid.slot),  physical_slot_length(rid.size))) {
    perror("memcpy");
    abort();
  }

  fsckSlottedPage(page);
  unlock(page->rwlatch);
  
}

void slottedWrite(int xid, Page * page, lsn_t lsn, recordid rid, const byte *data) {

  readlock(page->rwlatch, 529);  
  
  slottedWriteUnlocked(xid, page, lsn, rid, data);

  unlock(page->rwlatch); 


  /*  fsckSlottedPage(page);
  //  printf("Writing to rid = {%d,%d,%d}\n", rid.page, rid.slot, rid.size);


  //  assert(rid.size < PAGE_SIZE); 
  assert(page->id == rid.page);
  
  slot_length = *slot_length_ptr(page, rid.slot); 
  assert((rid.size == slot_length)); // || (rid.size == BLOB_SLOT && slot_length == sizeof(blob_record_t))|| (slot_length >= PAGE_SIZE));

  if(!memcpy(record_ptr(page, rid.slot), data, physical_slot_length(rid.size))) {
    perror("memcpy");
    abort();
  }

  fsckSlottedPage(page); */

}
void slottedWriteUnlocked(int xid, Page * page, lsn_t lsn, recordid rid, const byte *data) {
  int slot_length;
  fsckSlottedPage(page);

  //  assert(rid.size < PAGE_SIZE); 
  assert(page->id == rid.page);
  
  slot_length = *slot_length_ptr(page, rid.slot); 
  assert((rid.size == slot_length)); // ||  (rid.size == BLOB_SLOT && slot_length == sizeof(blob_record_t))|| (slot_length >= PAGE_SIZE));

  if(!memcpy(record_ptr(page, rid.slot), data, physical_slot_length(rid.size))) {
    perror("memcpy");
    abort();
  }
  fsckSlottedPage(page);
}
