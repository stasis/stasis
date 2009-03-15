#include "config.h"
#include <stasis/page.h>
#include <stasis/page/slotted.h>
#include <assert.h>
/** @todo should page implementations provide readLSN / writeLSN??? */
#include <stasis/truncation.h>

/**
   Run sanity checks to make sure page is in a consistent state.

   If SLOTTED_PAGE_SANITY_CHECKS and SLOTTED_PAGE_CHECK_FOR_OVERLAP
   are defined at compile time then this method will be more thorough
   and more expensive.
 */
static inline void slottedFsck(const Page const * page) {

  assertlocked(page->rwlatch);

  Page dummy;

  dummy.id = -1;
  dummy.memAddr = 0;

  const short page_type = *stasis_page_type_cptr(page);
  const short numslots  = *numslots_cptr(page);
  const short freespace = *freespace_cptr(page);
  const short freelist  = *freelist_cptr(page);

  const long slotListStart = (long)slot_length_ptr(&dummy, numslots-1);
  assert(slotListStart < PAGE_SIZE && slotListStart >= 0);
  assert(page_type == SLOTTED_PAGE ||
	 page_type == BOUNDARY_TAG_PAGE ||
         page_type == SLOTTED_LSN_FREE_PAGE);
  assert(numslots >= 0);
  assert(numslots * SLOTTED_PAGE_OVERHEAD_PER_RECORD < PAGE_SIZE);
  assert(freespace >= 0);
  assert(freespace <= slotListStart);
  assert(freelist >= INVALID_SLOT);
  assert(freelist < numslots);

#ifdef SLOTTED_PAGE_SANITY_CHECKS

  // Check integrity of freelist.  All free slots less than
  // numslots should be on it, in order.

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
      short slot_len = stasis_record_type_to_size(*slot_length_ptr(page, i));
    
      for(short j = 0; j < slot_len; j++) { 
	assert(image[slot_offset + j] == 0xFF);
	image[slot_offset + j] = ci;
      }
    }
  }
#endif // SLOTTED_PAGE_CHECK_FOR_OVERLAP
#endif // SLOTTED_PAGE_SANITY_CHECKS

}

/**

Move all of the records to the beginning of the page in order to
increase the available free space.

The caller of this function must have a writelock on the page.
*/

static void slottedCompact(Page * page) { 
  assertlocked(page->rwlatch);
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
      short physicalSize = stasis_record_type_to_size(logicalSize);

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

  slottedFsck(page);

}

void slottedPageInit() {
#ifdef SLOTTED_PAGE_CHECK_FOR_OVERLAP
  printf("slotted.c: Using expensive page sanity checking.\n");
#endif  
}

void slottedPageDeinit() {
}


void stasis_slotted_initialize_page(Page * page) {
  assertlocked(page->rwlatch);
  stasis_page_cleanup(page);
  *stasis_page_type_ptr(page) = SLOTTED_PAGE;
  *freespace_ptr(page) = 0;
  *numslots_ptr(page)  = 0;
  *freelist_ptr(page)  = INVALID_SLOT;
}

/**
   Check to see how many bytes can fit in a given slot.  This
   makes it possible for callers to guarantee the safety
   of a subsequent call to really_do_ralloc().
*/
static size_t slottedFreespaceForSlot(Page * page, int slot) { 
  assertlocked(page->rwlatch);
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

/**
  Allocate data on a page after deciding which recordid to allocate,
  and making sure there is enough freespace.

  Allocation is complicated without locking.  Consider this situation:

   (1) *numslot_ptr(page) is 10
   (2) An aborting transcation calls really_do_ralloc(page) with rid.slot = 12
   (3) *numslot_ptr(page) must be incremented to 12.  Now, what happens to 11?
     - If 11 was also deleted by a transaction that could abort, we should lock it so that it won't be reused.
   (4) This function adds it to the freelist to avoid leaking space.  (Therefore, Talloc() can return recordids that will
       be reused by aborting transactions...)

  For now, we make sure that we don't alloc off a page that another active
  transaction dealloced from.

  @param page A pointer to the page.
  @param rid Recordid with 'internal' size.  The size should have already been translated to a type if necessary.
*/
static void really_do_ralloc(Page * page, recordid rid) {
  assertlocked(page->rwlatch);

  short freeSpace;
  
  // Compact the page if we don't have enough room.
  if(slottedFreespaceForSlot(page, rid.slot) < stasis_record_type_to_size(rid.size)) {
    slottedCompact(page);
    
    // Make sure we have enough enough free space for the new record
    assert (slottedFreespaceForSlot(page, rid.slot) >= stasis_record_type_to_size(rid.size));
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
  *freespace_ptr(page) = freeSpace + stasis_record_type_to_size(rid.size);
  *slot_ptr(page, rid.slot)  = freeSpace;

  *slot_length_ptr(page, rid.slot) = rid.size; 

}

// --------------------------------------------------------------------------
// PUBLIC API IS BELOW THIS LINE
// --------------------------------------------------------------------------

static inline void sanityCheck(Page * p, recordid rid) {
  assert(p->id == rid.page);
  assert(rid.size < BLOB_THRESHOLD_SIZE); // Caller deals with this now!
  slottedFsck(p);
}

static const byte* slottedRead (int xid, Page *p, recordid rid) {
  sanityCheck(p, rid);

  return record_ptr(p, rid.slot);
}

static byte* slottedWrite(int xid, Page *p, recordid rid) {
  sanityCheck(p, rid);

  return record_ptr(p, rid.slot);
}
static int slottedGetType(int xid, Page *p, recordid rid) {
  //sanityCheck(p, rid); <-- Would fail if rid.size is a blob
  assert(p->id == rid.page);
  slottedFsck(p);
  if(rid.slot >= *numslots_ptr(p)) { return INVALID_SLOT; }
  if(*slot_ptr(p, rid.slot) == INVALID_SLOT) { return INVALID_SLOT; }
  int ret = *slot_length_ptr(p, rid.slot);
  return ret >= 0 ? NORMAL_SLOT : ret;
}
static void slottedSetType(int xid, Page *p, recordid rid, int type) {
  sanityCheck(p, rid);

  int old_type = *slot_length_ptr(p, rid.slot);
  assert(rid.slot < *numslots_ptr(p));
  assert(old_type != INVALID_SLOT);

  if(type == NORMAL_SLOT) {
    // set slot_length_ptr to the physical length.
    *slot_length_ptr(p, rid.slot) = stasis_record_type_to_size(old_type);
  } else {
    // Changing to a special slot type; make sure doing so doesn't change
    // the record size.
    assert(stasis_record_type_to_size(type) == stasis_record_type_to_size(old_type));
    *slot_length_ptr(p, rid.slot) = type;
  }
}

static int slottedGetLength(int xid, Page *p, recordid rid) {
  assert(p->id == rid.page);
  slottedFsck(p);
  if( slottedGetType(xid, p, rid) == INVALID_SLOT)
    return INVALID_SLOT;
  else
    return stasis_record_type_to_size(*slot_length_ptr(p, rid.slot));
}

static recordid slottedNext(int xid, Page *p, recordid rid) {
  sanityCheck(p, rid);

  short n = *numslots_ptr(p);
  rid.slot ++;
  while(rid.slot < n && slottedGetType(xid,p,rid)==INVALID_SLOT) {
    rid.slot++;
  }
  if(rid.slot != n) { 
    rid.size = *slot_length_ptr(p, rid.slot);
    return rid;
  } else {
    return NULLRID;
  }
}

static recordid slottedFirst(int xid, Page *p) {
  slottedFsck(p);

  recordid rid = { p->id, -1, 0 };
  return slottedNext(xid, p, rid);
}

static int notSupported(int xid, Page * p) { return 0; }

static int slottedFreespace(int xid, Page * p) {
  slottedFsck(p);

  return slottedFreespaceForSlot(p, INVALID_SLOT);
}

static recordid slottedPreRalloc(int xid, Page * p, int type) {
  assert(type != INVALID_SLOT);
  slottedFsck(p);

  recordid rid;
  rid.page = p->id;
  rid.slot = *numslots_ptr(p);
  rid.size = type;

  if(*freelist_ptr(p) != INVALID_SLOT) {
    rid.slot = *freelist_ptr(p);
  }

  if(slottedFreespaceForSlot(p, rid.slot) < stasis_record_type_to_size(type)) {
    rid = NULLRID;
  }

  return rid;
}

static void slottedPostRalloc(int xid, Page * p, recordid rid) {
  sanityCheck(p, rid);

  really_do_ralloc(p, rid);
}

static void slottedFree(int xid, Page * p, recordid rid) {
  sanityCheck(p, rid);

  if(*freespace_ptr(p) == *slot_ptr(p, rid.slot) + stasis_record_type_to_size(rid.size)) {
    (*freespace_ptr(p)) -= stasis_record_type_to_size(rid.size);
  }

  assert(rid.slot < *numslots_ptr(p));
  if(rid.slot == *numslots_ptr(p)-1) {
    (*numslots_ptr(p))--;
    assert(slottedGetType(xid,p,rid)==INVALID_SLOT);
  } else {
    *slot_ptr(p, rid.slot) =  INVALID_SLOT;
    *slot_length_ptr(p, rid.slot) = *freelist_ptr(p);
    *freelist_ptr(p) = rid.slot;
    assert(slottedGetType(xid,p,rid)==INVALID_SLOT);
  }

  slottedFsck(p);
}


// XXX dereferenceRID

static void slottedLoaded(Page *p) {
  p->LSN = *stasis_page_lsn_ptr(p);
  // @todo arrange for pagefsck to run on load/flush, but nowhere else.
  slottedFsck(p);
}
static void slottedFlushed(Page *p) {
  *stasis_page_lsn_ptr(p) = p->LSN;
  slottedFsck(p);
}
static void slottedCleanup(Page *p) { }

page_impl slottedImpl() {
static page_impl pi =  {
    SLOTTED_PAGE,
    slottedRead,
    slottedWrite,
    0,// readDone
    0,// writeDone
    slottedGetType,
    slottedSetType,
    slottedGetLength,
    slottedFirst,
    slottedNext,
    notSupported, // is block supported
    stasis_block_first_default_impl,
    stasis_block_next_default_impl,
    stasis_block_done_default_impl,
    slottedFreespace,
    slottedCompact,
    slottedPreRalloc,
    slottedPostRalloc,
    slottedFree,
    0, //XXX page_impl_dereference_identity,
    slottedLoaded,
    slottedFlushed,
    slottedCleanup
  };
  return pi;
}

page_impl boundaryTagImpl() {
  page_impl p =  slottedImpl();
  p.page_type = BOUNDARY_TAG_PAGE;
  return p;
}
