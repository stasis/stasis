#include "config.h"
#include <stasis/page.h>
#include <stasis/page/slotted.h>
#include <assert.h>

#define SLOTTED_PAGE_OVERHEAD_PER_RECORD (2 * sizeof(short))
#define SLOTTED_PAGE_HEADER_OVERHEAD (3 * sizeof(short))

#ifdef LONG_TEST
#define SLOTTED_PAGE_CHECK_FOR_OVERLAP 1
#endif

// plan: slotted fsck on read / write.  Make it more thorough so that the other methods only check for record existence.
//       intermediate ops assume that slotted.c is correctly implemented (ie: fsck passes iff page is OK, page always "stays" OK)
// benchmark page ops (real time) + hash table (user time)

//#define SLOTTED_PAGE_OLD_CHECKS
#define SLOTTED_PAGE_NEW_CHECKS
#ifdef SLOTTED_PAGE_NEW_CHECKS
#define SLOTTED_PAGE_SANITY_CHECKS
#define SLOTTED_PAGE_CHECK_FOR_OVERLAP
#endif
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

  const short page_type = page->pageType;
  const short numslots  = *stasis_page_slotted_numslots_cptr(page);
  const short freespace = *stasis_page_slotted_freespace_cptr(page);
  const short freelist  = *stasis_page_slotted_freelist_cptr(page);

  const long slotListStart = (long)stasis_page_slotted_slot_length_ptr(&dummy, numslots-1);
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
    slot_offsets[i] = *stasis_page_slotted_slot_cptr(page, i);
    slot_lengths[i] = *stasis_page_slotted_slot_length_cptr(page, i);
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
  for(short i = *stasis_page_slotted_freespace_cptr(page); i < slotListStart; i++) {
    image[i] = FREE_SPACE;
  }

  dummy.memAddr = image;

  for(short i = 0; i < *stasis_page_slotted_numslots_cptr(page); i++) {
    *stasis_page_slotted_slot_ptr(&dummy, i) = S_SLOT_LIST;
    *stasis_page_slotted_slot_length_ptr(&dummy, i) = S_SLOT_LIST;
  }
  for(short i = 0; i < *stasis_page_slotted_numslots_cptr(page); i++) {
    short slot_offset = *stasis_page_slotted_slot_cptr(page, i);
    if(slot_offset != INVALID_SLOT) {
      const unsigned char ci = i % 0xFF;
      short slot_len = stasis_record_type_to_size(*stasis_page_slotted_slot_length_cptr(page, i));

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

  *stasis_page_slotted_freespace_ptr(&bufPage) = 0;
  // numslots_ptr will be set later.
  *stasis_page_slotted_freelist_ptr(&bufPage) = INVALID_SLOT;

  const short numSlots = *stasis_page_slotted_numslots_ptr(page);
  short lastFreeSlot = INVALID_SLOT;
  short lastFreeSlotBeforeUsedSlot = INVALID_SLOT;
  short lastUsedSlot = -1;

  // Rebuild free list.

  for(short i = 0; i < numSlots; i++) {
    if(*stasis_page_slotted_slot_ptr(page, i) == INVALID_SLOT) {
      if(lastFreeSlot == INVALID_SLOT) {
        *stasis_page_slotted_freelist_ptr(&bufPage) = i;
      } else {
        *stasis_page_slotted_slot_length_ptr(&bufPage, lastFreeSlot) = i;
      }
      *stasis_page_slotted_slot_ptr(&bufPage, i) = INVALID_SLOT;
      lastFreeSlot = i;
    } else {
      lastUsedSlot = i;
      lastFreeSlotBeforeUsedSlot = lastFreeSlot;

      short logicalSize = *stasis_page_slotted_slot_length_ptr(page, i);
      short physicalSize = stasis_record_type_to_size(logicalSize);

      memcpy(&(buffer[*stasis_page_slotted_freespace_ptr(&bufPage)]), stasis_page_slotted_record_ptr(page, i), physicalSize);

      *stasis_page_slotted_slot_ptr(&bufPage, i) = *stasis_page_slotted_freespace_ptr(&bufPage);
      *stasis_page_slotted_slot_length_ptr(&bufPage, i) = logicalSize;

      (*stasis_page_slotted_freespace_ptr(&bufPage)) += physicalSize;

    }
  }

  // Truncate linked list, and update numslots_ptr.
  *stasis_page_slotted_slot_length_ptr(&bufPage, lastFreeSlotBeforeUsedSlot) = INVALID_SLOT;
  *stasis_page_slotted_numslots_ptr(&bufPage) = lastUsedSlot+1;

  memcpy(page->memAddr, buffer, PAGE_SIZE);
#ifdef SLOTTED_PAGE_OLD_CHECKS
  slottedFsck(page);
#endif // SLOTTED_PAGE_OLD_CHECKS
}

static void slottedCompactSlotIDs(int xid, Page * p) {
  int16_t numSlots = *stasis_page_slotted_numslots_ptr(p);
  int16_t out = 0;
  for(int16_t in = 0; in < numSlots; in++) {
    if(*stasis_page_slotted_slot_ptr(p, in) == INVALID_SLOT) {
      // nop
    } else {
      *stasis_page_slotted_slot_ptr(p, out) = *stasis_page_slotted_slot_cptr(p, in);
      *stasis_page_slotted_slot_length_ptr(p, out) = *stasis_page_slotted_slot_length_cptr(p, in);
      out++;
    }
  }
  *stasis_page_slotted_numslots_ptr(p) = out;
  *stasis_page_slotted_freelist_ptr(p) = INVALID_SLOT;
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
    slotOverhead = (*stasis_page_slotted_freelist_ptr(page) == INVALID_SLOT) ? SLOTTED_PAGE_OVERHEAD_PER_RECORD : 0;
  } else if(slot < *stasis_page_slotted_numslots_ptr(page)) {
    slotOverhead = 0;
  } else {
    //    slotOverhead = SLOTTED_PAGE_OVERHEAD_PER_RECORD * (*numslots_ptr(page) - slot);
    slotOverhead = SLOTTED_PAGE_OVERHEAD_PER_RECORD * ((slot+1) - *stasis_page_slotted_numslots_ptr(page));
  }
  // end_of_free_space points to the beginning of the slot header at the bottom of the page header.
  byte* end_of_free_space = (byte*)stasis_page_slotted_slot_length_ptr(page, (*stasis_page_slotted_numslots_ptr(page))-1);

  // start_of_free_space points to the first unallocated byte in the page
  // (ignoring space that could be reclaimed by compaction)
  byte* start_of_free_space =  (byte*)(page->memAddr + *stasis_page_slotted_freespace_ptr(page));

  assert(end_of_free_space >= start_of_free_space);

  if(end_of_free_space < start_of_free_space + slotOverhead) {
    // The regions would overlap after allocation.  There is no free space.
    return 0;
  } else {
    // The regions would not overlap.  There might be free space.
    return (size_t) (end_of_free_space - start_of_free_space - slotOverhead);
  }
}

static inline void slottedSanityCheck(Page * p, recordid rid) {
#ifdef SLOTTED_PAGE_OLD_CHECKS
  assert(p->id == rid.page);
  assert(rid.size < BLOB_THRESHOLD_SIZE); // Caller deals with this now!
  slottedFsck(p);
#endif
}

// --------------------------------------------------------------------------
// PUBLIC API IS BELOW THIS LINE
// --------------------------------------------------------------------------

static const byte* slottedRead (int xid, Page *p, recordid rid) {
  slottedSanityCheck(p, rid);

  return stasis_page_slotted_record_ptr(p, rid.slot);
}

static byte* slottedWrite(int xid, Page *p, recordid rid) {
  slottedSanityCheck(p, rid);

  return stasis_page_slotted_record_ptr(p, rid.slot);
}
static int slottedGetType(int xid, Page *p, recordid rid) {
#ifdef SLOTTED_PAGE_OLD_CHECKS
  //sanityCheck(p, rid); <-- Would fail if rid.size is a blob
  assert(p->id == rid.page);
  slottedFsck(p);
#endif
  if(rid.slot >= *stasis_page_slotted_numslots_ptr(p)) { return INVALID_SLOT; }
  if(*stasis_page_slotted_slot_ptr(p, rid.slot) == INVALID_SLOT) { return INVALID_SLOT; }
  int ret = *stasis_page_slotted_slot_length_ptr(p, rid.slot);
  return ret >= 0 ? NORMAL_SLOT : ret;
}
static void slottedSetType(int xid, Page *p, recordid rid, int type) {
  slottedSanityCheck(p, rid);

  int old_type = *stasis_page_slotted_slot_length_ptr(p, rid.slot);
  assert(rid.slot < *stasis_page_slotted_numslots_ptr(p));
  assert(old_type != INVALID_SLOT);

  if(type == NORMAL_SLOT) {
    // set slot_length_ptr to the physical length.
    *stasis_page_slotted_slot_length_ptr(p, rid.slot) = stasis_record_type_to_size(old_type);
  } else {
    // Changing to a special slot type; make sure doing so doesn't change
    // the record size.
    assert(stasis_record_type_to_size(type) == stasis_record_type_to_size(old_type));
    *stasis_page_slotted_slot_length_ptr(p, rid.slot) = type;
  }
}

static int slottedGetLength(int xid, Page *p, recordid rid) {
#ifdef SLOTTED_PAGE_OLD_CHECKS
  assert(p->id == rid.page);
  slottedFsck(p);
#endif
  if( slottedGetType(xid, p, rid) == INVALID_SLOT)
    return INVALID_SLOT;
  else
    return stasis_record_type_to_size(*stasis_page_slotted_slot_length_ptr(p, rid.slot));
}

static recordid slottedNext(int xid, Page *p, recordid rid) {
  slottedSanityCheck(p, rid);

  short n = *stasis_page_slotted_numslots_ptr(p);
  rid.slot ++;
  while(rid.slot < n && slottedGetType(xid,p,rid)==INVALID_SLOT) {
    rid.slot++;
  }
  if(rid.slot != n) {
    rid.size = *stasis_page_slotted_slot_length_ptr(p, rid.slot);
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
  rid.slot = *stasis_page_slotted_numslots_ptr(p);
  rid.size = type;

  if(*stasis_page_slotted_freelist_ptr(p) != INVALID_SLOT) {
    rid.slot = *stasis_page_slotted_freelist_ptr(p);
  }

  if(slottedFreespaceForSlot(p, rid.slot) < stasis_record_type_to_size(type)) {
    rid = NULLRID;
  }

  return rid;
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

  @param xid The transaction allocating the record.
  @param page A pointer to the page.
  @param rid Recordid with 'internal' size.  The size should have already been translated to a type if necessary.
*/
static void slottedPostRalloc(int xid, Page * page, recordid rid) {
  slottedSanityCheck(page, rid);

  short freeSpace;

  // Compact the page if we don't have enough room.
  if(slottedFreespaceForSlot(page, rid.slot) < stasis_record_type_to_size(rid.size)) {
    slottedCompact(page);

    // Make sure we have enough enough free space for the new record
    assert (slottedFreespaceForSlot(page, rid.slot) >= stasis_record_type_to_size(rid.size));
  }

  freeSpace = *stasis_page_slotted_freespace_ptr(page);

  // Remove this entry from the freelist (if necessary) slottedCompact
  // assumes that this does not change the order of items in the list.
  // If it did, then slottedCompact could leaks slot id's (or worse!)
  if(rid.slot < *stasis_page_slotted_numslots_ptr(page) && *stasis_page_slotted_slot_ptr(page,rid.slot) == INVALID_SLOT) {
    short next = *stasis_page_slotted_freelist_ptr(page);
    short last = INVALID_SLOT;
    // special case:  is the slot physically before us the predecessor?
    if(rid.slot > 0) {
      if(*stasis_page_slotted_slot_length_ptr(page, rid.slot-1) == rid.slot && *stasis_page_slotted_slot_ptr(page, rid.slot-1) == INVALID_SLOT) {
    next = rid.slot;
    last = rid.slot-1;
      }
    }
    while(next != INVALID_SLOT && next != rid.slot) {
      last = next;
      assert(next < *stasis_page_slotted_numslots_ptr(page));
      short next_slot_ptr = *stasis_page_slotted_slot_ptr(page, next);
      assert(next_slot_ptr == INVALID_SLOT);
      next = *stasis_page_slotted_slot_length_ptr(page, next);
    }
    if(next == rid.slot) {
      if(last == INVALID_SLOT) {
    *stasis_page_slotted_freelist_ptr(page) = *stasis_page_slotted_slot_length_ptr(page, rid.slot);
      } else {
    *stasis_page_slotted_slot_length_ptr(page, last) = *stasis_page_slotted_slot_length_ptr(page, rid.slot);
      }
    }
  }

  // Insert any slots that come between the previous numslots_ptr()
  // and the slot we're allocating onto the freelist.  In order to
  // promote the reuse of free slot numbers, we go out of our way to make sure
  // that we put them in the list in increasing order.  (Note:  slottedCompact's
  // correctness depends on this behavior!)


  if(rid.slot > *stasis_page_slotted_numslots_ptr(page)) {
    short lastSlot;
    short numSlots = *stasis_page_slotted_numslots_ptr(page);
    if(*stasis_page_slotted_freelist_ptr(page) == INVALID_SLOT) {

      *stasis_page_slotted_freelist_ptr(page) = numSlots;
      lastSlot = numSlots;

      *stasis_page_slotted_slot_ptr(page, lastSlot) = INVALID_SLOT;
      // will set slot_length_ptr on next iteration.


      (*stasis_page_slotted_numslots_ptr(page))++;
    } else {
      lastSlot = INVALID_SLOT;
      short next = *stasis_page_slotted_freelist_ptr(page);
      while(next != INVALID_SLOT) {
    lastSlot = next;
    next = *stasis_page_slotted_slot_length_ptr(page, lastSlot);
    assert(lastSlot < *stasis_page_slotted_numslots_ptr(page));
    assert(*stasis_page_slotted_slot_ptr(page, lastSlot) == INVALID_SLOT);
      }
      *stasis_page_slotted_slot_ptr(page, lastSlot) = INVALID_SLOT;

    }

    // lastSlot now contains the tail of the free list.  We can start adding slots to the list starting at *numslots_ptr.

    while(*stasis_page_slotted_numslots_ptr(page) < rid.slot) {
      *stasis_page_slotted_slot_length_ptr(page, lastSlot) = *stasis_page_slotted_numslots_ptr(page);
      lastSlot = *stasis_page_slotted_numslots_ptr(page);
      *stasis_page_slotted_slot_ptr(page, lastSlot) = INVALID_SLOT;
      (*stasis_page_slotted_numslots_ptr(page))++;
    }

    // Terminate the end of the list.
    assert(lastSlot < *stasis_page_slotted_numslots_ptr(page));
    *stasis_page_slotted_slot_length_ptr(page, lastSlot) = INVALID_SLOT;

  }

  if(*stasis_page_slotted_numslots_ptr(page) == rid.slot) {
    *stasis_page_slotted_numslots_ptr(page) = rid.slot+1;
  }

  assert(*stasis_page_slotted_numslots_ptr(page) > rid.slot);

  DEBUG("Num slots %d\trid.slot %d\n", *stasis_page_slotted_numslots_ptr(page), rid.slot);

  // Reserve space for this record and record the space's offset in
  // the slot header.

  assert(rid.slot < *stasis_page_slotted_numslots_ptr(page));
  *stasis_page_slotted_freespace_ptr(page) = freeSpace + stasis_record_type_to_size(rid.size);
  *stasis_page_slotted_slot_ptr(page, rid.slot)  = freeSpace;

  *stasis_page_slotted_slot_length_ptr(page, rid.slot) = rid.size;

}

static void slottedSpliceSlot(int xid, Page *p, slotid_t a, slotid_t b) {
  assert(a < b);
  int16_t b_slot = *stasis_page_slotted_slot_cptr(p, b);
  int16_t b_slot_len = *stasis_page_slotted_slot_length_cptr(p, b);
  for(int16_t i = b-1; i >= a; i--) {
    *stasis_page_slotted_slot_ptr(p, i+1) = *stasis_page_slotted_slot_cptr(p, i);
    *stasis_page_slotted_slot_length_ptr(p, i+1) = *stasis_page_slotted_slot_length_cptr(p, i);
  }
  *stasis_page_slotted_slot_ptr(p, a) = b_slot;
  *stasis_page_slotted_slot_length_ptr(p, b) = b_slot_len;
}

static void slottedFree(int xid, Page * p, recordid rid) {
  slottedSanityCheck(p, rid);

  if(*stasis_page_slotted_freespace_ptr(p) == *stasis_page_slotted_slot_ptr(p, rid.slot) + stasis_record_type_to_size(rid.size)) {
    (*stasis_page_slotted_freespace_ptr(p)) -= stasis_record_type_to_size(rid.size);
  }

  assert(rid.slot < *stasis_page_slotted_numslots_ptr(p));
  if(rid.slot == *stasis_page_slotted_numslots_ptr(p)-1) {
    (*stasis_page_slotted_numslots_ptr(p))--;
    assert(slottedGetType(xid,p,rid)==INVALID_SLOT);
  } else {
    *stasis_page_slotted_slot_ptr(p, rid.slot) =  INVALID_SLOT;
    *stasis_page_slotted_slot_length_ptr(p, rid.slot) = *stasis_page_slotted_freelist_ptr(p);
    *stasis_page_slotted_freelist_ptr(p) = rid.slot;
    assert(slottedGetType(xid,p,rid)==INVALID_SLOT);
  }

  slottedFsck(p);
}


// XXX dereferenceRID

static void slottedLoaded(Page *p) {
  p->LSN = *stasis_page_lsn_ptr(p);
  slottedFsck(p);
}
static void slottedFlushed(Page *p) {
  *stasis_page_lsn_ptr(p) = p->LSN;
  slottedFsck(p);
}
static void slottedCleanup(Page *p) { }

void stasis_page_slotted_init() {
#ifdef SLOTTED_PAGE_CHECK_FOR_OVERLAP
#ifdef SLOTTED_PAGE_OLD_CHECKS
  printf("slotted.c: Using expensive page sanity checking.\n");
#endif
#endif
}

void stasis_page_slotted_deinit() {
}

void stasis_page_slotted_initialize_page(Page * page) {
  assertlocked(page->rwlatch);
  stasis_page_cleanup(page);
  page->pageType = SLOTTED_PAGE;
  *stasis_page_slotted_freespace_ptr(page) = 0;
  *stasis_page_slotted_numslots_ptr(page)  = 0;
  *stasis_page_slotted_freelist_ptr(page)  = INVALID_SLOT;
}

page_impl stasis_page_slotted_impl() {
static page_impl pi =  {
    SLOTTED_PAGE,
    1,
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
    slottedCompactSlotIDs,
    slottedPreRalloc,
    slottedPostRalloc,
    slottedSpliceSlot,
    slottedFree,
    0, //XXX page_impl_dereference_identity,
    slottedLoaded,
    slottedFlushed,
    slottedCleanup
  };
  return pi;
}

page_impl stasis_page_boundary_tag_impl() {
  page_impl p =  stasis_page_slotted_impl();
  p.page_type = BOUNDARY_TAG_PAGE;
  return p;
}
