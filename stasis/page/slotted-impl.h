/*
 * slotted-impl.h
 *
 *  Created on: Nov 7, 2011
 *      Author: sears
 */

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

#define SLOTTED_PAGE_OVERHEAD_PER_RECORD (2 * sizeof(short))
#define SLOTTED_PAGE_HEADER_OVERHEAD (3 * sizeof(short))

static inline int16_t* stasis_page(slotted_freespace_ptr)  (PAGE * p)             { return stasis_page(int16_ptr_from_end) (p, 1); }
static inline int16_t* stasis_page(slotted_numslots_ptr)   (PAGE * p)             { return stasis_page(int16_ptr_from_end) (p, 2); }
static inline int16_t* stasis_page(slotted_freelist_ptr)   (PAGE * p)             { return stasis_page(int16_ptr_from_end) (p, 3); }
static inline int16_t* stasis_page(slotted_slot_ptr)       (PAGE * p, slotid_t n) { return stasis_page(int16_ptr_from_end) (p, (2*(n))+4); }
static inline int16_t* stasis_page(slotted_slot_length_ptr)(PAGE * p, slotid_t n) { return stasis_page(int16_ptr_from_end) (p, (2*(n))+5); }
static inline byte*    stasis_page(slotted_record_ptr)     (PAGE * p, slotid_t n) { return stasis_page(byte_ptr_from_start)(p, *stasis_page(slotted_slot_ptr)(p, n)); }

static inline const int16_t* stasis_page(slotted_freespace_cptr)  (const PAGE * p)             { return stasis_page(slotted_freespace_ptr)  ((PAGE*)p); }
static inline const int16_t* stasis_page(slotted_numslots_cptr)   (const PAGE * p)             { return stasis_page(slotted_numslots_ptr)   ((PAGE*)p); }
static inline const int16_t* stasis_page(slotted_freelist_cptr)   (const PAGE * p)             { return stasis_page(slotted_freelist_ptr)   ((PAGE*)p); }
static inline const int16_t* stasis_page(slotted_slot_cptr)       (const PAGE * p, slotid_t n) { return stasis_page(slotted_slot_ptr)       ((PAGE*)p, n); }
static inline const int16_t* stasis_page(slotted_slot_length_cptr)(const PAGE * p, slotid_t n) { return stasis_page(slotted_slot_length_ptr)((PAGE*)p, n); }
static inline const byte*    stasis_page(slotted_record_cptr)     (const PAGE * p, slotid_t n) { return stasis_page(slotted_record_ptr)     ((PAGE*)p, n); }

static inline void stasis_page(slotted_initialize_page_raw)(PAGE * page) {
  *stasis_page(slotted_freespace_ptr)(page) = 0;
  *stasis_page(slotted_numslots_ptr)(page)  = 0;
  *stasis_page(slotted_freelist_ptr)(page)  = INVALID_SLOT;
}

static inline void stasis_page(slotted_check)(const PAGE * page) {

  const short numslots  = *stasis_page(slotted_numslots_cptr)(page);
  const short freespace = *stasis_page(slotted_freespace_cptr)(page);
  const short freelist  = *stasis_page(slotted_freelist_cptr)(page);

  const long slotListStart = (const byte*)stasis_page(slotted_slot_length_cptr)(page, numslots-1)
                                  - (const byte*)stasis_page(memaddr)(page);
  assert(slotListStart < PAGE_SIZE && slotListStart >= 0);
  assert(numslots >= 0);
  assert(numslots * SLOTTED_PAGE_OVERHEAD_PER_RECORD < PAGE_SIZE);
  assert(freespace >= 0);
  assert(freespace <= slotListStart);
  assert(freelist >= INVALID_SLOT);
  assert(freelist < numslots);

#ifdef SLOTTED_PAGE_SANITY_CHECKS

  // Check integrity of freelist.  All free slots less than
  // numslots should be on it, in order.

  short * slot_offsets = (short*)alloca(numslots * sizeof(short));
  short * slot_lengths = (short*)alloca(numslots * sizeof(short));
  for(int i = 0; i < numslots; i++) {
    slot_offsets[i] = *stasis_page(slotted_slot_cptr)(page, i);
    slot_lengths[i] = *stasis_page(slotted_slot_length_cptr)(page, i);
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

  //  Page dummy;

    //dummy.id = -1;
    //dummy.memAddr = 0;

  const byte UNUSED = 0xFF;
  const byte PAGE_HEADER = 0xFE;
  const byte SLOTTED_HEADER = 0xFD;
  //  const byte SLOT_LIST = 0xFC;
  const byte FREE_SPACE = 0xFB;

  const unsigned short S_SLOT_LIST = 0xFCFC;

  byte image[PAGE_SIZE];
  for(unsigned short i = 0; i < PAGE_SIZE; i++) {
    image[i] = UNUSED;
  }
  for(unsigned short i = USABLE_SIZE_OF_PAGE; i < PAGE_SIZE; i++) {
    image[i] = PAGE_HEADER;
  }
  for(unsigned short i = USABLE_SIZE_OF_PAGE - SLOTTED_PAGE_HEADER_OVERHEAD; i < USABLE_SIZE_OF_PAGE; i++) {
    image[i] = SLOTTED_HEADER;
  }
  for(unsigned short i = *stasis_page(slotted_freespace_cptr)(page); i < slotListStart; i++) {
    image[i] = FREE_SPACE;
  }

  for(unsigned short i = 0; i < *stasis_page(slotted_numslots_cptr)(page); i++) {
    *stasis_mempage_slotted_slot_ptr(image, i) = S_SLOT_LIST;
    *stasis_mempage_slotted_slot_length_ptr(image, i) = S_SLOT_LIST;
  }
  for(unsigned short i = 0; i < *stasis_page(slotted_numslots_cptr)(page); i++) {
    short slot_offset = *stasis_page(slotted_slot_cptr)(page, i);
    if(slot_offset != INVALID_SLOT) {
      const unsigned char ci = i % 0xFF;
      short slot_len = stasis_record_type_to_size(*stasis_page(slotted_slot_length_cptr)(page, i));

      for(unsigned short j = 0; j < slot_len; j++) {
        assert(image[slot_offset + j] == 0xFF);
        image[slot_offset + j] = ci;
      }
    }
  }
#endif // SLOTTED_PAGE_CHECK_FOR_OVERLAP
#endif // SLOTTED_PAGE_SANITY_CHECKS

}

/**
   Check to see how many bytes can fit in a given slot.  This
   makes it possible for callers to guarantee the safety
   of a subsequent call to really_do_ralloc().

   This call can return negative numbers.  This lets it differentiate between
   situations in which a zero-byte record would fit, and when it would not.
*/
static inline ssize_t stasis_page(slotted_freespace_for_slot)(PAGE * page, slotid_t slot) {
  ssize_t slotOverhead;

  if(slot == INVALID_SLOT) {
    slotOverhead = (*stasis_page(slotted_freelist_cptr)(page) == INVALID_SLOT) ? SLOTTED_PAGE_OVERHEAD_PER_RECORD : 0;
  } else if(slot < *stasis_page(slotted_numslots_cptr)(page)) {
    slotOverhead = 0;
  } else {
    //    slotOverhead = SLOTTED_PAGE_OVERHEAD_PER_RECORD * (*numslots_ptr(page) - slot);
    slotOverhead = SLOTTED_PAGE_OVERHEAD_PER_RECORD * ((slot+1) - *stasis_page(slotted_numslots_cptr)(page));
  }
  // end_of_free_space points to the beginning of the slot header at the bottom of the page header.
  byte* end_of_free_space = (byte*)stasis_page(slotted_slot_length_cptr)(page, (*stasis_page(slotted_numslots_cptr)(page))-1);

  // start_of_free_space points to the first unallocated byte in the page
  // (ignoring space that could be reclaimed by compaction)
  const byte* start_of_free_space =  (stasis_page(byte_cptr_from_start)(page, 0) + *stasis_page(slotted_freespace_cptr)(page));

  assert(end_of_free_space >= start_of_free_space);

  if(end_of_free_space < start_of_free_space + slotOverhead) {
    // The regions would overlap after allocation.  There is no free space.
    return 0;
  } else {
    // The regions would not overlap.  There might be free space.
    return (ssize_t) (end_of_free_space - start_of_free_space - slotOverhead);
  }
}

static inline void stasis_page(slotted_compact)(PAGE * page) {
  byte buffer[PAGE_SIZE];

  // Copy external headers into bufPage.

  memcpy(&buffer[USABLE_SIZE_OF_PAGE], &(stasis_page(memaddr)(page)[USABLE_SIZE_OF_PAGE]), PAGE_SIZE - USABLE_SIZE_OF_PAGE);

  // Now, build new slotted page in the bufPage struct.

  *stasis_mempage_slotted_freespace_ptr(buffer) = 0;
  // numslots_ptr will be set later.
  *stasis_mempage_slotted_freelist_ptr(buffer) = INVALID_SLOT;

  const short numSlots = *stasis_page(slotted_numslots_cptr)(page);
  short lastFreeSlot = INVALID_SLOT;
  short lastFreeSlotBeforeUsedSlot = INVALID_SLOT;
  short lastUsedSlot = -1;

  // Rebuild free list.

  for(short i = 0; i < numSlots; i++) {
    if(*stasis_page(slotted_slot_cptr)(page, i) == INVALID_SLOT) {
      if(lastFreeSlot == INVALID_SLOT) {
        *stasis_mempage_slotted_freelist_ptr(buffer) = i;
      } else {
        *stasis_mempage_slotted_slot_length_ptr(buffer, lastFreeSlot) = i;
      }
      *stasis_mempage_slotted_slot_ptr(buffer, i) = INVALID_SLOT;
      lastFreeSlot = i;
    } else {
      lastUsedSlot = i;
      lastFreeSlotBeforeUsedSlot = lastFreeSlot;

      short logicalSize = *stasis_page(slotted_slot_length_cptr)(page, i);
      short physicalSize = stasis_record_type_to_size(logicalSize);

      memcpy(&(buffer[*stasis_mempage_slotted_freespace_ptr(buffer)]), stasis_page(slotted_record_cptr)(page, i), physicalSize);

      *stasis_mempage_slotted_slot_ptr(buffer, i) = *stasis_mempage_slotted_freespace_cptr(buffer);
      *stasis_mempage_slotted_slot_length_ptr(buffer, i) = logicalSize;

      (*stasis_mempage_slotted_freespace_ptr(buffer)) += physicalSize;

    }
  }

  // Truncate linked list, and update numslots_ptr.
  *stasis_mempage_slotted_slot_length_ptr(buffer, lastFreeSlotBeforeUsedSlot) = INVALID_SLOT;
  *stasis_mempage_slotted_numslots_ptr(buffer) = lastUsedSlot+1;

  memcpy(stasis_page(memaddr)(page), buffer, PAGE_SIZE);
#ifdef SLOTTED_PAGE_OLD_CHECKS
  stasis_page(slotted_fsck)(page);
#endif // SLOTTED_PAGE_OLD_CHECKS
}


static inline void stasis_page(slotted_compact_slot_ids)(PAGE * p) {
  int16_t numSlots = *stasis_page(slotted_numslots_cptr)(p);
  int16_t out = 0;
  for(int16_t in = 0; in < numSlots; in++) {
    if(*stasis_page(slotted_slot_cptr)(p, in) == INVALID_SLOT) {
      // nop
    } else {
      *stasis_page(slotted_slot_ptr)(p, out) = *stasis_page(slotted_slot_cptr)(p, in);
      *stasis_page(slotted_slot_length_ptr)(p, out) = *stasis_page(slotted_slot_length_cptr)(p, in);
      out++;
    }
  }
  *stasis_page(slotted_numslots_ptr)(p) = out;
  *stasis_page(slotted_freelist_ptr)(p) = INVALID_SLOT;
}

static inline int stasis_page(slotted_get_type)(PAGE *p, slotid_t slot) {
  if(slot >= *stasis_page(slotted_numslots_cptr)(p)) { return INVALID_SLOT; }
  if(*stasis_page(slotted_slot_cptr)(p, slot) == INVALID_SLOT) { return INVALID_SLOT; }
  int ret = *stasis_page(slotted_slot_length_cptr)(p, slot);
  return ret >= 0 ? NORMAL_SLOT : ret;
}
static inline void stasis_page(slotted_set_type)(PAGE *p, slotid_t slot, int type) {
  int old_type = *stasis_page(slotted_slot_length_cptr)(p, slot);
  assert(slot < *stasis_page(slotted_numslots_cptr)(p));
  assert(old_type != INVALID_SLOT);

  if(type == NORMAL_SLOT) {
    // set slot_length_ptr to the physical length.
    *stasis_page(slotted_slot_length_ptr)(p, slot) = stasis_record_type_to_size(old_type);
  } else {
    // Changing to a special slot type; make sure doing so doesn't change
    // the record size.
    assert(stasis_record_type_to_size(type) == stasis_record_type_to_size(old_type));
    *stasis_page(slotted_slot_length_ptr)(p, slot) = type;
  }
}
static inline int stasis_page(slotted_get_length)(PAGE *p, slotid_t slot) {
  if( stasis_page(slotted_get_type)(p, slot) == INVALID_SLOT)
    return INVALID_SLOT;
  else
    return stasis_record_type_to_size(*stasis_page(slotted_slot_length_cptr)(p, slot));
}
static inline slotid_t stasis_page(slotted_next_record)(PAGE *p, slotid_t slot) {
  short n = *stasis_page(slotted_numslots_cptr)(p);

  do {
    slot++;
  } while(slot < n && stasis_page(slotted_get_type)(p,slot)==INVALID_SLOT);

  return n > slot ? slot : INVALID_SLOT;
}
static inline slotid_t stasis_page(slotted_last_record)(PAGE *p) {
  return (*stasis_page(slotted_numslots_cptr)(p)) - 1;
}
static inline slotid_t stasis_page(slotted_pre_alloc)(PAGE * p, int type) {
  assert(type != INVALID_SLOT);

  slotid_t slot = *stasis_page(slotted_numslots_cptr)(p);

  if(*stasis_page(slotted_freelist_cptr)(p) != INVALID_SLOT) {
    slot = *stasis_page(slotted_freelist_cptr)(p);
  }

  // int casts are there to deal with sign vs. unsigned types
  if((int)stasis_page(slotted_freespace_for_slot)(p, slot) < (int)stasis_record_type_to_size(type)) {
    return INVALID_SLOT;
  } else {
    return slot;
  }
}
static inline void stasis_page(slotted_post_alloc)(PAGE * page, slotid_t slot, short type) {
  // Compact the page if we don't have enough room.
  if((int)stasis_page(slotted_freespace_for_slot)(page, slot) < (int)stasis_record_type_to_size(type)) {
    stasis_page(slotted_compact)(page);

    // Make sure we have enough enough free space for the new record
    assert ((int)stasis_page(slotted_freespace_for_slot)(page, slot) >= (int)stasis_record_type_to_size(type));
  }

  short freeSpace = *stasis_page(slotted_freespace_cptr)(page);

  // Remove this entry from the freelist (if necessary) slottedCompact
  // assumes that this does not change the order of items in the list.
  // If it did, then slottedCompact could leak slot id's (or worse!)
  if(slot < *stasis_page(slotted_numslots_cptr)(page) && *stasis_page(slotted_slot_cptr)(page,slot) == INVALID_SLOT) {
    short next = *stasis_page(slotted_freelist_cptr)(page);
    short last = INVALID_SLOT;
    // special case:  is the slot physically before us the predecessor?
    if(slot > 0) {
      if(*stasis_page(slotted_slot_length_cptr)(page, slot-1) == slot &&
         *stasis_page(slotted_slot_cptr)(page, slot-1) == INVALID_SLOT) {
        next = slot;
        last = slot-1;
      }
    }
    while(next != INVALID_SLOT && next != slot) {
      last = next;
      assert(next < *stasis_page(slotted_numslots_cptr)(page));
      short next_slot_ptr = *stasis_page(slotted_slot_cptr)(page, next);
      assert(next_slot_ptr == INVALID_SLOT);
      next = *stasis_page(slotted_slot_length_cptr)(page, next);
    }
    if(next == slot) {
      if(last == INVALID_SLOT) {
        *stasis_page(slotted_freelist_ptr)(page)
            = *stasis_page(slotted_slot_length_cptr)(page, slot);
      } else {
        *stasis_page(slotted_slot_length_ptr)(page, last)
            = *stasis_page(slotted_slot_length_cptr)(page, slot);
      }
    }
  }

  // Insert any slots that come between the previous numslots_ptr()
  // and the slot we're allocating onto the freelist.  In order to
  // promote the reuse of free slot numbers, we go out of our way to make sure
  // that we put them in the list in increasing order.  (Note:  slottedCompact's
  // correctness depends on this behavior!)

  if(slot > *stasis_page(slotted_numslots_cptr)(page)) {
    short lastSlot;
    if(*stasis_page(slotted_freelist_cptr)(page) == INVALID_SLOT) {
      short numSlots = *stasis_page(slotted_numslots_cptr)(page);
      *stasis_page(slotted_freelist_ptr)(page) = numSlots;
      lastSlot = numSlots;

      *stasis_page(slotted_slot_ptr)(page, lastSlot) = INVALID_SLOT;
      // will set slot_length_ptr on next iteration.


      (*stasis_page(slotted_numslots_ptr)(page))++;
    } else {
      lastSlot = INVALID_SLOT;
      short next = *stasis_page(slotted_freelist_cptr)(page);
      while(next != INVALID_SLOT) {
        lastSlot = next;
        next = *stasis_page(slotted_slot_length_cptr)(page, lastSlot);
        assert(lastSlot < *stasis_page(slotted_numslots_cptr)(page));
        assert(*stasis_page(slotted_slot_ptr)(page, lastSlot) == INVALID_SLOT);
      }
      *stasis_page(slotted_slot_ptr)(page, lastSlot) = INVALID_SLOT;

    }

    // lastSlot now contains the tail of the free list.  We can start adding slots to the list starting at *numslots_ptr.

    while(*stasis_page(slotted_numslots_cptr)(page) < slot) {
      *stasis_page(slotted_slot_length_ptr)(page, lastSlot) = *stasis_page(slotted_numslots_cptr)(page);
      lastSlot = *stasis_page(slotted_numslots_cptr)(page);
      *stasis_page(slotted_slot_ptr)(page, lastSlot) = INVALID_SLOT;
      (*stasis_page(slotted_numslots_ptr)(page))++;
    }

    // Terminate the end of the list.
    assert(lastSlot < *stasis_page(slotted_numslots_cptr)(page));
    *stasis_page(slotted_slot_length_ptr)(page, lastSlot) = INVALID_SLOT;

  }

  if(*stasis_page(slotted_numslots_cptr)(page) == slot) {
    *stasis_page(slotted_numslots_ptr)(page) = slot+1;
  }

  assert(*stasis_page(slotted_numslots_cptr)(page) > slot);

  DEBUG("Num slots %d\trid.slot %d\n", *stasis_page(slotted_numslots_cptr)(page), slot);

  // Reserve space for this record and record the space's offset in
  // the slot header.

  assert(slot < *stasis_page(slotted_numslots_cptr)(page));
  assert(freeSpace == *stasis_page(slotted_freespace_cptr)(page)); // XXX not sure how this could not be the case.
  *stasis_page(slotted_freespace_ptr)(page) = freeSpace + stasis_record_type_to_size(type);
  *stasis_page(slotted_slot_ptr)(page, slot)  = freeSpace;

  *stasis_page(slotted_slot_length_ptr)(page, slot) = type;

}
static inline void stasis_page(slotted_splice_slot)(PAGE *p, slotid_t a, slotid_t b) {
  if(a==b) { return; } // no-op

  if(a > b) {
    slotid_t c = a;
    a = b;
    b = c;
  }

  int16_t b_slot = *stasis_page(slotted_slot_cptr)(p, b);
  int16_t b_slot_len = *stasis_page(slotted_slot_length_cptr)(p, b);
  for(int16_t i = b-1; i >= a; i--) {
    *stasis_page(slotted_slot_ptr)(p, i+1)        = *stasis_page(slotted_slot_cptr)(p, i);
    *stasis_page(slotted_slot_length_ptr)(p, i+1) = *stasis_page(slotted_slot_length_cptr)(p, i);
  }
  *stasis_page(slotted_slot_ptr)(p, a) = b_slot;
  *stasis_page(slotted_slot_length_ptr)(p, a) = b_slot_len;
}
static inline void stasis_page(slotted_free)(PAGE * p, slotid_t slot) {
  ssize_t rec_size = stasis_record_type_to_size(*stasis_page(slotted_slot_length_cptr)(p, slot));

  if(*stasis_page(slotted_freespace_cptr)(p) == *stasis_page(slotted_slot_cptr)(p, slot) + rec_size) {
    (*stasis_page(slotted_freespace_ptr)(p)) -= rec_size;
  }

  assert(slot < *stasis_page(slotted_numslots_cptr)(p));
  if(slot == *stasis_page(slotted_numslots_cptr)(p)-1) {
    (*stasis_page(slotted_numslots_ptr)(p))--;
    assert(stasis_page(slotted_get_type)(p,slot)==INVALID_SLOT);
  } else {
    *stasis_page(slotted_slot_ptr)(p, slot) =  INVALID_SLOT;
    *stasis_page(slotted_slot_length_ptr)(p, slot) = *stasis_page(slotted_freelist_cptr)(p);
    *stasis_page(slotted_freelist_ptr)(p) = slot;
    assert(stasis_page(slotted_get_type)(p,slot)==INVALID_SLOT);
  }
#ifdef SLOTTED_PAGE_OLD_CHECKS
  stasis_page(slotted_fsck)(p);
#endif
}
