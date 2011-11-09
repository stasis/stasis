/*
 * fixed-impl.h
 *
 *  Created on: Nov 7, 2011
 *      Author: sears
 */

static inline int16_t* stasis_page(fixed_recordsize_ptr) (PAGE * p)             { return stasis_page(int16_ptr_from_end) (p, 1); }
static inline int16_t* stasis_page(fixed_recordcount_ptr)(PAGE * p)             { return stasis_page(int16_ptr_from_end) (p, 2); }
static inline byte*    stasis_page(fixed_record_ptr)     (PAGE * p, slotid_t n) { return stasis_page(byte_ptr_from_start)(p, *stasis_page(fixed_recordsize_ptr)(p) * n); }

static inline const int16_t* stasis_page(fixed_recordsize_cptr) (const PAGE * p)             { return stasis_page(fixed_recordsize_ptr) ((PAGE*)p); }
static inline const int16_t* stasis_page(fixed_recordcount_cptr)(const PAGE * p)             { return stasis_page(fixed_recordcount_ptr)((PAGE*)p); }
static inline const byte*    stasis_page(fixed_record_cptr)     (const PAGE * p, slotid_t n) { return stasis_page(fixed_record_ptr)     ((PAGE*)p, n); }

static inline slotid_t stasis_page(fixed_next_slot)(PAGE *p, slotid_t slot) {
  slot++;
  if(*stasis_page(fixed_recordcount_cptr)(p) > slot) {
    return slot;
  } else {
    return -1;
  }
}
static inline slotid_t stasis_page(fixed_first_slot)(PAGE *p) {
  return stasis_page(fixed_next_slot)(p, -1);
}
static inline slotid_t stasis_page(fixed_last_slot)(PAGE *p) {
  return -(*stasis_page(fixed_recordcount_cptr)(p)) - 1;
}

static inline int stasis_page(fixed_records_per_page)(size_t size) {
  return (USABLE_SIZE_OF_PAGE - 2*sizeof(short)) / size;
}
static inline void stasis_page(fixed_initialize_page_raw)(PAGE * page, size_t size, int count) {
  // Zero out the page contents, since callers often do so anyway.
  // blows away LSN, but the copy that's in p->LSN will be put back on page at flush.
  memset(stasis_page(memaddr)(page), 0, PAGE_SIZE);
  *stasis_page(fixed_recordsize_ptr)(page) = size;
  assert(count <= stasis_page(fixed_records_per_page)(size));
  *stasis_page(fixed_recordcount_ptr)(page)= count;
}

static inline int stasis_page(fixed_get_type)(PAGE *p, slotid_t slot) {
  //  checkRid(p, rid);
  if(slot < *stasis_page(fixed_recordcount_cptr)(p)) {
    int type = *stasis_page(fixed_recordsize_cptr)(p);
    if(type > 0) {
      type = NORMAL_SLOT;
    }
    return type;
  } else {
    return INVALID_SLOT;
  }
}
static inline void stasis_page(fixed_set_type)(PAGE *p, slotid_t slot, int type) {
//XXX  stasis_page(checkRid)(p,rid);
  assert(slot < *stasis_page(fixed_recordcount_cptr)(p));
  assert(stasis_record_type_to_size(type) == stasis_record_type_to_size(*stasis_page(fixed_recordsize_cptr)(p)));
}
static inline int stasis_page(fixed_get_length)(PAGE *p, slotid_t slot) {
  return slot > *stasis_page(fixed_recordcount_cptr)(p) ?
      INVALID_SLOT : stasis_record_type_to_size(*stasis_page(fixed_recordsize_cptr)(p));
}

static inline int stasis_page(fixed_freespace_raw)(PAGE * p) {
  if(stasis_page(fixed_records_per_page)(*stasis_page(fixed_recordsize_cptr)(p)) > *stasis_page(fixed_recordcount_cptr)(p)) {
    // Return the size of a slot; that's the biggest record we can take.
    return stasis_record_type_to_size(*stasis_page(fixed_recordsize_cptr)(p));
  } else {
    // Page full; return zero.
    return 0;
  }
}
static inline int16_t stasis_page(fixed_pre_alloc)(PAGE *p, int size) {
  if(stasis_page(fixed_records_per_page)(*stasis_page(fixed_recordsize_cptr)(p)) > *stasis_page(fixed_recordcount_cptr)(p)) {
    return *stasis_page(fixed_recordcount_cptr)(p);
  } else {
    return -1;
  }
}
static inline void stasis_page(fixed_post_alloc)(PAGE *p, slotid_t n) {
  assert(*stasis_page(fixed_recordcount_cptr)(p) == n);
  (*stasis_page(fixed_recordcount_ptr)(p))++;
}

static inline void stasis_page(fixed_free)(PAGE *p, slotid_t n) {
  if(*stasis_page(fixed_recordsize_cptr)(p) == n+1) {
    (*stasis_page(fixed_recordsize_ptr)(p))--;
  } else {
    // leak space; there's no way to track it with this page format.
  }
}

