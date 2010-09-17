/*
 * lfSlotted.c
 *
 *  Created on: Aug 19, 2010
 *      Author: sears
 */

#include <stasis/common.h>
#include <stasis/page.h>
#include <stasis/page/latchFree/lfSlotted.h>

#define CAS(_a,_o,_n) __sync_bool_compare_and_swap(_a,_o,_n)
#define BARRIER() __sync_synchronize()
#define ATOMIC_ADD(_a, _i) __sync_add_and_fetch(_a, _i)

static int notSupported(int xid, Page * p) { return 0; }

static const byte* lfSlottedRead (int xid, Page *p, recordid rid) {
  return stasis_page_slotted_record_cptr(p, rid.slot);
}

static byte* lfSlottedWrite(int xid, Page *p, recordid rid) {
  assert(*stasis_page_slotted_numslots_cptr(p) == rid.slot);
  return stasis_page_slotted_record_ptr(p, rid.slot);
}
static void lfSlottedWriteDone(int xid, Page *p, recordid rid, byte *buf) {
  BARRIER();
  int succ = CAS(stasis_page_slotted_numslots_ptr(p), rid.slot, rid.slot+1);
  DEBUG("write done %d\n", rid.slot+1);
  assert(succ);
}
static int lfSlottedRecordGetType(int xid, Page *p, recordid rid) {
  if(*stasis_page_slotted_numslots_cptr(p) <= rid.slot) { return INVALID_SLOT; }
  int ret = *stasis_page_slotted_slot_length_cptr(p, rid.slot);
  return ret >= 0 ? NORMAL_SLOT : ret;
}
static int lfSlottedRecordGetLength(int xid, Page *p, recordid rid) {
  if(*stasis_page_slotted_numslots_cptr(p) <= rid.slot) { return INVALID_SLOT; }
  int ret = *stasis_page_slotted_slot_length_cptr(p, rid.slot);
  return stasis_record_type_to_size(ret);
}
static recordid lfSlottedRecordFirst(int xid, Page *p) {
  recordid ret;
  ret.page = p->id;
  ret.slot = 0;
  ret.size = lfSlottedRecordGetType(xid, p, ret);
  return ret.size == INVALID_SLOT ? NULLRID : ret;

}
static recordid lfSlottedRecordNext(int xid, Page *p, recordid ret) {
  ret.slot++;
  ret.size = lfSlottedRecordGetType(xid, p, ret);
  DEBUG("next %d %d %d\n", ret.slot, (int)ret.size, *stasis_page_slotted_numslots_ptr(p));
  return ret.size == INVALID_SLOT ? NULLRID : ret;
}
static recordid lfSlottedRecordLast(int xid, Page *p) {
  recordid ret;
  ret.page = p->id;
  ret.slot = (*stasis_page_slotted_numslots_cptr(p))-1;
  ret.size = lfSlottedRecordGetType(xid, p, ret);
  return ret.size == INVALID_SLOT ? NULLRID : ret;
}

static void lfSlottedPostAlloc(int xid, Page * page, recordid rid) {
  int16_t off = __sync_fetch_and_add(stasis_page_slotted_freespace_ptr(page), stasis_record_type_to_size(rid.size));
  *stasis_page_slotted_slot_ptr(page, rid.slot) = off;
  *stasis_page_slotted_slot_length_ptr(page, rid.slot) = rid.size;
  // don't update numslots_ptr yet.  Note that we can only have one append at a time with this update protocol...
}
void lfSlottedRecordFree(int xid, Page *p, recordid rid) {
  *stasis_page_slotted_slot_length_ptr(p, rid.slot) = INVALID_SLOT;
  BARRIER();
}

void stasis_page_slotted_latch_free_initialize_page(Page * page) {
  stasis_page_slotted_initialize_page(page);
  page->pageType = SLOTTED_LATCH_FREE_PAGE;

}

page_impl stasis_page_slotted_latch_free_impl(void) {
  page_impl slotted = stasis_page_slotted_impl();
  page_impl pi =  {
    SLOTTED_LATCH_FREE_PAGE,
    1,
    lfSlottedRead,
    lfSlottedWrite,
    0,// readDone (no-op)
    lfSlottedWriteDone,
    lfSlottedRecordGetType,
    0, // not supported
    lfSlottedRecordGetLength,
    lfSlottedRecordFirst,
    lfSlottedRecordNext,
    lfSlottedRecordLast,
    notSupported, // is block supported
    stasis_block_first_default_impl,
    stasis_block_next_default_impl,
    stasis_block_done_default_impl,
    slotted.pageFreespace, // this should work as is.
    NULL, // slotted.pageCompact, // there is no chance of supporting this
    NULL, //slotted.pageCompactSlotIDs, // ditto
    slotted.recordPreAlloc, // this is fine; it's read only...
    lfSlottedPostAlloc,
    0, // can't splice lots of records atomically with the current scheme.
    lfSlottedRecordFree,
    0, // page_impl_dereference_identity,
    slotted.pageLoaded,
    slotted.pageFlushed,
    slotted.pageCleanup
  };
  return pi;
}
