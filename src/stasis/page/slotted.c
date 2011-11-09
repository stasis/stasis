#include <config.h>
#include <stasis/common.h>
#ifdef HAVE_ALLOCA_H
#include <alloca.h>
#endif

#include <stdlib.h>

#include <stasis/page.h>
#include <stasis/page/slotted.h>

#include <stasis/truncation.h>
/**
   Run sanity checks to make sure page is in a consistent state.

   If SLOTTED_PAGE_SANITY_CHECKS and SLOTTED_PAGE_CHECK_FOR_OVERLAP
   are defined at compile time then this method will be more thorough
   and more expensive.
 */
static inline void slottedFsck(const Page const * page) {
  const short page_type = page->pageType;
  assert(page_type == SLOTTED_PAGE ||
         page_type == BOUNDARY_TAG_PAGE ||
         page_type == SLOTTED_LSN_FREE_PAGE ||
         page_type == SLOTTED_LATCH_FREE_PAGE);
  stasis_page_slotted_check(page);
}

/**
Move all of the records to the beginning of the page in order to
increase the available free space.
*/
static void slottedCompact(Page * page) {
  stasis_page_slotted_compact(page);
}
static void slottedCompactSlotIDs(int xid, Page * p) {
  stasis_page_slotted_compact_slot_ids(p);
}
// --------------------------------------------------------------------------
// PUBLIC API IS BELOW THIS LINE
// --------------------------------------------------------------------------

static const byte* slottedRead (int xid, Page *p, recordid rid) {
  return stasis_page_slotted_record_ptr(p, rid.slot);
}
static byte* slottedWrite(int xid, Page *p, recordid rid) {
  return stasis_page_slotted_record_ptr(p, rid.slot);
}
static int slottedGetType(int xid, Page *p, recordid rid) {
  return stasis_page_slotted_get_type(p, rid.slot);
}
static void slottedSetType(int xid, Page *p, recordid rid, int type) {
  stasis_page_slotted_set_type(p, rid.slot, type);
}
static int slottedGetLength(int xid, Page *p, recordid rid) {
  return stasis_page_slotted_get_length(p, rid.slot);
}
static recordid slottedNext(int xid, Page *p, recordid rid) {

  rid.slot = stasis_page_slotted_next_record(p, rid.slot);

  if(rid.slot != INVALID_SLOT) {
    rid.size = *stasis_page_slotted_slot_length_ptr(p, rid.slot);
    return rid;
  } else {
    return NULLRID;
  }
}
static recordid slottedFirst(int xid, Page *p) {
  recordid rid = { p->id, -1, 0 };
  return slottedNext(xid, p, rid);
}
static recordid slottedLast(int xid, Page *p) {
  recordid rid = {p->id, -1, 0 };
  rid.slot = stasis_page_slotted_last_record(p);
  rid.size = *stasis_page_slotted_slot_length_cptr(p, rid.slot);
  return rid;
}
static int notSupported(int xid, Page * p) { return 0; }

static int slottedFreespace(int xid, Page * p) {
  return stasis_page_slotted_freespace_for_slot(p, INVALID_SLOT);
}

static recordid slottedPreRalloc(int xid, Page * p, int type) {
  slotid_t slot = stasis_page_slotted_pre_alloc(p, type);
  if(slot == INVALID_SLOT) {
    return NULLRID;
  } else {
    recordid rid = { p->id, slot, type };
    return rid;
  }
}
/**
  Allocate data on a page after deciding which recordid to allocate,
  and making sure there is enough freespace.

  Allocation is complicated without locking.  Consider this situation:

   (1) *numslot_ptr(page) is 10
   (2) An aborting transaction calls slottedPostRalloc(page) with rid.slot = 12
   (3) *numslot_ptr(page) must be incremented to 12.  Now, what happens to 11?
     - If 11 was also deleted by a transaction that could abort, we should lock it so that it won't be reused.
   (4) This function adds it to the freelist to avoid leaking space.  (Therefore, Talloc() can return recordids that will
       be reused by aborting transactions...)

  For now, we make sure that we don't alloc off a page that another active
  transaction dealloced from.

  @param xid The transaction allocating the record.
  @param page A pointer to the page.
  @param rid rid.size should be a size or (for special records) a type that
             stasis_record_type_to_size() can interpret.  This allows callers
             to store record type information in the page's size field.
*/
static void slottedPostRalloc(int xid, Page * p, recordid rid) {
  assert(((short)rid.size) == rid.size);
  stasis_page_slotted_post_alloc(p, rid.slot, rid.size);
}

static void slottedSpliceSlot(int xid, Page *p, slotid_t a, slotid_t b) {
  stasis_page_slotted_splice_slot(p, a, b);
}
static void slottedFree(int xid, Page * p, recordid rid) {
  stasis_page_slotted_free(p, rid.slot);
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

void stasis_page_slotted_initialize_page(Page * page) {
  stasis_page_cleanup(page);
  page->pageType = SLOTTED_PAGE;
  stasis_page_slotted_initialize_page_raw(page);
}

void stasis_page_slotted_init() {
#ifdef SLOTTED_PAGE_CHECK_FOR_OVERLAP
#ifdef SLOTTED_PAGE_OLD_CHECKS
  printf("slotted.c: Using expensive page sanity checking.\n");
#endif
#endif
}

void stasis_page_slotted_deinit() {
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
    slottedLast,
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
