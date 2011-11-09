#include <stasis/common.h>
#include <stasis/page.h>
#include <stasis/page/fixed.h>
#include <stasis/truncation.h>

#include <assert.h>

//-------------- New API below this line

static inline void stasis_page_fixed_checkRid(Page * page, recordid rid) {
  assert(page->pageType); // any more specific breaks pages based on this one
  assert(page->id == rid.page);
  assert(*stasis_page_fixed_recordsize_cptr(page) == rid.size);
  assert(stasis_page_fixed_records_per_page(rid.size) > rid.slot);
}
void stasis_page_fixed_initialize_page(Page * page, size_t size, int count) {
  stasis_page_cleanup(page);
  page->pageType = FIXED_PAGE;
  stasis_page_fixed_initialize_page_raw(page, size, count);
}

static int stasis_page_fixed_get_record_type(int xid, Page *p, recordid rid) {
  return stasis_page_fixed_get_type(p, rid.slot);
}
static void stasis_page_fixed_set_record_type(int xid, Page *p, recordid rid, int type) {
  stasis_page_fixed_set_type(p, rid.slot, type);
}

static int stasis_page_fixed_not_supported(int xid, Page * p) { return 0; }

static const byte* stasis_page_fixed_read(int xid, Page *p, recordid rid) {
  stasis_page_fixed_checkRid(p, rid);
  assert(rid.slot < *stasis_page_fixed_recordcount_cptr(p));
  return stasis_page_fixed_record_cptr(p, rid.slot);
}

static inline byte* stasis_page_fixed_write(int xid, Page *p, recordid rid) {
  stasis_page_fixed_checkRid(p, rid);
  assert(rid.slot < *stasis_page_fixed_recordcount_cptr(p));
  return stasis_page_fixed_record_ptr(p, rid.slot);
}


static int stasis_page_fixed_get_length_record(int xid, Page *p, recordid rid) {
  assert(p->pageType);
  return stasis_page_fixed_get_length(p, rid.slot);
}

static recordid stasis_page_fixed_last_record(int xid, Page *p) {
  recordid rid = { p->id, -1, 0 };
  rid.size = *stasis_page_fixed_recordsize_cptr(p);
  rid.slot = -stasis_page_fixed_last_slot(p);
  return rid;
}
recordid stasis_page_fixed_next_record(int xid, Page *p, recordid rid) {
  slotid_t slot = stasis_page_fixed_next_slot(p, rid.slot);
  if(slot == INVALID_SLOT) {
    return NULLRID;
  } else {
    assert(rid.page == p->id);
    rid.size = *stasis_page_fixed_recordsize_cptr(p);
    rid.slot = slot;
    return rid;
  }
}
static recordid stasis_page_fixed_first_record(int xid, Page *p) {
  recordid rid = {
    p->id,
    INVALID_SLOT,
    *stasis_page_fixed_recordsize_cptr(p)
  };
  return stasis_page_fixed_next_record(xid, p, rid);
}

static int stasis_page_fixed_freespace(int xid, Page * p) {
  return stasis_page_fixed_freespace_raw(p);
}

static inline void stasis_page_fixed_compact(Page * p) {
  // no-op
}
static inline void stasis_page_fixed_compact_slot_ids(int xid, Page * p) {
  abort();
}

static recordid stasis_page_fixed_pre_alloc_record(int xid, Page *p, int size) {
  int slot = stasis_page_fixed_pre_alloc(p, size);
  if(slot ==-1) {
    return NULLRID;
  }
  recordid rid = { p->id, slot, *stasis_page_fixed_recordsize_cptr(p) };
  return rid;
}
static void stasis_page_fixed_post_alloc_record(int xid, Page *p, recordid rid) {
  assert(*stasis_page_fixed_recordsize_cptr(p) == rid.size);
  stasis_page_fixed_post_alloc(p, rid.slot);
}

static inline void stasis_page_fixed_splice(int xid, Page *p, slotid_t first, slotid_t second) {
  abort();
}

static void stasis_page_fixed_free_record(int xid, Page *p, recordid rid) {
  stasis_page_fixed_free(p, rid.slot);
}

// XXX dereferenceRID

static void stasis_page_fixed_loaded(Page *p) {
  p->LSN = *stasis_page_lsn_ptr(p);
}
static void stasis_page_fixed_flushed(Page *p) {
  *stasis_page_lsn_ptr(p) = p->LSN;
}
static void stasis_page_fixed_cleanup(Page *p) { }

page_impl stasis_page_fixed_impl() {
  static page_impl pi = {
    FIXED_PAGE,
    1,
    stasis_page_fixed_read,
    stasis_page_fixed_write,
    0,// readDone
    0,// writeDone
    stasis_page_fixed_get_record_type,
    stasis_page_fixed_set_record_type,
    stasis_page_fixed_get_length_record,
    stasis_page_fixed_first_record,
    stasis_page_fixed_next_record,
    stasis_page_fixed_last_record,
    stasis_page_fixed_not_supported,
    stasis_block_first_default_impl,
    stasis_block_next_default_impl,
    stasis_block_done_default_impl,
    stasis_page_fixed_freespace,
    stasis_page_fixed_compact,
    stasis_page_fixed_compact_slot_ids,
    stasis_page_fixed_pre_alloc_record,
    stasis_page_fixed_post_alloc_record,
    stasis_page_fixed_splice,
    stasis_page_fixed_free_record,
    0, // XXX dereference
    stasis_page_fixed_loaded,
    stasis_page_fixed_flushed,
    stasis_page_fixed_cleanup
  };
  return pi;
}

/**
 @todo arrayListImpl belongs in arrayList.c
*/
page_impl stasis_page_array_list_impl() {
  page_impl pi = stasis_page_fixed_impl();
  pi.page_type = ARRAY_LIST_PAGE;
  return pi;
}

void stasis_page_fixed_init() { }
void stasis_page_fixed_deinit() { }
