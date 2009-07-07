#include <assert.h>
#include <stasis/page.h>
#include <stasis/page/fixed.h>
/** @todo should page implementations provide readLSN / writeLSN??? */
#include <stasis/truncation.h>



int stasis_fixed_records_per_page(size_t size) {
  return (USABLE_SIZE_OF_PAGE - 2*sizeof(short)) / size;
}
/** @todo CORRECTNESS  Locking for stasis_fixed_initialize_page? (should hold writelock)*/
void stasis_fixed_initialize_page(Page * page, size_t size, int count) {
  assertlocked(page->rwlatch);
  stasis_page_cleanup(page);
  page->pageType = FIXED_PAGE;
  *recordsize_ptr(page) = size;
  assert(count <= stasis_fixed_records_per_page(size));
  *recordcount_ptr(page)= count;
}

static void checkRid(Page * page, recordid rid) {
  assertlocked(page->rwlatch);
  assert(page->pageType); // any more specific breaks pages based on this one
  assert(page->id == rid.page);
  assert(*recordsize_ptr(page) == rid.size);
  assert(stasis_fixed_records_per_page(rid.size) > rid.slot);
}

//-------------- New API below this line

static const byte* fixedRead(int xid, Page *p, recordid rid) {
  assertlocked(p->rwlatch);
  checkRid(p, rid);
  assert(rid.slot < *recordcount_ptr(p));
  return fixed_record_ptr(p, rid.slot);
}

static byte* fixedWrite(int xid, Page *p, recordid rid) {
  assertlocked(p->rwlatch);
  checkRid(p, rid);
  assert(rid.slot < *recordcount_ptr(p));
  return fixed_record_ptr(p, rid.slot);
}

static int fixedGetType(int xid, Page *p, recordid rid) {
  assertlocked(p->rwlatch);
  //  checkRid(p, rid);
  if(rid.slot < *recordcount_ptr(p)) {
    int type = *recordsize_ptr(p);
    if(type > 0) {
      type = NORMAL_SLOT;
    }
    return type;
  } else {
    return INVALID_SLOT;
  }
}
static void fixedSetType(int xid, Page *p, recordid rid, int type) {
  assertlocked(p->rwlatch);
  checkRid(p,rid);
  assert(rid.slot < *recordcount_ptr(p));
  assert(stasis_record_type_to_size(type) == stasis_record_type_to_size(*recordsize_ptr(p)));
  *recordsize_ptr(p) = rid.size;
}
static int fixedGetLength(int xid, Page *p, recordid rid) {
  assertlocked(p->rwlatch);
  assert(p->pageType);
  return rid.slot > *recordcount_ptr(p) ?
      INVALID_SLOT : stasis_record_type_to_size(*recordsize_ptr(p));
}

static int notSupported(int xid, Page * p) { return 0; }

static int fixedFreespace(int xid, Page * p) {
  assertlocked(p->rwlatch);
  if(stasis_fixed_records_per_page(*recordsize_ptr(p)) > *recordcount_ptr(p)) {
    // Return the size of a slot; that's the biggest record we can take.
    return stasis_record_type_to_size(*recordsize_ptr(p));
  } else {
    // Page full; return zero.
    return 0;
  }
}
static void fixedCompact(Page * p) {
  // no-op
}
static recordid fixedPreAlloc(int xid, Page *p, int size) {
  assertlocked(p->rwlatch);
  if(stasis_fixed_records_per_page(*recordsize_ptr(p)) > *recordcount_ptr(p)) {
    recordid rid;
    rid.page = p->id;
    rid.slot = *recordcount_ptr(p);
    rid.size = *recordsize_ptr(p);
    return rid;
  } else {
    return NULLRID;
  }
}
static void fixedPostAlloc(int xid, Page *p, recordid rid) {
  assertlocked(p->rwlatch);
  assert(*recordcount_ptr(p) == rid.slot);
  assert(*recordsize_ptr(p) == rid.size);
  (*recordcount_ptr(p))++;
}
static void fixedFree(int xid, Page *p, recordid rid) {
  assertlocked(p->rwlatch);
  if(*recordsize_ptr(p) == rid.slot+1) {
    (*recordsize_ptr(p))--;
  } else {
    // leak space; there's no way to track it with this page format.
  }
}

// XXX dereferenceRID

void fixedLoaded(Page *p) {
  p->LSN = *stasis_page_lsn_ptr(p);
}
void fixedFlushed(Page *p) {
  *stasis_page_lsn_ptr(p) = p->LSN;
}
void fixedCleanup(Page *p) { }
page_impl fixedImpl() {
  static page_impl pi = {
    FIXED_PAGE,
    1,
    fixedRead,
    fixedWrite,
    0,// readDone
    0,// writeDone
    fixedGetType,
    fixedSetType,
    fixedGetLength,
    fixedFirst,
    fixedNext,
    notSupported, // notSupported,
    stasis_block_first_default_impl,
    stasis_block_next_default_impl,
    stasis_block_done_default_impl,
    fixedFreespace,
    fixedCompact,
    fixedPreAlloc,
    fixedPostAlloc,
    fixedFree,
    0, // XXX dereference
    fixedLoaded, // loaded
    fixedFlushed, // flushed
    fixedCleanup
  };
  return pi;
}

/**
 @todo arrayListImpl belongs in arrayList.c
*/
page_impl arrayListImpl() {
  page_impl pi = fixedImpl();
  pi.page_type = ARRAY_LIST_PAGE;
  return pi;
}

void fixedPageInit() { }
void fixedPageDeinit() { }
