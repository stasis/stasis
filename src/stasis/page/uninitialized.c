/*
 * uninitialized.c
 *
 *  Created on: Nov 9, 2009
 *      Author: sears
 */
#include <stasis/page/uninitialized.h>

static int notSupported(int xid, Page * p) { return 0; }

static void uninitializedLoaded(Page *p) {
  p->LSN = *stasis_page_lsn_cptr(p);
  p->impl = 0;
}
static void uninitializedFlushed(Page *p) {
  *stasis_page_type_ptr(p)= p->pageType;
  *stasis_page_lsn_ptr(p) = p->LSN;

}
static void uninitializedCleanup(Page *p) {

}

page_impl stasis_page_uninitialized_impl(void) {
  static page_impl pi = {
    UNINITIALIZED_PAGE,
    1, //has header
    0, //read,
    0, //write,
    0,// readDone
    0,// writeDone
    0, //GetType,
    0, //SetType,
    0, //fixedGetLength,
    0, //fixedFirst,
    0, //fixedNext,
    0, //fixedLast,
    notSupported, // notSupported,
    0,//stasis_block_first_default_impl,
    0,//stasis_block_next_default_impl,
    0,//stasis_block_done_default_impl,
    0,//fixedFreespace,
    0,//fixedCompact,
    0,//fixedCompactSlotIds,
    0,//fixedPreAlloc,
    0,//fixedPostAlloc,
    0,//fixedSplice,
    0,//fixedFree,
    0, // XXX dereference
    uninitializedLoaded, // loaded
    uninitializedFlushed, // flushed
    uninitializedCleanup
  };
  return pi;
}
