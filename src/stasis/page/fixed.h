#include "../page.h"

#ifndef __FIXED_H
#define __FIXED_H
/**
   @todo rename fixed.h macros turn them into static inline functions.
*/
#define recordsize_ptr(page)  stasis_page_int16_ptr_from_end((page), 1)
#define recordsize_cptr(page)  stasis_page_int16_cptr_from_end((page), 1)
#define recordcount_ptr(page) stasis_page_int16_ptr_from_end((page), 2)
#define fixed_record_ptr(page, n) stasis_page_byte_ptr_from_start((page), *recordsize_ptr((page)) * (n))

static inline recordid fixedNext(int xid, Page *p, recordid rid) {
  short n = *recordcount_ptr(p);
  rid.slot++;
  rid.size = *recordsize_ptr(p);
  if(rid.slot >= n) {
    return NULLRID;
  } else {
    return rid;
  }
}
static inline recordid fixedFirst(int xid, Page *p) {
  recordid rid = { p->id, -1, 0 };
  rid.size = *recordsize_ptr(p);
  return fixedNext(xid, p, rid);
}

void fixedPageInit();
void fixedPageDeinit();
page_impl fixedImpl();
page_impl arrayListImpl();
// @todo move lsmTreeImpl() to lsmTree.h (but first, move page.h...)
page_impl lsmRootImpl();
#endif
