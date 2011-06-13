#ifndef _ROSE_COMPRESSION_PSTAR_H__
#define _ROSE_COMPRESSION_PSTAR_H__

#include <limits.h>

#include <stasis/page.h>
#include <stasis/constants.h>

#include "compression.h"

// Copyright 2007 Google Inc. All Rights Reserved.
// Author: sears@google.com (Rusty Sears)

namespace rose {

template <class COMPRESSOR, class TYPE>
void pStarLoaded(Page *p);

template <class COMPRESSOR, class TYPE> class Pstar {
 public:
  // Initialize a new Pstar page
  Pstar(int xid, Page *p): p_(p), plug_(COMPRESSOR(xid, p->memAddr)) {
    stasis_page_cleanup(p);
    *stasis_page_type_ptr(p) = plugin_id<Pstar<COMPRESSOR,TYPE>,COMPRESSOR,TYPE>();
    p->pageType = *stasis_page_type_ptr(p);
    *freespace_ptr() = (intptr_t)recordsize_ptr() - (intptr_t)p_->memAddr;
    *recordsize_ptr() = sizeof(TYPE);
    free_bytes_ = *freespace_ptr() - plug_.bytes_used() - plug_.max_overrun();
    p->impl = this;
  }
  inline void pack() { };
  /**
     Append a new value to a page managed by pstar.

     @param xid the transaction adding the data to the page
     @param dat the value to be added to the page.
  */
  slot_index_t append(int xid, const TYPE dat);

  // @todo If we want to support multiple columns per page, then recordSize
  // and recordType need to be handled by the compressor.
  inline record_size_t recordType(int xid, slot_index_t slot) {
    return *recordsize_ptr();
  }
  inline void recordType(int xid, slot_index_t slot,
                         record_size_t type) {
    *recordsize_ptr() = type;
  }
  inline record_size_t recordLength(int xid, slot_index_t slot) {
    return physical_slot_length(recordType(xid, slot));
  }
  /**
      Read a value from a page managed by pstar.

      @param xid the transaction reading the record.
      @param slot The slot in this page that should be read.
      @param buf scratch space for recordRead.

      @return NULL if there is no such slot, or a pointer to rhe
      value.

      If a pointer is returned, it might point to the memory passed via
      scratch, or it might point to memory managed by the page
      implementation.  The return value will not be invalidated as long as
      the following two conditions apply:

      1) The page is pinned; loadPage() has been called, but releasePage()
      has not been called.

      2) The memory that scratch points to has not been freed, or reused
      in a more recent call to recordRead().

  */
  inline TYPE * recordRead(int xid, slot_index_t slot, TYPE * buf) {
    //    byte_off_t except = 0;
    TYPE * ret = plug_.recordRead(xid, slot, p_->memAddr, buf);
    //    if (ret == reinterpret_cast<TYPE*>(INVALID_SLOT)) { return 0; }

    /*    if (ret == reinterpret_cast<TYPE*>(EXCEPTIONAL)) {
      return reinterpret_cast<TYPE*>(
          &(p_->memAddr[except-recordLength(xid, rid.slot)]));
          } */
    return ret;
  }
  inline COMPRESSOR * compressor() { return &plug_; }

  static page_impl impl();

  static const plugin_id_t PAGE_FORMAT_ID = 0;

 private:

  // Load an existing Pstar page
  Pstar(Page *p): p_(p), plug_(COMPRESSOR(p->memAddr)) {
    free_bytes_ = *freespace_ptr() - plug_.bytes_used() - plug_.max_overrun();
  }

  inline byte_off_t * freespace_ptr() {
    return reinterpret_cast<byte_off_t*>(p_->memAddr+USABLE_SIZE_OF_PAGE)-1;
  }
  inline record_size_t * recordsize_ptr() {
    return reinterpret_cast<record_size_t*>(freespace_ptr())-1;
  }

  inline void page(Page * p) {
    p_ = p;
    plug_.memAddr(p->memAddr);
  }

  Page *p_;


  COMPRESSOR plug_;
  int free_bytes_;
  friend void pStarLoaded<COMPRESSOR, TYPE>(Page *p);
};

} // namespace rose
#endif  // _ROSE_COMPRESSION_PSTAR_H__
