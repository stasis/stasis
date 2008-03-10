#ifndef _ROSE_COMPRESSION_MULTICOLUMN_IMPL_H__
#define _ROSE_COMPRESSION_MULTICOLUMN_IMPL_H__

#include "multicolumn.h"
#include "for-impl.h"
#include "rle-impl.h"
namespace rose {

/**
   Initialize a new multicolumn page
*/
template <class TUPLE>
Multicolumn<TUPLE>::Multicolumn(int xid, Page *p, column_number_t column_count,
                                plugin_id_t * plugins) :
    p_(p),
    columns_(new byte*[column_count]),
    first_exception_byte_(USABLE_SIZE_OF_PAGE),
    exceptions_(new byte[USABLE_SIZE_OF_PAGE]),
    dispatcher_(column_count),
    unpacked_(1)
{

  *column_count_ptr() = column_count;

  bytes_left_ = first_header_byte_ptr()- p->memAddr;

  for(int i = 0; i < column_count; i++) {
    *column_plugin_id_ptr(i) = plugins[i];
    columns_[i] = new byte[USABLE_SIZE_OF_PAGE];
    dispatcher_.set_plugin(columns_[i],i,plugins[i]);
    dispatcher_.init_mem(columns_[i],i);
    bytes_left_ -= (dispatcher_.bytes_used(i) + dispatcher_.max_overrun(i));
  }

  *stasis_page_type_ptr(p) = plugin_id();
  p->impl = this;
}

template<class TUPLE>
Multicolumn<TUPLE>::Multicolumn(Page * p) :
    p_(p),
    columns_(new byte*[*column_count_ptr()]),
    first_exception_byte_(USABLE_SIZE_OF_PAGE - *exceptions_len_ptr()),
    exceptions_(p_->memAddr + *exceptions_offset_ptr()),
    dispatcher_(*column_count_ptr()),
    unpacked_(0)  {

  byte_off_t first_free = 0;
  for(int i = 0; i < *column_count_ptr(); i++) {

    byte * page_column_ptr = p_->memAddr + *column_offset_ptr(i);

    dispatcher_.set_plugin(page_column_ptr,i, *column_plugin_id_ptr(i));

    byte_off_t column_length = dispatcher_.bytes_used(i);
    columns_[i] = p_->memAddr + *column_offset_ptr(i);
    //    dispatcher_.set_plugin(columns_[i],i, *column_plugin_id_ptr(i));
    //    assert(columns_[i] == page_column_ptr);

    first_free = *column_offset_ptr(i) + column_length;
  }

  assert(first_free <= *exceptions_offset_ptr());
  assert(first_exception_byte_ <= USABLE_SIZE_OF_PAGE);

  bytes_left_ = *exceptions_offset_ptr() - first_free;

  assert(*stasis_page_type_ptr(p) == (Multicolumn<TUPLE>::plugin_id()));
}

template <class TUPLE>
void Multicolumn<TUPLE>::pack() {
  byte_off_t first_free = 0;
  byte_off_t last_free  = (intptr_t)(first_header_byte_ptr() - p_->memAddr);
  if(unpacked_) {
    *exceptions_len_ptr() = USABLE_SIZE_OF_PAGE - first_exception_byte_;
    last_free -= *exceptions_len_ptr();

    *exceptions_offset_ptr() = last_free;
    memcpy(&(p_->memAddr[*exceptions_offset_ptr()]),
	   exceptions_ + first_exception_byte_, *exceptions_len_ptr());

    for(int i = 0; i < *column_count_ptr(); i++) {
      *column_offset_ptr(i) = first_free;

      byte_off_t bytes_used = dispatcher_.bytes_used(i);
      memcpy(column_base_ptr(i), columns_[i], bytes_used);

      first_free += bytes_used;
      assert(first_free <= last_free);

      delete [] columns_[i];
      columns_[i] = column_base_ptr(i);
      dispatcher_.mem(columns_[i],i); //compressor(i))->mem(columns_[i]);
    }
    delete [] exceptions_;
    exceptions_ = p_->memAddr + *exceptions_offset_ptr();
    unpacked_ = 0;
  }
}

template <class TUPLE>
Multicolumn<TUPLE>::~Multicolumn() {
  for(int i = 0; i < *column_count_ptr(); i++) {
    if(unpacked_) delete [] columns_[i];
  }

  if(unpacked_) delete [] exceptions_;

  delete [] columns_;
}

/// Begin performance-critical code -------------------------------------------

/**
   Append a record to the page.  This function is complicated by
   the fact that each column was produced by a potentially
   different template instantiation.  Rather than harcode
   compressor implementations, or fall back on virtual methods,
   this function delegates compressor calls to PluginDispatcher.

   Pstar<> (and potential future implementations of multicolumn)
   benefit from this scheme as they can hardcode compressors at
   compile time, allowing the correct append method to be inlined,
   rather than invoked via a virtual method.
*/
template <class TUPLE>
inline slot_index_t Multicolumn<TUPLE>::append(int xid,
                                               TUPLE const & dat) {

  slot_index_t ret = NOSPACE;
  column_number_t i = 0;

  const column_number_t cols = dat.column_count();

  do {

    slot_index_t newret = dispatcher_.recordAppend(xid, i, dat.get(i),
                                                   &first_exception_byte_,
                                                   exceptions_, &bytes_left_);
    //assert(ret == NOSPACE || newret == NOSPACE || newret == ret);
    ret = newret;
    i++;
  } while(i < cols);

  return bytes_left_ < 0 ? NOSPACE : ret;

}

/**
   Read a record (tuple) from the page.

   @see append for a discussion of the implementation and
   associated design tradeoffs.
*/
template <class TUPLE>
inline TUPLE* Multicolumn<TUPLE>::recordRead(int xid, slot_index_t slot,
                                             TUPLE *buf) {
  column_number_t i = 0;
  column_number_t cols = buf->column_count();

  do {
    void * ret = dispatcher_.recordRead(xid,columns_[i],i,slot,exceptions_,
                                        buf->get(i));
    if(!ret) {
      return 0;
    }
    i++;
  } while(i < cols);
  return buf;
}

template <class TUPLE>
slot_index_t inline Multicolumn<TUPLE>::recordCount(int xid) {
  slot_index_t rec = dispatcher_.recordCount(0);
  //  printf("column count ptr %d\n", *column_count_ptr());
  for(slot_index_t i = 1; i < *column_count_ptr(); i++){
    slot_index_t c = dispatcher_.recordCount(i);
    rec = rec > c ? c : rec;
  }
  return rec;
}

template <class TUPLE>
inline TUPLE*
Multicolumn<TUPLE>::recordFind(int xid, TUPLE& val, TUPLE& scratch) {

  column_number_t cols = scratch.column_count();

  std::pair<slot_index_t,slot_index_t> pair_scratch;
  std::pair<slot_index_t,slot_index_t> * ret = &pair_scratch;
  pair_scratch.first = 0;
  pair_scratch.second = recordCount(xid);

  for(column_number_t i = 0; ret && i < cols; i++) {
    ret = dispatcher_.recordFind(xid, i, ret->first, ret->second,
				 exceptions_, val.get(i), pair_scratch);
  }
  if(ret) {
    recordRead(xid,ret->first, &scratch);
    return &scratch;
  } else {
    return 0;
  }
}
/// End performance-critical code ---------------------------------------------

/// Stuff below this line interfaces with Stasis' buffer manager --------------

/**
   Basic page_impl for multicolumn pages

   @see stasis/page.h and pstar-impl.h

*/
static const page_impl multicolumn_impl = {
  -1,
  0,  // multicolumnRead,
  0,       // multicolumnWrite,
  0,  // multicolumnReadDone,
  0,       // multicolumnWriteDone,
  0,  // multicolumnGetType,
  0,  // multicolumnSetType,
  0,  // multicolumnGetLength,
  0,  // multicolumnFirst,
  0,  // multicolumnNext,
  0,  // multicolumnIsBlockSupported,
  0,  // multicolumnBlockFirst,
  0,  // multicolumnBlockNext,
  0,  // multicolumnBlockDone,
  0,  // multicolumnFreespace,
  0,       // multicolumnCompact,
  0,       // multicolumnPreRalloc,
  0,       // multicolumnPostRalloc,
  0,       // multicolumnFree,
  0,       // dereference_identity,
  0,  // multicolumnLoaded,
  0,  // multicolumnFlushed
  0,  // multicolumnCleanup
};

// XXX implement plugin_id().  Currently, it treats all instantiations of the
// same TUPLE template interchangably; this will break for binaries that
// manipulate more than one type of tuple..
template <class TUPLE>
inline plugin_id_t
Multicolumn<TUPLE>::plugin_id() {
  return USER_DEFINED_PAGE(0) + 32 + TUPLE::TUPLE_ID;
}

template <class TUPLE>
void multicolumnLoaded(Page *p) {
  p->LSN = *stasis_page_lsn_ptr(p);
  assert(*stasis_page_type_ptr(p) == Multicolumn<TUPLE>::plugin_id());
  p->impl = new Multicolumn<TUPLE>(p);
}

template <class TUPLE>
static void multicolumnFlushed(Page *p) {
  *stasis_page_lsn_ptr(p) = p->LSN;
  ((Multicolumn<TUPLE>*)(p->impl))->pack();
}
template <class TUPLE>
static void multicolumnCleanup(Page *p) {
  delete (Multicolumn<TUPLE>*)p->impl;
  p->impl = 0;
}

template <class TUPLE>
page_impl Multicolumn<TUPLE>::impl() {
  page_impl ret = multicolumn_impl;
  ret.page_type = Multicolumn<TUPLE>::plugin_id();
  ret.pageLoaded = multicolumnLoaded<TUPLE>;
  ret.pageFlushed = multicolumnFlushed<TUPLE>;
  ret.pageCleanup = multicolumnCleanup<TUPLE>;
  return ret;
}

}

#endif  // _ROSE_COMPRESSION_MULTICOLUMN_IMPL_H__
