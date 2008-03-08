#ifndef _ROSE_COMPRESSION_STATIC_MULTICOLUMN_H__
#define _ROSE_COMPRESSION_STATIC_MULTICOLUMN_H__

#include <limits.h>

#include <stasis/common.h>
#include <stasis/page.h>
#include <stasis/constants.h>

#include "compression.h"

//#include "pstar.h"  // for typedefs + consts (XXX add new header?)
#include "tuple.h" // XXX rename tuple.hx
//#include "pluginDispatcher.h"
// Copyright 2007 Google Inc. All Rights Reserved.
// Author: sears@google.com (Rusty Sears)

/**
  @file Page implementation for multi-column, compressed data

 STRUCTURE OF A MULTICOLUMN PAGE

<pre>
 +----------------------------------------------------------------------+
 | col #0 compressed data (opaque) | col #1 compressed data (opaque)    |
 +-----+---------------------------+-----+------------------------------|
 | ... | col #N compressed data (opaque) |                              |
 +-----+---------------------------------+                              |
 |  Free space                                                          |
 |                                                                      |
 |                                                                      |
 |                             +----------------------------------------+
 |                             | Exceptions:                            |
 +-----------------------------+ Includes data from multiple cols       |
 |                                                                      |
 | Exception data is managed (bytes are copied in and out of this       |
 | region) by the column implementations.  Multicolumn mediates between |
 | the columns, by recording the length and offset of this region.      |
 |                                                                      |
 |                                      +---------------+---------------+
 |                                  ... | exception # 1 | exception # 0 |
 +-----------------------+--------------------+----+--------------------+
 |  first header byte -> | col #N off, plugin | .. | col #1 off, plugin |
 +--------------------+--+-------------+------+----+----+-----------+---+
 | col #0 off, plugin | exceptions len | exceptions off | # of cols | ? |
 +--------------------+----------------+----------------+-----------+---+
</pre>

 Notes:

   The 'exceptions' portion of the page grows down from
   first_header_byte, while the column data portion grows up from byte
   zero...  This was an arbitrary decision, and complicated the
   implementation somewhat...

   Functions whose names end in "_ptr" return pointers to bytes in the
   page.  That memory is persistant; and will eventually be written
   back to the page file.

*/

namespace rose {

/**
 * A "pageLoaded()" callback function for Stasis' buffer manager.
 */
template <int N, class TUPLE,
  class COMP0, class COMP1, class COMP2, class COMP3, class COMP4,
  class COMP5, class COMP6, class COMP7, class COMP8, class COMP9>
void staticMulticolumnLoaded(Page *p);

template <int N, class TUPLE,
  class COMP0, class COMP1 = For<bool>, class COMP2 = For<bool>,
  class COMP3=For<bool>, class COMP4=For<bool>, class COMP5=For<bool>,
  class COMP6=For<bool>, class COMP7=For<bool>, class COMP8=For<bool>,
  class COMP9=For<bool> >
class StaticMulticolumn {
 public:
  static page_impl impl();
  static const plugin_id_t PAGE_FORMAT_ID = 1;
  typedef TUPLE TUP;

 typedef COMP0 CMP0;
 typedef COMP1 CMP1;
 typedef COMP2 CMP2;
 typedef COMP3 CMP3;
 typedef COMP4 CMP4;
 typedef COMP5 CMP5;
 typedef COMP6 CMP6;
 typedef COMP7 CMP7;
 typedef COMP8 CMP8;
 typedef COMP9 CMP9;

 StaticMulticolumn(int xid, Page *p) :
    p_(p),
    first_exception_byte_(USABLE_SIZE_OF_PAGE),
    exceptions_(new byte[USABLE_SIZE_OF_PAGE]),
    unpacked_(1)
{
  stasis_page_cleanup(p);
  *column_count_ptr() = N;

  bytes_left_ = first_header_byte_ptr()- p->memAddr;

#define STATIC_MC_INIT(i,typ,cmp)			   \
  if(i < N) {						   \
    columns_[i] = new byte[USABLE_SIZE_OF_PAGE];           \
    cmp = new typ(xid,(void*)columns_[i]);		   \
    cmp->init_mem(columns_[i]);				   \
    *column_plugin_id_ptr(i) = cmp->PLUGIN_ID;		   \
    bytes_left_ -= cmp->bytes_used();			   \
  }

  STATIC_MC_INIT(0, CMP0, plugin0) STATIC_MC_INIT(1, CMP1, plugin1) STATIC_MC_INIT(2, CMP2, plugin2) ;
  STATIC_MC_INIT(3, CMP3, plugin3) STATIC_MC_INIT(4, CMP4, plugin4) STATIC_MC_INIT(5, CMP5, plugin5) ;
  STATIC_MC_INIT(6, CMP6, plugin6) STATIC_MC_INIT(7, CMP7, plugin7) STATIC_MC_INIT(8, CMP8, plugin8) ;
  STATIC_MC_INIT(9, CMP9, plugin9);

#undef STATIC_MC_INIT

  *stasis_page_type_ptr(p) = plugin_id();
  p->impl = this;
}

 ~StaticMulticolumn() {

#define STATIC_MC_DEINIT(i,plug)		            \
    if(i < N) {						    \
      if(unpacked_) delete [] columns_[i];		    \
      delete plug;                                          \
    }

    STATIC_MC_DEINIT(0,plugin0);
    STATIC_MC_DEINIT(1,plugin1);
    STATIC_MC_DEINIT(2,plugin2);
    STATIC_MC_DEINIT(3,plugin3);
    STATIC_MC_DEINIT(4,plugin4);
    STATIC_MC_DEINIT(5,plugin5);
    STATIC_MC_DEINIT(6,plugin6);
    STATIC_MC_DEINIT(7,plugin7);
    STATIC_MC_DEINIT(8,plugin8);
    STATIC_MC_DEINIT(9,plugin9);

    if(unpacked_) delete [] exceptions_;

 }

  /**
     @return the compressor used for a column.  The nature of the
     mapping between table region and compressor instance is
     implementation defined, but there will never be more than one
     compressor per-column, per-page.

     @param col The column whose compressor should be returned.
     @return A pointer to a compressor.  This pointer is guaranteed to
     be valid until the next call to this Multicolumn object.  After
     that, the pointer returned here is invalid.
  */
  //  void* compressor(column_number_t col) {
    // XXX return dispatcher_.compressor(col);
  //  }
  COMP0 * compressor0() const { return plugin0; }
  COMP1 * compressor1() const { return plugin1; }
  COMP2 * compressor2() const { return plugin2; }
  COMP3 * compressor3() const { return plugin3; }
  COMP4 * compressor4() const { return plugin4; }
  COMP5 * compressor5() const { return plugin5; }
  COMP6 * compressor6() const { return plugin6; }
  COMP7 * compressor7() const { return plugin7; }
  COMP8 * compressor8() const { return plugin8; }
  COMP9 * compressor9() const { return plugin9; }

  inline slot_index_t append(int xid, TUPLE const & dat) {
    slot_index_t ret = 0;
    slot_index_t newret = 0;
    if(0 < N) ret = plugin0->append(xid, *dat.get0(),&first_exception_byte_,
				    exceptions_, &bytes_left_);
    //    if(bytes_left_ >= 0) {
    if(1 < N) newret = plugin1->append(xid, *dat.get1(),&first_exception_byte_,
				    exceptions_, &bytes_left_);
    //    if(bytes_left_ >= 0) {
    //      assert(newret == ret);
    if(2 < N) newret = plugin2->append(xid, *dat.get2(),&first_exception_byte_,
				    exceptions_, &bytes_left_);
    //    if(bytes_left_ >= 0) {
    //      assert(newret == ret);
    if(3 < N) newret = plugin3->append(xid, *dat.get3(),&first_exception_byte_,
				    exceptions_, &bytes_left_);
    //    if(bytes_left_ >= 0) {
    //      assert(newret == ret);
    if(4 < N) newret = plugin4->append(xid, *dat.get4(),&first_exception_byte_,
				    exceptions_, &bytes_left_);
    //    if(bytes_left_ >= 0) {
    //      assert(newret == ret);
    if(5 < N) newret = plugin5->append(xid, *dat.get5(),&first_exception_byte_,
				    exceptions_, &bytes_left_);
    //    if(bytes_left_ >= 0) {
    //      assert(newret == ret);
    if(6 < N) newret = plugin6->append(xid, *dat.get6(),&first_exception_byte_,
				    exceptions_, &bytes_left_);
    //    if(bytes_left_ >= 0) {
    //      assert(newret == ret);
    if(7 < N) newret = plugin7->append(xid, *dat.get7(),&first_exception_byte_,
				   exceptions_, &bytes_left_);
    //    if(bytes_left_ >= 0) {
    //      assert(newret == ret);
    if(8 < N) newret = plugin8->append(xid, *dat.get8(),&first_exception_byte_,
				   exceptions_, &bytes_left_);
    //    if(bytes_left_ >= 0) {
    //      assert(newret == ret);
    if(9 < N) newret = plugin9->append(xid, *dat.get9(),&first_exception_byte_,
				   exceptions_, &bytes_left_);
    //    }}}}}}}}}
    assert(N == 1 || bytes_left_ < 0 || newret == ret);
    return (bytes_left_ < 0) ? NOSPACE : ret;
  }
 inline TUPLE * recordRead(int xid, slot_index_t slot, TUPLE * buf) {
   bool ret = 1;
   if(0 < N) ret = plugin0->recordRead(xid,slot,exceptions_,const_cast<typename TUP::TYP0*>(buf->get0())) ? ret : 0;
   if(1 < N) ret = plugin1->recordRead(xid,slot,exceptions_,const_cast<typename TUP::TYP1*>(buf->get1())) ? ret : 0;
   if(2 < N) ret = plugin2->recordRead(xid,slot,exceptions_,const_cast<typename TUP::TYP2*>(buf->get2())) ? ret : 0;
   if(3 < N) ret = plugin3->recordRead(xid,slot,exceptions_,const_cast<typename TUP::TYP3*>(buf->get3())) ? ret : 0;
   if(4 < N) ret = plugin4->recordRead(xid,slot,exceptions_,const_cast<typename TUP::TYP4*>(buf->get4())) ? ret : 0;
   if(5 < N) ret = plugin5->recordRead(xid,slot,exceptions_,const_cast<typename TUP::TYP5*>(buf->get5())) ? ret : 0;
   if(6 < N) ret = plugin6->recordRead(xid,slot,exceptions_,const_cast<typename TUP::TYP6*>(buf->get6())) ? ret : 0;
   if(7 < N) ret = plugin7->recordRead(xid,slot,exceptions_,const_cast<typename TUP::TYP7*>(buf->get7())) ? ret : 0;
   if(8 < N) ret = plugin8->recordRead(xid,slot,exceptions_,const_cast<typename TUP::TYP8*>(buf->get8())) ? ret : 0;
   if(9 < N) ret = plugin9->recordRead(xid,slot,exceptions_,const_cast<typename TUP::TYP9*>(buf->get9())) ? ret : 0;
   return ret ? buf : 0;
 }
 inline slot_index_t recordCount(int xid) {
   slot_index_t recordCount;
   slot_index_t c;
   // XXX memoize this function
   if(0 < N) recordCount = plugin0->recordCount();
   if(1 < N) { c = plugin1->recordCount(); recordCount = recordCount > c ? c :recordCount; }
   if(2 < N) { c = plugin2->recordCount(); recordCount = recordCount > c ? c :recordCount; }
   if(3 < N) { c = plugin3->recordCount(); recordCount = recordCount > c ? c :recordCount; }
   if(4 < N) { c = plugin4->recordCount(); recordCount = recordCount > c ? c :recordCount; }
   if(5 < N) { c = plugin5->recordCount(); recordCount = recordCount > c ? c :recordCount; }
   if(6 < N) { c = plugin6->recordCount(); recordCount = recordCount > c ? c :recordCount; }
   if(7 < N) { c = plugin7->recordCount(); recordCount = recordCount > c ? c :recordCount; }
   if(8 < N) { c = plugin8->recordCount(); recordCount = recordCount > c ? c :recordCount; }
   if(9 < N) { c = plugin9->recordCount(); recordCount = recordCount > c ? c :recordCount; }
   return recordCount;
 }
 /* inline slot_index_t recordCount(int xid) {
   if(1 == N) return plugin0->recordCount(xid);
   if(2 == N) return plugin1->recordCount(xid);
   if(3 == N) return plugin2->recordCount(xid);
   if(4 == N) return plugin3->recordCount(xid);
   if(5 == N) return plugin4->recordCount(xid);
   if(6 == N) return plugin5->recordCount(xid);
   if(7 == N) return plugin6->recordCount(xid);
   if(8 == N) return plugin7->recordCount(xid);
   if(9 == N) return plugin8->recordCount(xid);
   if(10 == N) return plugin9->recordCount(xid);
   abort();
   } */
 inline TUPLE * recordFind(int xid, TUPLE& val, TUPLE& scratch) {
   std::pair<slot_index_t,slot_index_t> pair_scratch;
   std::pair<slot_index_t,slot_index_t> * ret;
   //   printf("static multiclumn record find\n"); fflush(stdout);

   if(0 < N) ret = plugin0->recordFind(xid, 0, recordCount(xid),
				       exceptions_, *val.get0(), pair_scratch);
   //assert(ret);
   if(1 < N) if(ret) ret = plugin1->recordFind(xid, ret->first, ret->second,
				       exceptions_, *val.get1(), pair_scratch);
   //assert(ret);
   if(2 < N) if(ret) ret = plugin2->recordFind(xid, ret->first, ret->second,
				       exceptions_, *val.get2(), pair_scratch);
   //assert(ret);
   if(3 < N) if(ret) ret = plugin3->recordFind(xid, ret->first, ret->second,
				       exceptions_, *val.get3(), pair_scratch);
   //assert(ret);
   if(4 < N) if(ret) ret = plugin4->recordFind(xid, ret->first, ret->second,
				       exceptions_, *val.get4(), pair_scratch);
   //assert(ret);
   if(5 < N) if(ret) ret = plugin5->recordFind(xid, ret->first, ret->second,
				       exceptions_, *val.get5(), pair_scratch);
   //assert(ret);
   if(6 < N) if(ret) ret = plugin6->recordFind(xid, ret->first, ret->second,
				       exceptions_, *val.get6(), pair_scratch);
   //assert(ret);
   if(7 < N) if(ret) ret = plugin7->recordFind(xid, ret->first, ret->second,
				       exceptions_, *val.get7(), pair_scratch);
   //assert(ret);
   if(8 < N) if(ret) ret = plugin8->recordFind(xid, ret->first, ret->second,
				       exceptions_, *val.get8(), pair_scratch);
   //assert(ret);
   if(9 < N) if(ret) ret = plugin9->recordFind(xid, ret->first, ret->second,
				       exceptions_, *val.get9(), pair_scratch);
   //assert(ret);
   if(ret) {
     // XXX slow, doesn't return whole range...
     recordRead(xid, ret->first, &scratch);
     return &scratch;
   } else {
     return 0;
   }
 }
  inline void pack() {
    byte_off_t first_free = 0;
    byte_off_t last_free  = (intptr_t)(first_header_byte_ptr() - p_->memAddr);
    if(unpacked_) {
      *exceptions_len_ptr() = USABLE_SIZE_OF_PAGE - first_exception_byte_;
      last_free -= *exceptions_len_ptr();

      *exceptions_offset_ptr() = last_free;
      memcpy(&(p_->memAddr[*exceptions_offset_ptr()]),
	     exceptions_ + first_exception_byte_, *exceptions_len_ptr());

#define STATIC_MC_PACK(i,comp)				              \
      if(i < N) {						      \
	*column_offset_ptr(i) = first_free;			      \
	byte_off_t bytes_used = comp->bytes_used();		      \
	memcpy(column_base_ptr(i), columns_[i], bytes_used);	      \
	first_free += bytes_used;				      \
	assert(first_free <= last_free);			      \
	delete [] columns_[i];					      \
	columns_[i] = column_base_ptr(i);			      \
	comp->mem(columns_[i]);					      \
      }

      STATIC_MC_PACK(0,plugin0) STATIC_MC_PACK(1,plugin1) ;
      STATIC_MC_PACK(2,plugin2) STATIC_MC_PACK(3,plugin3) ;
      STATIC_MC_PACK(4,plugin4) STATIC_MC_PACK(5,plugin5) ;
      STATIC_MC_PACK(6,plugin6) STATIC_MC_PACK(7,plugin7) ;
      STATIC_MC_PACK(8,plugin8) STATIC_MC_PACK(9,plugin9) ;

#undef STATIC_MC_PACK

      delete [] exceptions_;
      exceptions_ = p_->memAddr + *exceptions_offset_ptr();
      unpacked_ = 0;
    }
  }
 private:
  COMP0* plugin0; COMP1* plugin1; COMP2* plugin2; COMP3* plugin3;
  COMP4* plugin4; COMP5* plugin5; COMP6* plugin6; COMP7* plugin7;
  COMP8* plugin8; COMP9* plugin9;

  typedef struct column_header {
    byte_off_t off;
    plugin_id_t plugin_id;
  } column_header;

  /**
     Load an existing multicolumn Page
  */
 StaticMulticolumn(Page * p) :
    p_(p),
    first_exception_byte_(USABLE_SIZE_OF_PAGE - *exceptions_len_ptr()),
    exceptions_(p_->memAddr + *exceptions_offset_ptr()),
    unpacked_(0)  {
      byte_off_t first_free = 0;
      assert(N == *column_count_ptr());

#define STATIC_MC_INIT(i,plug,cmp)					\
      if(i < N) {							\
	columns_[i] = p_->memAddr + *column_offset_ptr(i);		\
	plug = new cmp((void*)columns_[i]);				\
	first_free = *column_offset_ptr(i) + plug->bytes_used();	\
      }

      STATIC_MC_INIT(0, plugin0,COMP0) ;
      STATIC_MC_INIT(1, plugin1,COMP1) ;
      STATIC_MC_INIT(2, plugin2,COMP2) ;
      STATIC_MC_INIT(3, plugin3,COMP3) ;
      STATIC_MC_INIT(4, plugin4,COMP4) ;
      STATIC_MC_INIT(5, plugin5,COMP5) ;
      STATIC_MC_INIT(6, plugin6,COMP6) ;
      STATIC_MC_INIT(7, plugin7,COMP7) ;
      STATIC_MC_INIT(8, plugin8,COMP8) ;
      STATIC_MC_INIT(9, plugin9,COMP9) ;

#undef STATIC_MC_INIT

      assert(first_free <= *exceptions_offset_ptr());
      assert(first_exception_byte_ <= USABLE_SIZE_OF_PAGE);

      bytes_left_ = *exceptions_offset_ptr() - first_free;

      assert(*stasis_page_type_ptr(p) == (plugin_id()));
    }

  /**
     The following functions perform pointer arithmetic.  This code is
     performance critical.  These short, inlined functions mostly
     perform simple arithmetic expression involving constants.  g++'s
     optimizer seems to combine and simplify these expressions for us.

     See the page layout diagram at the top of this file for an
     explanation of where these pointers are stored
   */

  inline column_number_t * column_count_ptr() {
    return reinterpret_cast<column_number_t*>(p_->memAddr+USABLE_SIZE_OF_PAGE)-1;
  }
  inline byte_off_t * exceptions_offset_ptr() {
    return reinterpret_cast<byte_off_t*>(column_count_ptr())-1;
  }
  inline byte_off_t * exceptions_len_ptr() {
    return exceptions_offset_ptr()-1;;
  }
  inline column_header * column_header_ptr(column_number_t column_number) {
    return reinterpret_cast<column_header*>(exceptions_len_ptr())-(1+column_number);
  }
  inline byte_off_t * column_offset_ptr(column_number_t column_number) {
    return &(column_header_ptr(column_number)->off);
  }
  /**
     This stores the plugin_id associated with this page's compressor.

     @see rose::plugin_id()
  */
  inline plugin_id_t * column_plugin_id_ptr(column_number_t column_number) {
    return &(column_header_ptr(column_number)->plugin_id);
  }
  /**
     The first byte that contains data for this column.

     The length of the column data can be determined by calling
     COMPRESSOR's bytes_used() member function.  (PluginDispatcher
     can handle this).
  */
  inline byte * column_base_ptr(column_number_t column_number) {
    return *column_offset_ptr(column_number) + p_->memAddr;
  }
  inline byte * first_header_byte_ptr() {
    return reinterpret_cast<byte*>(column_header_ptr((*column_count_ptr())-1));
  }

  static inline plugin_id_t plugin_id() {
    // XXX collides with multicolumn.h
    return USER_DEFINED_PAGE(0) + 32 + TUPLE::TUPLE_ID;
  }
  Page * p_;
  byte * columns_[N];
  byte_off_t first_exception_byte_;
  byte * exceptions_;
  int bytes_left_;
  int unpacked_;
  friend void staticMulticolumnLoaded<N,TUPLE,COMP0,COMP1,COMP2,COMP3,COMP4,COMP5,COMP6,COMP7,COMP8,COMP9>(Page *p);
};


/// End performance-critical code ---------------------------------------------

/// Stuff below this line interfaces with Stasis' buffer manager --------------

/**
   Basic page_impl for multicolumn pages

   @see stasis/page.h and pstar-impl.h

*/
static const page_impl static_multicolumn_impl = {
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

template <int N, class TUPLE,
  class COMP0, class COMP1, class COMP2, class COMP3, class COMP4,
  class COMP5, class COMP6, class COMP7, class COMP8, class COMP9>
void staticMulticolumnLoaded(Page *p) {
  p->LSN = *stasis_page_lsn_ptr(p);
  assert(*stasis_page_type_ptr(p) == (StaticMulticolumn<N,TUPLE,COMP0,COMP1,COMP2,COMP3,COMP4,COMP5,COMP6,COMP7,COMP8,COMP9>::plugin_id()));
  p->impl = new StaticMulticolumn<N,TUPLE,COMP0,COMP1,COMP2,COMP3,COMP4,COMP5,COMP6,COMP7,COMP8,COMP9>(p);
}
template <int N, class TUPLE,
  class COMP0, class COMP1, class COMP2, class COMP3, class COMP4,
  class COMP5, class COMP6, class COMP7, class COMP8, class COMP9>
static void staticMulticolumnFlushed(Page *p) {
  *stasis_page_lsn_ptr(p) = p->LSN;
  ((StaticMulticolumn<N,TUPLE,COMP0,COMP1,COMP2,COMP3,COMP4,COMP5,COMP6,COMP7,COMP8,COMP9>*)(p->impl))->pack();
}
template <int N, class TUPLE,
  class COMP0, class COMP1, class COMP2, class COMP3, class COMP4,
  class COMP5, class COMP6, class COMP7, class COMP8, class COMP9>
static void staticMulticolumnCleanup(Page *p) {
  delete (StaticMulticolumn<N,TUPLE,COMP0,COMP1,COMP2,COMP3,COMP4,COMP5,COMP6,COMP7,COMP8,COMP9>*)p->impl;
  p->impl = 0;
}

template <int N, class TUPLE,
  class COMP0, class COMP1, class COMP2, class COMP3, class COMP4,
  class COMP5, class COMP6, class COMP7, class COMP8, class COMP9>
page_impl StaticMulticolumn<N,TUPLE,COMP0,COMP1,COMP2,COMP3,COMP4,COMP5,COMP6,COMP7,COMP8,COMP9>::impl() {
  page_impl ret = static_multicolumn_impl;
  ret.page_type = StaticMulticolumn<N,TUPLE,COMP0,COMP1,COMP2,COMP3,COMP4,COMP5,COMP6,COMP7,COMP8,COMP9>::plugin_id();
  ret.pageLoaded = staticMulticolumnLoaded<N,TUPLE,COMP0,COMP1,COMP2,COMP3,COMP4,COMP5,COMP6,COMP7,COMP8,COMP9>;
  ret.pageFlushed = staticMulticolumnFlushed<N,TUPLE,COMP0,COMP1,COMP2,COMP3,COMP4,COMP5,COMP6,COMP7,COMP8,COMP9>;
  ret.pageCleanup = staticMulticolumnCleanup<N,TUPLE,COMP0,COMP1,COMP2,COMP3,COMP4,COMP5,COMP6,COMP7,COMP8,COMP9>;
  return ret;
}



}  // namespace rose


#endif // _ROSE_COMPRESSION_STATIC_MULTICOLUMN_H__
