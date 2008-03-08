#ifndef _ROSE_COMPRESSION_MULTICOLUMN_H__
#define _ROSE_COMPRESSION_MULTICOLUMN_H__

#include <limits.h>

#include <stasis/page.h>
#include <stasis/constants.h>

#include "compression.h"
//#include "pstar.h"  // for typedefs + consts (XXX add new header?)
#include "tuple.h" // XXX rename tuple.hx
#include "pluginDispatcher.h"
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

template <class TUPLE>
/**
 * A "pageLoaded()" callback function for Stasis' buffer manager.
 */
void multicolumnLoaded(Page *p);

template <class TUPLE> class Multicolumn {
 public:
  static page_impl impl();
  static const plugin_id_t PAGE_FORMAT_ID = 1;
  typedef TUPLE TUP;

  Multicolumn(int xid, Page *p, column_number_t column_count,
              plugin_id_t * plugins);

  ~Multicolumn();

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
  void* compressor(column_number_t col) {
    return dispatcher_.compressor(col);
  }
  inline slot_index_t append(int xid, TUPLE const & dat);
  inline TUPLE * recordRead(int xid, slot_index_t slot, TUPLE * buf);
  inline TUPLE * recordFind(int xid, TUPLE& val, TUPLE& scratch);
  inline slot_index_t recordCount(int xid);
  inline void pack();

 private:

  typedef struct column_header {
    byte_off_t off;
    plugin_id_t plugin_id;
  } column_header;

  /**
     Load an existing multicolumn Page
  */
  Multicolumn(Page * p);

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

  static inline plugin_id_t plugin_id();
  Page * p_;
  byte ** columns_;
  byte_off_t first_exception_byte_;
  byte * exceptions_;

 public:
  PluginDispatcher dispatcher_;
 private:
  int bytes_left_;
  int unpacked_;
  friend void multicolumnLoaded<TUPLE>(Page *p);
};

}  // namespace rose


#endif // _ROSE_COMPRESSION_MULTICOLUMN_H__
