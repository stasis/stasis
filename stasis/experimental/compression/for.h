#ifndef _ROSE_COMPRESSION_FOR_H__
#define _ROSE_COMPRESSION_FOR_H__

#undef try
#undef catch
#undef end

#include <algorithm>

// Copyright 2007 Google Inc. All Rights Reserved.
// Author: sears@google.com (Rusty Sears)

/**
   @file

   Implementation of Frame of Reference compression

   This file implements a COMPRESSOR plugin that handles compresion
   for a column of data in a page.  Rather than hardcoding a
   particular page layout, these plugins work with two ranges of
   memory that must be contiguous.  The first range contains the
   compressed data.  A pointer to this region is stored in the mem_
   member of this class.  The second region is shared amongst multiple
   compressor implementations and is passed into methods via the
   "exceptions" parameter.  A second parameter, "except" provides the
   offset of the first byte of exceptions that is in use.  If
   necessary, the compressor implementation may store data as an
   exception, by prepending it to the exceptions array, and
   decrementing "except".

   A third parameter, "free_bytes" is used to manage byte allocation
   out of some global (per page) pool.  If free_bytes becomes
   negative, the page is out of space, and all further allocation
   attempts will fail.  Compressor plugins modify it as more data is
   inserted into the page.

   Finally, the compressor may define a volatile region of bytes after
   the end of the compressed data.  This region is used as scratch
   space, and will not be written to disk.  However, it <i>is</i>
   deducted from the total number of free bytes on the page, wasting a
   small amount of storage on disk.  The max_overrun() method returns
   the size of this extra scratch space buffer.
*/

#include <limits.h>

#include "pstar.h"
namespace rose {

template <class TYPE, class DELTA_TYPE=int8_t>
class For {
 public:
  typedef TYPE TYP;

  static const int PLUGIN_ID = 0;
  /**
      Set the page offset.  For frame of reference, this is used to
      calculate deltas.
  */
  inline void offset(TYPE o);
  /**
     The size of the scratch space reserved at the end of the page for
     speculative execution.
  */
  inline size_t max_overrun() { return sizeof(delta_t) + sizeof(TYPE); }

  /**
   Append a new value to a compressed portion of a page.  This
   function is meant to be called by pstar, not by end users.

   @param xid the transaction that is appending the value (currently unused)
   @param dat contains the value to be appended to the end of the page.
   @param except the offset of the first exceptional value on the page.  This
          should initially be set to the end of the exceptional array;
          append() will modify it as needed.
   @param exceptions a pointer to the beginning of the exceptions region.
   @param free_bytes The number of free bytes on the page.  This number will be
          decremented (or incremented) by append to reflect changes in the
	  number of bytes in use.
   @return The slot index of the newly returned value or an undefined if the
           page is too full to accomodate the value (that is, free_bytes is
           negative). Implementations may set free_bytes to a negative value if
           an implementation defined limit prevents them from accomodating more
           data, even if there are free bytes on the page.
  */
  inline slot_index_t append(int xid, const TYPE dat,
                             byte_off_t * except, byte * exceptions,
			     int * free_bytes);

  /**
   Read a compressed value.  This function is meant to be called by
   pstar, not by end users.

   @param xid Tracked for locking.  Currently unused.
   @param slot The index number of the slot that should be read.
   @param exceptions A byte array that contains the exceptional values.
   @param buf Storage space for the value to be read.  This function will
          return this pointer after succesfully reading the value.
          The caller manages the memory passed via buf.

   @return NULL if the slot is off the end of the page, or buf if the
           record exists, and has been read.

   @see Pstar::recordRead() and Multicolumn::recordRead()

  */
  inline TYPE *recordRead(int xid, slot_index_t slot, byte *exceptions,
                           TYPE * buf);
  /**
     Find the offset of a compressed value, assuming it falls within
     the given range.

     @return NULL if the value is not found.
   */
  inline std::pair<slot_index_t,slot_index_t>*
    recordFind(int xid, slot_index_t start, slot_index_t stop,
	       byte *exceptions, TYPE value,
	       std::pair<slot_index_t,slot_index_t>& scratch);
  /**
    This constructor initializes a new FOR region.

    @param xid the transaction that created the new region.
    @param mem A pointer to the buffer manager's copy of this page.
  */
  For(int xid, void * mem): mem_(mem) {
    *base_ptr() = -1;
    *numdeltas_ptr() = 0;
  };
 For(void * mem): mem_(mem) {}

  inline slot_index_t recordCount() {
    return *numdeltas_ptr();
  }

 For() : mem_(0) {}
  /**
      @return the length of the FOR region, in bytes
  */
  inline byte_off_t bytes_used() {
    return ((intptr_t)(last_delta_ptr()+1)) - (intptr_t)mem_;
  }

  inline void mem(byte * mem) { mem_ = mem; }

  inline void init_mem(byte * mem) {
    mem_=mem;
    *base_ptr() = -1;
    *numdeltas_ptr() = 0;
  }

 private:

  typedef DELTA_TYPE delta_t;
  static const delta_t DELTA_MAX = (sizeof(delta_t) == 1) ? CHAR_MAX : (sizeof(delta_t) == 2) ? SHRT_MAX : INT_MAX;
  static const delta_t DELTA_MIN = (sizeof(delta_t) == 1) ? CHAR_MIN : (sizeof(delta_t) == 2) ? SHRT_MIN : INT_MIN;

  inline TYPE offset() { return *base_ptr(); }

  inline TYPE* base_ptr() { return reinterpret_cast<TYPE*>(mem_); }

  inline slot_index_t* numdeltas_ptr() {
    return reinterpret_cast<slot_index_t*>(base_ptr()+1);
  }
  inline delta_t * nth_delta_ptr(slot_index_t n) {
    return reinterpret_cast<delta_t*>(numdeltas_ptr()+1) + n;
  }
  inline delta_t * last_delta_ptr() {
    return nth_delta_ptr(*numdeltas_ptr()-1);
  }
  inline delta_t * next_delta_ptr() {
    return nth_delta_ptr(*numdeltas_ptr());
  }
  void * mem_;
};

} // namespace rose
#endif  // _ROSE_COMPRESSION_FOR_H__
