#ifndef _ROSE_COMPRESSION_RLE_IMPL_H__
#define _ROSE_COMPRESSION_RLE_IMPL_H__

// Copyright 2007 Google Inc. All Rights Reserved.
// Author: sears@google.com (Rusty Sears)

#include <assert.h>

#include "rle.h"


namespace rose {
/**
   Store a new value in run length encoding.  If this value matches
   the previous one, increment a counter.  Otherwise, create a new
   triple_t to hold the new value and its count.  Most of the
   complexity comes from dealing with integer overflow, and running
   out of space.
*/
template <class TYPE>
inline slot_index_t
Rle<TYPE>::append(int xid, const TYPE dat,
		  byte_off_t* except, byte * exceptions, //char *exceptional,
		  int *free_bytes) {
  int64_t ret;

  ret = last_block_ptr()->index + last_block_ptr()->copies;

  if (dat != last_block_ptr()->data  ||
      last_block_ptr()->copies == MAX_COPY_COUNT) {
    // this key is not the same as the last one, or
    // the block is full

    *free_bytes -= sizeof(triple_t);

    // Write the changes in our overrun space
    triple_t *n = new_block_ptr();
    n->index = ret;
    n->copies = 1;
    n->data = dat;

    // Finalize the changes unless we're out of space
    (*block_count_ptr()) += (*free_bytes >= 0);

  } else if(ret == MAX_INDEX) {
    // out of address space
    *free_bytes = -1;
    ret = NOSPACE;
  } else {
    // success; bump number of copies of this item, and return.
    last_block_ptr()->copies++;
  }

  return (slot_index_t)ret;
}
template <class TYPE>
inline TYPE *
Rle<TYPE>::recordRead(int xid, slot_index_t slot, byte* exceptions,
                      TYPE * scratch) {
  block_index_t n =  nth_block_ptr(last_)->index <= slot ? last_ : 0;
  // while (n < *block_count_ptr()) {
  do {
    triple_t * t = nth_block_ptr(n);
    if (t->index <= slot && t->index + t->copies > slot) {
      *scratch = t->data;
      last_ = n;
      return scratch;
    }
    n++;
  } while (n < *block_count_ptr());
  return 0;
}

} // namespace rose

#endif  // _ROSE_COMPRESSION_RLE_IMPL_H__
