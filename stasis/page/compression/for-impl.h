#ifndef _ROSE_COMPRESSION_FOR_IMPL_H__
#define _ROSE_COMPRESSION_FOR_IMPL_H__

#include <stasis/page/compression/binary_search.h>

// Copyright 2007 Google Inc. All Rights Reserved.
// Author: sears@google.com (Rusty Sears)

#include <assert.h>

#include "for.h"

namespace rose {
template <class TYPE,class DELTA_TYPE>
inline void
For<TYPE,DELTA_TYPE>::offset(TYPE o) {
  assert(*numdeltas_ptr() == 0);
  *base_ptr() = o;
}
/**
   Store a new value as a delta from the page's base offset, then update
   numdeltas_ptr so that we remember that we stored the value.
*/
template <class TYPE,class DELTA_TYPE>
inline slot_index_t
For<TYPE,DELTA_TYPE>::append(int xid, const TYPE dat,
		  byte_off_t* except, byte* exceptions, //char *exceptional,
		  int *free_bytes) {
  // Can dat be represented as a delta from the page's base value?
  // XXX this can overflow if dat and / or offset are 64 bit...
  int64_t delta = (int64_t)dat - (int64_t)offset();

  if(delta > DELTA_MAX || delta < 0) {

    // Store dat's location as a delta
    *next_delta_ptr() = *except - PAGE_SIZE;

    // Store dat as an exception
    *(((TYPE*)(&exceptions[*except]))-1) = dat;

    // Allocate the delta and the exception (if possible)
    (*free_bytes) -= (sizeof(TYPE) + sizeof(delta_t));
    int incr = *free_bytes >= 0;
    (*numdeltas_ptr()) += incr;
    (*except) -= (incr * sizeof(TYPE));

    /* This does the same thing as the last few lines, but with a branch.  It's
       marginally slower:

       *next_delta_ptr() = *except - PAGE_SIZE;
       *free_bytes -= sizeof(TYPE) + sizeof(delta_t);
       if(*free_bytes >= 0) {
          (*numdeltas_ptr())++;
          *except -= sizeof(TYPE);
          *(TYPE*)(&exceptions[*except]) = dat;
       } */

  } else {
    // Store the delta
    *next_delta_ptr() = (delta_t) delta;

    // Allocate space for it, if possible
    (*free_bytes) -= sizeof(delta_t);
    (*numdeltas_ptr()) += (*free_bytes >= 0);
  }

  return *numdeltas_ptr() - 1;

}

template <class TYPE,class DELTA_TYPE>
inline TYPE *
For<TYPE,DELTA_TYPE>::recordRead(int xid, slot_index_t slot, byte *exceptions,
                      TYPE * scratch) {
  if (slot >= *numdeltas_ptr()) {
    return 0;
  }
  delta_t d = *nth_delta_ptr(slot);
  if (d >= 0) {
    *scratch = d + *base_ptr();
    return scratch;
  } else {
    *scratch = *(TYPE*)(exceptions + d + PAGE_SIZE - sizeof(TYPE));
    return scratch;
  }
}

#ifndef COMPRESSION_BINARY_FIND
template <class TYPE,class DELTA_TYPE>
inline std::pair<slot_index_t,slot_index_t>*
For<TYPE,DELTA_TYPE>::recordFind(int xid, slot_index_t start, slot_index_t stop,
		      byte *exceptions, TYPE value,
		      std::pair<slot_index_t,slot_index_t>& scratch) {
  std::pair<slot_index_t,slot_index_t>* ret = 0;
  delta_t delta = value - *base_ptr();
  slot_index_t i;
  for(i = start; i < stop; i++) {
    delta_t d = *nth_delta_ptr(i);
    if(d >= 0) {
      if(d == delta) {
	scratch.first = i;
	scratch.second = stop;
	ret = &scratch;
	i++;
	break;
      }
    } else {
      if(value == *(TYPE*)(exceptions + d + PAGE_SIZE - sizeof(TYPE))) {
	scratch.first = i;
	scratch.second = stop;
	ret = &scratch;
	i++;
	break;
      }
    }
  }
  for(;i < stop; i++) {
    delta_t d = *nth_delta_ptr(i);
    if(d >= 0) {
      if(d != delta) {
	scratch.second = i;
	break;
      }
    } else {
      if(value != *(TYPE*)(exceptions +d + PAGE_SIZE - sizeof(TYPE))) {
	scratch.second = i;
	break;
      }
    }
  }
  return ret;
 }
#else // COMPRESSION_BINARY_FIND
template <class TYPE,class DELTA_TYPE>
inline std::pair<slot_index_t,slot_index_t>*
For<TYPE,DELTA_TYPE>::recordFind(int xid, slot_index_t low, slot_index_t high,
		      byte *exceptions, TYPE value,
		      std::pair<slot_index_t,slot_index_t>& scratch) {
  delta_t delta = value - *base_ptr();
  int64_t bs_ret;

  //printf("delta = %d\n", delta);
  if(delta >= 0) {

    {
      int64_t bs_low = low;
      int64_t bs_high = high;
      while(nth_delta_ptr(bs_low) < 0 && bs_low < bs_high) { bs_low++; }
      while(nth_delta_ptr(bs_high) < 0 && bs_low < bs_high) { bs_high--; }

      DEBUG("low: %d->%ld, high %d->%ld\n", low, bs_low, high, bs_high);

      delta_t bs_value = delta;
      rose_binary_search(nth_delta_ptr);
    }
    if(bs_ret == -1) { printf("not found by for\n"); return 0; }

    while(scratch.first != low) {
      if(*nth_delta_ptr(scratch.first-1) == delta) {
	scratch.first --;
      } else {
	break;
      }
    }
    while(scratch.second != high) {
      if(*nth_delta_ptr(scratch.second+1) == delta) {
	scratch.second++;
      } else {
	break;
      }
    }
    DEBUG("front: %ld->%d, back: %ld\n",bs_ret, scratch.first, scratch.second);
    return &scratch;
  } else { // @todo Optimize lookup of exceptional data.  (It can be binary searched too...)
    std::pair<slot_index_t,slot_index_t>* ret = 0;
    slot_index_t i;
    for(i = low; i < high; i++) {
      delta_t d = *nth_delta_ptr(i);
      if(d >= 0) {
	if(d == delta) {
	  scratch.first = i;
	  scratch.second = high;
	  ret = &scratch;
	  i++;
	  break;
	}
      } else {
	if(value == *(TYPE*)(exceptions + d + PAGE_SIZE - sizeof(TYPE))) {
	  scratch.first = i;
	  scratch.second = high;
	  ret = &scratch;
	  i++;
	  break;
	}
      }
    }
    for(;i < high; i++) {
      delta_t d = *nth_delta_ptr(i);
      if(d >= 0) {
	if(d != delta) {
	  scratch.second = i;
	  break;
	}
      } else {
	if(value != *(TYPE*)(exceptions +d + PAGE_SIZE - sizeof(TYPE))) {
	  scratch.second = i;
	  break;
	}
      }
    }
    return ret;
  }
 }
#endif // COMPRESSION_BINARY_FIND
} // namespace rose
#endif  // _ROSE_COMPRESSION_FOR_IMPL_H__
