#ifndef _ROSE_COMPRESSION_RLE_IMPL_H__
#define _ROSE_COMPRESSION_RLE_IMPL_H__

// Copyright 2007 Google Inc. All Rights Reserved.
// Author: sears@google.com (Rusty Sears)

#include <assert.h>
#include <algorithm>

#include "rle.h"


namespace rose {
/**
   Store a new value in run length encoding.  If this value matches
   the previous one, increment a counter.  Otherwise, create a new
   triple_t to hold the new value and its count.  Most of the
   complexity comes from dealing with integer overflow, and running
   out of space.
*/
template <class TYPE, class COUNT_TYPE>
inline slot_index_t
Rle<TYPE,COUNT_TYPE>::append(int xid, const TYPE dat,
		  byte_off_t* except, byte * exceptions, //char *exceptional,
		  int *free_bytes) {
  int64_t ret;

  DEBUG("\trle got %lld\n", (long long)dat);

  ret = last_block_ptr()->index + last_block_ptr()->copies;

  if(ret == MAX_INDEX) {
    // out of address space
    *free_bytes = -1;
    ret = NOSPACE;
  } else if (dat != last_block_ptr()->data  ||
      last_block_ptr()->copies == MAX_COPY_COUNT) {
    // this key is not the same as the last one, or
    // the block is full

    (*free_bytes) -= sizeof(triple_t);

    // Write the changes in our overrun space
    triple_t *n = new_block_ptr();
    n->index = ret;
    n->copies = 1;
    n->data = dat;

    // Finalize the changes unless we're out of space
    (*block_count_ptr()) += (*free_bytes >= 0);
  } else {
    // success; bump number of copies of this item, and return.
    last_block_ptr()->copies++;
  }

  return (slot_index_t)ret;
}
template <class TYPE,class COUNT_TYPE>
inline TYPE *
Rle<TYPE,COUNT_TYPE>::recordRead(int xid, slot_index_t slot, byte* exceptions,
                      TYPE * scratch) {
  pthread_mutex_lock(&last_mut_);
  block_index_t n =  nth_block_ptr(last_)->index <= slot ? last_ : 0;
  do {
    triple_t * t = nth_block_ptr(n);
    if (t->index <= slot && t->index + t->copies > slot) {
      *scratch = t->data;
      last_ = n;
      pthread_mutex_unlock(&last_mut_);
      return scratch;
    }
    n++;
  } while (n < *block_count_ptr());
  pthread_mutex_unlock(&last_mut_);
  return 0;
}
#ifndef COMPRESSION_BINARY_FIND
template <class TYPE>
inline std::pair<slot_index_t,slot_index_t>*
Rle<TYPE>::recordFind(int xid, slot_index_t start, slot_index_t stop,
		      byte *exceptions, TYPE value,
		      std::pair<slot_index_t,slot_index_t>& scratch) {
  block_index_t n = 0;
  std::pair<slot_index_t,slot_index_t>* ret = 0;
  do {
    triple_t * t = nth_block_ptr(n);
    if(t->data  >= value) {
      if(t->data == value) {
	scratch.first = t->index;
	do {
	  scratch.second = t->index + t->copies;
	  n++;
	  t = nth_block_ptr(n);
	} while(n < *block_count_ptr() && t->data == value);
	ret = &scratch;
      }
      break;
    }
    n++;
  } while (n < *block_count_ptr());
  if(scratch.first >= stop) {
    return 0;
  } else if(scratch.second > stop) {
    scratch.second=stop;
  }
  return ret;
}
#else // COMPRESSION_BINARY_FIND
template <class TYPE,class COUNT_TYPE>
inline std::pair<slot_index_t,slot_index_t>*
Rle<TYPE, COUNT_TYPE>::recordFind(int xid, slot_index_t start, slot_index_t stop,
		      byte *exceptions, TYPE value,
		      std::pair<slot_index_t,slot_index_t>& scratch) {

  DEBUG("\n\ncalled with start = %lld stop = %lld, val = %lld\n", (long long)start, (long long)stop, (long long)value);
  DEBUG("1th data: %lld\n", (long long)nth_block_ptr(1)->data);


  block_index_t low = 0;
  block_index_t high = *block_count_ptr();
  int64_t bs_ret;

  int64_t bs_low = low;
  int64_t bs_high = high;

  DEBUG("low: %d->%ld, high %d->%ld\n", low, bs_low, high, bs_high);

  TYPE bs_value = start;
  rose_binary_search_greatest_lower(nth_high_index);
  if(bs_ret != -1) {
    assert(nth_low_index(bs_ret) <= start);
    assert(nth_high_bound(bs_ret) > start);
    //    if(bs_ret < (*block_count_ptr()-1)) {
    //      assert(nth_low_index(bs_ret+1) > start);
    //    }
    low = bs_ret;
  } // else start not found because page starts after it (leave at block zero)

  DEBUG("real low is %d\n", low);
  bs_low  = low;
  bs_high = (*block_count_ptr());
  bs_value = stop;

  DEBUG("bs_low = %lld bs_high = %lld bs_val = %lld\n", (long long)bs_low, (long long)bs_high, (long long)bs_value);
  rose_binary_search_greatest_lower(nth_high_bound);

  if(bs_ret == -1) {
    high = (*block_count_ptr()); //-1;
  } else {

    DEBUG("bs_ret = %lld\n", (long long)bs_ret);
    // bs_high might not contain the index we're looking for
    //    high = bs_ret + XXX; /// where XXX is 1 sometimes, and zero others.
    //    if(nth_high_index(bs_ret+1) >= stop) { high = bs_ret+1; } else { high = bs_ret + 2; }

    high = bs_ret;
    // high contains the top of the range
    assert(nth_high_bound(high) >= stop);
    assert(nth_low_index(high) < stop);
    // set high to the first block that we don't need to consider.
    high++;
    //    if(high > 0) {
    //      assert(nth_low_index(high-1) <= stop);
    //    }

  }

  DEBUG("real high is %lld (last block slots: %lld - %lld) val = %lld\n", (long long int)high, (long long int)nth_low_index(high-1), (long long int)nth_high_index(high-1), (long long int)bs_value);

  // now low is the smallest block we need consider; high the highest.

  bs_low = low;
  bs_high = high;
  bs_value = value;

  DEBUG("B low: %lld->%lld, high %lld->%lld val = %lld low_idx = %lld low_end = %lld low val = %lld high_idx = %lld high_end = %lld high_val = %lld\n", (long long int)low, (long long int)bs_low, (long long int)high,(long long int) bs_high,(long long int) value,(long long int) nth_low_index(bs_low),(long long int) nth_high_index(bs_low), (long long int)*nth_data_ptr(bs_low),(long long int) nth_low_index(bs_high),(long long int) nth_high_index(bs_high), (long long int)*nth_data_ptr(bs_high));
  DEBUG("bs val =%lld bs low = %lld bs high = %lld\n", (long long) bs_value, (long long) bs_low, (long long) bs_high);

  rose_binary_search(nth_data_ptr);

  DEBUG("bs ret = %lld\n", (long long int)bs_ret);


  if(bs_ret == -1) {
    printf("[ %lld %lld %lld ]\n", (long long int)*(TYPE*)nth_data_ptr(0), (long long int)*(TYPE*)nth_data_ptr(1), (long long int)*(TYPE*)nth_data_ptr(2));
    printf("not found by rle: start = %d stop = %d val = %lld count = %d zero copies %d low = %lld high = %lld\n", start, stop, (long long)value, *block_count_ptr(), nth_block_ptr(0)->copies, (long long)low, (long long)high);
    abort();
    return 0;
  } else {
    block_index_t firstBlock = bs_ret;
    block_index_t lastBlock = bs_ret;  // lastBlock is the offset of the last block in range
    while(firstBlock > low && *nth_data_ptr(firstBlock-1) == value) { firstBlock--; }
    // firstblock is >= 0 and it contains items of the desired value

    while(lastBlock < high && *nth_data_ptr(lastBlock) == value) { lastBlock++; }
    // lastblock == block_count_ptr or it contains items of the wrong value
    lastBlock--;
    // lastblock valid, and it contains items of the correct value

    DEBUG("looking at blocks %d - %d\n", firstBlock, lastBlock);

    scratch.first = nth_block_ptr(firstBlock)->index;
    // set second to one past last valid index.
    scratch.second = nth_block_ptr(lastBlock)->index + nth_block_ptr(lastBlock)->copies;
    if(scratch.first < start) { scratch.first = start; }
    if(scratch.second > stop) { scratch.second = stop; }

    DEBUG("startstop = %d,%d scratch = %d,%d\n",start, stop, scratch.first, scratch.second);

    return &scratch;
  }
 }
#endif // COMPRESSION_BINARY_FIND
} // namespace rose
#endif  // _ROSE_COMPRESSION_RLE_IMPL_H__
