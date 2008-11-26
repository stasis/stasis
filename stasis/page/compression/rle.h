#ifndef _ROSE_COMPRESSION_RLE_H__
#define _ROSE_COMPRESSION_RLE_H__

// Copyright 2007 Google Inc. All Rights Reserved.
// Author: sears@google.com (Rusty Sears)

#include <limits.h>
#include <ctype.h>
#include "pstar.h"

namespace rose {

template <class TYPE, class COUNT_TYPE=uint16_t>
class Rle {

 public:
  /** XXX this stuff is performance critical; should be template param.*/
  typedef byte_off_t block_index_t;
  typedef COUNT_TYPE copy_count_t;
  static const copy_count_t MAX_COPY_COUNT = (copy_count_t)-1;

  typedef TYPE TYP;


  struct triple_t {
    slot_index_t index;
    copy_count_t copies;
    //byte foo[100]; // <-- Uncomment to test boundaries
    TYPE data;
  };

  static const int PLUGIN_ID = 1;

  inline void offset(TYPE off) { nth_block_ptr(0)->data = off; };
  inline size_t max_overrun() { return sizeof(triple_t); }

  /** @see For::append */
  inline slot_index_t append(int xid, const TYPE dat,
			     byte_off_t* except, byte * exceptions,
			     int * free_bytes);
  /** @see For::recordRead */
  inline TYPE *recordRead(int xid, slot_index_t slot, byte *exceptions,
                          TYPE *scratch);
  /** @see For::recordFind */
  inline std::pair<slot_index_t,slot_index_t>*
    recordFind(int xid, slot_index_t start, slot_index_t stop,
	       byte *exceptions, TYPE value,
	       std::pair<slot_index_t,slot_index_t>& scratch);
  /**
    This constructor initializes a new Rle region.

    @param xid the transaction that created the new region.  Passing
           in a XID implies that the page has just been allocated.
    @param mem A pointer to the buffer manager's copy of the Rle page.
  */
 Rle(int xid, void * mem): mem_(mem), last_(0) {
    *block_count_ptr() = 1;
    triple_t * n = last_block_ptr();
    n->index = 0;
    n->copies = 0;
    n->data = 0;
    pthread_mutex_init(&last_mut_,0);
  }
  inline slot_index_t recordCount() {
    triple_t *n = last_block_ptr();
    return (n->index) + (n->copies);
  }
  /**
     This constructor is called when existing RLE data is read from
     disk.
  */
  Rle(void * mem): mem_(mem), last_(0) {
    pthread_mutex_init(&last_mut_,0);
 }

  Rle() : mem_(0), last_(0) {
    pthread_mutex_init(&last_mut_,0);
  }

  ~Rle() {
    pthread_mutex_destroy(&last_mut_);
  }
  /**
      @see For::bytes_used();
  */
  inline byte_off_t bytes_used() {
    return ((intptr_t)(last_block_ptr()+1))-(intptr_t)mem_;
  }

  inline void init_mem(void * mem) {
    mem_=mem;
    last_=0;
    *block_count_ptr() = 1;
    triple_t * n = nth_block_ptr(0);
    n->index = 0;
    n->copies = 0;
    n->data = 0;
  }

  inline void mem(void * mem) {
    mem_=mem;
    last_=0;
  }
 private:
  inline TYPE offset() { return nth_block_ptr(0)->dat; }
  inline block_index_t* block_count_ptr() {
    return reinterpret_cast<block_index_t*>(mem_);
  }
  inline triple_t* nth_block_ptr(block_index_t n) {
    return reinterpret_cast<triple_t*>(block_count_ptr()+1) + n;
  }
  inline slot_index_t nth_low_index(block_index_t n) {
    //    return (reinterpret_cast<triple_t*>(block_count_ptr()+1) + n)->index;
    return nth_block_ptr(n)->index;
  }
  inline slot_index_t nth_high_index(block_index_t n) {
    triple_t* r = nth_block_ptr(n); // reinterpret_cast<triple_t*>(block_count_ptr()+1) + n;
    return r->index + r->copies - 1;
  }
  inline slot_index_t nth_high_bound(block_index_t n) {
    triple_t* r = nth_block_ptr(n); // reinterpret_cast<triple_t*>(block_count_ptr()+1) + n;
    return r->index + r->copies;
  }
  inline TYPE* nth_data_ptr(block_index_t n) {
    //    printf("data %d: %lld\n", n, (long long) ((reinterpret_cast<triple_t*>(block_count_ptr()+1) + n)->data));
    //    return &((reinterpret_cast<triple_t*>(block_count_ptr()+1) + n)->data);
    return &(nth_block_ptr(n)->data);
  }
  inline triple_t* last_block_ptr() {
    return nth_block_ptr(*block_count_ptr()-1);
  }
  inline triple_t* new_block_ptr() {
    return nth_block_ptr(*block_count_ptr());
  }
  void * mem_;
  block_index_t last_;
  pthread_mutex_t last_mut_;
};

} // namespace rose
#endif  // _ROSE_COMPRESSION_RLE_H__
