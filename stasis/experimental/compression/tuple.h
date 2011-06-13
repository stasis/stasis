#ifndef _ROSE_COMPRESSION_TUPLE_H__
#define _ROSE_COMPRESSION_TUPLE_H__

// Copyright 2007 Google Inc. All Rights Reserved.
// Author: sears@google.com (Rusty Sears)

/**
  @file

  Implementation of tuples (Tuple) and dispatch routines for
  column wide compression (PluginDispatcher).
*/

#include <limits.h>
#include <ctype.h>

#include "compression.h"
#include "for.h"
#include "rle.h"
#include "nop.h"
#include "string.h"

namespace rose {

template<class TYPE>
class Tuple {
 public:
  explicit Tuple(column_number_t count) : count_(count),
    cols_(new TYPE[count]), 
    byteArray_(new byte[sizeof(count_)+count_*sizeof(TYPE)]) {}

  /*explicit Tuple(byte* b) : count_(*(column_number_t*)b),
    cols_(new TYPE[count_]),
    byteArray_(new byte[sizeof(count_)+count_*sizeof(TYPE)]) {
       memcpy(cols_,b+sizeof(column_number_t), sizeof(TYPE)*count_);
       } */
 explicit Tuple(Tuple& t) : count_(t.count_), cols_(new TYPE[count_]),
    byteArray_(new byte[sizeof(count_)+count_*sizeof(TYPE)]) {
    for(column_number_t c = 0; c < count_; c++) {
      cols_[c] = t.cols_[c];
    }
  }
  Tuple(TYPE t) : count_(0), cols_(new TYPE[1]),
    byteArray_(new byte[sizeof(count_)+sizeof(TYPE)]) {
    cols_[0] = t;
  }
  /* Tuple(Tuple *t) : count_(t->count_),cols_(new TYPE[count_]) {
    for(column_number_t c = 0; c < count_; c++) {
       cols_[c] = t->cols_[c];
     }
     } */
  inline ~Tuple() { delete[] cols_; delete[] byteArray_; }
  inline bool tombstone() {
    return false;
  }
  inline TYPE * set(column_number_t col,void* val) {
    cols_[col] = *(TYPE*)val;
    return (TYPE*)val;
  }
  inline TYPE * get(column_number_t col) const {
    return &(cols_[col]);
  }
  inline void copyFrom(Tuple<TYPE> t) {
    for(int i = 0; i < count_; i++) {
      set(i,t.get(i));
    }
  }
  inline column_number_t column_count() const {
    return count_;
  }
  inline byte_off_t column_len(column_number_t col) const {
    return sizeof(TYPE);
  }
  /*  inline void fromByteArray(byte * b) {
    assert(count_ == *(column_number_t*)b);
    //    memcpy(cols_,b+sizeof(column_number_t),sizeof(TYPE)*count_);
    TYPE *newCols = (int*)(b + sizeof(column_number_t));
    for(column_number_t i = 0; i < count_; i++) {
      cols_[i] = newCols[i];
    }
    } */
  inline byte* toByteArray() {
    byte* ret = byteArray_;
    memcpy(ret, &count_, sizeof(count_));
    memcpy(ret+sizeof(count_), cols_, count_ * sizeof(TYPE));
    return ret;
  }
  /*  inline operator const byte * () {
    return toByteArray();
    } */
  inline operator TYPE () { 
    return cols_[0]; //*get(0);
  }
  /*  inline operator TYPE () {
    assert(count_ == 0);
    return cols_[0];
    } */
  static inline size_t sizeofBytes(column_number_t cols) {
    return sizeof(column_number_t) + cols * sizeof(TYPE);
  }
  static const int TUPLE_ID = 0;

  /*  inline bool operator==(Tuple *t) { 
    return *this == *t;
    } */
   inline bool operator==(Tuple &t) {
     //if(t.count_ != count_) return 0;
    for(column_number_t i = 0; i < count_; i++) {
      if(cols_[i] != t.cols_[i]) { return 0;}
    }
    return 1;
   }
   inline bool operator<(Tuple &t) {
     //if(t.count_ != count_) return 0;
    for(column_number_t i = 0; i < count_; i++) {
      if(cols_[i] < t.cols_[i]) { return 1;}
    }
    return 0;
   }

  /*  inline bool operator==(TYPE val) { 
    assert(count_ == 1);
    return cols_[0] == val;
    }*/
  class iterator {
   public:
    inline iterator(column_number_t c, TYPE const *const *const dataset, int offset) :
        c_(c),
        dat_(dataset),
        off_(offset),
        scratch_(c_) {}
    inline explicit iterator(const iterator &i) : c_(i.c_), dat_(i.dat_), off_(i.off_),
	scratch_(c_) {}

    inline Tuple<TYPE>& operator*() {
      for(column_number_t i = 0; i < c_; i++) {
        scratch_.set(i,(void*)&dat_[i][off_]);
      }
      return scratch_;
    }
    inline bool operator==(const iterator &a) const {
      //assert(dat_==a.dat_ && c_==a.c_);
      return (off_==a.off_);
    }
    inline bool operator!=(const iterator &a) const {
      //assert(dat_==a.dat_ && c_==a.c_);
      return (off_!=a.off_);
    }
    inline void operator++() { off_++; }
    inline void operator--() { off_--; }
    inline void operator+=(int i) { abort(); }
    inline int  operator-(iterator&i) {
      return off_ - i.off_;
    }
    inline void operator=(iterator &i) {
      assert(c_==i.c_);
      assert(dat_==i.dat_);
      off_=i.off_;
    }
    inline void offset(int off) {
      off_=off;
    }
   private:
    column_number_t c_;
    TYPE const * const * dat_;
    int off_;
    Tuple<TYPE> scratch_;
  };
  static const uint32_t TIMESTAMP = 0;
 private:
  Tuple() { abort(); }
  explicit Tuple(const Tuple& t) { abort(); }
  column_number_t count_;
  TYPE * const cols_;
  byte * byteArray_;
 };
}
#endif  // _ROSE_COMPRESSION_TUPLE_H__
