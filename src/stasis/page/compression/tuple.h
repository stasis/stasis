#ifndef _ROSE_COMPRESSION_TUPLE_H__
#define _ROSE_COMPRESSION_TUPLE_H__

// Copyright 2007 Google Inc. All Rights Reserved.
// Author: sears@google.com (Rusty Sears)

/**
  @file Implementation of tuples (Tuple) and dispatch routines for
  column wide compression (PluginDispatcher).
*/

#include <limits.h>
#include <ctype.h>

#include "compression.h"
#include "pstar-impl.h"
#include "multicolumn.h"
namespace rose {

template <class TUPLE> class Multicolumn;
template<class TYPE> class Tuple;

/**
   PluginDispatcher essentially just wraps calls to compressors in
   switch statements.

   It has a number of deficiencies:

     1) Performance.  The switch statement is the main CPU bottleneck
        for both of the current compression schemes.

     2) PluginDispatcher has to "know" about all compression
        algorithms and all data types that it may encounter.

   This approach has one advantage; it doesn't preclude other
   (templatized) implementations that hardcode schema formats a
   compile time.

   Performance could be partially addressed by using a blocking append
   algorithm:

     A Queue up multiple append requests (or precompute read requests)
       when appropriate.

     B Before appending, calculate a lower (pessimistic) bound on the
       number of inserted tuples that can fit in the page:

         n = (free bytes) / (maximum space per tuple)

     C Compress n tuples from each column at a time.  Only evaluate the
       switch statement once for each column.

     D Repeat steps B and C until n is below some threshold, then
       revert the current behavior.

   Batching read requests is simpler, and would be useful for
   sequential scans over the data.

*/

class PluginDispatcher{
 public:

#define dispatchSwitch(col,cases,...) \
    static const int base = USER_DEFINED_PAGE(0) + 2 * 2 * 4;\
    switch(plugin_ids_[col]-base) {                     \
      cases(0, For<uint8_t>, col,uint8_t, __VA_ARGS__); \
      cases(1, For<uint16_t>,col,uint16_t,__VA_ARGS__); \
      cases(2, For<uint32_t>,col,uint32_t,__VA_ARGS__); \
      cases(3, For<uint64_t>,col,uint64_t,__VA_ARGS__); \
      cases(4, For<int8_t>,  col,int8_t,  __VA_ARGS__); \
      cases(5, For<int16_t>, col,int16_t, __VA_ARGS__); \
      cases(6, For<int32_t>, col,int32_t, __VA_ARGS__); \
      cases(7, For<int64_t>, col,int64_t, __VA_ARGS__); \
      cases(8, Rle<uint8_t>, col,uint8_t, __VA_ARGS__); \
      cases(9, Rle<uint16_t>,col,uint16_t,__VA_ARGS__); \
      cases(10,Rle<uint32_t>,col,uint32_t,__VA_ARGS__); \
      cases(11,Rle<uint64_t>,col,uint64_t,__VA_ARGS__); \
      cases(12,Rle<int8_t>,  col,int8_t,  __VA_ARGS__); \
      cases(13,Rle<int16_t>, col,int16_t, __VA_ARGS__); \
      cases(14,Rle<int32_t>, col,int32_t, __VA_ARGS__); \
      cases(15,Rle<int64_t>, col,int64_t, __VA_ARGS__); \
      default: abort();                                 \
    };

#define caseAppend(off,plug_type,col,type,fcn,ret,xid,dat,...)      \
  case off: {                                                          \
    ret = ((plug_type*)plugins_[col])->fcn(xid,*(type*)dat,__VA_ARGS__); } break

#define caseSetPlugin(off,plug_type,col,type,m) \
  case off: { plugins_[col] = new plug_type(m); } break

#define caseDelPlugin(off,plug_type,col,type,m) \
  case off: { delete (plug_type*)plugins_[col]; } break

#define caseRead(off,plug_type,col,type,m,ret,fcn,xid,slot,except,scratch) \
      case off: { ret = ((plug_type*)plugins_[col])->fcn(xid,slot,except,(type*)scratch); } break

#define caseNoArg(off,plug_type,col,type,m,ret,fcn) \
      case off: { ret = ((plug_type*)plugins_[col])->fcn(); } break

#define caseInitMem(off,plug_type,col,type,m) \
      case off: { ((plug_type*)plugins_[col])->init_mem(m); } break

#define caseMem(off,plug_type,col,type,m) \
      case off: { ((plug_type*)plugins_[col])->mem(m); } break

#define caseCompressor(off,plug_type,col,type,nil) \
      case off: { ret = (plug_type*)plugins_[col]; } break

  inline slot_index_t recordAppend(int xid, column_number_t col,
				 const void *dat, byte_off_t* except,
				 byte *exceptions, int *free_bytes) {
    slot_index_t ret;
    dispatchSwitch(col,caseAppend,append,ret,xid,dat,except,exceptions,
		   free_bytes);
    return ret;
  }

  inline void *recordRead(int xid, byte *mem, column_number_t col,
                          slot_index_t slot, byte* exceptions, void *scratch) {
    void * ret;
    dispatchSwitch(col,caseRead,mem,ret,recordRead,xid,slot,exceptions,scratch);
    return ret;
  }

  inline byte_off_t bytes_used(column_number_t col) {
    byte_off_t ret;
    dispatchSwitch(col,caseNoArg,mem,ret,bytes_used);
    return ret;
  }

  inline void init_mem(byte * mem, column_number_t col) {
    dispatchSwitch(col,caseInitMem,mem);
  }
  inline void mem(byte * mem, column_number_t col) {
    dispatchSwitch(col,caseMem,mem);
  }

  inline void * compressor(column_number_t col) {
    void * ret;
    dispatchSwitch(col,caseCompressor,0);
    return ret;
  }
  PluginDispatcher(column_number_t column_count) :
  column_count_(column_count), plugin_ids_(new plugin_id_t[column_count]), plugins_(new void*[column_count]) {
    for(column_number_t i = 0; i < column_count; i++) {
      plugin_ids_[i] = 0;
    }
  }

  PluginDispatcher(int xid, byte *mem,column_number_t column_count, plugin_id_t * plugins) :
      column_count_(column_count), plugin_ids_(new plugin_id_t[column_count]), plugins_(new void*[column_count]) {
    for(column_number_t i = 0; i < column_count; i++) {
      plugin_ids_[i] = 0;
      set_plugin(mem,i,plugins[i]);
    }
  }

  inline void set_plugin(byte *mem,column_number_t c, plugin_id_t p) {
    if(plugin_ids_[c]) {
      dispatchSwitch(c,caseDelPlugin,0);
    }
    plugin_ids_[c] = p;
    dispatchSwitch(c,caseSetPlugin,mem);
  }

  ~PluginDispatcher() {
    for(column_number_t i = 0; i < column_count_; i++) {
      dispatchSwitch(i,caseDelPlugin,0);
    }
    delete[] plugin_ids_;
    delete[] plugins_;
  }

#undef caseAppend
#undef caseSetPlugin
#undef caseDelPlugin
#undef caseRead
#undef caseNoArg
#undef caseInitMem
#undef caseCompressor

 private:

  column_number_t column_count_;
  plugin_id_t * plugin_ids_;
  void ** plugins_;
};

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

  inline TYPE * set(column_number_t col,void* val) {
    cols_[col] = *(TYPE*)val;
    return (TYPE*)val;
  }
  inline TYPE * get(column_number_t col) const {
    return &(cols_[col]);
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
 private:
  Tuple() { abort(); }
  explicit Tuple(const Tuple& t) { abort(); }
  column_number_t count_;
  TYPE * const cols_;
  byte * byteArray_;
 };
}
#endif  // _ROSE_COMPRESSION_TUPLE_H__
