#ifndef _ROSE_COMPRESSION_NOP_H__
#define _ROSE_COMPRESSION_NOP_H__

#undef try
#undef catch
#undef end

#include <algorithm>

/**
   @file A 'no-op' compression implementation

   This file implements a COMPRESSOR plugin that stores data in
   uncompressed form.
*/

#include <limits.h>

#include "pstar.h"

namespace rose {

  template <class TYPE> class Nop {
  public:
    typedef TYPE TYP;

    static const int PLUGIN_ID = 2;
    inline void offset(TYPE o) {}
    inline size_t max_overrun() { return 0; }
    inline slot_index_t append(int xid, const TYPE dat, byte_off_t * except,
			       byte * exceptions, int *free_bytes) {
      if(*free_bytes >= (int)sizeof(TYPE)) {
	slot_index_t ret = *numentries_ptr();
	((TYPE*)(numentries_ptr()+1))[ret] = dat;
	(*free_bytes)-=sizeof(TYPE);
	(*numentries_ptr())++;
	return ret;
      } else {
	return NOSPACE;
      }
    }
    inline TYPE *recordRead(int xid, slot_index_t slot, byte *exceptions,
			    TYPE *buf) {
      if(slot < *numentries_ptr()) {
	*buf = ((TYPE*)(numentries_ptr()+1))[slot];
	return buf;
      } else {
	return 0;
      }
    }
    inline std::pair<slot_index_t,slot_index_t>*
      recordFind(int xid, slot_index_t start, slot_index_t stop,
		 byte *exceptions, TYPE value,
		 std::pair<slot_index_t,slot_index_t>& scratch) {
      slot_index_t i;
      std::pair<slot_index_t,slot_index_t>*ret = 0;
      TYPE *rec = 0;
      for(i = start; i< stop; i++) {
	TYPE t;
	rec = recordRead(xid,i,0,&t);
	if(*rec == value) {
	  scratch.first = i;
	  scratch.second = stop;
	  ret = &scratch;
	  i++;
	  break;
	}
      }
      for(;i<stop; i++) {
	TYPE t;
	TYPE *rec = recordRead(xid,i,0,&t);
	if(*rec != value) {
	  scratch.second = i;
	  break;
	}
      }
      return ret;
    }
    Nop(int xid, void * mem): mem_(mem) {
      *numentries_ptr() = 0;
    }
    Nop(void * mem): mem_(mem) { }
    Nop() : mem_(0) {}

    inline slot_index_t recordCount() {
      return *numentries_ptr();
    }

    inline byte_off_t bytes_used() {return sizeof(slot_index_t) + ( *numentries_ptr() * sizeof(TYPE) ); }
    inline void mem(byte * mem) { mem_=mem; }
    inline void init_mem(byte* mem) {
      mem_=mem;
      *numentries_ptr() = 0;
    }
  private:
    inline slot_index_t* numentries_ptr() {
      return reinterpret_cast<slot_index_t*>(mem_);
    }
    void * mem_;
  };

}
#endif
