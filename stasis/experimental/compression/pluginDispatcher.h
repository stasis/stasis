#ifndef _ROSE_COMPRESSION_PLUGINDISPATCHER_H__
#define _ROSE_COMPRESSION_PLUGINDISPATCHER_H__

namespace rose {

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
// Silence GCC variadic macro warnings...
#pragma GCC system_header

#define dispatchSwitch(col,cases,...) \
    static const int base = USER_DEFINED_PAGE(0) + 3 * 2 * 4;\
    /*printf("page = %d pluginid = %d base = %d\n", USER_DEFINED_PAGE(0), plugin_ids_[col], base); fflush(stdout);*/ \
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
      cases(16,Nop<uint8_t>, col,uint8_t, __VA_ARGS__); \
      cases(17,Nop<uint16_t>,col,uint16_t,__VA_ARGS__); \
      cases(18,Nop<uint32_t>,col,uint32_t,__VA_ARGS__); \
      cases(19,Nop<uint64_t>,col,uint64_t,__VA_ARGS__); \
      cases(20,Nop<int8_t>,  col,int8_t,  __VA_ARGS__); \
      cases(21,Nop<int16_t>, col,int16_t, __VA_ARGS__); \
      cases(22,Nop<int32_t>, col,int32_t, __VA_ARGS__); \
      cases(23,Nop<int64_t>, col,int64_t, __VA_ARGS__); \
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

#define caseFind(off,plug_type,col,type,ret,fcn,xid,first,last,except,key,scratch) \
 case off: { ret = ((plug_type*)plugins_[col])->fcn(xid,first,last,except,*(type*)key,scratch); } break

#define caseNoArg(off,plug_type,col,type,m,ret,fcn) \
      case off: { ret = ((plug_type*)plugins_[col])->fcn(); } break

#define caseInitMem(off,plug_type,col,type,m) \
      case off: { ((plug_type*)plugins_[col])->init_mem(m); } break

#define caseMem(off,plug_type,col,type,m) \
      case off: { ((plug_type*)plugins_[col])->mem(m); } break

#define caseCompressor(off,plug_type,col,type,nil) \
      case off: { ret = (plug_type*)plugins_[col]; } break

#define caseOffset(off,plug_type,col,type,val) \
      case off: { ((plug_type*)plugins_[col])->offset(*(type*)val); } break

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
  inline std::pair<slot_index_t,slot_index_t> *recordFind(int xid, column_number_t col,
                          slot_index_t first, slot_index_t last, byte* exceptions,
			  const void * key, std::pair<slot_index_t,slot_index_t>& pair_scratch) {
    std::pair<slot_index_t,slot_index_t> * ret;
    dispatchSwitch(col,caseFind,ret,recordFind,xid,first,last,exceptions,key,pair_scratch);
    return ret;
  }

  inline byte_off_t bytes_used(column_number_t col) {
    byte_off_t ret;
    dispatchSwitch(col,caseNoArg,mem,ret,bytes_used);
    return ret;
  }

  inline byte_off_t max_overrun(column_number_t col) {
    byte_off_t ret;
    dispatchSwitch(col,caseNoArg,mem,ret,max_overrun);
    return ret;
  }

  inline slot_index_t recordCount(column_number_t col) {
    byte_off_t ret;
    dispatchSwitch(col,caseNoArg,mem,ret,recordCount);
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
  inline void offset(column_number_t col, void * val) {
    dispatchSwitch(col,caseOffset,val);
  }
  PluginDispatcher(column_number_t column_count) :
  column_count_(column_count), plugin_ids_(new plugin_id_t[column_count]), plugins_(new void*[column_count]) {
    for(column_number_t i = 0; i < column_count; i++) {
      plugin_ids_[i] = 0;
      plugins_[i] = 0;
    }
  }

  /*  PluginDispatcher(int xid, byte *mem,column_number_t column_count, plugin_id_t * plugins) :
      column_count_(column_count), plugin_ids_(new plugin_id_t[column_count]), plugins_(new void*[column_count]) {
    for(column_number_t i = 0; i < column_count; i++) {
      plugin_ids_[i] = 0;
      set_plugin(mem,i,plugins[i]);
    }
    } */

  inline void set_plugin(byte *mem,column_number_t c, plugin_id_t p) {
    /*    if(plugin_ids_[c]) {
      dispatchSwitch(c,caseDelPlugin,0);
      } */
    plugin_ids_[c] = p;
    dispatchSwitch(c,caseSetPlugin,mem);
  }

  ~PluginDispatcher(void) {
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
#undef caseFind
#undef caseNoArg
#undef caseInitMem
#undef caseCompressor
#undef caseOffset

 private:

  column_number_t column_count_;
  plugin_id_t * plugin_ids_;
  void ** plugins_;
};

}

#endif  // _ROSE_COMPRESSION_PLUGINDISPATCHER_H__
