#ifndef _ROSE_COMPRESSION_PAGELAYOUT_H__
#define _ROSE_COMPRESSION_PAGELAYOUT_H__
#include "compression.h" // for plugin_id
namespace rose {
  // XXX need to be able to de-init this stuff.
  static int cmp_num = 1;
  static int init_num = 1;
  template <class FORMAT, class COMPRESSOR>
    class SingleColumnTypePageLayout {
  public:
    typedef FORMAT FMT;
    static inline void initPageLayout() {
      stasis_page_impl_register(FMT::impl());

      // XXX these should register template instantiations of worker
      // threads that are statically compiled to deal with the tree
      // we're instantiating.
      lsmTreeRegisterComparator(cmp_num, FMT::TUP::cmp);
      lsmTreeRegisterPageInitializer
	(init_num, (lsm_page_initializer_t)initPage);
      my_cmp_num = cmp_num;
      cmp_num++;
      my_init_num = init_num;
      init_num++;
    }
    static inline FORMAT * initPage(Page *p, const typename FORMAT::TUP * t) {
      const column_number_t column_count = t->column_count();

      plugin_id_t pluginid = plugin_id<FORMAT, COMPRESSOR, typename COMPRESSOR::TYP>();

      plugin_id_t * plugins = (plugin_id_t*)malloc(column_count * sizeof(plugin_id_t));
      for(column_number_t c = 0; c < column_count; c++) {
	plugins[c] = pluginid;
      }
      FORMAT * f = new FORMAT(-1,p,column_count,plugins);
      for(column_number_t c = 0; c < column_count; c++) {
	COMPRESSOR* com = (COMPRESSOR*) f->compressor(c);
	typename COMPRESSOR::TYP val = *(typename COMPRESSOR::TYP*)(t->get(c));
	com->offset(val);
      }
      free(plugins);
      return f;
    }
    static inline int cmp_id() {
      return my_cmp_num;
    }
    static inline int init_id() {
      return my_init_num;
    }
  private:
    static int my_cmp_num;
    static int my_init_num;
  };
  template <class FORMAT, class COMPRESSOR>
    int SingleColumnTypePageLayout<FORMAT,COMPRESSOR>::my_cmp_num = -1;
  template <class FORMAT, class COMPRESSOR>
    int SingleColumnTypePageLayout<FORMAT,COMPRESSOR>::my_init_num = -1;

  template <class PAGELAYOUT>
    recordid TlsmTableAlloc();

  //// --- multicolumn static page layout

  template <int N, class FORMAT>
    class MultiColumnTypePageLayout {
  public:
    typedef FORMAT FMT;
    static inline void initPageLayout() {
      stasis_page_impl_register(FMT::impl());

      // XXX these should register template instantiations of worker
      // threads that are statically compiled to deal with the tree
      // we're instantiating.
      lsmTreeRegisterComparator(cmp_num, FMT::TUP::cmp);
      lsmTreeRegisterPageInitializer
	(init_num, (lsm_page_initializer_t)initPage);
      my_cmp_num = cmp_num;
      cmp_num++;
      my_init_num = init_num;
      init_num++;
    }
    static inline FORMAT * initPage(Page *p, const typename FORMAT::TUP * t) {

      plugin_id_t plugins[N];
      if(0 < N) plugins[0] = plugin_id<FORMAT, typename FORMAT::CMP0, typename FORMAT::CMP0::TYP>();
      if(1 < N) plugins[1] = plugin_id<FORMAT, typename FORMAT::CMP1, typename FORMAT::CMP1::TYP>();
      if(2 < N) plugins[2] = plugin_id<FORMAT, typename FORMAT::CMP2, typename FORMAT::CMP2::TYP>();
      if(3 < N) plugins[3] = plugin_id<FORMAT, typename FORMAT::CMP3, typename FORMAT::CMP3::TYP>();
      if(4 < N) plugins[4] = plugin_id<FORMAT, typename FORMAT::CMP4, typename FORMAT::CMP4::TYP>();
      if(5 < N) plugins[5] = plugin_id<FORMAT, typename FORMAT::CMP5, typename FORMAT::CMP5::TYP>();
      if(6 < N) plugins[6] = plugin_id<FORMAT, typename FORMAT::CMP6, typename FORMAT::CMP6::TYP>();
      if(7 < N) plugins[7] = plugin_id<FORMAT, typename FORMAT::CMP7, typename FORMAT::CMP7::TYP>();
      if(8 < N) plugins[8] = plugin_id<FORMAT, typename FORMAT::CMP8, typename FORMAT::CMP8::TYP>();
      if(9 < N) plugins[9] = plugin_id<FORMAT, typename FORMAT::CMP9, typename FORMAT::CMP9::TYP>();

      FORMAT * f = new FORMAT(-1,p);

      if(0 < N) f->compressor0()->offset(*t->get0());
      if(1 < N) f->compressor1()->offset(*t->get1());
      if(2 < N) f->compressor2()->offset(*t->get2());
      if(3 < N) f->compressor3()->offset(*t->get3());
      if(4 < N) f->compressor4()->offset(*t->get4());
      if(5 < N) f->compressor5()->offset(*t->get5());
      if(6 < N) f->compressor6()->offset(*t->get6());
      if(7 < N) f->compressor7()->offset(*t->get7());
      if(8 < N) f->compressor8()->offset(*t->get8());
      if(9 < N) f->compressor9()->offset(*t->get9());

      return f;
    }
    static inline int cmp_id() {
      return my_cmp_num;
    }
    static inline int init_id() {
      return my_init_num;
    }
  private:
    static int my_cmp_num;
    static int my_init_num;
  };
  template <int N, class FORMAT>
    int MultiColumnTypePageLayout<N,FORMAT>::my_cmp_num = -1;
  template <int N, class FORMAT>
    int MultiColumnTypePageLayout<N,FORMAT>::my_init_num = -1;

  template <class PAGELAYOUT>
    recordid TlsmTableAlloc();



}
#endif  // _ROSE_COMPRESSION_PAGELAYOUT_H__
