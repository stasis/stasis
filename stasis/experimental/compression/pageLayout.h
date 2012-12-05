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
    static inline void initPageLayout(void) {
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

      plugin_id_t * plugins = stasis_malloc(column_count, plugin_id_t);
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
    static inline int cmp_id(void) {
      return my_cmp_num;
    }
    static inline int init_id(void) {
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
    class StaticMultiColumnTypePageLayout {
  public:
    typedef FORMAT FMT;
    static inline void initPageLayout(void) {
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
      if(10 < N) f->compressor10()->offset(*t->get10());
      if(11 < N) f->compressor11()->offset(*t->get11());
      if(12 < N) f->compressor12()->offset(*t->get12());
      if(13 < N) f->compressor13()->offset(*t->get13());
      if(14 < N) f->compressor14()->offset(*t->get14());
      if(15 < N) f->compressor15()->offset(*t->get15());
      if(16 < N) f->compressor16()->offset(*t->get16());
      if(17 < N) f->compressor17()->offset(*t->get17());
      if(18 < N) f->compressor18()->offset(*t->get18());
      if(19 < N) f->compressor19()->offset(*t->get19());

      return f;
    }
    static inline int cmp_id(void) {
      return my_cmp_num;
    }
    static inline int init_id(void) {
      return my_init_num;
    }
  private:
    static int my_cmp_num;
    static int my_init_num;
  };
  template <int N, class FORMAT>
    int StaticMultiColumnTypePageLayout<N,FORMAT>::my_cmp_num = -1;
  template <int N, class FORMAT>
    int StaticMultiColumnTypePageLayout<N,FORMAT>::my_init_num = -1;

  template <class PAGELAYOUT>
    recordid TlsmTableAlloc();

  template <class FORMAT>
    class DynamicMultiColumnTypePageLayout {
  public:
    typedef FORMAT FMT;
    static inline void initPageLayout(plugin_id_t * plugins) {
      stasis_page_impl_register(FMT::impl());

      lsmTreeRegisterComparator(cmp_num, FMT::TUP::cmp);
      lsmTreeRegisterPageInitializer
	(init_num, (lsm_page_initializer_t)initPage);
      my_cmp_num = cmp_num;
      cmp_num++;
      my_init_num = init_num;
      init_num++;
      my_plugins = plugins;
    }
    static inline FORMAT * initPage(Page *p, const typename FORMAT::TUP * t) {
      const column_number_t column_count = t->column_count();

      FORMAT * f = new FORMAT(-1, p, column_count, my_plugins);

      for(column_number_t i = 0; i < column_count; i++) {
	f->dispatcher_.offset(i, t->get(i));
      }
      return f;
    }
    static inline int cmp_id(void) {
      return my_cmp_num;
    }
    static inline int init_id(void) {
      return my_init_num;
    }
  private:
    static int my_cmp_num;
    static plugin_id_t* my_plugins;
    static int my_init_num;
  };
  template <class FORMAT>
    int DynamicMultiColumnTypePageLayout<FORMAT>::my_cmp_num = -1;
  template <class FORMAT>
    plugin_id_t* DynamicMultiColumnTypePageLayout<FORMAT>::my_plugins = 0;
  template <class FORMAT>
    int DynamicMultiColumnTypePageLayout<FORMAT>::my_init_num = -1;

}
#endif  // _ROSE_COMPRESSION_PAGELAYOUT_H__
