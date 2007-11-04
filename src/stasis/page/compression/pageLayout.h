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

}
#endif  // _ROSE_COMPRESSION_PAGELAYOUT_H__
