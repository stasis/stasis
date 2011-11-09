//#define LEAK_TEST

#include <stasis/transactional.h>
#include "roseTable.h"
#include "stasis/experimental/compression/compression.h"

int main(int argc, char **argv) {

  typedef int32_t typ0;
  typedef int32_t typ1;
  typedef int32_t typ2;
  typedef int64_t typ3;
  typedef int32_t typ4;
  typedef int32_t typ5;
  typedef int32_t typ6;
  typedef int32_t typ7;
  typedef int32_t typ8;
  typedef int32_t typ9;

  #define COLS 10
  typedef rose::StaticTuple<COLS,1,typ0,typ1,typ2,typ3,typ4,typ5,typ6,typ7,typ8,typ9> tup;
  using rose::For;
  using rose::Rle;
  using rose::Nop;
  int ret;
  // multicolumn is deprecated; want static dispatch!

  rose::plugin_id_t * plugins = (rose::plugin_id_t*)malloc(10 * sizeof(rose::plugin_id_t));

  plugins[0] = rose::plugin_id<rose::Multicolumn<tup>, Rle<typ0>, typ0>();
  plugins[1] = rose::plugin_id<rose::Multicolumn<tup>, Nop<typ1>, typ1>(); // rle
  plugins[2] = rose::plugin_id<rose::Multicolumn<tup>, For<typ2>, typ2>();
  plugins[3] = rose::plugin_id<rose::Multicolumn<tup>, Rle<typ3>, typ3>();
  plugins[4] = rose::plugin_id<rose::Multicolumn<tup>, Nop<typ4>, typ4>(); // rle
  plugins[5] = rose::plugin_id<rose::Multicolumn<tup>, Rle<typ5>, typ5>();
  plugins[6] = rose::plugin_id<rose::Multicolumn<tup>, For<typ6>, typ6>();
  plugins[7] = rose::plugin_id<rose::Multicolumn<tup>, Nop<typ7>, typ7>(); // for
  plugins[8] = rose::plugin_id<rose::Multicolumn<tup>, For<typ8>, typ8>();
  plugins[9] = rose::plugin_id<rose::Multicolumn<tup>, Rle<typ9>, typ9>();

  rose::DynamicMultiColumnTypePageLayout<rose::Multicolumn<tup> >::initPageLayout(plugins);

  ret = rose::main
  <rose::DynamicMultiColumnTypePageLayout<rose::Multicolumn<tup> > >(argc,argv); 

  /*  return rose::main
    <rose::MultiColumnTypePageLayout
    <COLS,
    rose::StaticMulticolumn<COLS,tup,
    For<typ0>,Rle<typ1>,
    Rle<typ2>,Rle<typ3>,
    Rle<typ4>,Rle<typ5>,
    Rle<typ6>,Rle<typ7>,
    Rle<typ8>,For<typ9> >
    >
    >
    (argc,argv);
  */

  /*  rose::StaticMultiColumnTypePageLayout
    <COLS,
    rose::StaticMulticolumn<COLS,tup,
    Rle<typ0>,Rle<typ1>,
    For<typ2>,Rle<typ3>,
    Rle<typ4>,Rle<typ5>,
    For<typ6>,For<typ7>,
    For<typ8>,Rle<typ9> >
    >::initPageLayout();

  ret = rose::main
    <rose::StaticMultiColumnTypePageLayout
    <COLS,
    rose::StaticMulticolumn<COLS,tup,
    Rle<typ0>,Rle<typ1>,
    For<typ2>,Rle<typ3>,
    Rle<typ4>,Rle<typ5>,
    For<typ6>,For<typ7>,
    For<typ8>,Rle<typ9> >
    >
    >
    (argc,argv);
  */
  return ret;
}
