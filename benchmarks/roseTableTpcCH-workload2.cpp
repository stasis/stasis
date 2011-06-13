//#define LEAK_TEST

#define XID_COL 254 // XXX

#include "roseTableTpcCH.h"
#include "stasis/experimental/compression/compression.h"

int main(int argc, char **argv) {

  typedef int8_t typ0;
  typedef int8_t typ1;
  typedef int8_t typ2;
  typedef int32_t typ3;
  typedef int8_t typ4;
  typedef int32_t typ5;
  typedef int32_t typ6;
  typedef int16_t typ7;
  typedef int8_t typ8;
  typedef int8_t typ9;

  typedef rose::epoch_t typ10;
  typedef int64_t typ11;
  typedef int64_t  typ12;
  typedef int64_t  typ13;
  typedef int64_t typ14;
  typedef int64_t typ15;
  typedef int64_t  typ16;
  typedef int64_t typ17;
  typedef int64_t  typ18;
  typedef int64_t  typ19;

  #define COLS 11
  typedef rose::StaticTuple<COLS,1,typ0,typ1,typ2,typ3,typ4,typ5,typ6,typ7,typ8,typ9,typ10,typ11,typ12,typ13,typ14,typ15,typ16,typ17,typ18,typ19> tup;
  using rose::For;
  using rose::Rle;
  using rose::Nop;
  int ret;
  // multicolumn is deprecated; want static dispatch!

  rose::plugin_id_t * plugins = (rose::plugin_id_t*)malloc(20 * sizeof(rose::plugin_id_t));

  plugins[0] = rose::plugin_id<rose::Multicolumn<tup>, Rle<typ0>, typ0>();
  plugins[1] = rose::plugin_id<rose::Multicolumn<tup>, Rle<typ1>, typ1>(); // rle
  plugins[2] = rose::plugin_id<rose::Multicolumn<tup>, Rle<typ2>, typ2>();
  plugins[3] = rose::plugin_id<rose::Multicolumn<tup>, For<typ3>, typ3>();
  plugins[4] = rose::plugin_id<rose::Multicolumn<tup>, Nop<typ4>, typ4>(); // rle
  plugins[5] = rose::plugin_id<rose::Multicolumn<tup>, Nop<typ5>, typ5>();
  plugins[6] = rose::plugin_id<rose::Multicolumn<tup>, Nop<typ6>, typ6>();
  plugins[7] = rose::plugin_id<rose::Multicolumn<tup>, For<typ7>, typ7>(); // for
  plugins[8] = rose::plugin_id<rose::Multicolumn<tup>, Nop<typ8>, typ8>();
  plugins[9] = rose::plugin_id<rose::Multicolumn<tup>, Nop<typ9>, typ9>();

  // todo try Rle / For
  plugins[10] = rose::plugin_id<rose::Multicolumn<tup>, Rle<typ10>, typ10>();
  plugins[11] = rose::plugin_id<rose::Multicolumn<tup>, Rle<typ11>, typ11>(); // rle
  plugins[12] = rose::plugin_id<rose::Multicolumn<tup>, Rle<typ12>, typ12>();
  plugins[13] = rose::plugin_id<rose::Multicolumn<tup>, Rle<typ13>, typ13>();
  // todo try nop / for
  plugins[14] = rose::plugin_id<rose::Multicolumn<tup>, For<typ14>, typ14>(); // rle
  plugins[15] = rose::plugin_id<rose::Multicolumn<tup>, Rle<typ15>, typ15>();
  plugins[16] = rose::plugin_id<rose::Multicolumn<tup>, Nop<typ16>, typ16>();
  // todo try nop
  plugins[17] = rose::plugin_id<rose::Multicolumn<tup>, For<typ17>, typ17>(); // for
  plugins[18] = rose::plugin_id<rose::Multicolumn<tup>, Rle<typ18>, typ18>();
  plugins[19] = rose::plugin_id<rose::Multicolumn<tup>, Rle<typ19>, typ19>();


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
    For<typ8>,Rle<typ9>,
    Rle<typ10>,Rle<typ11>,
    Rle<typ12>,Rle<typ13>,
    For<typ14>,Rle<typ15>,
    Nop<typ16>,For<typ17>,
    Rle<typ18>,Rle<typ19>
    >
    >::initPageLayout();

  ret = rose::main
    <rose::StaticMultiColumnTypePageLayout
    <COLS,
    rose::StaticMulticolumn<COLS,tup,
    Rle<typ0>,Rle<typ1>,
    For<typ2>,Rle<typ3>,
    Rle<typ4>,Rle<typ5>,
    For<typ6>,For<typ7>,
    For<typ8>,Rle<typ9>,
    Rle<typ10>,Rle<typ11>,
    Rle<typ12>,Rle<typ13>,
    For<typ14>,Rle<typ15>,
    Nop<typ16>,For<typ17>,
    Rle<typ18>,Rle<typ19>
    >
    >
    >
    (argc,argv);
  */
  return ret;
}
