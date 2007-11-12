#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include "stasis/operations/lsmTable.h"

#include "stasis/transactional.h"

#include "stasis/page/compression/multicolumn-impl.h"
#include "stasis/page/compression/staticMulticolumn.h"
#include "stasis/page/compression/for-impl.h"
#include "stasis/page/compression/rle-impl.h"
#include "stasis/page/compression/staticTuple.h"
#include "stasis/page/compression/pageLayout.h"

namespace rose {
  template<class PAGELAYOUT> 
  void getTuple(long int i, typename PAGELAYOUT::FMT::TUP & t) {
    typename PAGELAYOUT::FMT::TUP::TYP0 m = i;
    typename PAGELAYOUT::FMT::TUP::TYP1 j = i / 65536;
    typename PAGELAYOUT::FMT::TUP::TYP2 k = i / 12514500;
    typename PAGELAYOUT::FMT::TUP::TYP3 l = i / 10000000;

    t.set0(&m);
    t.set1(&j);
    t.set2(&k);
    t.set3(&l);
    t.set4(&j);
    t.set5(&k);
    t.set6(&l);
    t.set7(&j);
    t.set8(&k);
    t.set9(&l);
  }

  template<class PAGELAYOUT>
  int main(int argc, char **argv) {
    unlink("storefile.txt");
    unlink("logfile.txt");

    sync();

    PAGELAYOUT::initPageLayout();

    bufferManagerNonBlockingSlowHandleType = IO_HANDLE_PFILE;

    Tinit();

    int xid = Tbegin();

    recordid lsmTable = TlsmTableAlloc<PAGELAYOUT>(xid);

    Tcommit(xid);

    lsmTableHandle<PAGELAYOUT>* h = TlsmTableStart<PAGELAYOUT>(lsmTable);

    typename PAGELAYOUT::FMT::TUP t;
    typename PAGELAYOUT::FMT::TUP s;

    long INSERTS; 
    if(argc == 2) {
      INSERTS = atoll(argv[1]);
    } else {
      INSERTS = 10 * 1000 * 1000;
    }
    static const long COUNT = INSERTS / 100;
    long int count = COUNT;

    struct timeval start_tv, now_tv;
    double start, now, last_start;

    gettimeofday(&start_tv,0);
    start = rose::tv_to_double(start_tv);
    last_start = start;

    printf("tuple 'size'%d ; %ld\n", PAGELAYOUT::FMT::TUP::sizeofBytes(), sizeof(typename PAGELAYOUT::FMT::TUP));

    for(long int i = 0; i < INSERTS; i++) {
      getTuple<PAGELAYOUT>(i,t);
      TlsmTableInsert(h,t);
      //      getTuple<PAGELAYOUT>(i,t);
      //      assert(TlsmTableFind(xid,h,t,s));
      count --;
      if(!count) {
	count = COUNT;
	gettimeofday(&now_tv,0);
	now = tv_to_double(now_tv);
	printf("%3ld%% write "
	       "%9.3f Mtup/sec (avg) %9.3f Mtup/sec (cur) "
	       "%9.3f Mbyte/sec (avg) %9.3f Mbyte/sec (cur)\n",
	       ((i+1) * 100) / INSERTS,
	       ((double)i/1000000.0)/(now-start),
	       ((double)count/1000000.0)/(now-last_start),
	       (((double)PAGELAYOUT::FMT::TUP::sizeofBytes())*(double)i/1000000.0)/(now-start),
	       (((double)PAGELAYOUT::FMT::TUP::sizeofBytes())*(double)count/1000000.0)/(now-last_start)
	       );
	last_start = now;
      }
    }
    printf("insertions done.\n"); fflush(stdout);
    count = COUNT;

    gettimeofday(&start_tv,0);
    start = rose::tv_to_double(start_tv);
    last_start = start;

    for(long int i = 0; i < INSERTS; i++) {

      getTuple<PAGELAYOUT>(i,t);

      typename PAGELAYOUT::FMT::TUP const * const sp = TlsmTableFind(xid,h,t,s);
      assert(sp);
      assert(*sp == s);
      count--;
      if(!count) {
	count = COUNT;
	gettimeofday(&now_tv,0);
	now = tv_to_double(now_tv);
	printf("%3ld%% read "
	       "%9.3f Mtup/sec (avg) %9.3f Mtup/sec (cur) "
	       "%9.3f Mbyte/sec (avg) %9.3f Mbyte/sec (cur)\n",
	       ((i+1) * 100) / INSERTS,
	       ((double)i/1000000.0)/(now-start),
	       ((double)count/1000000.0)/(now-last_start),
	       (((double)PAGELAYOUT::FMT::TUP::sizeofBytes())*(double)i/1000000.0)/(now-start),
	       (((double)PAGELAYOUT::FMT::TUP::sizeofBytes())*(double)count/1000000.0)/(now-last_start)
	       );
	last_start = now;
      }

    }

    TlsmTableStop<PAGELAYOUT>(h);

    Tdeinit();

    printf("test\n");
    return 0;
  }
}

int main(int argc, char **argv) {

  typedef int64_t typ0;
  typedef int64_t typ1;
  typedef int64_t typ2;
  typedef int64_t typ3;
  typedef int64_t typ4;
  typedef int64_t typ5;
  typedef int64_t typ6;
  typedef int64_t typ7;
  typedef int64_t typ8;
  typedef int64_t typ9;

  #define COLS 10

  typedef rose::StaticTuple<COLS,typ0,typ1,typ2,typ3,typ4,typ5,typ6,typ7,typ8,typ9> tup;
  using rose::For;
  using rose::Rle;

  // multicolumn is deprecated; want static dispatch!

  /*  return rose::main
    <rose::SingleColumnTypePageLayout
      <rose::Multicolumn<tup>,rose::For<int64_t> > >
      (argc,argv); */

  return rose::main
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

  return 0;
}
