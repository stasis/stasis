#include <stdio.h>
#include <errno.h>
#include "stasis/operations/lsmTable.h"

#include "stasis/transactional.h"

#include "stasis/page/compression/multicolumn-impl.h"
#include "stasis/page/compression/for-impl.h"
#include "stasis/page/compression/rle-impl.h"
#include "stasis/page/compression/staticTuple.h"
#include "stasis/page/compression/pageLayout.h"

typedef int32_t val_t; // XXX want multiple types!

namespace rose {
  template<class PAGELAYOUT>
  int main(int argc, char **argv) {
    static int cmp_num = 1;
    static int init_num = 1;

    unlink("storefile.txt");
    unlink("logfile.txt");

    sync();
    stasis_page_impl_register(Multicolumn<typename PAGELAYOUT::FMT::TUP >::impl());
    bufferManagerNonBlockingSlowHandleType = IO_HANDLE_PFILE;

    Tinit();

    int xid = Tbegin();

    recordid lsmTable = TlsmTableAlloc<PAGELAYOUT>(xid);

    Tcommit(xid);

    lsmTableHandle<PAGELAYOUT>* h = TlsmTableStart<PAGELAYOUT>(lsmTable);

    typename PAGELAYOUT::FMT::TUP t;


  static const long INSERTS = 10000000;
    //    static const long INSERTS = 10000000;
//  static const long INSERTS = 100000;
    static const long COUNT = INSERTS / 100;
    long int count = COUNT;

    struct timeval start_tv, now_tv;
    double start, now, last_start;

    gettimeofday(&start_tv,0);
    start = rose::tv_to_double(start_tv);
    last_start = start;

    printf("tuple 'size'%d\n", PAGELAYOUT::FMT::TUP::sizeofBytes());

    for(long int i = 0; i < INSERTS; i++) {
      t.set0(&i);
      t.set1(&i);
      t.set2(&i);
      t.set3(&i);
      TlsmTableInsert(h,t);
      count --;
      if(!count) {
	count = COUNT;
	gettimeofday(&now_tv,0);
	now = tv_to_double(now_tv);
	printf("%3d%% complete "
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
  }
}

int main(int argc, char **argv) {
  //  typedef rose::StaticTuple<4,int64_t,int32_t,int16_t,int8_t> tup;
  typedef rose::StaticTuple<4,int64_t,int64_t,int64_t,int64_t> tup;
  // XXX multicolumn is deprecated; want static dispatch!
  return rose::main
    <rose::SingleColumnTypePageLayout
      <rose::Multicolumn<tup>,rose::For<int64_t> > >
    (argc,argv);
  return 0;
}
