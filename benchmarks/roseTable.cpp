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

    bufferManagerNonBlockingSlowHandleType = IO_HANDLE_PFILE;

    Tinit();

    int xid = Tbegin();

    recordid lsmTable = TlsmTableAlloc<PAGELAYOUT>(xid);

    Tcommit(xid);

    TlsmTableStart<PAGELAYOUT>(lsmTable);

    TlsmTableStop<PAGELAYOUT>(lsmTable);

    Tdeinit();

    printf("test\n");
  }
}

int main(int argc, char **argv) {
  typedef rose::StaticTuple<4,int64_t,int32_t,int16_t,int8_t> tup;
  // XXX multicolumn is deprecated; want static dispatch!
  return rose::main
    <rose::SingleColumnTypePageLayout
      <rose::Multicolumn<tup>,rose::Rle<val_t> > >
    (argc,argv);
}
