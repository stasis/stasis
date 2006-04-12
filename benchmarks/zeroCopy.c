#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <lladd/transactional.h>
#include <unistd.h>
#include "../src/lladd/page/raw.h"
#include <string.h>

int main(int argc, char** argv) { 
  assert(argc == 3);
  
#define ZEROCOPY 0
#define LSNMODE  1

  int mode = atoi(argv[1]);

  int count = atoi(argv[2]);

  int longsPerPage = PAGE_SIZE / sizeof(long);

  if(ZEROCOPY == mode) { 
    printf("Running ZEROCOPY mode. Count = %d\n", count);
  }

  Tinit();
  int xid = Tbegin();

  long * buf = malloc(longsPerPage * sizeof(long));
  
  for(int i = 0; i < count; i++) { 
    int pageNum = TpageAlloc(xid);

    Page * p = loadPage(xid, pageNum);
    if(ZEROCOPY == mode) { 
      long * data = (long*) rawPageGetData(xid, p);
      for(int j = 0; j < longsPerPage; j++) {
	data[j] = j;
      }
      rawPageSetData(xid, 0, p);
    } else if(LSNMODE == mode) { 
      long * data = (long*) rawPageGetData(xid, p);
      memcpy(buf, data, PAGE_SIZE);
      for(int j = 0; j < longsPerPage; j++) {
	buf[j] = j;
      }
      memcpy(data, buf, PAGE_SIZE);

      rawPageSetData(xid, 0, p);
    }
    releasePage(p);

  }
  Tcommit(xid);
  Tdeinit();
}
