#include <stasis/transactional.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char** argv) {

  assert(argc == 3);

  int xact_count = atoi(argv[1]);
  int count = atoi(argv[2]);

  /*  unlink("storefile.txt");
  unlink("logfile.txt");
  unlink("blob0_file.txt");
  unlink("blob1_file.txt");*/

  Tinit();
  
  int xid = Tbegin();

  Tcommit(xid);

  recordid arrayList = TarrayListAlloc(xid, 512, 2, sizeof(int));
  recordid rid = arrayList;
  rid.slot = 0;
  int i = 0;
  int k;
  for(k = 0; k < xact_count; k++) {
    xid = Tbegin();
    for(; i < (count*(k+1)) ; i++) {
      TarrayListExtend(xid, arrayList, 1);
      Tset(xid, rid, &i);
      rid.slot++;
    }
    Tcommit(xid);

  }
  

  Tdeinit(); 
  
}
