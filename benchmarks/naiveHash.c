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

  recordid hash = TnaiveHashCreate(xid, sizeof(int), sizeof(int));

  Tcommit(xid);

  int i = 0;
  int k;
  for(k = 0; k < xact_count; k++) {
    xid = Tbegin();
    for(; i < (count*(k+1)) ; i++) {
      TnaiveHashInsert(xid, hash, &i, sizeof(int), &i, sizeof(int));
    }
    Tcommit(xid);

  }
  

  Tdeinit();

}
