#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <lladd/transactional.h>
#include <unistd.h>


int main(int argc, char** argv) {

  assert(argc == 2);

  int count = atoi(argv[1]);

  unlink("storefile.txt");
  unlink("logfile.txt");
  unlink("blob0_file.txt");
  unlink("blob1_file.txt");

  Tinit();
  
  int xid = Tbegin();

  recordid hash = ThashAlloc(xid, sizeof(int), sizeof(int));

  int i;
  
  for(i = 0; i < count ; i++) {
    
    ThashInsert(xid, hash, &i, sizeof(int), &i, sizeof(int));
    
  }
  
  Tcommit(xid);

  Tdeinit();

}
