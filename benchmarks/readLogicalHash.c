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
    
    TlogicalHashInsert(xid, hash, &i, sizeof(int), &i, sizeof(int));
    
  }
  
  Tcommit(xid);

  xid = Tbegin();


  ThashOpen(xid, hash, sizeof(int), sizeof(int));
  int k;
  for(k = 0; k < 10; k++) {

    for(i = 0; i < count ; i++) {
      int j;
      int exists = TlogicalHashLookup(xid, hash, &i, sizeof(int), &j, sizeof(int));
      assert(exists);
      assert(i == j);
      
    }

  }
  Tcommit(xid);

  Tdeinit();

}
