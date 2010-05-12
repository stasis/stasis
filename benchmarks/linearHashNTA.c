#include <stasis/transactional.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>


int main(int argc, char** argv) {

  assert(argc == 3 || argc == 4);

  int xact_count = atoi(argv[1]);
  int count = atoi(argv[2]);

  int fixed_len = (argc == 4);
  
  Tinit();

  int xid = Tbegin();

  recordid hash;
  if(fixed_len) {
    hash = ThashCreate(xid, sizeof(int), sizeof(int));
  } else {
    hash = ThashCreate(xid, VARIABLE_LENGTH, VARIABLE_LENGTH);
  }

  Tcommit(xid);

  int i = 0;

  for(int k = 0; k < xact_count; k++) {

    xid = Tbegin();

    for(;i < count *(k+1) ; i++) {
      ThashInsert(xid, hash, (byte*)&i, sizeof(int), (byte*)&i, sizeof(int));
    }

    Tcommit(xid);

  }

  Tdeinit();

}
