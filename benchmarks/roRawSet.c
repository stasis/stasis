#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <lladd/transactional.h>
#include <unistd.h>

int main(int argc, char** argv) {

  assert(argc == 3);

  int xact_count = atoi(argv[1]);
  int count = atoi(argv[2]);

  Tinit();
  
  int xid = Tbegin();

  Tcommit(xid);

  recordid * rids = malloc (count * sizeof(recordid));

  int i = 0;
  int k;
  for(k = 0; k < xact_count; k++) {
    xid = Tbegin();
    for(; i < (count*(k+1)) ; i++) {
      rids[i] = Talloc(xid, sizeof(int));
      Tset(xid, rids[i], &i);
    }
    Tcommit(xid);

  }
  
  xid = Tbegin();
  int j, l;
  for(j = 0; j < 1000; j++) {
    i = 0;
    for(k = 0; k < xact_count; k++) {
      for(; i < (count*(k+1)) ; i++) {
	Tread(xid, rids[i], &l);
      }
    }
  }
  Tcommit(xid);


  Tdeinit(); 
  
}
