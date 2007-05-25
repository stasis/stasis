#include <lladd/transactional.h>

int main (int argc, char ** argv) { 

  Tinit();
  
  int i = 42;
  
  int xid = Tbegin();
  recordid rid = Talloc(xid, sizeof(int)); 
  Tset(xid, rid, &i);   // the application is responsible for memory management.
  // Here, stack-allocated integers are used, although memory
  // from malloc() works as well.
  Tcommit(xid); 
  
  int j;
  
  xid = Tbegin();
  Tread(xid, rid, &j);  // j is now 42.
  Tdealloc(xid, rid); 
  Tabort(xid); 
  Tdeinit();
}
