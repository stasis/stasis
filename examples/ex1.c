#include <stasis/transactional.h>

int main (int argc, char ** argv) {

  Tinit();

  // First transaction

  int xid = Tbegin();
  recordid rid = Talloc(xid, sizeof(int));

  // The application is responsible for memory management.
  // Tset() will copy i; it can be freed immediately after the call is made.

  int i = 42;
  Tset(xid, rid, &i);
  Tcommit(xid);

  int j;

  xid = Tbegin();
  Tread(xid, rid, &j);  // j is now 42.
  Tdealloc(xid, rid);
  Tabort(xid);

  Tdeinit();
}
