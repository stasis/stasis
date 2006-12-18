#include <stdio.h>
#include <assert.h>
#include <lladd/transactional.h>

/*
  Basic first program

  Transactionally stores one integer, which is the version number of the store.
  New store gets version 1.  Each rerun increments the version number.
  If you delete logfile.txt and storefile.txt, it recreates them at version 1.
*/

int main(const int argc, const char **argv) {
  int version = 1;

  Tinit();
  printf("Initialized\n");

  // create/load the store
  recordid rootEntry;

  int xid = Tbegin();
  if (TrecordType(xid, ROOT_RECORD) == UNINITIALIZED_RECORD) {
    // new store, must create empty page
    printf("Creating new store\n");
    rootEntry = Talloc(xid, sizeof(int));
    
    assert(ROOT_RECORD.page == rootEntry.page);
    assert(ROOT_RECORD.slot == rootEntry.slot);

    Tset(xid, rootEntry, &version);
    Tcommit(xid);
  } else {
    // store is valid
    printf("Reusing existing store\n");
    rootEntry = ROOT_RECORD;
    rootEntry.size = sizeof(int);

    Tread(xid, rootEntry, &version);
    version++;
    printf("read valid\n");
    Tset(xid, rootEntry, &version);
    Tcommit(xid);
  }
  printf("Version %d\n", version);

  // wrap up

  Tdeinit();
  return 0;
}
