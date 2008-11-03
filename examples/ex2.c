#include <stasis/transactional.h>
#include <stdio.h>
#include <assert.h>

int main (int argc, char ** argv) {

  Tinit();

  recordid rootEntry;

  int xid = Tbegin();

  if(TrecordType(xid, ROOT_RECORD) == INVALID_SLOT) {

    // ThashCreate() will work here as well.
    rootEntry = Talloc(xid, sizeof(int));

    assert(ROOT_RECORD.page == rootEntry.page);
    assert(ROOT_RECORD.slot == rootEntry.slot);
    // rootEntry.size will be sizeof(int) from above.
    int zero = 0;

    Tset(xid, rootEntry, &zero);

    printf("New store; root = 0\n");

  } else {

    // The store already is initialized.  If this were a real program,
    // it would use some magic to make sure that it is compatible with
    // the program that created the store...

    rootEntry = ROOT_RECORD;
    rootEntry.size = sizeof(int);  // Same as sizeof(int) above.

    int root;

    Tread(xid, rootEntry, &root);

    printf("Old store: %d -> ", root);
    root++;
    printf("%d\n", root);

    Tset(xid, rootEntry, &root);

  }

  Tcommit(xid);

  Tdeinit();
}
