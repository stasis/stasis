#include <lladd/transactional.h>
#include <stdio.h>
#include <assert.h>

int main (int argc, char ** argv) { 
  
  Tinit();
  
  recordid rootEntry;
  
  int xid = Tbegin();
  if(TrecordType(xid, ROOT_RECORD) == UNINITIALIZED_RECORD) {

    // ThashAlloc() will work here as well.
    rootEntry = Talloc(xid, sizeof(int)); 
    
    assert(ROOT_RECORD.page == rootEntry.page);
    assert(ROOT_RECORD.slot == rootEntry.slot);
    // newRoot.size will be sizeof(something) from above.
    int zero = 0;


    Tset(xid, rootEntry, &zero);
    Tcommit(xid);
  
    printf("New store; root = 0\n");
  } else {
    
    // The store already is initialized.  
    
    rootEntry = ROOT_RECORD;
    rootEntry.size = sizeof(int);  // Same as sizeof(something) above.
    
    // Perform any application initialization based upon its contents...
    int root;

    Tread(xid, rootEntry, &root); 
    printf("Old store: %d -> ", root);
    root++;
    Tset(xid, rootEntry, &root);
    printf("%d\n", root);
    Tcommit(xid);
    
  }
  Tdeinit();
}
