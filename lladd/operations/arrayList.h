#include <lladd/operations.h>

#ifndef __ARRAY_LIST_H
#define __ARRAY_LIST_H

/**
   @file 

   @ingroup OPERATIONS

   $Id$
*/

recordid TarrayListAlloc(int xid, int count, int multiplier, int size);

Operation getArrayListAlloc();
Operation getInitFixed();
Operation getUnInitPage();

/** Initialized a fixed page with the maximum possible number of slots
    allocated.  The rid.size field is used to determine the size of
    record that the slots can hold. */
#define TinitFixed(xid, rid) Tupdate(xid, rid, NULL, OPERATION_INITIALIZE_FIXED_PAGE)
/** Un-initializes a page. */
#define TunInitPage(xid, rid) Tupdate(xid, rid, NULL, OPERATION_UNINITIALIZE_PAGE)

recordid dereferenceArrayListRid(Page * p, int offset);
int TarrayListExtend(int xid, recordid rid, int slots);
int TarrayListInstantExtend(int xid, recordid rid, int slots);
#endif
