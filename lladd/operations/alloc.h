#include <lladd/operations.h>

#ifndef __ALLOC_H
#define __ALLOC_H

/**
   @file 

   @ingroup OPERATIONS

   $Id$
*/

Operation getAlloc();
Operation getDealloc();
Operation getRealloc();

/** 
    Allocate a record.  

    @param The transaction responsible for the allocation @param The
    size of the new record to be allocated.  (Talloc may allocate a
    blob if the record will not easily fit on a page.)
*/
recordid Talloc(int xid, long size);

/** @todo Currently, we just leak store space on dealloc. */
void Tdealloc(int xid, recordid rid);

#endif
