/**
   @file 

   Allocates and deallocates records.

   @todo Talloc() should reuse space freed by Tdealloc(), but
   currently just leaks it.

   @ingroup OPERATIONS

   $Id$
*/


#ifndef __ALLOC_H
#define __ALLOC_H 1

Operation getAlloc();
Operation getDealloc();
Operation getRealloc();

/** 
    Allocate a record.  

    @param The transaction responsible for the allocation @param The
    size of the new record to be allocated.  (Talloc may allocate a
    blob if the record will not easily fit on a page.)

    @return the recordid of the new record.
*/
recordid Talloc(int xid, long size);

/** 
   Free a record.  
    @todo Currently, we just leak store space on dealloc. 
*/
void Tdealloc(int xid, recordid rid);

/**
   Return the type of a record, as returned by getRecordType.
    
    @todo document TrecordType
    @see getRecordType

*/
int TrecordType(int xid, recordid rid);

#endif


