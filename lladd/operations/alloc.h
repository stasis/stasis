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

    @param xid The transaction responsible for the allocation 
    @param size The size of the new record to be allocated.  Talloc will allocate a
    blob if the record will not easily fit on a page.

    @return the recordid of the new record.
*/
recordid Talloc(int xid, long size);

recordid TallocFromPage(int xid, long page, long size);

/** 
   Free a record.  
    @todo Currently, we just leak store space on dealloc. 
*/
void Tdealloc(int xid, recordid rid);

/**
   Obtain the type of a record, as returned by getRecordType.  

   @param xid the transaction id.  

   @param rid the record of interest.  The size field will be ignored,
   allowing this function to be used to probe for records in pages.
    
   @return UNINITIALIZED_RECORD, BLOB_RECORD, SLOTTED_RECORD, or FIXED_RECORD.

   @see getRecordType

*/
int TrecordType(int xid, recordid rid);

/**
   Obtain the length of the data stored in a record.

   @param xid the transaction id.  

   @param rid the record of interest.  The size field will be ignored,
   allowing this function to be used to probe for records in pages.

   @return -1 if the record does not exist, the size of the record otherwise.
*/
int TrecordSize(int xid, recordid rid);

/** Return the number of records stored in page pageid */
int TrecordsInPage(int xid, int pageid);

#endif
