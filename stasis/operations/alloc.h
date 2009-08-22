/**
   @file

   Allocates and deallocates records.

   @ingroup OPERATIONS

   $Id$
*/


#ifndef __ALLOC_H
#define __ALLOC_H 1

#include <stasis/operations.h>
#include <stasis/allocationPolicy.h>
stasis_operation_impl stasis_op_impl_alloc();
stasis_operation_impl stasis_op_impl_dealloc();
stasis_operation_impl stasis_op_impl_realloc();

typedef struct stasis_alloc_t stasis_alloc_t;

void stasis_alloc_aborted(stasis_alloc_t* alloc, int xid);
void stasis_alloc_committed(stasis_alloc_t* alloc, int xid);

stasis_alloc_t* stasis_alloc_init(stasis_allocation_policy_t * allocPolicy);
void stasis_alloc_post_init(stasis_alloc_t* alloc);
void stasis_alloc_deinit(stasis_alloc_t* alloc);
/**
    Allocate a record.

    @param xid The transaction responsible for the allocation
    @param size The size of the new record to be allocated.  Talloc will allocate a
    blob if the record will not easily fit on a page.

    @return the recordid of the new record.
*/
compensated_function recordid Talloc(int xid, unsigned long size);

compensated_function recordid TallocFromPage(int xid, pageid_t page, unsigned long size);

/**
   Free a record.
*/
compensated_function void Tdealloc(int xid, recordid rid);

/**
   Obtain the type of a record, as returned by getRecordType.

   @param xid the transaction id.

   @param rid the record of interest.  The size field will be ignored,
   allowing this function to be used to probe for records in pages.

   @return UNINITIALIZED_RECORD, BLOB_RECORD, SLOTTED_RECORD, or FIXED_RECORD.

   @see getRecordType

*/
compensated_function int TrecordType(int xid, recordid rid);

/**
   Obtain the length of the data stored in a record.

   @param xid the transaction id.

   @param rid the record of interest.  The size field will be ignored,
   allowing this function to be used to probe for records in pages.

   @return -1 if the record does not exist, the size of the record otherwise.
*/
compensated_function int TrecordSize(int xid, recordid rid);

/** Return the number of records stored in page pageid */
compensated_function int TrecordsInPage(int xid, pageid_t page);

#endif
