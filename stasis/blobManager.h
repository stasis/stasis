#ifndef __BLOB_MANAGER_H
#define __BLOB_MANAGER_H

#include <stasis/common.h>
#include <stasis/page.h>
BEGIN_C_DECLS

/**    
       @file
       blobManager - Provides blob handling.
 
       Plan for modularity: Exactly one blob manager per storeFile.
       Alternatively, we could use the bufferManager's record length
       field to allow for more than one blob manager per buffer
       manager.  (Right now, this field is set to a special value for
       blobs; thie could be extended for other types of records.)

       Blob manager interacts with page manger via page manager's
       public api.

       Blob updates work as follows:

       (1) A transaction obtains an exclusive write lock on a blob
           (not implemented yet.)

       (2) When it requests a write, the blob it writes to is added to
           a data structure that lists all dirty blobs by transaction,
           and the page containing the blob entry is updated. (The fd
           bit in the record is flipped, and the LSN is updated.)  The
           write to the blob store is not flushed to disk.
       
       (3) All subsequent writes to the same blob just update the
           backing file.

       (4) On commit and rollback, the data structure containing the xid's
           dirty blobs is destroyed.

       (5) recovery2.c handles the restoration of the fd bits using
           physical logging (this is automatic, since we use Tset()
           calls to update the records.)

       @todo Set range??

       @todo Update blobManager to (partially) provide a page api

       @todo Move blobManager to page and/or operations directory

       @ingroup LLADD_CORE
*/


/** 
    Read the blob from the recordid rid into buf.
*/
void stasis_blob_read(int xid, Page * p,  recordid rid, void * buf);


/** 
    Write the contents of buf to the blob in recordid rid.
*/
void stasis_blob_write(int xid, Page * p, recordid rid, const void * buf);

recordid preAllocBlob(int xid, long blobsize);
recordid preAllocBlobFromPage(int xid, long page, long blobsize);

/**
   Allocate a blob of size blobSize. 
*/

void stasis_blob_alloc(int xid, recordid rid);
void stasis_blob_dealloc(int xid, blob_record_t* r);

page_impl stasis_page_blob_impl();

END_C_DECLS

#endif
