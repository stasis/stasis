#ifndef __BLOB_MANAGER_H
#define __BLOB_MANAGER_H

#include <lladd/common.h>
#include <lladd/page.h>
BEGIN_C_DECLS

/**    
       @file
       blobManager - Provides blob handling @todo Set range??
       Plan for modularity: Exactly one blob manager per storeFile.
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
       
       @todo Should the tripleHash be its own little support library? 
*/


/** 
    If blob is resident, return a pointer to it.  Otherwise, check if
    it's dirty (it could have been stolen), an retrieve it from the
    appropriate blob file. 
*/
void readBlob(int xid, recordid rid, void * buf);


/** 
    If you write to a blob, call this function to mark it dirty.
*/
void writeBlob(int xid, lsn_t lsn, recordid rid, const void * buf);


/**
   Atomically (with respect to recovery) make the dirty version of the
   blob the primary copy and mark it not-dirty.
*/

void commitBlobs(int xid);

/**
   Revert the blob to the last clean version.
*/

void abortBlobs(int xid);

typedef struct {
  unsigned offset;
  unsigned long size;
  unsigned fd : 1;
} blob_record_t;

recordid allocBlob(int xid, lsn_t lsn, size_t blobSize);
void openBlobStore();
void closeBlobStore();

END_C_DECLS

#endif
