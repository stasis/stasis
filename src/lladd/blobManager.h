#ifndef __BLOB_MANAGER_H
#define __BLOB_MANAGER_H

#include <lladd/common.h>
#include <lladd/page.h>
BEGIN_C_DECLS

/**    blobManager - Provides blob handling @todo Set range??
       Plan for modularity: Exactly one blob manager per storeFile.
       Blob manager interacts with page manger via page manager's
       public api.
*/


/** 
    If blob is resident, return a pointer to it.  Otherwise, check if
    it's dirty (it could have been stolen), an retrieve it from the
    appropriate blob file. 
*/
void readBlob(recordid rid, void * buf); 

/** 
    If you write to a blob, call this function to mark it dirty.
*/
void writeBlob(recordid rid, lsn_t lsn, void * buf);

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
