#include "blobManager.h"
#include <lladd/constants.h>
#include <lladd/bufferManager.h>
#include <lladd/page.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <assert.h>

static int blobfd0, blobfd1;
static pblhashtable_t touchedBlobs;

/* moved verbatim from bufferManger.c */
void openBlobStore() {
  if( (blobfd0 = open(BLOB0_FILE, O_RDWR, 0)) == -1 ) { /* file may not exist */
    if( (blobfd0 = creat(BLOB0_FILE, 0666)) == -1 ) { /* cannot even create it */
      printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
      assert(0);
      /*			exit(errno); */
    }
    if( close(blobfd0) || ((blobfd0 = open(BLOB0_FILE, O_RDWR, 0)) == -1) ) { /* need to reopen with read perms */
      printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
      assert(0);
      /*      exit(errno); */
    }
	}
  if( (blobfd1 = open(BLOB1_FILE, O_RDWR, 0)) == -1 ) { /* file may not exist */
    if( (blobfd1 = creat(BLOB1_FILE, 0666)) == -1 ) { /* cannot even create it */
      printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
      /* exit(errno); */
      assert(0);
    }
    if( close(blobfd1) || ((blobfd1 = open(BLOB1_FILE, O_RDWR, 0)) == -1) ) { /* need to reopen with read perms */
      printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
      /* exit(errno); */
      assert(0);
    }
  }
}

/** @todo: Assumes no blobs are in memory.  Will break if called in the presence of dirty blobs. */
void closeBlobStore() {
  assert(!close(blobfd0));
  assert(!close(blobfd1));
  blobfd0 = -1;
  blobfd1 = -1;
}


recordid allocBlob(int xid, lsn_t lsn, size_t blobSize) {  
  
  long fileSize = lseek(blobfd1, 0, SEEK_END);
  blob_record_t blob_rec;
  Page p;
  /* Allocate space for the blob entry. */
  
  /* First in buffer manager. */
  recordid rid = ralloc(xid, sizeof(blob_record_t));

  readRecord(xid, rid, &blob_rec);

  /** Then in the blob file. @todo: BUG How can we get around doing a
      force here?  If the user allocates space and we crash, could we
      double allocate space, since the file won't have grown.  Could
      we write a log entry with the new size?  Alternatively, is
      forcing the files before writing a commit to log enough?*/
			
  lseek(blobfd0, fileSize + blobSize - 1, SEEK_SET);
  write(blobfd0, 0, 1);
  lseek(blobfd1, fileSize + blobSize - 1, SEEK_SET);
  write(blobfd1, 0, 1);

  /** Finally, fix up the fields in the record that points to the blob. */

  blob_rec.fd = 0;
  blob_rec.size = blobSize;
  blob_rec.offset = fileSize;
	
  p = loadPage(rid.page);

  setSlotType(p, rid.slot, BLOB_SLOT);
  rid.size = BLOB_SLOT;
  /* writeRecord needs to know to 'do the right thing' here, since
     we've changed the size it has recorded for this record. */
  writeRecord  (xid, rid, lsn, &blob_rec);

  return rid;
  

}

void readBlob(recordid rid, void * buf) { 

  

}
