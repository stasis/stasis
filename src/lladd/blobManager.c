#include "blobManager.h"
#include <lladd/constants.h>
#include <lladd/bufferManager.h>
#include <lladd/page.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <assert.h>
#include <stdlib.h>
#include <pbl/pbl.h>

static FILE * blobf0, * blobf1;
/**
   This is a hash of hash tables.  The outer hash maps from xid to
   inner hash.  The inner hash maps from rid to lsn.
*/
static pblHashTable_t * dirtyBlobs;

/* moved verbatim from bufferManger.c */
void openBlobStore() {
  int blobfd0, blobfd1;
  if( ! (blobf0 = fopen(BLOB0_FILE, "w+"))) { /* file may not exist */
    if( (blobfd0 = creat(BLOB0_FILE, 0666)) == -1 ) { /* cannot even create it */
      printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
      perror("Creating blob 0 file"); abort();
    }
    if( close(blobfd0)) {
      printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
      perror(NULL); abort();
    }
   if(!(blobf0 = fopen(BLOB0_FILE, "w+"))) { perror("Couldn't open or create blob 0 file"); abort(); }
  }
  if( ! (blobf1 = fopen(BLOB1_FILE, "w+"))) { /* file may not exist */
    if( (blobfd1 = creat(BLOB1_FILE, 0666)) == -1 ) { /* cannot even create it */
      printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
      perror("Creating blob 1 file"); abort();
    }
    if( close(blobfd1)) {
      printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
      perror(NULL); abort();
    }
    if(!(blobf1 = fopen(BLOB1_FILE, "w+"))) { perror("Couldn't open or create blob 1 file"); abort(); }
  }
  dirtyBlobs = pblHtCreate();
}

/** Discards all changes to dirty blobs, and closes the blob store. 
    
    @todo memory leak: Will leak memory if there are any outstanding
    xacts that have written to blobs.  Should explicitly abort them
    instead of just invalidating the dirtyBlobs hash.
*/
void closeBlobStore() {
  assert(!fclose(blobf0));
  assert(!fclose(blobf1));
  blobf0 = NULL;
  blobf1 = NULL;

  pblHtDelete(dirtyBlobs);
}

static long myFseek(FILE * f, long offset, int whence) {
  long ret;
  if(!fseek(blobf1, offset, whence)) { perror (NULL); abort(); }
  if(-1 == (ret = ftell(f))) { perror(NULL); abort(); }
  return ret;
}

recordid allocBlob(int xid, lsn_t lsn, size_t blobSize) {  
  
  long fileSize = myFseek(blobf1, 0, SEEK_END);
  blob_record_t blob_rec;
  Page p;
  char zero = 0;
  /* Allocate space for the blob entry. */
  
  /* First in buffer manager. */
  recordid rid = ralloc(xid, sizeof(blob_record_t));

  readRecord(xid, rid, &blob_rec);

  /** Then in the blob file. @todo: BUG How can we get around doing a
      force here?  If the user allocates space and we crash, could we
      double allocate space, since the file won't have grown.  Could
      we write a log entry with the new size?  Alternatively, is
      forcing the files before writing a commit to log enough?*/

  /** @todo Should this be -1, not -2?  Aren't we writing one byte after the end of the blob? */
  myFseek(blobf0, fileSize + blobSize - 1, SEEK_SET);
  myFseek(blobf1, fileSize + blobSize - 1, SEEK_SET);

  if(1 != fwrite(&zero, 0, sizeof(char), blobf0)) { perror(NULL); abort(); }
  if(1 != fwrite(&zero, 0, sizeof(char), blobf1)) { perror(NULL); abort(); }

  /** Finally, fix up the fields in the record that points to the blob. */

  blob_rec.fd = 0;
  blob_rec.size = blobSize;
  blob_rec.offset = fileSize;
	
  p = loadPage(rid.page);

  setSlotType(p, rid.slot, BLOB_SLOT);
  rid.size = BLOB_SLOT;
  /* writeRecord needs to know to 'do the right thing' here, since
     we've changed the size it has recorded for this record. */
  /* @todo What should writeRawRecord do with the lsn? */
  writeRawRecord  (rid, lsn, &blob_rec, sizeof(blob_record_t));

  return rid;
}

static lsn_t * tripleHashLookup(int xid, recordid rid) {
  pblHashTable_t * xidHash = pblHtLookup(dirtyBlobs, &xid, sizeof(xid));
  if(xidHash == NULL) {
    return NULL;
  } else {
    pblHashTable_t * pageXidHash = pblHtLookup(xidHash, &(rid.page), sizeof(int));
    if(pageXidHash == NULL) {
      return NULL;
    }
    return pblHtLookup(pageXidHash, &rid, sizeof(recordid));
  }
}

static void tripleHashInsert(int xid, recordid rid, lsn_t newLSN) {
  pblHashTable_t * xidHash;
  pblHashTable_t * pageXidHash;
  lsn_t * copy;

  xidHash = pblHtLookup(dirtyBlobs, &xid, sizeof(int)); /* Freed in doubleHashRemove */

  if(xidHash == NULL) {
    xidHash = pblHtCreate();
    pblHtInsert(dirtyBlobs, &xid, sizeof(int), xidHash);
  }

  pageXidHash = pblHtLookup(xidHash, &(rid.page), sizeof(int));
  
  if(pageXidHash == NULL) {
    pageXidHash = pblHtCreate();
    pblHtInsert(xidHash, &(rid.page), sizeof(int), pageXidHash);
  }

  copy = malloc(sizeof(lsn_t)); /* Freed in doubleHashRemove */
  *copy = newLSN;

  pblHtInsert(pageXidHash, &rid, sizeof(recordid), copy);
}

static void tripleHashRemove(int xid, recordid rid) {
  pblHashTable_t * xidHash = pblHtLookup(dirtyBlobs, &xid, sizeof(int));
  
  if(xidHash) {  /* Else, there was no xid, rid pair. */
    pblHashTable_t * pageXidHash = pblHtLookup(xidHash, &(rid.page), sizeof(int));

    if(pageXidHash) {

      lsn_t * delme = pblHtLookup(pageXidHash, &rid, sizeof(recordid));
      pblHtRemove(pageXidHash, &rid, sizeof(recordid));
      free(delme);

      /* We freed a member of pageXidHash.  Is it empty? */
      if(!pblHtFirst(pageXidHash)) {
	pblHtRemove(xidHash, &(rid.page), sizeof(int));
	
	/* Is xidHash now empty? */
	if(!pblHtFirst(xidHash)) {
	  pblHtRemove(dirtyBlobs, &xid, sizeof(int));
	  free(xidHash);
	}

	free(pageXidHash);
      }
    }
  }
}
void readBlob(int xid, recordid rid, void * buf) { 
  /* First, determine if the blob is dirty. */
  
  lsn_t * dirty = tripleHashLookup(xid, rid);

  blob_rec_t rec;

  FILE * fd;

  readRawRecord(rid, &rec, sizeof(blob_rec_t));

  if(dirty) {
    fd = rec.fd ? blobf0 : blobf1;  /* Read the updated version */
  } else {
    fd = rec.fd ? blobf1 : blobf0;  /* Read the clean version */
  }

  assert(rec.offset == myFseek(fd, rec.offset, SEEK_SET));
  assert(1 == fread(buf, rec.size, 1, fd));

}

static int one = 1;
/** @todo dirtyBlobs should contain the highest LSN that wrote to the
    current version of the dirty blob, and the lsn field should be
    checked to be sure that it increases monotonically. */
void writeBlob(int xid, recordid rid, lsn_t lsn, void * buf) { 
  /* First, determine if the blob is dirty. */
  
  lsn_t * dirty = tripleHashLookup(xid, rid);
  blob_rec_t rec;

  FILE * fd;

  assert(rid.size == BLOB_SLOT);

  if(dirty) { 
    assert(lsn > *dirty);
    *dirty = lsn;  /* Updates value in dirty blobs (works because of pointer aliasing.) */
  } else {
    tripleHashInsert(xid, rid, lsn);
  }
  readRawRecord(rid, &rec, sizeof(blob_record_t));

  fd = rec.fd ? blobf0 : blobf1;  /* Read the slot for the dirty (updated) version. */
  
  assert(rec.offset == myFseek(fd, rec.offset, SEEK_SET));
  assert(1 == fwrite(buf, rec.size, 1, fd));

  /* No need to update the raw blob record. */

}
/** @todo check return values */
void commitBlobs(int xid, lsn_t lsn) {
  lsn_t * dirty = tripleHashLookup(xid, rid);
  
  /* Because this is a commit, we must update each page atomically.
  Therefore, we need to re-group the dirtied blobs by page id, and
  then issue one write per page.  Since we write flip the bits of each
  dirty blob record on the page, we can't get away with incrementally
  updating things. */

  pblHashTable_t * rid_buckets = pblHtLookup(dirtyBlobs, &xid, sizeof(int));
  lsn_t * lsn;
  int last_page = -1;
  Page p;

  pblHashTable_t * this_bucket;
  
  for(this_bucket = pblHtFirst(rid_buckets); this_bucket; this_bucket = pblHtNext(rid_buckets)) {
    blob_record_t buf;
    recordid * rid_ptr;
    lsn_t * rid_lsn;
    int first = 1;
    int page_number;
    /* All right, this_bucket contains all of the rids for this page. */

    for(rid_lsn = pblHtFirst(this_bucket); rid_lsn; rid_lsn = pblHtNext(this_bucket)) {
      /** @todo INTERFACE VIOLATION Can only use bufferManager's
	  read/write record since we're single threaded, and this calling
	  sequence cannot possibly call kick page. Really, we sould use
	  pageReadRecord / pageWriteRecord, and bufferManager should let
	  us write out the whole page atomically... */

      rid_ptr = pblCurrentKey(this_bucket);

      if(first) {
	page_number = rid_ptr->page;
	first = 0;
      } else {
	assert(page_number == rid_ptr->page);
      }

      readRawRecord(*rid_ptr, &buf, sizeof(blob_record_t));
      /* This rid is dirty, so swap the fd pointer. */
      buf.fd = (buf.fd ? 0 : 1);
      writeRawRecord(*rid_ptr, lsn, &buf, sizeof(blob_record_t));
      pblHtRemove(rid_ptr);
      free(rid_ptr);
    }

    if(!first) {
      pblHtRemove(rid_buckets, &page_number, sizeof(int));
    } else {
      abort();  /* Bucket existed, but was empty?!? */
    }
  
    pblHtDelete(this_bucket);
  }  
}

/** Easier than commit blobs.  Just clean up the dirty list for this xid. @todo Check return values. */
void abortBlobs(int xid) {
  pblHashTable_t * rid_buckets = pblHtLookup(dirtyBlobs, &xid, sizeof(int));
  lsn_t * lsn;
  int last_page = -1;
  Page p;

  pblHashTable_t * this_bucket;
  
  for(this_bucket = pblHtFirst(rid_buckets); this_bucket; this_bucket = pblHtNext(rid_buckets)) {
    blob_record_t buf;
    recordid * rid_ptr;
    lsn_t * rid_lsn;
    int first = 1;
    int page_number;
    /* All right, this_bucket contains all of the rids for this page. */

    for(rid_lsn = pblHtFirst(this_bucket); rid_lsn; rid_lsn = pblHtNext(this_bucket)) {
      recordid * rid = pblHtCurrentKey(this_bucket);
      pblHtRemove(this_bucket, rid, sizeof(recordid));
      free(rid_lsn);
      page_number = rid->page;
    }

    pblHtRemove(rid_buckets, &page_number, sizeof(int));
    pblHtDelete(this_bucket);
  }
  pblHtDelete(rid_buckets);
    
}
