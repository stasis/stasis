#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <pbl/pbl.h>
#include <lladd/transactional.h>
#include <lladd/bufferManager.h>
#include <lladd/page.h>
#include <lladd/constants.h>

#include "blobManager.h"



static FILE * blobf0 = NULL, * blobf1 = NULL;
/**
   This is a hash of hash tables.  The outer hash maps from xid to
   inner hash.  The inner hash maps from rid to lsn.
*/
static pblHashTable_t * dirtyBlobs;

/** Plays a nasty trick on bufferManager to force it to read and write
    blob_record_t items for us.  Relies upon bufferManager (and
    page.c's) trust in the rid.size field... */
static void readRawRecord(int xid, recordid rid, void * buf, int size) {
  recordid blob_rec_rid = rid;
  blob_rec_rid.size = size;
  readRecord(xid, blob_rec_rid, buf);
}

static void writeRawRecord(int xid, lsn_t lsn, recordid rid, const void * buf, int size) {
  recordid blob_rec_rid = rid;
  blob_rec_rid.size = size;
  Tset(xid, blob_rec_rid, buf);
}



/* moved verbatim from bufferManger.c, then hacked up to use FILE * instead of ints. */
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

  DEBUG("blobf0 opened.\n");

  if( ! (blobf1 = fopen(BLOB1_FILE, "r+"))) { /* file may not exist */
    if( (blobfd1 = creat(BLOB1_FILE, 0666)) == -1 ) { /* cannot even create it */
      printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
      perror("Creating blob 1 file"); abort();
    }
    if( close(blobfd1)) {
      printf("ERROR: %i on %s line %d", errno, __FILE__, __LINE__);
      perror(NULL); abort();
    }
    if(!(blobf1 = fopen(BLOB1_FILE, "r+"))) { perror("Couldn't open or create blob 1 file"); abort(); }
  }

  DEBUG("blobf1 opened.\n");

  dirtyBlobs = pblHtCreate();
}

/** Discards all changes to dirty blobs, and closes the blob store. 
    
    @todo memory leak: Will leak memory if there are any outstanding
    xacts that have written to blobs.  Should explicitly abort them
    instead of just invalidating the dirtyBlobs hash.  

    (If the you fix the above @todo, don't forget to fix
    bufferManager's simulateBufferManagerCrash.)
*/
void closeBlobStore() {
  int ret = fclose(blobf0);
  assert(!ret);
  ret = fclose(blobf1);
  assert(!ret);
  blobf0 = NULL;
  blobf1 = NULL;

  pblHtDelete(dirtyBlobs);
}

static long myFseek(FILE * f, long offset, int whence) {
  long ret;
  if(0 != fseek(f, offset, whence)) { perror ("fseek"); fflush(NULL); abort(); }
  if(-1 == (ret = ftell(f))) { perror("ftell"); fflush(NULL); abort(); }
  return ret;
}

recordid allocBlob(int xid, lsn_t lsn, size_t blobSize) {  
  
  long fileSize = myFseek(blobf1, 0, SEEK_END);
  blob_record_t blob_rec;
  Page p;
  char zero = 0;
  /* Allocate space for the blob entry. */
  
  assert(blobSize > 0); /* Don't support zero length blobs right now... */

  /* First in buffer manager. */

  recordid rid = Talloc(xid, sizeof(blob_record_t));

  readRecord(xid, rid, &blob_rec);

  /** Then in the blob file. @todo: BUG How can we get around doing a
      force here?  If the user allocates space and we crash, could we
      double allocate space, since the file won't have grown.  Could
      we write a log entry with the new size?  Alternatively, is
      forcing the files before writing a commit to log enough?*/

  /** @todo Should this be -1, not -2?  Aren't we writing one byte after the end of the blob? */
  myFseek(blobf0, fileSize + blobSize - 1, SEEK_SET);
  myFseek(blobf1, fileSize + blobSize - 1, SEEK_SET);

  if(1 != fwrite(&zero, sizeof(char), 1, blobf0)) { perror(NULL); abort(); }
  if(1 != fwrite(&zero, sizeof(char), 1, blobf1)) { perror(NULL); abort(); }

  /** Finally, fix up the fields in the record that points to the blob. */

  blob_rec.fd = 0;
  blob_rec.size = blobSize;
  blob_rec.offset = fileSize;
	
  p = loadPage(rid.page);

  setSlotType(p, rid.slot, BLOB_SLOT);
  rid.size = BLOB_SLOT;

  /* Tset() needs to know to 'do the right thing' here, since we've
     changed the size it has recorded for this record, and
     writeRawRecord makes sure that that is the case. */
  writeRawRecord  (xid, lsn, rid, &blob_rec, sizeof(blob_record_t));

  rid.size = blob_rec.size;

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
/* 
static void tripleHashRemove(int xid, recordid rid) {
  pblHashTable_t * xidHash = pblHtLookup(dirtyBlobs, &xid, sizeof(int));
  
  if(xidHash) {  / * Else, there was no xid, rid pair. * /
    pblHashTable_t * pageXidHash = pblHtLookup(xidHash, &(rid.page), sizeof(int));

    if(pageXidHash) {

      lsn_t * delme = pblHtLookup(pageXidHash, &rid, sizeof(recordid));
      pblHtRemove(pageXidHash, &rid, sizeof(recordid));
      free(delme);

      / * We freed a member of pageXidHash.  Is it empty? * /
      if(!pblHtFirst(pageXidHash)) {
	pblHtRemove(xidHash, &(rid.page), sizeof(int));
	
	/ * Is xidHash now empty? * /
	if(!pblHtFirst(xidHash)) {
	  pblHtRemove(dirtyBlobs, &xid, sizeof(int));
	  free(xidHash);
	}

	free(pageXidHash);
      }
    }
  }
}*/

void readBlob(int xid, recordid rid, void * buf) { 

  /* We don't care if the blob is dirty, since the record from the
     buffer manager will reflect that if it is.. */
  
  blob_record_t rec;
  FILE * fd;
  long offset;

  assert(buf);

  readRawRecord(xid, rid, &rec, sizeof(blob_record_t));

  fd = rec.fd ? blobf1 : blobf0;

  offset = myFseek(fd, (long int) rec.offset, SEEK_SET);

  DEBUG("reading blob at offset %d (%ld), size %ld, buffer %x\n", rec.offset, offset, rec.size, (unsigned int) buf);

  assert(rec.offset == offset);
  if(1 != fread(buf, rec.size, 1, fd)) { 

    if(feof(fd)) { printf("Unexpected eof!\n"); fflush(NULL); abort(); }
    if(ferror(fd)) { printf("Error reading stream! %d", ferror(fd)); fflush(NULL); abort(); }

  }

}

/** @todo dirtyBlobs should contain the highest LSN that wrote to the
    current version of the dirty blob, and the lsn field should be
    checked to be sure that it increases monotonically. */
void writeBlob(int xid, lsn_t lsn, recordid rid, const void * buf) { 

  /* First, determine if the blob is dirty. */
  lsn_t * dirty = tripleHashLookup(xid, rid);

  blob_record_t rec;
  long offset;
  FILE * fd;
  int readcount;

  /* Tread() raw record */
  readRawRecord(xid, rid, &rec, sizeof(blob_record_t));

  if(dirty) { 
    assert(lsn > *dirty);
    *dirty = lsn;  /* Updates value in triple hash (works because of pointer aliasing.) */
    DEBUG("Blob already dirty.\n");
    


  } else {
    DEBUG("Marking blob dirty.\n");
    tripleHashInsert(xid, rid, lsn);
    /* Flip the fd bit on the record. */
    rec.fd = rec.fd ? 0 : 1;

    /* Tset() raw record */
    writeRawRecord(xid, lsn, rid, &rec, sizeof(blob_record_t));
  }

  fd = rec.fd ? blobf1 : blobf0; /* rec's fd is up-to-date, so use it directly */

  offset = myFseek(fd, rec.offset, SEEK_SET);

  printf("Writing at offset = %d, size = %ld\n", rec.offset, rec.size);
  assert(offset == rec.offset);
  readcount = fwrite(buf, rec.size, 1, fd);
  assert(1 == readcount);

  /* No need to update the raw blob record. */

}

void commitBlobs(int xid) {
  abortBlobs(xid);
}

/** 
    Just clean up the dirty list for this xid. @todo Check return values. 
    
    (Functionally equivalent to the old rmTouch() function.  Just
    deletes this xid's dirty list.)

    @todo doesn't take lsn_t, since it doesnt write any blobs.  Change
    the api?

    @todo The tripleHash data structure is overkill here.  We only
    need two layers of hash tables, but it works, and it would be a
    pain to change it, unless we need to touch this file for some
    other reason.

*/
void abortBlobs(int xid) {
  /*
    At first glance, it may seem easier to keep track of which blobs
    are dirty only in blobManager, and then propogate those updates to
    bufferManager later.  It turns out that it's much easier to
    propogate the changes to bufferManger, since otherwise, recovery
    and undo have to reason about lazy propogation of values to the
    bufferManager, and also have to preserve *write* ordering, even
    though the writes may be across many transactions, and could be
    propogated in the wrong order.  If we generate a Tset() (for the
    blob record in bufferManager) for each write, things become much
    easier.
  */
 
  pblHashTable_t * rid_buckets = pblHtLookup(dirtyBlobs, &xid, sizeof(int));
  pblHashTable_t * this_bucket;
  
  if(!rid_buckets) { return; } /* No dirty blobs for this xid.. */

  for(this_bucket = pblHtFirst(rid_buckets); this_bucket; this_bucket = pblHtNext(rid_buckets)) {
    lsn_t * rid_lsn;
    int page_number;

    /* All right, this_bucket contains all of the rids for this page. */

    for(rid_lsn = pblHtFirst(this_bucket); rid_lsn; rid_lsn = pblHtNext(this_bucket)) {
      recordid * rid = pblHtCurrentKey(this_bucket);
      page_number = rid->page;
      pblHtRemove(this_bucket, rid, sizeof(recordid));
      free(rid_lsn);
    }

    pblHtRemove(rid_buckets, &page_number, sizeof(int));
    pblHtDelete(this_bucket);
  }
  pblHtDelete(rid_buckets);
    
}
