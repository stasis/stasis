#include <config.h>
#include <lladd/common.h>

#include <assert.h>

#include <lladd/transactional.h>

#include <lladd/bufferManager.h>
#include <lladd/constants.h>

#include "latches.h"

#include "blobManager.h"
#include "io.h"
#include <pbl/pbl.h>
#include "page.h"
#include <stdio.h>

pthread_mutex_t blob_hash_mutex;

static FILE * blobf0 = NULL, * blobf1 = NULL;
/**
   This is a hash of hash tables.  The outer hash maps from xid to
   inner hash.  The inner hash maps from rid to lsn.
*/
static pblHashTable_t * dirtyBlobs;

/** Plays a nasty trick on bufferManager to force it to read and write
    blob_record_t items for us.  Relies upon bufferManager (and
    page.c's) trust in the rid.size field... */
static void readRawRecord(int xid, Page * p, recordid rid, void * buf, int size) {
  recordid blob_rec_rid = rid;
  blob_rec_rid.size = size;
  readRecord(xid, p, blob_rec_rid, buf);
    /*  Tread(xid, blob_rec_rid, buf); */
}

static void writeRawRecord(int xid, Page * p, recordid rid, lsn_t lsn, const void * buf, int size) {
  recordid blob_rec_rid = rid;
  blob_rec_rid.size = size;
  writeRecord(xid, p, lsn, blob_rec_rid, buf); 
  /*  Tset(xid, blob_rec_rid, buf); - We no longer need to write a log
      record out here, since we're called by something that is the
      result of a log record.*/ 
}
static lsn_t * tripleHashLookup(int xid, recordid rid) {
  lsn_t * ret;
  pthread_mutex_lock(&blob_hash_mutex);
  pblHashTable_t * xidHash = pblHtLookup(dirtyBlobs, &xid, sizeof(xid));
  if(xidHash == NULL) {
    ret = NULL;
  } else {
    pblHashTable_t * pageXidHash = pblHtLookup(xidHash, &(rid.page), sizeof(int));
    if(pageXidHash == NULL) {
      ret = NULL;
    } else {
      ret = pblHtLookup(pageXidHash, &rid, sizeof(recordid));
    }
  }
  pthread_mutex_unlock(&blob_hash_mutex);
  return ret;
}

static void tripleHashInsert(int xid, recordid rid, lsn_t newLSN) {
  pblHashTable_t * xidHash;
  pblHashTable_t * pageXidHash;
  lsn_t * copy;
  pthread_mutex_lock(&blob_hash_mutex);
  xidHash = pblHtLookup(dirtyBlobs, &xid, sizeof(int)); /* Freed in doubleHashRemove */

  if(xidHash == NULL) {
    xidHash = pblHtCreate();
    DEBUG("New blob xact: xid = %d\n", xid);
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

  pthread_mutex_unlock(&blob_hash_mutex);
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

/* moved verbatim from bufferManger.c, then hacked up to use FILE * instead of ints. */
void openBlobStore() {

  /* the r+ mode opens an existing file read /write */
  if( ! (blobf0 = fopen(BLOB0_FILE, "r+"))) { /* file may not exist */
    /* the w+ mode truncates, creates, and opens read / write */
   if(!(blobf0 = fopen(BLOB0_FILE, "w+"))) { perror("Couldn't open or create blob 0 file"); abort(); }
  }

  DEBUG("blobf0 opened.\n");

  if( ! (blobf1 = fopen(BLOB1_FILE, "r+"))) { /* file may not exist */
    if(!(blobf1 = fopen(BLOB1_FILE, "w+"))) { perror("Couldn't open or create blob 1 file"); abort(); }
  }

  DEBUG("blobf1 opened.\n");

  dirtyBlobs = pblHtCreate();
  pthread_mutex_init(&blob_hash_mutex, NULL);
}

/** Discards all changes to dirty blobs, and closes the blob store. 
    
    @todo memory leak: Will leak memory if there are any outstanding
    xacts that have written to blobs.  Should explicitly abort them
    instead of just invalidating the dirtyBlobs hash.  

    (If the you fix the above todo, don't forget to fix
    bufferManager's simulateBufferManagerCrash.)
*/
void closeBlobStore() {
  int ret = fclose(blobf0);
  assert(!ret);
  ret = fclose(blobf1);
  assert(!ret);
  blobf0 = NULL;
  blobf1 = NULL;
  pblHashTable_t * xidhash;
  for(xidhash = pblHtFirst(dirtyBlobs); xidhash; xidhash = pblHtNext(dirtyBlobs)) {
    fflush(NULL);
    sync();
    printf("WARNING!: Found list of dirty blobs for transaction: %ld\nIt is possible that these blobs were not synced to disk properly.\n\nThe data has now been flushed to disk, but this warning could indicate a bug that could cause data corruption.", *(lsn_t*)pblHtCurrentKey(dirtyBlobs));
    fflush(NULL);
    sync();

    pblHtRemove(dirtyBlobs, 0, 0);
  }

  pblHtDelete(dirtyBlobs);

  pthread_mutex_destroy(&blob_hash_mutex);
}

/** 
    blob allocation:

    generate rid
    log rid (+ alloc)
    alloc space in store
    log rid write
    perform rid write
    write space allocation to store

    (It used to be:)
    
    allocate space in store
    generate rid
    write space in store
    log rid
    write rid alloc
    log rid

    The trick here is to make sure that the write to the record
    happens after the record's allocation has been logged.

*/

recordid preAllocBlob(int xid, long blobSize) {

  /* Allocate space for the blob entry. */
 
  DEBUG("Allocing blob (size %ld)\n", blobSize);

  assert(blobSize > 0); /* Don't support zero length blobs right now... */

  /* First in buffer manager. */

  recordid rid = Talloc(xid, sizeof(blob_record_t));

  rid.size = blobSize;

  return rid;

}

recordid preAllocBlobFromPage(int xid, long page, long blobSize) {

  /* Allocate space for the blob entry. */
 
  DEBUG("Allocing blob (size %ld)\n", blobSize);

  assert(blobSize > 0); /* Don't support zero length blobs right now... */

  /* First in buffer manager. */

  recordid rid = TallocFromPage(xid, page, sizeof(blob_record_t));

  rid.size = blobSize;

  return rid;

}

void allocBlob(int xid, Page * p, lsn_t lsn, recordid rid) {  
  
  long fileSize;
  blob_record_t blob_rec;

  char zero = 0;

  DEBUG("post Allocing blob (size %ld)\n", rid.size);

  /** Finally, fix up the fields in the record that points to the blob. 
      The rest of this also should go into alloc.c 
  */
  blob_rec.fd = 0;
  blob_rec.size = rid.size;
  flockfile(blobf1);
  flockfile(blobf0);
  fileSize = myFseek(blobf1, 0, SEEK_END);
  blob_rec.offset = fileSize;
  
  /*  setRecordType(p, rid, BLOB_SLOT); */
  /*  rid.size = BLOB_SLOT; */

  /*    releasePage(p); */
  rid.size = blob_rec.size;

  /* Allocate space for the blob entry. */

  assert(rid.size > 0); /* Don't support zero length blobs right now... */

  /* First in buffer manager. */

  /* Read in record to get the correct offset, size for the blob*/

  /* * @ todo blobs deadlock... */
  /*  readRawRecord(xid, p, rid, &blob_rec, sizeof(blob_record_t)); */

  myFseek(blobf0, fileSize + rid.size - 1, SEEK_SET);
  myFseek(blobf1, fileSize + rid.size - 1, SEEK_SET);

  if(1 != fwrite(&zero, sizeof(char), 1, blobf0)) { perror(NULL); abort(); }
  if(1 != fwrite(&zero, sizeof(char), 1, blobf1)) { perror(NULL); abort(); }

  fdatasync(fileno(blobf0));
  fdatasync(fileno(blobf1));


  funlockfile(blobf0);
  funlockfile(blobf1);

  /* Tset() needs to know to 'do the right thing' here, since we've
     changed the size it has recorded for this record, and
     writeRawRecord makes sure that that is the case. 

     (This call must be after the files have been extended, and synced to disk, since it marks completion of the blob allocation.)
  */
  writeRawRecord  (xid, p, rid, lsn, &blob_rec, sizeof(blob_record_t));


}

void readBlob(int xid, Page * p, recordid rid, void * buf) { 

  /* We don't care if the blob is dirty, since the record from the
     buffer manager will reflect that if it is.. */
  
  blob_record_t rec;
  FILE * fd;
  long offset;

  assert(buf);

  readRawRecord(xid, p, rid, &rec, sizeof(blob_record_t));

  fd = rec.fd ? blobf1 : blobf0;


  DEBUG("reading blob at offset %d, size %ld, buffer %x\n", rec.offset, rec.size, (unsigned int) buf);

  flockfile(fd);

  offset = myFseek(fd, (long int) rec.offset, SEEK_SET);

  assert(rec.offset == offset);

  if(1 != fread(buf, rec.size, 1, fd)) { 

    if(feof(fd)) { printf("Unexpected eof!\n"); fflush(NULL); abort(); }
    if(ferror(fd)) { printf("Error reading stream! %d", ferror(fd)); fflush(NULL); abort(); }

  }

  funlockfile(fd);
}

/**
   Examines the blob in question, marks it dirty, and returns the
   appropriate file descriptor.
*/
static FILE * getDirtyFD(int xid, Page * p, lsn_t lsn, recordid rid) {
  lsn_t * dirty = tripleHashLookup(xid, rid);
  FILE * fd;
  blob_record_t rec;


  /* First, determine if the blob is dirty. */

  /* Tread() raw record */
  readRawRecord(xid, p, rid, &rec, sizeof(blob_record_t));

  assert(rec.size == rid.size);

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
    writeRawRecord(xid, p, rid, lsn, &rec, sizeof(blob_record_t));
  }

  fd = rec.fd ? blobf1 : blobf0; /* rec's fd is up-to-date, so use it directly */

  return fd;
}
/*  This function cannot be safely implemented on top of the current
    blob implementation since at recovery, we have no way of knowing
    whether or not a future write to the blob was performed.  (This is
    the same reason why we cannot steal pages whose LSN's may be too
    low.
void setRangeBlob(int xid, Page * p, lsn_t lsn, recordid rid, const void * buf, long offset, long length) {
  FILE * fd;
  int readcount;
  blob_record_t rec;

  fd = getDirtyFD(xid, p, lsn, rid);
  readRawRecord(xid, p, rid, &rec, sizeof(blob_record_t));

  flockfile(fd);
  offset = myFseek(fd, rec.offset + offset, SEEK_SET);
  assert(offset == rec.offset);
  readcount = fwrite(buf, length, 1, fd);
  assert(1 == readcount);
  fdatasync(fileno(fd));
  funlockfile(fd);
  } */
/** @todo dirtyBlobs should contain the highest LSN that wrote to the
    current version of the dirty blob, and the lsn field should be
    checked to be sure that it increases monotonically. 

    @todo Correctness / performance problem: Currently, we cannot
    manually pin pages in memory, so the record pointing to the blob
    may be stolen.  Therefore, we must fdatasync() the blob file's
    updates to disk each time writeBlob is called.

    If we could pin the page, this problem would be solved, and
    writeblob would not have to call fdatasync().  The same problem
    applies to setRangeBlob.
*/
void writeBlob(int xid, Page * p, lsn_t lsn, recordid rid, const void * buf) { 

  long offset;
  FILE * fd;
  int readcount;
  blob_record_t rec;

  DEBUG("Writing blob (size %ld)\n", rid.size);
  
  fd = getDirtyFD(xid, p, lsn, rid);
  readRawRecord(xid, p, rid, &rec, sizeof(blob_record_t));

  DEBUG("Writing at offset = %d, size = %ld\n", rec.offset, rec.size);
 
  flockfile(fd);

  offset = myFseek(fd, rec.offset, SEEK_SET);
  assert(offset == rec.offset);

  readcount = fwrite(buf, rec.size, 1, fd);
  assert(1 == readcount);

  fdatasync(fileno(fd));

  funlockfile(fd);

  /* No need to update the raw blob record. */
}
/** @todo check to see if commitBlobs actually needs to flush blob
    files when it's called (are there any dirty blobs associated with
    this transaction? 

    @todo when writeBlob is fixed, add the fdatasync calls back into commitBlobs().
*/
void commitBlobs(int xid) {
  flockfile(blobf0);
  flockfile(blobf1);
  /*  fdatasync(fileno(blobf0));
      fdatasync(fileno(blobf1)); */
  funlockfile(blobf0);
  funlockfile(blobf1);
  abortBlobs(xid);
}

/** 
    Just clean up the dirty list for this xid. @todo Check return values. 
    
    (Functionally equivalent to the old rmTouch() function.  Just
    deletes this xid's dirty list.)

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
 
  pthread_mutex_lock(&blob_hash_mutex);

  pblHashTable_t * rid_buckets = pblHtLookup(dirtyBlobs, &xid, sizeof(int));
  pblHashTable_t * this_bucket;
  
  DEBUG("Blob cleanup xid=%d\n", xid);

  if(rid_buckets) {  /* Otherwise, there are no dirty blobs for this xid.. */
    
    for(this_bucket = pblHtFirst(rid_buckets); this_bucket; this_bucket = pblHtNext(rid_buckets)) {
      lsn_t * rid_lsn;
      int page_number = *(int*)pblHtCurrentKey(rid_buckets);

      /* All right, this_bucket contains all of the rids for this page. */
      
      for(rid_lsn = pblHtFirst(this_bucket); rid_lsn; rid_lsn = pblHtNext(this_bucket)) {
	recordid * rid = pblHtCurrentKey(this_bucket);
	/*	page_number = rid->page; */
	assert(page_number == rid->page);
	pblHtRemove(this_bucket, 0, 0);/*rid, sizeof(recordid)); */
	free(rid_lsn);
      }
      
      pblHtRemove(rid_buckets, 0, 0);
      pblHtDelete(this_bucket);
    }
    pblHtDelete(rid_buckets);
    pblHtRemove(dirtyBlobs, &xid, sizeof(int));
  }

  pthread_mutex_unlock(&blob_hash_mutex);

}
