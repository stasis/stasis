#include <lladd/operations/linearHash.h>
#include <lladd/hash.h>
#include <limits.h>
#include <assert.h>
#include <stdlib.h>
#include "../page.h"
/**

   A from-scratch implementation of linear hashing.  Uses the
   arrayList operations to implement its hashbuckets.

*/

#define BUCKETS_OFFSET (2)

#define headerKeySize (headerRidA.page)
#define headerValSize (headerRidA.slot)

#define headerHashBits (headerRidB->page)
#define headerNextSplit (headerRidB->slot)

#include <math.h>
#include <malloc.h>
#include <string.h>
#include <lladd/operations/linearHash.h>
#include <pbl/pbl.h>

typedef struct {
  recordid next;
} hashEntry;

void instant_expand (int xid, recordid hash, int next_split, int i, int keySize, int valSize);


extern pblHashTable_t * openHashes ;
/*pblHashTable_t * openHashes = NULL; */

static int operateUndoInsert(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) {

  int keySize = rid.size;
  rid.size = sizeof(recordid);

  if(!pblHtLookup(openHashes, &rid.page, sizeof(int))) {
    ThashOpen(xid, rid);
  }

  ThashInstantDelete(xid, rid, dat, keySize);
  return 0;
}

typedef struct {
  int keySize;
  int valSize;
} undoDeleteArg;

static int operateUndoDelete(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) {
  const undoDeleteArg * arg = dat;
  int keySize = arg->keySize;
  int valSize = arg->valSize;

  const byte * argBytes = (byte*)(arg+1);


  rid.slot = 0;
  /*  Tread(xid, dereferenceArrayListRid(p, rid.slot), &headerRidA); */
  /*  Tread(xid, rid, &headerRidA); */

  assert(keySize == sizeof(int));
  assert(valSize == sizeof(recordid));

  ThashInstantInsert(xid, rid, argBytes, keySize, 
		     argBytes + keySize, valSize);
  return 0;
}
static int noop (int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) { pageWriteLSN(p, lsn); return 0; }

Operation getLinearInsert() {
  Operation o = { 
    OPERATION_LINEAR_INSERT,
    SIZEOF_RECORD,
    OPERATION_UNDO_LINEAR_INSERT,
    &noop
  };
  return o;
}
Operation getLinearDelete() {
  Operation o = { 
    OPERATION_LINEAR_DELETE,
    SIZEOF_RECORD,
    OPERATION_UNDO_LINEAR_DELETE,
    &noop
  };
  return o;
}


Operation getUndoLinearInsert() {
  Operation o = { 
    OPERATION_UNDO_LINEAR_INSERT,
    SIZEOF_RECORD,
    OPERATION_NOOP,
    &operateUndoInsert
  };
  return o;
}

Operation getUndoLinearDelete() {
  Operation o = { 
    OPERATION_UNDO_LINEAR_DELETE,
    SIZEOF_RECORD,
    OPERATION_NOOP,
    &operateUndoDelete
  };
  return o;
}

void TlogicalHashInsert(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize) {

  /* Write undo-only log entry. */

  recordid  * headerRidB = pblHtLookup(openHashes, &hashRid.page, sizeof(int));
  
  assert(headerRidB);

  hashRid.size = keySize;
  Tupdate(xid, hashRid, key, OPERATION_LINEAR_INSERT);
  
  /* Perform redo-only insert. */
  hashRid.size = sizeof(recordid);
  ThashInstantInsert(xid, hashRid, key, keySize, val, valSize);
  instant_expand(xid, hashRid, headerNextSplit, headerHashBits, keySize, valSize); 

}
int TlogicalHashDelete(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize) {
  if(ThashLookup(xid, hashRid, key, keySize, val, valSize)) {
    undoDeleteArg * arg = malloc(sizeof(undoDeleteArg) + keySize+valSize);
    arg->keySize = keySize;
    arg->valSize = valSize;
    memcpy(arg+1, key, keySize);
    memcpy(((byte*)(arg+1)) + keySize, val, valSize);

    hashRid.size = sizeof(undoDeleteArg) + keySize + valSize;

    Tupdate(xid, hashRid, arg, OPERATION_LINEAR_DELETE);
    free(arg);
    hashRid.size = sizeof(recordid);
    ThashInstantDelete(xid, hashRid, key, keySize);
    return 1;
  } else {
    return 0;
  }
  
}


void instant_rehash(int xid, recordid hash, int next_split, int i, int keySize, int valSize);
void instant_update_hash_header(int xid, recordid hash, int i, int next_split);
int instant_deleteFromBucket(int xid, recordid hash, int bucket_number, recordid bucket_rid, const void * key, int keySize, recordid * deletedEntry);
void instant_insertIntoBucket(int xid, recordid hashRid, int bucket_number, recordid bucket_rid, hashEntry * e, int keySize, int valSize, recordid deletedEntry, int skipDelete);
int findInBucket(int xid, recordid hashRid, int bucket_number, const void * key, int keySize, void * val, int valSize);


/*int findInBucket(int xid, recordid hashRid, int bucket_number, const void * key, int keySize, void * val, int valSize) {

  hashEntry * e = malloc(sizeof(hashEntry) + keySize + valSize);
  recordid bucket = hashRid;
  recordid nextEntry;

  bucket.slot = bucket_number;
  Tread(xid, bucket, &nextEntry);

  if(nextEntry.size) {
    assert(nextEntry.size == sizeof(hashEntry) + keySize + valSize);
  }

  int found = 0;

  while(nextEntry.size > 0) {
    Tread(xid, nextEntry, e);
    if(!memcmp(key, e+1, keySize)) {
      memcpy(val, ((byte*)(e+1))+keySize, valSize);
      found = 1;
      break;
    }
    nextEntry = e->next;
  } 
  free(e);
  return found;
  }*/

int extendCount = 0;
void instant_expand (int xid, recordid hash, int next_split, int i, int keySize, int valSize) {
  extendCount ++;
  if(extendCount >= 70) {
    TarrayListInstantExtend(xid, hash, 100);
    int j;
    for(j = 0; j < 100; j++) {
      if(next_split >= twoToThe(i-1)+2) {
	i++;
	next_split = 2;
      }
      instant_rehash(xid, hash, next_split, i, keySize, valSize);
      next_split++;
    }
    instant_update_hash_header(xid, hash, i, next_split);
    extendCount = 0;
  }

}

void instant_update_hash_header(int xid, recordid hash, int i, int next_split) {
  recordid  * headerRidB = pblHtLookup(openHashes, &hash.page, sizeof(int));

  headerHashBits = i;
  headerNextSplit = next_split;
  
  TinstantSet(xid, hash, headerRidB);
}

void instant_rehash(int xid, recordid hashRid, int next_split, int i, int keySize, int valSize) {

  /* Picture of what's going on:
     
     [BucketA]->ba_contents-> ... -> A -> B -> C -> ...
	  
     [BucketB]->bb_contents-> ... -> D -> NULL
     
     We want to move to another bucket, but can't allow the page
     file to contain a set of pages where B, C, or D are
     inaccesible.
     
     Here is the set of pointers that we want:
     
     [BucketA]->ba_contents-> ... -> A -> C -> ...
     
     [BucketB]->bb_contents-> D -> B
     
     Here is the order in which we must write pages:
     
     D -> B
     A -> C
     B -> NULL
     
     We do this in a while loop until the buckets are split.  
     
     Once this is queued up for the log, we can write the new
     metadate for the hash table, and release our locks on A and
     B.
     
     On recovery, if the metadata is stale, then we look for the
     first entry in BucketB that is also in BucketA  and do this:
     
     (Duplicate, B,  should be in bucket B)
     
     [BucketA]->ba_contents-> ... -> A -> B -> C
     [BucketB]->ba_contents-> ... -> D -> B -> C
     
     A -> C
     B -> NULL
     
     Next case (Duplicate, C, should be in bucket A):
     
     [BucketA]->ba_contents-> ... -> A -> C -> ... 
     [BucketB]->ba_contents-> ... -> D -> B -> C -> ...
     
     B -> NULL
     
     Now that there are no duplicates, we simply re-run split
     (whether we found a duplicate, or not).
  */


  /** @todo Actually implement recovery for linearHash. */
  
  int firstA = 1;  /* Is 'A' the recordid of a bucket? */
  int firstD = 1;  /* What about 'D'? */

  assert(hashRid.size == sizeof(recordid));

  recordid ba = hashRid; ba.slot = next_split;
  recordid bb = hashRid; bb.slot = next_split + twoToThe(i-1);
  recordid NULLRID; NULLRID.page = 0; NULLRID.slot=0; NULLRID.size = 0;
  
  recordid ba_contents; Tread(xid, ba, &ba_contents);
  recordid bb_contents = NULLRID; 
  TinstantSet(xid, bb, &bb_contents);/*Tread(xid, bb, &bb_contents);*/

  recordid A = ba; /*ba_contents; */
  recordid D = bb; /*bb_contents; */
  recordid B = ba_contents;
  recordid C; 
  
  
  hashEntry * D_contents = calloc(1,sizeof(hashEntry) + keySize + valSize);
  hashEntry * A_contents = calloc(1,sizeof(hashEntry) + keySize + valSize);
  hashEntry * B_contents = calloc(1,sizeof(hashEntry) + keySize + valSize);

  while(B.size) {
    assert(B.size == sizeof(hashEntry) + keySize + valSize);
    Tread(xid, B, B_contents);
    C = B_contents->next;

    int old_hash = hash(B_contents+1, keySize, i-1, ULONG_MAX) + 2;
    int new_hash = hash(B_contents+1, keySize, i,   ULONG_MAX) + 2;

    assert(next_split == old_hash); 
    assert(new_hash   == old_hash || new_hash == old_hash + twoToThe(i-1));
 
    if(new_hash == old_hash) {
      A = B;
      B = C;
      C.size = -1;
      firstA = 0;
    } else {
      if(firstD) {
	/* D is a bucket entry */ 
	assert(B.size == sizeof(hashEntry) + keySize + valSize);
	assert(D.size == sizeof(recordid));
	TinstantSet(xid, D, &B);
      } else {
	/* D is the tail of our list. */
	assert(D.size == sizeof(hashEntry) + keySize + valSize);
	assert(B.size == 0 || B.size == sizeof(hashEntry) + keySize + valSize);
	Tread(xid, D, D_contents);
	D_contents->next = B;
	TinstantSet(xid, D, D_contents);
      }

      if(firstA) {
	assert(C.size == 0 || C.size == sizeof(hashEntry) + keySize + valSize);
	assert(A.size == sizeof(recordid));
	TinstantSet(xid, A, &C);
      } else {
	/* A is somewhere in the first list. */
	assert(A.size == sizeof(hashEntry) + keySize + valSize);
	assert(C.size == 0 || C.size == sizeof(hashEntry) + keySize + valSize);
	Tread(xid, A, A_contents);
	A_contents->next = C;
	TinstantSet(xid, A, A_contents);
      }

      /* B _can't_ be a bucket.*/


      assert(B.size == sizeof(hashEntry) + keySize + valSize);
      Tread(xid, B, B_contents);
      B_contents->next = NULLRID;
      TinstantSet(xid, B, B_contents);

      /* Update Loop State */
      D = B;
      B = C;
      C.size = -1;
      firstD = 0;
    }
  }
  free(D_contents);
  free(A_contents);
  free(B_contents);

}
/** @todo fine grained locking for insertIntoBucket and the other operations in this file. */
void instant_insertIntoBucket(int xid, recordid hashRid, int bucket_number, recordid bucket_contents, hashEntry * e, int keySize, int valSize, recordid newEntry, int skipDelete) {
  assert(newEntry.size == (sizeof(hashEntry) + keySize + valSize));
  recordid deleteMe; 
  if(!skipDelete) {
    if(instant_deleteFromBucket(xid, hashRid, bucket_number, bucket_contents, e+1, keySize, &deleteMe)) {
      /*      Tdealloc(xid, deleteMe); */
      hashRid.slot = bucket_number;
      Tread(xid, hashRid, &bucket_contents);
      hashRid.slot = 0;
    }
  }

  /*@todo consider recovery for insertIntoBucket. */

  recordid bucket   = hashRid;
  bucket.slot = bucket_number;
  assert(bucket_contents.size == 0 || bucket_contents.size == sizeof(hashEntry) + keySize + valSize);
  assert(newEntry.size == sizeof(hashEntry) + keySize + valSize);
  e->next = bucket_contents;
  TinstantSet(xid, newEntry, e);
  TinstantSet(xid, bucket, &newEntry);
}

int instant_deleteFromBucket(int xid, recordid hash, int bucket_number, recordid bucket_contents, 
			     const void * key, int keySize, recordid * deletedEntry) {
  hashEntry * e;
  recordid bucket = hash;
  bucket.slot = bucket_number;
  recordid nextEntry;
  nextEntry = bucket_contents;

  if(nextEntry.size) {
    e = calloc(1,nextEntry.size);
  } else {
    e = calloc(1,1);
  }
  int first = 1;
  int found = 0;
  recordid lastEntry;
  while(nextEntry.size > 0) {
    Tread(xid, nextEntry, e);
    if(!memcmp(key, e+1, keySize)) {
      if(first) {
	assert(e->next.size < 40);
	TinstantSet(xid, bucket, &(e->next));
      } else {
	recordid next = e->next;
	Tread(xid, lastEntry, e);
	assert(next.size < 40);
	e->next = next;
	TinstantSet(xid, lastEntry, e);
      }
      *deletedEntry = nextEntry;
      found = 1;
      break;
    }
    lastEntry = nextEntry;
    first = 0;
    nextEntry = e->next;
  }
  free(e);
  return found;
}

/* Same as normal implementation, so commented it out. 
   / *
recordid ThashAlloc(int xid, int keySize, int valSize) {
  / * Want 16 buckets, doubling on overflow. * /
  recordid rid = TarrayListAlloc(xid, 16, 2, sizeof(recordid)); 
  TarrayListExtend(xid, rid, 32+2);

  recordid headerRidA;
  recordid  * headerRidB = malloc (sizeof(recordid));

  headerKeySize = keySize;
  headerValSize = valSize;
  
  headerNextSplit = INT_MAX;
  headerHashBits  = 4;

  rid.slot =0;
  Tset(xid, rid, &headerRidA);
  rid.slot =1;
  Tset(xid, rid, headerRidB);

  pblHtInsert(openHashes, &rid.page, sizeof(int), headerRidB);

  rid.slot =0;
  return rid;
}
*/
/*
void ThashInit() {
  openHashes = pblHtCreate();
}

void ThashDeinit() {
  pblHtDelete(openHashes);
}
*/
void ThashInstantInsert(int xid, recordid hashRid, 
	    const void * key, int keySize, 
	    const void * val, int valSize) {

  recordid  * headerRidB = pblHtLookup(openHashes, &hashRid.page, sizeof(int));
  
  assert(headerRidB);
  
  int bucket = hash(key, keySize, headerHashBits, headerNextSplit - 2) + 2;
  
  hashEntry * e = calloc(1,sizeof(hashEntry) + keySize + valSize);
  memcpy(e+1, key, keySize);
  memcpy(((byte*)(e+1)) + keySize, val, valSize);

  /** @todo instantAlloc / instantDealloc */
  recordid newEntry =  Talloc(xid, sizeof(hashEntry) + keySize + valSize);
  recordid bucket_contents;

  hashRid.slot = bucket;
  Tread(xid, hashRid, &bucket_contents);
  hashRid.slot = 0;
  instant_insertIntoBucket(xid, hashRid, bucket, bucket_contents, e, keySize, valSize, newEntry, 0);

  free(e);

}
/** @todo hash hable probably should track the number of items in it,
    so that expand can be selectively called. */
void ThashInstantDelete(int xid, recordid hashRid, 
	    const void * key, int keySize) {
  recordid  * headerRidB = pblHtLookup(openHashes, &hashRid.page, sizeof(int));
  recordid tmp = hashRid;
  tmp.slot = 1;

  int bucket_number = hash(key, keySize, headerHashBits, headerNextSplit - 2) + 2;
  recordid deleteMe;
  hashRid.slot = bucket_number;
  recordid bucket_contents;
  Tread(xid, hashRid, &bucket_contents);
  hashRid.slot = 0;
  if(instant_deleteFromBucket(xid, hashRid, bucket_number, bucket_contents, key, keySize, &deleteMe)) {
    /*    Tdealloc(xid, deleteMe);  */
  }
}

/*int ThashOpen(int xid, recordid hashRid) {
  recordid * headerRidB = malloc(sizeof(recordid));
  hashRid.slot = 1;
  Tread(xid, hashRid, headerRidB);
  
  pblHtInsert(openHashes, &hashRid.page, sizeof(int), headerRidB);

  return 0;
  }*/

void TlogicalHashUpdate(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize) {
  void * dummy = malloc(valSize);
  TlogicalHashDelete(xid, hashRid, key, keySize, dummy, valSize);
  free(dummy);
  TlogicalHashInsert(xid, hashRid, key, keySize, val, valSize);

}
/*

int ThashClose(int xid, recordid hashRid) {
  recordid * freeMe = pblHtLookup(openHashes,  &hashRid.page, sizeof(int));
  pblHtRemove(openHashes, &hashRid.page, sizeof(int));
  free(freeMe);
  return 0;
}

int ThashLookup(int xid, recordid hashRid, void * key, int keySize, void * buf, int valSize) {

  recordid  * headerRidB = pblHtLookup(openHashes, &hashRid.page, sizeof(int));
  recordid tmp = hashRid;
  tmp.slot = 1;
  int bucket_number = hash(key, keySize, headerHashBits, headerNextSplit - 2) + 2;
  int ret = findInBucket(xid, hashRid, bucket_number, key, keySize, buf, valSize);
  return ret;
}

*/
