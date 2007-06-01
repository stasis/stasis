
#include <lladd/operations.h>
#include <lladd/hash.h>
#include <limits.h>
#include <assert.h>
#include <stdlib.h>
#include "../page.h"

/**

   A from-scratch implementation of linear hashing.  Uses the
   arrayList operations to implement its hashbuckets.
 
	 Also, uses same on disk format as naiveLinearHash, and uses
	 that structures methods when appropriate.
*/

#define BUCKETS_OFFSET (2)

#define headerKeySize (headerRidA->page)
#define headerValSize (headerRidA->slot)

#define headerHashBits (headerRidB->page)
#define headerNextSplit (headerRidB->slot)

#include <math.h>
#include <string.h>
#include <lladd/operations/linearHash.h>
#include <pbl/pbl.h>

typedef struct {
  recordid next;
} hashEntry;

void instant_expand (int xid, recordid hash, int next_split, int i, int keySize, int valSize);

extern pthread_mutex_t linearHashMutex;// = PTHREAD_MUTEX_INITIALIZER;

extern pblHashTable_t * openHashes ;

static int operateUndoInsert(int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) {
  
  int keySize = rid.size;
  int valSize = rid.slot;
  rid.slot = 0;
//  rid.size = sizeof(recordid);
  rid.slot = sizeof(hashEntry) + keySize + valSize;

  if(!pblHtLookup(openHashes, &rid.page, sizeof(int))) {
    abort();
    /*    ThashOpen(xid, rid); */
  }

  ThashInstantDelete(xid, rid, dat, keySize, valSize);
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
  /*  TreadUnlocked(xid, dereferenceArrayListRid(p, rid.slot), &headerRidA); */
  /*  TreadUnlocked(xid, rid, &headerRidA); */

  assert(keySize == sizeof(int));
  assert(valSize == sizeof(recordid));

  rid.size = sizeof(hashEntry) + keySize + valSize;

  ThashInstantInsert(xid, rid, argBytes, keySize, 
		     argBytes + keySize, valSize);
  return 0;
}
//statiint noop (int xid, Page * p, lsn_t lsn, recordid rid, const void * dat) { pageWriteLSN(xid, p, lsn); return 0; }

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


  hashRid.slot = valSize;
  hashRid.size = keySize;
  TupdateRaw(xid, hashRid, key, OPERATION_LINEAR_INSERT);
  
  /* Perform redo-only insert. */
  hashRid.size = sizeof(hashEntry) + keySize + valSize;

  ThashInstantInsert(xid, hashRid, key, keySize, val, valSize);

  pthread_mutex_lock(&linearHashMutex); /* @todo Finer grained locking for linear hash's expand? */
  recordid  * headerRidB = pblHtLookup(openHashes, &hashRid.page, sizeof(int));
  
  assert(headerRidB);
  instant_expand(xid, hashRid, headerNextSplit, headerHashBits, keySize, valSize); 
  pthread_mutex_unlock(&linearHashMutex);



}
int TlogicalHashDelete(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize) {
  if(TnaiveHashLookup(xid, hashRid, key, keySize, val, valSize)) {
    undoDeleteArg * arg = malloc(sizeof(undoDeleteArg) + keySize+valSize);
    arg->keySize = keySize;
    arg->valSize = valSize;
    memcpy(arg+1, key, keySize);
    memcpy(((byte*)(arg+1)) + keySize, val, valSize);

    hashRid.size = sizeof(undoDeleteArg) + keySize + valSize;

    TupdateRaw(xid, hashRid, arg, OPERATION_LINEAR_DELETE);
    hashRid.size = sizeof(hashEntry) + keySize + valSize;
    free(arg);
    /*    hashRid.size = sizeof(recordid); */
    ThashInstantDelete(xid, hashRid, key, keySize, valSize);
    return 1;
  } else {
    return 0;
  }
  
}


void instant_rehash(int xid, recordid hash, int next_split, int i, int keySize, int valSize);
void instant_update_hash_header(int xid, recordid hash, int i, int next_split);
int instant_deleteFromBucket(int xid, recordid hash, int bucket_number, hashEntry * bucket_contents, 
			     const void * key, int keySize, int valSize, recordid * deletedEntry);
void instant_insertIntoBucket(int xid, recordid hashRid, int bucket_number, hashEntry * bucket_contents, 
		      hashEntry * e, int keySize, int valSize, int skipDelete);
int findInBucket(int xid, recordid hashRid, int bucket_number, const void * key, int keySize, void * val, int valSize);


extern pthread_mutex_t exp_mutex, exp_slow_mutex;

/** @todo Pulled out amortization code.  (Will add it back in once everything is
    stable without it.)  You must hold linearHashMutex to call this function, 
    which will release, and reaquire the mutex for you, as apprpriate. */
      
void instant_expand (int xid, recordid hash, int next_split, int i, int keySize, int valSize) {
  /* Total hack; need to do this better, by storing stuff in the hash table headers.*/
  /*  static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
      static pthread_mutex_t slow_mutex = PTHREAD_MUTEX_INITIALIZER; */

/*  static int count = 4096 * .25;

  pthread_mutex_lock(&exp_mutex);
  count --;
  int mycount = count;
  pthread_mutex_unlock(&exp_mutex);

#define AMORTIZE 1000
#define FF_AM    750

if(mycount <= 0 && !(mycount * -1) % FF_AM) { */
    pthread_mutex_lock(&exp_slow_mutex);

//    int j;
    TarrayListInstantExtend(xid, hash, 1 /*AMORTIZE*/);

  //  pthread_mutex_lock(&linearHashMutex);  //Already hold this!
    
    recordid * headerRidB = pblHtLookup(openHashes, &(hash.page), sizeof(int));

    //for(j = 0; j < AMORTIZE; j++) {

      if(next_split >= twoToThe(i-1)+2) {
	i++;
	next_split = 2;
      } 
      
      lockBucket(next_split);

      int other_bucket = next_split + twoToThe(i-1);
      lockBucket(other_bucket);

      pthread_mutex_unlock(&linearHashMutex);

      instant_rehash(xid, hash, next_split, i, keySize, valSize); 


      pthread_mutex_lock(&linearHashMutex);

      unlockBucket(next_split);
      unlockBucket(other_bucket);
      next_split++;
      headerNextSplit = next_split;
      headerHashBits = i; 

    //}
    instant_update_hash_header(xid, hash, i, next_split);  

    pthread_mutex_unlock(&linearHashMutex);
    pthread_mutex_unlock(&exp_slow_mutex);
//  }
}



void instant_update_hash_header(int xid, recordid hash, int i, int next_split) {
  recordid  * headerRidB = pblHtLookup(openHashes, &hash.page, sizeof(int));

  headerHashBits = i;
  headerNextSplit = next_split;
  hash.slot = 1;
  TinstantSet(xid, hash, headerRidB);
}

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
	@todo Actually implement recovery for linearHash. 
	
*/
/**
    If a bucket split may have been interrupted, call this function to restore
    logical consistency to the buckets' linked lists.  Then, call instant_rehash
    to complete the split.
    
    @XXX Need to test recover_split, and figure out where to call it!
 */
static void recover_split(int xid, recordid hashRid, int i, int next_split, int keySize, int valSize) __attribute__((unused));

static void recover_split(int xid, recordid hashRid, int i, int next_split, int keySize, int valSize) {
  // This function would be simple, except for the ridiculous mount
  // of state that it must maintain.  See above for a description of what it does.
  recordid headerRidBstack;
  recordid * headerRidB = &headerRidBstack;
  hashRid.slot = 1;
  Tread(xid, hashRid, headerRidB);
  hashRid.slot = 0;
  recordid ba = hashRid; ba.slot = next_split;
  recordid bb = hashRid; bb.slot = next_split + twoToThe(i-1);
  
  if(headerHashBits <= i && headerNextSplit <= next_split) {
  
    // The split has not been completed.  First, look for a duplicate entry in 
    // the two linked lists.
    pblHashTable_t * firstList = pblHtCreate();
    recordid * next = malloc(hashRid.size);
    recordid current_rid;
    recordid last_rid;
    last_rid = NULLRID;
    current_rid = hashRid;
    Tread(xid, ba, next);
    while(next->size) { // size = 0 -> nothing in bucket.  size = -1 -> last bucket.
      
      // Add an entry to the hash table:
      //      key -> recordid of entry pointing to current entry
      
      byte * val = malloc(sizeof(recordid));
      memcpy(val, &last_rid, sizeof(recordid));
      pblHtInsert(firstList, next+1, keySize, val);
      
      if(next->size == -1)
	break; 
      
      last_rid = current_rid;
      current_rid = *next;
      Tread(xid, current_rid, next);
    }
    
    // firstList now contains a copy of each entry in the first list.
    
    // Find any duplicates.
   
    Tread(xid, bb, next);
    int foundDup = 0;
    recordid lastRead = bb;
    recordid * A;
    while(next->size) {
      if(NULL != (A = pblHtLookup(firstList, next+1, keySize))) {
	 foundDup = 1;
	 break;
      }
      if(next->size == -1) 
	 break;
      lastRead = *next;
      Tread(xid, *next, next);
    }
    if(foundDup) {
      long new_hash = hash(next+1, keySize, i,   UINT_MAX) + 2;  
      if(new_hash == next_split) {
	  // set B->next = 0 
	  *next = NULLRID;
	  TinstantSet(xid, lastRead, next);
	
      } else {
	  // set A->next = C
	  if(A->size != 0 && A->size != -1) {
	    hashEntry * A_contents = malloc(hashRid.size);
	    Tread(xid, *A, A_contents);
	    A_contents->next = *next;
	    TinstantSet(xid, *A, A_contents);
	  } else {
	    // B is a bucket. :o
	    
	    void * C_contents = malloc(next->size);
	    
	    // Copy the next item in the list into the bucket (C)
	    Tread(xid, *next, C_contents);
	    TinstantSet(xid, bb, C_contents);
	    
	    // And free the record used by C
	    Tdealloc(xid, *next);
	    
	    free(C_contents);
	    
	  }
	  // set B->next = 0  We don't care if it's a bucket.
	  *next = NULLRID;
	  TinstantSet(xid, lastRead, next);
      }
    }
  }
}
void instant_rehash(int xid, recordid hashRid, int next_split, int i, int keySize, int valSize) {
  int firstA = 1;  // Is 'A' the recordid of a bucket? 
  int firstD = 1;  // What about 'D'? 

  /*  assert(hashRid.size == sizeof(recordid)); */
  assert(hashRid.size == sizeof(hashEntry) + keySize + valSize);
  recordid ba = hashRid; ba.slot = next_split;
  recordid bb = hashRid; bb.slot = next_split + twoToThe(i-1);
  
  //  recordid ba_contents; TreadUnlocked(xid, ba, &ba_contents);
  //  recordid bb_contents = NULLRID; 
  /*  Tset(xid, bb, &bb_contents); */ //TreadUnlocked(xid, bb, &bb_contents);

  hashEntry * D_contents = calloc(1,sizeof(hashEntry) + keySize + valSize);
  hashEntry * A_contents = calloc(1,sizeof(hashEntry) + keySize + valSize);
  hashEntry * B_contents = calloc(1,sizeof(hashEntry) + keySize + valSize);

  TreadUnlocked(xid, ba, A_contents);
  TreadUnlocked(xid, bb, D_contents);
  recordid A = ba; //ba_contents; 
  recordid D = bb; //bb_contents; 
  recordid B = A_contents->next;
  recordid C; 
  
  if(!A_contents->next.size) { 
    /* Bucket A is empty, so we're done. */
    free(D_contents);
    free(A_contents);
    free(B_contents);
    /*    printf("Expand was a noop.\n");
	  fflush(NULL); */
    return; 
  }
  while(D_contents->next.size) {
    firstD = 0;
    if(D_contents->next.size != -1) {
      D = D_contents->next;
      TreadUnlocked(xid, D, D_contents);
    } else {
      abort(); // Got here?  We're starting a new bucket, but found that it is already -1 terminated...
    }
  } 

  int old_hash;
  int new_hash = hash(A_contents+1, keySize, i,   UINT_MAX) + 2;

  while(new_hash != next_split) {
    // Move things into the new bucket until we find something that belongs in the first bucket... 
    
    recordid oldANext = A_contents->next;

    A_contents->next = NULLRID;

    if(firstD) {
      //      assert(memcmp(&A_contents->next, &D_contents->next, sizeof(recordid)));
      TinstantSet(xid, D, A_contents);
      firstD = 0;
    } else {
      /* D at end of list => can overwrite next. */
      D_contents->next = Talloc(xid, sizeof(hashEntry) + keySize + valSize); /* @todo
										unfortunate
										to
										dealloc
										A's
										successor,
										then
										alloc.. */
      //      assert(memcmp(&A_contents->next, &D_contents->next, sizeof(recordid)));
      TinstantSet(xid, D_contents->next, A_contents);
      //      assert(memcmp(&D, &D_contents->next, sizeof(recordid)));
      TinstantSet(xid, D, D_contents);
      D = A;
    }
    hashEntry * swap = D_contents;
    D_contents = A_contents;
    A_contents = swap;

    /* A_contents is now garbage. */

    assert(A.size == sizeof(hashEntry) + keySize + valSize);
    if(oldANext.size == -1) {
      memset(A_contents, 0, sizeof(hashEntry) + keySize + valSize);
      A_contents->next.size = -1; // added
      //      assert(memcmp(&A_contents->next, &A, sizeof(recordid)));
      TinstantSet(xid, A, A_contents);
      free(D_contents);
      free(A_contents);
      free(B_contents);
      /*      printf("Loop 1 returning.\n");
	      fflush(NULL); */
      return;
    } 
    assert(oldANext.size == sizeof(hashEntry) + keySize + valSize);
    TreadUnlocked(xid, oldANext, A_contents);
    //    assert(memcmp(&A_contents->next, &A, sizeof(recordid)));
    TinstantSet(xid, A, A_contents);
    Tdealloc(xid, oldANext);
    
    new_hash = hash(A_contents+1, keySize, i,   UINT_MAX) + 2;
  }
  /*  printf("Got past loop 1\n");
      fflush(NULL); */

  B = A_contents->next;

  while(B.size != -1) {
    assert(B.size == sizeof(hashEntry) + keySize + valSize);
    TreadUnlocked(xid, B, B_contents);
    C = B_contents->next;

    old_hash = hash(B_contents+1, keySize, i-1, UINT_MAX) + 2;
    new_hash = hash(B_contents+1, keySize, i,   UINT_MAX) + 2;

    assert(next_split == old_hash); 
    assert(new_hash   == old_hash || new_hash == old_hash + twoToThe(i-1));
 
    if(new_hash == old_hash) {
      A = B;
      B = C;
      C.size = -1;
      firstA = 0;
    } else {

  // D is the tail of our list. 
	assert(D.size == sizeof(hashEntry) + keySize + valSize);
	assert(B.size == -1 || B.size == sizeof(hashEntry) + keySize + valSize);
	TreadUnlocked(xid, D, D_contents); 
	D_contents->next = B;
	assert(B.size != 0);
	//	assert(memcmp(&D, &D_contents->next, sizeof(recordid)));
	TinstantSet(xid, D, D_contents);

	// A is somewhere in the first list. 
	assert(A.size == sizeof(hashEntry) + keySize + valSize);
	assert(C.size == -1 || C.size == sizeof(hashEntry) + keySize + valSize);
	TreadUnlocked(xid, A, A_contents);
	A_contents->next = C;
	assert(C.size != 0);
	

	//	assert(memcmp(&A, &A_contents->next, sizeof(recordid)));

	TinstantSet(xid, A, A_contents);
	/*   } */

      // B _can't_ be a bucket.


      assert(B.size == sizeof(hashEntry) + keySize + valSize);
      TreadUnlocked(xid, B, B_contents);
      B_contents->next = NULLRID;
      TinstantSet(xid, B, B_contents);

      // Update Loop State 
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

void instant_insertIntoBucket(int xid, recordid hashRid, int bucket_number, hashEntry * bucket_contents, 
		      hashEntry * e, int keySize, int valSize, int skipDelete) {

  assert(hashRid.size == sizeof(hashEntry) + valSize + keySize);
  recordid deleteMe; 
  if(!skipDelete) {
    if(instant_deleteFromBucket(xid, hashRid, bucket_number, bucket_contents, e+1, keySize, valSize, &deleteMe)) {
      if(deleteMe.size) {
	Tdealloc(xid, deleteMe);
	hashRid.slot = bucket_number;
	assert(hashRid.size == sizeof(hashEntry) + valSize + keySize);
	TreadUnlocked(xid, hashRid, bucket_contents);
	hashRid.slot = 0;
      }
    }
  }

  /*@todo consider recovery for insertIntoBucket. */

  hashRid.slot = bucket_number;

  TreadUnlocked(xid, hashRid, bucket_contents);

  assert(hashRid.size == sizeof(hashEntry) + keySize + valSize);
  
  /* hashRid points to bucket_contents 
     e contains the data to be inserted. */
  
  if(!bucket_contents->next.size) {  // Size = 0 -> nothing in bucket.  Size != 0 -> bucket occupied.
    /* Change: 

      bucket(empty) -> NIL
    
     To: 
    
      bucket(e)     -> NIL
    */
    e->next.page = 0;
    e->next.slot = 0;
    e->next.size = -1;
    TinstantSet(xid, hashRid, e);
  } else {
    /* Change:
    
       bucket(entry) -> *
    
    To:
    
       bucket(entry) -> *
       newEntry -> *
    */

    recordid newEntry =  Talloc(xid, sizeof(hashEntry) + keySize + valSize);
    assert(newEntry.size == sizeof(hashEntry) + keySize + valSize);

    e->next = bucket_contents->next;
    TinstantSet(xid, newEntry, e);
    
    /*
    And then finally to:
    
       bucket(entry) -> newEntry -> *
    
    */

    assert(hashRid.size == sizeof(hashEntry) + keySize + valSize);

    bucket_contents->next = newEntry;
    TinstantSet(xid, hashRid,  bucket_contents);
  }

}

int instant_deleteFromBucket(int xid, recordid hash, int bucket_number, hashEntry * bucket_contents,
		     const void * key, int keySize, int valSize, recordid * deletedEntry) {
  if(bucket_contents->next.size == 0) { return 0; }
  
  recordid this = hash;
  this.slot = bucket_number;

  int found = 0;
  if(!memcmp(bucket_contents+1, key, keySize)) {
    /* The bucket contains the entry that should be deleted. */
    if(deletedEntry)
      deletedEntry->size = 0;  /* size = 0 -> Tell caller not to deallocate the 
				  memory used by this entry.  (the entry is in a
				  bucket!) */
    
    if(bucket_contents->next.size == -1) {
      /* There isn't anything else in this bucket. */
      memset(bucket_contents, 0, sizeof(hashEntry) + keySize + valSize);
      TinstantSet(xid, this, bucket_contents);
    } else {
      /* There is something else in this bucket, so copy it into the bucket */
      assert(bucket_contents->next.size ==  sizeof(hashEntry) + keySize + valSize);
      recordid oldNext = bucket_contents->next;
      TreadUnlocked(xid, bucket_contents->next, bucket_contents);
      TinstantSet(xid, this, bucket_contents);
      *deletedEntry = oldNext; /* @todo delete from bucket really should do its own deallocation.. */
    }
    return 1;
  }
  /* We didn't find anything in the bucket's record.  Is there anything else in 
     this bucket? */
  if(bucket_contents->next.size == -1) { return 0; }
  
  // if so, things become complicated.

  hashEntry * A = malloc(sizeof(hashEntry) + keySize + valSize);
  hashEntry * B = malloc(sizeof(hashEntry) + keySize + valSize);

  recordid Aaddr, Baddr;

  memcpy(B, bucket_contents, sizeof(hashEntry) + keySize + valSize);
  Baddr = this;
  while(B->next.size != -1) {
    hashEntry * tmp = A;
    A = B;
    Aaddr = Baddr;
    B = tmp;
    assert(A->next.size == sizeof(hashEntry) + keySize + valSize);
    Baddr = A->next;
    TreadUnlocked(xid, Baddr, B);
    // At this point, A points at B, and we're considering B for deletion. 

    if(!memcmp(B+1, key, keySize)) {
      /* If B is the entry that is to be deleted, update A so that it points to
	 B's successor. */
      A->next = B->next;
      assert(Aaddr.size == sizeof(hashEntry) + keySize + valSize);
      TinstantSet(xid, Aaddr, A);
      if(deletedEntry) { 
	*deletedEntry = Baddr;
      }
      found = 1; 
      break;
    }

  }

  free(A);
  free(B);

  return found;
}


void ThashInstantInsert(int xid, recordid hashRid, 
	    const void * key, int keySize, 
	    const void * val, int valSize) {

  pthread_mutex_lock(&linearHashMutex);

  recordid  * headerRidB = pblHtLookup(openHashes, &hashRid.page, sizeof(int));
  
  assert(headerRidB);
  
  int bucket = lockBucketForKey(key, keySize, headerRidB);
	      
  headerRidB = NULL;
  
  pthread_mutex_unlock(&linearHashMutex);

  hashEntry * e = calloc(1,sizeof(hashEntry) + keySize + valSize);
  memcpy(e+1, key, keySize);
  memcpy(((byte*)(e+1)) + keySize, val, valSize);

  /** @todo instantAlloc / instantDealloc */
  /*   recordid newEntry =  Talloc(xid, sizeof(hashEntry) + keySize + valSize); */
  hashEntry *  bucket_contents = malloc(sizeof(hashEntry) + keySize + valSize);

  hashRid.slot = bucket;
  TreadUnlocked(xid, hashRid, bucket_contents);
  hashRid.slot = 0;
  instant_insertIntoBucket(xid, hashRid, bucket, bucket_contents, e, keySize, valSize, 0);

  pthread_mutex_lock(&linearHashMutex);
  unlockBucket(bucket);
  pthread_mutex_unlock(&linearHashMutex);

  free(e);

}
/** @todo hash table probably should track the number of items in it,
    so that expand can be selectively called. */
void ThashInstantDelete(int xid, recordid hashRid, 
	    const void * key, int keySize, int valSize) {

  pthread_mutex_lock(&linearHashMutex);


  recordid  * headerRidB = pblHtLookup(openHashes, &hashRid.page, sizeof(int));
  recordid tmp = hashRid;
  tmp.slot = 1;

  int bucket_number = lockBucketForKey(key, keySize, headerRidB);
  headerRidB = NULL;

  pthread_mutex_unlock(&linearHashMutex);

  recordid deleteMe;
  hashRid.slot = bucket_number;
  hashEntry * bucket_contents = malloc(sizeof(hashEntry) + keySize + valSize);
  TreadUnlocked(xid, hashRid, bucket_contents);
  hashRid.slot = 0;
  if(instant_deleteFromBucket(xid, hashRid, bucket_number, bucket_contents, key, keySize, valSize, &deleteMe)) {

    pthread_mutex_lock(&linearHashMutex);
    unlockBucket(bucket_number);
    pthread_mutex_unlock(&linearHashMutex);
    if(deleteMe.size) {
      Tdealloc(xid, deleteMe);
    }
  }
}

/** @todo  TlogicalHashUpdate is not threadsafe, is very slow, and is otherwise in need of help */
void TlogicalHashUpdate(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize) {
  void * dummy = malloc(valSize);
  TlogicalHashDelete(xid, hashRid, key, keySize, dummy, valSize);
  free(dummy);
  TlogicalHashInsert(xid, hashRid, key, keySize, val, valSize);

}

int TlogicalHashLookup(int xid, recordid hashRid, void * key, int keySize, void * buf, int valSize) {
  pthread_mutex_lock(&linearHashMutex);
  recordid  * headerRidB = pblHtLookup(openHashes, &(hashRid.page), sizeof(int));
  /*  printf("lookup header: %d %d\n", headerHashBits, headerNextSplit); */
  recordid tmp = hashRid;
  tmp.slot = 1;

  int bucket_number = lockBucketForKey(key, keySize, headerRidB);
  
  pthread_mutex_unlock(&linearHashMutex);
  int ret = findInBucket(xid, hashRid, bucket_number, key, keySize, buf, valSize);

  pthread_mutex_lock(&linearHashMutex);
  unlockBucket(bucket_number);
  pthread_mutex_unlock(&linearHashMutex);
  return ret;
}



linearHash_iterator * TlogicalHashIterator(int xid, recordid hashRid) {
  linearHash_iterator * ret = malloc(sizeof(linearHash_iterator));
  ret->current_hashBucket = 0;
  ret->current_rid = NULLRID;
  ret->current_rid.slot = 2;
  return ret;
}
void TlogicalHashIteratorFree(linearHash_iterator * it) {
  free(it);
}
linearHash_iteratorPair TlogicalHashIteratorNext(int xid, recordid hashRid, linearHash_iterator * it, int keySize, int valSize) {
  recordid  * headerRidB = pblHtLookup(openHashes, &hashRid.page, sizeof(int));
  hashEntry * e = malloc(sizeof(hashEntry) + keySize + valSize);
  
  linearHash_iteratorPair p;// = malloc(sizeof(linearHash_iteratorPair));
    
  //next.size == 0 -> empty bucket.  == -1 -> end of list.
  int inBucket = 0;
  //while(!memcmp(&(it->current_rid), &(NULLRID), sizeof(recordid)) 
 // printf("--- %d %d %d\n", it->current_rid.size, it->current_hashBucket, max_bucket(headerHashBits, headerNextSplit)); fflush(NULL);	 
  int found = 0;
  while(/*it->current_rid.size == -1
	&& */it->current_hashBucket <= max_bucket(headerHashBits, headerNextSplit)) {
    hashRid.slot = it->current_hashBucket;
    Tread(xid, hashRid, e);
    it->current_rid = hashRid;
    it->current_hashBucket++;
    if(e->next.size == 0) {
//      printf("aaa {%d, %d, %d} {%d, %d, %d} %d %d\n", e->next.page, e->next.slot, e->next.size, it->current_rid.page, it->current_rid.slot, it->current_rid.size, it->current_hashBucket, max_bucket(headerHashBits, headerNextSplit)); fflush(NULL);	 
      inBucket = 1;
    } else {
      found = 1;
//      printf("bbb {%d, %d, %d} {%d, %d, %d} %d %d\n", e->next.page, e->next.slot, e->next.size, it->current_rid.page, it->current_rid.slot, it->current_rid.size, it->current_hashBucket, max_bucket(headerHashBits, headerNextSplit)); fflush(NULL);	 
      break;
    }      // else, it stays NULLRID.
  }
  if(it->current_hashBucket > max_bucket(headerHashBits, headerNextSplit)) {
      p.key   = NULL;
      p.value = NULL;
    it->current_rid = NULLRID;
    it->current_rid.slot = 2;
//      memcpy(&(it->current_rid), &(NULLRID), sizeof(recordid));
      it->current_hashBucket = 0;
  } else {
//      Tread(xid, e->next, e);
      p.key   = memcpy(malloc(keySize), e+1, keySize);
      p.value = memcpy(malloc(valSize), ((byte*)(e+1))+keySize, valSize);
      it->current_rid = e->next;
  }
  free(e);
  return p;
}
