#include <lladd/operations.h>
#include <lladd/hash.h>
#include <limits.h>
#include <assert.h>
#include <pthread.h>
/**

   A from-scratch implementation of linear hashing.  Uses the
   arrayList operations to implement its hashbuckets.

*/

#define BUCKETS_OFFSET (2)

#define headerKeySize (headerRidA->page)
#define headerValSize (headerRidA->slot)

#define headerHashBits (headerRidB->page)
#define headerNextSplit (headerRidB->slot)

#include <math.h>
#include <malloc.h>
#include <string.h>
#include <lladd/operations/linearHash.h>
#include <pbl/pbl.h>
/**
  next.size == 0 implies empty bucket
  next.size == -1 implies end of list
*/
typedef struct {
  recordid next;
} hashEntry;

pblHashTable_t * openHashes = NULL;
pblHashTable_t * lockedBuckets = NULL;
pthread_mutex_t linearHashMutex;
pthread_cond_t bucketUnlocked;

void lockBucket(int bucket) {
  while(pblHtLookup(lockedBuckets, &bucket, sizeof(int))) {
    pthread_cond_wait(&bucketUnlocked, &linearHashMutex);
  }
  pblHtInsert(lockedBuckets, &bucket, sizeof(int), (void*)1);
}

int lockBucketForKey(const byte * key, int keySize, recordid * headerRidB) {
  int bucket = hash(key, keySize, headerHashBits, headerNextSplit - 2) + 2;

  while(pblHtLookup(lockedBuckets, &bucket, sizeof(int))) {
    pthread_cond_wait(&bucketUnlocked, &linearHashMutex);
    bucket = hash(key, keySize, headerHashBits, headerNextSplit - 2) + 2;
  }
  
  pblHtInsert(lockedBuckets, &bucket, sizeof(int), (void *) 1 );
  return bucket;
}

void unlockBucket(int bucket) {
  pblHtRemove(lockedBuckets, &bucket, sizeof(int));
  pthread_cond_broadcast(&bucketUnlocked);
}

void rehash(int xid, recordid hash, int next_split, int i, int keySize, int valSize);
void update_hash_header(int xid, recordid hash, int i, int next_split);
int deleteFromBucket(int xid, recordid hash, int bucket_number, hashEntry * bucket_contents,
		     void * key, int keySize, int valSize, recordid * deletedEntry);
void insertIntoBucket(int xid, recordid hashRid, int bucket_number, hashEntry * bucket_contents, 
		      hashEntry * e, int keySize, int valSize, int skipDelete);
int findInBucket(int xid, recordid hashRid, int bucket_number, const void * key, int keySize, void * val, int valSize);


int findInBucket(int xid, recordid hashRid, int bucket_number, const void * key, int keySize, void * val, int valSize) {

  hashEntry * e = malloc(sizeof(hashEntry) + keySize + valSize);

  recordid nextEntry;

  hashRid.slot = bucket_number;
  nextEntry = hashRid;

  int found = 0;

  while(nextEntry.size != -1 && nextEntry.size != 0) {
    assert(nextEntry.size == sizeof(hashEntry) + keySize + valSize);
    Tread(xid, nextEntry, e);
    if(!memcmp(key, e+1, keySize) && e->next.size != 0) {
      memcpy(val, ((byte*)(e+1))+keySize, valSize);
      found = 1;
      break;
    }
    nextEntry = e->next;
  } 
  free(e);
  return found;
}


void expand (int xid, recordid hash, int next_split, int i, int keySize, int valSize) {
  /* Total hack; need to do this better, by storing stuff in the hash table headers.*/
  static int count = 4096 * .25;
  count --;
#define AMORTIZE 1000
#define FF_AM    750
  if(count <= 0 && !(count * -1) % FF_AM) {
    recordid * headerRidB = pblHtLookup(openHashes, &(hash.page), sizeof(int));
    int j;
    TarrayListExtend(xid, hash, AMORTIZE);
    for(j = 0; j < AMORTIZE; j++) {

      if(next_split >= twoToThe(i-1)+2) {
	i++;
	next_split = 2;
      } 
      rehash(xid, hash, next_split, i, keySize, valSize); 
      next_split++;
      headerNextSplit = next_split;
      headerHashBits = i; 
    }
    update_hash_header(xid, hash, i, next_split);  
  }
}

void update_hash_header(int xid, recordid hash, int i, int next_split) {
  hashEntry * he = pblHtLookup(openHashes, &(hash.page), sizeof(int));
  assert(he);
  recordid  * headerRidB = &he->next;

  assert(headerRidB);

  headerHashBits = i;
  headerNextSplit = next_split;
  hash.slot = 1;
  Tset(xid, hash, headerRidB);
}

void rehash(int xid, recordid hashRid, int next_split, int i, int keySize, int valSize) {
  int firstA = 1;  // Is 'A' the recordid of a bucket? 
  int firstD = 1;  // What about 'D'? 

  assert(hashRid.size == sizeof(hashEntry) + keySize + valSize);
  recordid ba = hashRid; ba.slot = next_split;
  recordid bb = hashRid; bb.slot = next_split + twoToThe(i-1);
  
  hashEntry * D_contents = calloc(1,sizeof(hashEntry) + keySize + valSize);
  hashEntry * A_contents = calloc(1,sizeof(hashEntry) + keySize + valSize);
  hashEntry * B_contents = calloc(1,sizeof(hashEntry) + keySize + valSize);

  Tread(xid, ba, A_contents);
  Tread(xid, bb, D_contents);
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

  int old_hash;
  int new_hash = hash(A_contents+1, keySize, i,   ULONG_MAX) + 2;

  while(new_hash != next_split) {
    // Need a record in A that belongs in the first bucket... 
    
    recordid oldANext = A_contents->next;

    A_contents->next = NULLRID;

    if(firstD) {
      //      assert(memcmp(&A_contents->next, &D_contents->next, sizeof(recordid)));
      Tset(xid, D, A_contents);
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
      Tset(xid, D_contents->next, A_contents);
      //      assert(memcmp(&D, &D_contents->next, sizeof(recordid)));
      Tset(xid, D, D_contents);
      D = A;
    }
    hashEntry * swap = D_contents;
    D_contents = A_contents;
    A_contents = swap;

    /* A_contents is now garbage. */

    assert(A.size == sizeof(hashEntry) + keySize + valSize);
    if(oldANext.size == -1) {
      memset(A_contents, 0, sizeof(hashEntry) + keySize + valSize);
      //      assert(memcmp(&A_contents->next, &A, sizeof(recordid)));
      Tset(xid, A, A_contents);
      free(D_contents);
      free(A_contents);
      free(B_contents);
      /*      printf("Loop 1 returning.\n");
	      fflush(NULL); */
      return;
    } 
    assert(oldANext.size == sizeof(hashEntry) + keySize + valSize);
    Tread(xid, oldANext, A_contents);
    //    assert(memcmp(&A_contents->next, &A, sizeof(recordid)));
    Tset(xid, A, A_contents);
    Tdealloc(xid, oldANext);
    
    new_hash = hash(A_contents+1, keySize, i,   ULONG_MAX) + 2;
  }
  /*  printf("Got past loop 1\n");
      fflush(NULL); */

  B = A_contents->next;

  while(B.size != -1) {
    assert(B.size == sizeof(hashEntry) + keySize + valSize);
    Tread(xid, B, B_contents);
    C = B_contents->next;

    old_hash = hash(B_contents+1, keySize, i-1, ULONG_MAX) + 2;
    new_hash = hash(B_contents+1, keySize, i,   ULONG_MAX) + 2;

    assert(next_split == old_hash); 
    assert(new_hash   == old_hash || new_hash == old_hash + twoToThe(i-1));
 
    if(new_hash == old_hash) {
      A = B;
      B = C;
      C.size = -1;
      firstA = 0;
    } else {
			assert(D.size == sizeof(hashEntry) + keySize + valSize);
			assert(B.size == -1 || B.size == sizeof(hashEntry) + keySize + valSize);
			Tread(xid, D, D_contents); 
			D_contents->next = B;
			assert(B.size != 0);
			Tset(xid, D, D_contents);

			// A is somewhere in the first list. 
			assert(A.size == sizeof(hashEntry) + keySize + valSize);
			assert(C.size == -1 || C.size == sizeof(hashEntry) + keySize + valSize);
			Tread(xid, A, A_contents);
			A_contents->next = C;
			assert(C.size != 0);
	
			Tset(xid, A, A_contents);
	
      // B _can't_ be a bucket.
      assert(B.size == sizeof(hashEntry) + keySize + valSize);
      Tread(xid, B, B_contents);
      B_contents->next = NULLRID;
      Tset(xid, B, B_contents);

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
void insertIntoBucket(int xid, recordid hashRid, int bucket_number, hashEntry * bucket_contents, 
		      hashEntry * e, int keySize, int valSize, int skipDelete) {
  recordid deleteMe; 
  if(!skipDelete) {
    if(deleteFromBucket(xid, hashRid, bucket_number, bucket_contents, e+1, keySize, valSize, &deleteMe)) {
      Tdealloc(xid, deleteMe);
      hashRid.slot = bucket_number;
      assert(hashRid.size == sizeof(hashEntry) + valSize + keySize);
      Tread(xid, hashRid, bucket_contents);
      hashRid.slot = 0;
    }
  }

  /*@todo consider recovery for insertIntoBucket. */

  hashRid.slot = bucket_number;
  assert(hashRid.size == sizeof(hashEntry) + valSize + keySize);
  Tread(xid, hashRid, bucket_contents);

  assert(hashRid.size == sizeof(hashEntry) + keySize + valSize);
  if(!bucket_contents->next.size) {  // Size = 0 -> nothing in bucket.  Size != 0 -> bucket occupied.
    e->next.page = 0;
    e->next.slot = 0;
    e->next.size = -1;
    Tset(xid, hashRid, e);
  } else {
    recordid newEntry =  Talloc(xid, sizeof(hashEntry) + keySize + valSize);
    e->next = bucket_contents->next;
    bucket_contents->next = newEntry;
    assert(newEntry.size == sizeof(hashEntry) + keySize + valSize);
    Tset(xid, newEntry, e);
    assert(hashRid.size == sizeof(hashEntry) + keySize + valSize);
    Tset(xid, hashRid,  bucket_contents);
  }
}

int deleteFromBucket(int xid, recordid hash, int bucket_number, hashEntry * bucket_contents,
		     void * key, int keySize, int valSize, recordid * deletedEntry) {
  if(bucket_contents->next.size == 0) { return 0; }
  
  recordid this = hash;
  this.slot = bucket_number;

  int found = 0;
  if(!memcmp(bucket_contents+1, key, keySize)) {
    if(deletedEntry)
      deletedEntry->size = 0;  /* size = 0 -> don't delete (this is a bucket!) */
    if(bucket_contents->next.size == -1) {
      memset(bucket_contents, 0, sizeof(hashEntry) + keySize + valSize);
      Tset(xid, this, bucket_contents);
    } else {
      assert(bucket_contents->next.size ==  sizeof(hashEntry) + keySize + valSize);
      recordid oldNext = bucket_contents->next;
      Tread(xid, bucket_contents->next, bucket_contents);
      Tset(xid, this, bucket_contents);
      *deletedEntry = oldNext; /* @todo delete from bucket really should do its own deallocation.. */
    }
    return 1;
  }

  if(bucket_contents->next.size == -1) { return 0; }

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
    Tread(xid, Baddr, B);

    if(!memcmp(B+1, key, keySize)) {
      A->next = B->next;
      assert(Aaddr.size == sizeof(hashEntry) + keySize + valSize);
      Tset(xid, Aaddr, A);
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

recordid ThashAlloc(int xid, int keySize, int valSize) {
  /* Want 16 buckets, doubling on overflow. */
  recordid rid = TarrayListAlloc(xid, 4096, 2, sizeof(hashEntry) + keySize + valSize); 
  TarrayListExtend(xid, rid, 4096+2);

  recordid  * headerRidA = calloc (1, sizeof(recordid) + keySize + valSize);
  recordid  * headerRidB = calloc (1, sizeof(recordid) + keySize + valSize);

  assert(headerRidB);
  
  headerKeySize = keySize;
  headerValSize = valSize;
  
  headerNextSplit = INT_MAX;
  headerHashBits  = 12;

  assert(headerRidB);
  
  rid.slot =0;
  Tset(xid, rid, headerRidA);
  rid.slot =1;
  Tset(xid, rid, headerRidB);

  assert(headerRidB);
  
  pblHtInsert(openHashes, &(rid.page), sizeof(int), headerRidB);

  assert(headerRidB);	

  recordid * check = malloc(rid.size);
	
  rid.slot = 0;
  Tread(xid, rid, check);
  assert(headerRidB);
  assert(!memcmp(headerRidA, check, sizeof(recordid)));

  rid.slot = 1;
  Tread(xid, rid, check);
  assert(headerRidB);
  assert(!memcmp(headerRidB, check, sizeof(recordid)));

  free(headerRidA);
  free(check);
  rid.slot =0;
  return rid;
}

pthread_mutex_t exp_mutex;
pthread_mutex_t exp_slow_mutex;


void ThashInit() {
  openHashes = pblHtCreate();
  lockedBuckets = pblHtCreate();

  pthread_mutex_init(&linearHashMutex , NULL);
  pthread_cond_init(&bucketUnlocked , NULL);
  pthread_mutex_init(&exp_mutex, NULL);
  pthread_mutex_init(&exp_slow_mutex, NULL);
  
}

void ThashDeinit() {
  pblHtDelete(openHashes);
  pblHtDelete(lockedBuckets);
}

void TnaiveHashInsert(int xid, recordid hashRid, 
	    void * key, int keySize, 
	    void * val, int valSize) {

  recordid  * headerRidB = pblHtLookup(openHashes, &(hashRid.page), sizeof(int));

  /*   printf("header: %d %d\n", headerHashBits, headerNextSplit); */

  int bucket = hash(key, keySize, headerHashBits, headerNextSplit - 2) + 2;
  
  hashEntry * e = calloc(1,sizeof(hashEntry) + keySize + valSize);
  memcpy(e+1, key, keySize);
  memcpy(((byte*)(e+1)) + keySize, val, valSize);

  hashEntry * bucket_contents = calloc(1,sizeof(hashEntry) + keySize + valSize);
  hashRid.slot = bucket;
  Tread(xid, hashRid, bucket_contents);

  insertIntoBucket(xid, hashRid, bucket, bucket_contents, e, keySize, valSize, 0); 
  expand(xid, hashRid, headerNextSplit, headerHashBits, keySize, valSize);  

  free(bucket_contents);
  free(e);

}
/** @todo hash hable probably should track the number of items in it,
    so that expand can be selectively called. */
int TnaiveHashDelete(int xid, recordid hashRid, 
	    void * key, int keySize, int valSize) {
  recordid  * headerRidB = pblHtLookup(openHashes, &(hashRid.page), sizeof(int));

  int bucket_number = hash(key, keySize, headerHashBits, headerNextSplit - 2) + 2;
  recordid  deleteMe;
  hashRid.slot = bucket_number;

  hashEntry * bucket_contents = malloc(sizeof(hashEntry) + keySize + valSize);
  assert(hashRid.size == sizeof(hashEntry) + keySize + valSize);
  Tread(xid, hashRid, bucket_contents);
  hashRid.slot = 0;
  int ret = 0;
  if(deleteFromBucket(xid, hashRid, bucket_number, bucket_contents, key, keySize, valSize, &deleteMe)) {
    if(deleteMe.size) {
      Tdealloc(xid, deleteMe);
    }
    ret = 1;
  }
  free(bucket_contents);
  return ret;
}

int ThashOpen(int xid, recordid hashRid, int keySize, int valSize) {
  recordid * headerRidB = malloc(sizeof(recordid) + keySize + valSize);
  hashRid.slot = 1;
  Tread(xid, hashRid, headerRidB);
  
  pblHtInsert(openHashes, &(hashRid.page), sizeof(int), headerRidB);

  return 0;
}

void TnaiveHashUpdate(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize) {
  TnaiveHashDelete(xid, hashRid, key, keySize, valSize);
  TnaiveHashInsert(xid, hashRid, key, keySize, val, valSize);

}


int ThashClose(int xid, recordid hashRid) {
  recordid * freeMe = pblHtLookup(openHashes,  &(hashRid.page), sizeof(int));
  pblHtRemove(openHashes, &(hashRid.page), sizeof(int));
  free(freeMe);
  return 0;
}

int TnaiveHashLookup(int xid, recordid hashRid, void * key, int keySize, void * buf, int valSize) {

  recordid  * headerRidB = pblHtLookup(openHashes, &(hashRid.page), sizeof(int));
  /*  printf("lookup header: %d %d\n", headerHashBits, headerNextSplit); */
  recordid tmp = hashRid;
  tmp.slot = 1;
  int bucket_number = hash(key, keySize, headerHashBits, headerNextSplit - 2) + 2;
  int ret = findInBucket(xid, hashRid, bucket_number, key, keySize, buf, valSize);
  return ret;
}
