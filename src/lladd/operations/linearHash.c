#include <lladd/operations/linearHash.h>
#include <lladd/hash.h>
#include <limits.h>
#include <assert.h>
/**

   A from-scratch implementation of linear hashing.  Uses the
   arrayList operations to implement its hashbuckets.

*/

#define BUCKETS_OFFSET (2)

#define headerKeySize (headerRidA.page)
#define headerValSize (headerRidA.slot)

#define headerHashBits (headerRidB.page)
#define headerNextSplit (headerRidB.slot)

#include <math.h>
#include <malloc.h>
#include <string.h>
#include <lladd/operations/linearHash.h>


typedef struct {
  recordid next;
} hashEntry;

void rehash(int xid, recordid hash, int next_split, int i);
void update_hash_header(int xid, recordid hash, int i, int next_split);
int deleteFromBucket(int xid, recordid hash, int bucket_number, void * key, int keySize, recordid * deletedEntry);
void insertIntoBucket(int xid, recordid hashRid, int bucket_number, hashEntry * e, int keySize, int valSize, recordid deletedEntry);
int findInBucket(int xid, recordid hashRid, int bucket_number, const void * key, int keySize, void * val, int valSize);


int findInBucket(int xid, recordid hashRid, int bucket_number, const void * key, int keySize, void * val, int valSize) {
  hashEntry * e = malloc(sizeof(hashEntry) + keySize + valSize);

  recordid bucket = hashRid;

  bucket.slot = bucket_number;

  recordid nextEntry;

  Tread(xid, bucket, &nextEntry);
  if(nextEntry.size) {
    e = malloc(nextEntry.size);
  } else {
    e = malloc(1);
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
  return found;
}

void expand (int xid, recordid hash, int next_split, int i) {
  TarrayListExtend(xid, hash, 1);
  if(next_split >= powl(2,i-1)+2) {
    /*    printf("\n\n%d %d (i++)\n\n", next_split, i); */
    i++;
    next_split = 2;
  }
  /*  printf("-%d-", next_split); */
  /*   printf("rehash(%d, %d + 2)\n", i, next_split - 2); */
  rehash(xid, hash, next_split, i);
  next_split++;
  update_hash_header(xid, hash, i, next_split);
}

void update_hash_header(int xid, recordid hash, int i, int next_split) {
  recordid  headerRidB;

  hash.slot = 1;
  Tread(xid, hash, &headerRidB);
  /* headerHashBits and headerHashSplit are #defined to refer to headerRidB. */
  headerHashBits = i;
  headerNextSplit = next_split;
  
  Tset(xid, hash, &headerRidB);
}

void rehash(int xid, recordid hashRid, int next_split, int i) {
  recordid bucket = hashRid;
  bucket.slot = next_split;
  recordid headerRidA;
  Tread(xid, hashRid, &headerRidA);
  /*  recordid oldRid;
  oldRid.page = 0;
  oldRid.slot = 0;
  oldRid.size = 0; */
  hashEntry * e = calloc(1,sizeof(hashEntry) + headerValSize + headerKeySize);
  assert(bucket.size < 1000);

  if(bucket.size) {
    Tread(xid, bucket, &bucket);
  }

  while(bucket.size > 0) {
    Tread(xid, bucket, e);

    /*     printf("#%d", *(int*)(e+1)); */

    int old_hash = hash(e+1, headerKeySize, i-1, ULONG_MAX) + 2;
    assert(next_split == old_hash); 

    int new_hash = hash(e+1, headerKeySize, i, ULONG_MAX) + 2;

    bucket = e->next;
    /*    oldRid = bucket; */
    assert((!bucket.size )|| bucket.size ==  sizeof(hashEntry) + headerValSize + headerKeySize);
 
    if(new_hash != next_split) {

      assert(new_hash == next_split + powl(2, i-1)); 

      /* recordid newRid = hashRid;
      newRid.slot = new_hash;
            recordid ptr; 
	      Tread(xid, newRid, &ptr); */
      /*      printf("Moving from %d to %d.\n", next_split, new_hash);
	      fflush(NULL); */
      recordid oldEntry;

      /*     printf("!"); */

      assert(deleteFromBucket(xid, hashRid, next_split, e+1, headerKeySize, &oldEntry));
      insertIntoBucket(xid, hashRid, new_hash, e, headerKeySize, headerValSize, oldEntry);
    } else {

      /*      printf("-"); */

      /*      printf("Not moving %d.\n", next_split);
	      fflush(NULL); */
    }

  }
  free(e);
}

void insertIntoBucket(int xid, recordid hashRid, int bucket_number, hashEntry * e, int keySize, int valSize, recordid newEntry) {
  recordid deleteMe; 
  if(deleteFromBucket(xid, hashRid, bucket_number, e+1, keySize, &deleteMe)) {
    Tdealloc(xid, deleteMe);
  }

  /*@todo consider recovery for insertIntoBucket. */
  /*  recordid newEntry = Talloc(xid, sizeof(hashEntry) + keySize + valSize); */
  recordid bucket   = hashRid;
  bucket.slot = bucket_number;
  Tread(xid, bucket, &(e->next));
  Tset(xid, newEntry, e);
  Tset(xid, bucket, &newEntry);
}

int deleteFromBucket(int xid, recordid hash, int bucket_number, void * key, int keySize, recordid * deletedEntry) {
  hashEntry * e;
  recordid bucket = hash;
  bucket.slot = bucket_number;
  recordid nextEntry;
  Tread(xid, bucket, &nextEntry);
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
	assert(e->next.size < 1000);
	Tset(xid, bucket, &(e->next));
      } else {
	recordid next = e->next;
	Tread(xid, lastEntry, e);
	assert(next.size < 1000);
	e->next = next;
	Tset(xid, lastEntry, e);
      }
      *deletedEntry = nextEntry;
      /*      Tdealloc(xid, nextEntry); */
      found = 1;
      break;
    }
    lastEntry = nextEntry;
    first = 0;
    nextEntry = e->next;
  }
  return found;
}

recordid ThashAlloc(int xid, int keySize, int valSize) {
  /* Want 16 buckets + 2 header rids, doubling on overflow. */
  recordid rid = TarrayListAlloc(xid, 16 + 2, 2, sizeof(recordid)); 
  TarrayListExtend(xid, rid, 32+2);

  recordid headerRidA, headerRidB;
  
  headerKeySize = keySize;
  headerValSize = valSize;
  
  headerNextSplit = INT_MAX;
  headerHashBits  = 4;

  rid.slot =0;
  Tset(xid, rid, &headerRidA);
  rid.slot =1;
  Tset(xid, rid, &headerRidB);
  rid.slot =0;
  return rid;
}

void ThashInsert(int xid, recordid hashRid, 
	    void * key, int keySize, 
	    void * val, int valSize) {

  recordid headerRidA, headerRidB;
  recordid tmp = hashRid;
  tmp.slot = 0;
  Tread(xid, tmp, &headerRidA);
  assert(headerKeySize == keySize);
  tmp.slot = 1;
  Tread(xid, tmp, &headerRidB);
  assert(headerValSize == valSize);

  int bucket = hash(key, keySize, headerHashBits, headerNextSplit - 2) + 2;
  
  hashEntry * e = calloc(1,sizeof(hashEntry) + keySize + valSize);
  memcpy(e+1, key, keySize);
  memcpy(((byte*)(e+1)) + keySize, val, valSize);

  recordid newEntry =  Talloc(xid, sizeof(hashEntry) + keySize + valSize);
  /*  printf("%d -> %d\n", *(int*)(e+1), bucket); */
  insertIntoBucket(xid, hashRid, bucket, e, keySize, valSize, newEntry);
  expand(xid, hashRid, headerNextSplit, headerHashBits);

  free(e);

}
/** @todo hash hable probably should track the number of items in it,
    so that expand can be selectively called. */
void ThashDelete(int xid, recordid hashRid, 
	    void * key, int keySize) {

  recordid headerRidB;
  recordid tmp = hashRid;
  tmp.slot = 1;
  Tread(xid, tmp, &headerRidB);
  int bucket_number = hash(key, keySize, headerHashBits, headerNextSplit - 2) + 2;
  recordid deleteMe;
  if(deleteFromBucket(xid, hashRid, bucket_number, key, keySize, &deleteMe)) {
    Tdealloc(xid, deleteMe);
  }
}

void ThashUpdate(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize) {
  ThashDelete(xid, hashRid, key, keySize);
  ThashInsert(xid, hashRid, key, keySize, val, valSize);
}

int ThashLookup(int xid, recordid hashRid, void * key, int keySize, void * buf, int valSize) {
  recordid headerRidB;
  recordid tmp = hashRid;
  tmp.slot = 1;
  Tread(xid, tmp, &headerRidB);
  int bucket_number = hash(key, keySize, headerHashBits, headerNextSplit - 2) + 2;
  /*  printf("look in %d\n", bucket_number); */
  int ret = findInBucket(xid, hashRid, bucket_number, key, keySize, buf, valSize);
  return ret;
}
