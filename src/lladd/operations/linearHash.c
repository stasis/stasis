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

pblHashTable_t * openHashes = NULL;



void rehash(int xid, recordid hash, int next_split, int i, int keySize, int valSize);
void update_hash_header(int xid, recordid hash, int i, int next_split);
int deleteFromBucket(int xid, recordid hash, int bucket_number, recordid bucket_rid, void * key, int keySize, recordid * deletedEntry);
void insertIntoBucket(int xid, recordid hashRid, int bucket_number, recordid bucket_rid, hashEntry * e, int keySize, int valSize, recordid deletedEntry, int skipDelete);
int findInBucket(int xid, recordid hashRid, int bucket_number, const void * key, int keySize, void * val, int valSize);


int findInBucket(int xid, recordid hashRid, int bucket_number, const void * key, int keySize, void * val, int valSize) {
  hashEntry * e = malloc(sizeof(hashEntry) + keySize + valSize);

  recordid bucket = hashRid;

  bucket.slot = bucket_number;

  recordid nextEntry;

  Tread(xid, bucket, &nextEntry);
  if(nextEntry.size) {
    assert(nextEntry.size == sizeof(hashEntry) + keySize + valSize);
  }
  /*  if(nextEntry.size) {
    e = malloc(nextEntry.size);
  } else {
    e = malloc(1);
    } */
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
}

void expand (int xid, recordid hash, int next_split, int i, int keySize, int valSize) {
  TarrayListExtend(xid, hash, 1);
  if(next_split >= powl(2,i-1)+2) {
    /*    printf("\n\n%d %d (i++)\n\n", next_split, i); */
    i++;
    next_split = 2;
  }
  /*  printf("-%d-", next_split); */
  /*   printf("rehash(%d, %d + 2)\n", i, next_split - 2); */
  rehash(xid, hash, next_split, i, keySize, valSize);
  next_split++;
  update_hash_header(xid, hash, i, next_split);
}

void update_hash_header(int xid, recordid hash, int i, int next_split) {
  recordid  * headerRidB = pblHtLookup(openHashes, &hash.page, sizeof(int));

  /*  hash.slot = 1; */
  /*  Tread(xid, hash, headerRidB); */
  /* headerHashBits and headerHashSplit are #defined to refer to headerRidB. */
  headerHashBits = i;
  headerNextSplit = next_split;
  
  Tset(xid, hash, headerRidB);
}

void rehash(int xid, recordid hashRid, int next_split, int i, int keySize, int valSize) {
  recordid bucket = hashRid;
  bucket.slot = next_split;
  /*recordid headerRidA;
  Tread(xid, hashRid, &headerRidA); */
  /*  recordid oldRid;
  oldRid.page = 0;
  oldRid.slot = 0;
  oldRid.size = 0; */
  hashEntry * e = calloc(1,sizeof(hashEntry) + keySize + valSize /* headerValSize + headerKeySize */);

  if(bucket.size) {
    Tread(xid, bucket, &bucket);
  }

  while(bucket.size > 0) {
    Tread(xid, bucket, e);

    /*     printf("#%d", *(int*)(e+1)); */

    int old_hash = hash(e+1, keySize, i-1, ULONG_MAX) + 2;
    assert(next_split == old_hash); 

    int new_hash = hash(e+1, keySize, i, ULONG_MAX) + 2;

    bucket = e->next;

    assert((!bucket.size )|| bucket.size ==  sizeof(hashEntry) + keySize + valSize /*headerValSize + headerKeySize */);
 
    if(new_hash != next_split) {

      assert(new_hash == next_split + powl(2, i-1)); 

      recordid oldEntry;

      /** @todo could be optimized.  Why deleteFromBucket, then
	 insertIntoBucket?  Causes us to travers the bucket list an
	 extra time... */

      recordid next_split_contents, new_hash_contents;
      recordid tmp = hashRid;
      tmp.slot = next_split;
      Tread(xid, tmp, &next_split_contents);
      tmp.slot = new_hash;
      Tread(xid, tmp, &new_hash_contents);

      assert(deleteFromBucket(xid, hashRid, next_split, next_split_contents,  e+1, keySize,/* valSize, headerKeySize,*/ &oldEntry)); 
      insertIntoBucket(xid, hashRid, new_hash, new_hash_contents, e, keySize, valSize, /*headerKeySize, headerValSize, */oldEntry, 1);
    } else {

    }

  }
  free(e);
}

void insertIntoBucket(int xid, recordid hashRid, int bucket_number, recordid bucket_contents, hashEntry * e, int keySize, int valSize, recordid newEntry, int skipDelete) {
  recordid deleteMe; 
  if(!skipDelete) {
    if(deleteFromBucket(xid, hashRid, bucket_number, bucket_contents, e+1, keySize, &deleteMe)) {
      Tdealloc(xid, deleteMe);
      hashRid.slot = bucket_number;
      Tread(xid, hashRid, &bucket_contents);
      hashRid.slot = 0;
    }
  }

  /*@todo consider recovery for insertIntoBucket. */
  /*  recordid newEntry = Talloc(xid, sizeof(hashEntry) + keySize + valSize); */
  recordid bucket   = hashRid;
  bucket.slot = bucket_number;
  /*  Tread(xid, bucket, &(e->next)); */
  e->next = bucket_contents;
  Tset(xid, newEntry, e);
  Tset(xid, bucket, &newEntry);
}

int deleteFromBucket(int xid, recordid hash, int bucket_number, recordid bucket_contents, void * key, int keySize, recordid * deletedEntry) {
  hashEntry * e;
  recordid bucket = hash;
  bucket.slot = bucket_number;
  recordid nextEntry;
  nextEntry = bucket_contents;
  /*  Tread(xid, bucket, &nextEntry); */
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

void ThashInit() {
  openHashes = pblHtCreate();
}

void ThashDeinit() {
  pblHtDelete(openHashes);
}

void ThashInsert(int xid, recordid hashRid, 
	    void * key, int keySize, 
	    void * val, int valSize) {

  /*  recordid headerRidA; */
  recordid  * headerRidB = pblHtLookup(openHashes, &hashRid.page, sizeof(int));

  /*  recordid tmp = hashRid; */
  /*  tmp.slot = 0;
  Tread(xid, tmp, &headerRidA);
  assert(headerKeySize == keySize);
  tmp.slot = 1; */
  /*  Tread(xid, tmp, &headerRidB); */
  /*  assert(headerValSize == valSize); */

  int bucket = hash(key, keySize, headerHashBits, headerNextSplit - 2) + 2;
  
  hashEntry * e = calloc(1,sizeof(hashEntry) + keySize + valSize);
  memcpy(e+1, key, keySize);
  memcpy(((byte*)(e+1)) + keySize, val, valSize);

  recordid newEntry =  Talloc(xid, sizeof(hashEntry) + keySize + valSize);
  /*  printf("%d -> %d\n", *(int*)(e+1), bucket); */
  recordid bucket_contents;
  hashRid.slot = bucket;
  Tread(xid, hashRid, &bucket_contents);
  hashRid.slot = 0;
  insertIntoBucket(xid, hashRid, bucket, bucket_contents, e, keySize, valSize, newEntry, 0);
  expand(xid, hashRid, headerNextSplit, headerHashBits, keySize, valSize);

  free(e);

}
/** @todo hash hable probably should track the number of items in it,
    so that expand can be selectively called. */
void ThashDelete(int xid, recordid hashRid, 
	    void * key, int keySize) {
  recordid  * headerRidB = pblHtLookup(openHashes, &hashRid.page, sizeof(int));
  recordid tmp = hashRid;
  tmp.slot = 1;
  /*  Tread(xid, tmp, headerRidB); */
  int bucket_number = hash(key, keySize, headerHashBits, headerNextSplit - 2) + 2;
  recordid deleteMe;
  hashRid.slot = bucket_number;
  recordid bucket_contents;
  Tread(xid, hashRid, &bucket_contents);
  hashRid.slot = 0;
  if(deleteFromBucket(xid, hashRid, bucket_number, bucket_contents, key, keySize, &deleteMe)) {
    Tdealloc(xid, deleteMe);
  }
}

int ThashOpen(int xid, recordid hashRid) {
  recordid * headerRidB = malloc(sizeof(recordid));
  hashRid.slot = 1;
  Tread(xid, hashRid, headerRidB);
  
  pblHtInsert(openHashes, &hashRid.page, sizeof(int), headerRidB);

  return 0;
}

void ThashUpdate(int xid, recordid hashRid, void * key, int keySize, void * val, int valSize) {
  ThashDelete(xid, hashRid, key, keySize);
  ThashInsert(xid, hashRid, key, keySize, val, valSize);

}


int ThashClose(int xid, recordid hashRid) {
  recordid * freeMe = pblHtLookup(openHashes,  &hashRid.page, sizeof(int));
  pblHtRemove(openHashes, &hashRid.page, sizeof(int));
  free(freeMe);
  return 0;
}

int ThashLookup(int xid, recordid hashRid, void * key, int keySize, void * buf, int valSize) {
  /*  recordid headerRidB; */
  recordid  * headerRidB = pblHtLookup(openHashes, &hashRid.page, sizeof(int));
  recordid tmp = hashRid;
  tmp.slot = 1;
  int bucket_number = hash(key, keySize, headerHashBits, headerNextSplit - 2) + 2;
  /*  printf("look in %d\n", bucket_number); */
  int ret = findInBucket(xid, hashRid, bucket_number, key, keySize, buf, valSize);
  return ret;
}
