#include <lladd/transactional.h>
#include <lladd/hash.h>
#include <stdlib.h>
#include <assert.h>


typedef struct {
  recordid buckets;
  int keySize;
  int valueSize;
  long nextSplit;
  int bits;
  long numEntries;
} lladd_hash_header;



/* private methods... */
static void ThashSplitBucket(int xid, recordid hashHeader, lladd_hash_header * lhh);


#define HASH_INIT_ARRAY_LIST_COUNT twoToThe(HASH_INIT_BITS)
#define HASH_INIT_ARRAY_LIST_MULT    2

recordid ThashCreate(int xid, int keySize, int valueSize) {
  recordid hashHeader = Talloc(xid, sizeof(lladd_hash_header));
  lladd_hash_header lhh;
  lhh.buckets = TarrayListAlloc(xid, HASH_INIT_ARRAY_LIST_COUNT, HASH_INIT_ARRAY_LIST_MULT, sizeof(lladd_linkedList_entry) + keySize + valueSize);
  TarrayListExtend(xid, lhh.buckets, HASH_INIT_ARRAY_LIST_COUNT);
  int i;
  byte * entry = calloc(1, sizeof(lhh.buckets));
  recordid bucket = lhh.buckets;
  for(i = 0; i < HASH_INIT_ARRAY_LIST_COUNT; i++) {
    bucket.slot = i;
    Tset(xid, bucket, entry);
  }
  free (entry);
  lhh.keySize = keySize;
  lhh.valueSize = valueSize;
  lhh.nextSplit = 0;
  lhh.bits = HASH_INIT_BITS;
  lhh.numEntries = 0;
  
  Tset(xid, hashHeader, &lhh);
  return hashHeader;
}
  
void ThashDelete(int xid, recordid hash) {
  abort();
}


int ThashInsert(int xid, recordid hashHeader, const byte* key, int keySize, const byte* value, int valueSize) {
  lladd_hash_header lhh;
  Tread(xid, hashHeader, &lhh);

  lhh.numEntries ++;
  
  if(lhh.numEntries > (int)((double)(lhh.nextSplit + twoToThe(lhh.bits-1)) * HASH_FILL_FACTOR)) {
  	ThashSplitBucket(xid, hashHeader, &lhh);
  }
  
  Tset(xid, hashHeader, &lhh);
  
  assert(lhh.keySize == keySize); assert(lhh.valueSize == valueSize);
  
  recordid bucket = lhh.buckets;
  bucket.slot = hash(key, keySize, lhh.bits, lhh.nextSplit);
  
  int ret = TlinkedListInsert(xid, bucket, key, keySize, value, valueSize);

  return ret;
}

int ThashRemove(int xid, recordid hashHeader, const byte * key, int keySize) {
  lladd_hash_header lhh;
  Tread(xid, hashHeader, &lhh);

  lhh.numEntries--;
  
  Tset(xid, hashHeader, &lhh);
  
  assert(lhh.keySize == keySize);
  
  recordid bucket = lhh.buckets;
  bucket.slot = hash(key, keySize, lhh.bits, lhh.nextSplit);
  
  int ret = TlinkedListRemove(xid, bucket, key, keySize);
  
  return ret;
}

int ThashLookup(int xid, recordid hashHeader, const byte * key, int keySize, byte ** value) {
  lladd_hash_header lhh;
  Tread(xid, hashHeader, &lhh);
  
  assert(lhh.keySize == keySize);
  
  recordid bucket = lhh.buckets;
  bucket.slot = hash(key, keySize, lhh.bits, lhh.nextSplit);
  
  int ret = TlinkedListFind(xid, bucket, key, keySize, value);
  
  return ret;
}
/** @todo Write ThashSplitBucket */
static void ThashSplitBucket(int xid, recordid hashHeader, lladd_hash_header * lhh) {
  long old_bucket = lhh->nextSplit;
  long new_bucket = old_bucket + twoToThe(lhh->bits-1);
  recordid old_bucket_rid = lhh->buckets;
  recordid new_bucket_rid = lhh->buckets;
  old_bucket_rid.slot = old_bucket;
  new_bucket_rid.slot = new_bucket;

  TarrayListExtend(xid, lhh->buckets, 1);
  byte * entry = calloc(1, lhh->buckets.size);
  Tset(xid, new_bucket_rid, entry);
  free(entry);  
  if(lhh->nextSplit < twoToThe(lhh->bits-1)-1) {
    lhh->nextSplit++;
  } else {
    lhh->nextSplit = 0;
    lhh->bits++;    
  }
  lladd_linkedList_iterator * it = TlinkedListIterator(xid, old_bucket_rid, lhh->keySize, lhh->valueSize);
  byte * key, *value;
  int keySize, valueSize;
  while(TlinkedListNext(xid, it, &key, &keySize, &value, &valueSize)) {
    assert(valueSize == lhh->valueSize);
    assert(keySize == lhh->keySize);
    if(hash(key, keySize, lhh->bits, lhh->nextSplit) != old_bucket) {
      TlinkedListRemove(xid, old_bucket_rid, key, keySize);
      TlinkedListInsert(xid, new_bucket_rid, key, keySize, value, valueSize);
    }
    free(key);
    free(value);
  }
  return;
}
