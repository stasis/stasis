#include <lladd/transactional.h>
#include <lladd/hash.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#define __USE_GNU
#include <pthread.h>

static pthread_mutex_t linear_hash_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

/**
   re-entrant implementation of a linear hash hable, using nensted top actions.
   
   @file
   
   @todo Improve concurrency of linearHashNTA and linkedListNTA.
*/

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
  byte * entry = calloc(1, lhh.buckets.size);
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

static int __ThashInsert(int xid, recordid hashHeader, const byte* key, int keySize, const byte* value, int valueSize);
static int __ThashRemove(int xid, recordid hashHeader, const byte * key, int keySize);

typedef struct {
  recordid hashHeader;
  int keySize;
} linearHash_insert_arg;

typedef struct {
  recordid hashHeader;
  int keySize;
  int valueSize;
} linearHash_remove_arg;

static int operateInsert(int xid, Page *p,  lsn_t lsn, recordid rid, const void *dat) {
  const linearHash_remove_arg * args = dat;
  recordid hashHeader = args->hashHeader;
  int keySize = args->keySize;
  int valueSize = args->valueSize;
  
  byte * key = (byte*)(args+1);
  byte * value = ((byte*)(args+1))+ keySize;
  pthread_mutex_lock(&linear_hash_mutex);
  __ThashInsert(xid, hashHeader, key, keySize, value, valueSize);
  pthread_mutex_unlock(&linear_hash_mutex);
  return 0;  
}
static int operateRemove(int xid, Page *p,  lsn_t lsn, recordid rid, const void *dat) {
  const linearHash_insert_arg * args = dat;
  recordid hashHeader = args->hashHeader;
  int keySize = args->keySize;  
  
  byte * key = (byte*)(args + 1);
  
  pthread_mutex_lock(&linear_hash_mutex);
  __ThashRemove(xid, hashHeader, key, keySize);
  pthread_mutex_unlock(&linear_hash_mutex);
  return 0;
}
Operation getLinearHashInsert() {
  Operation o = {
    OPERATION_NOOP, 
    SIZEIS_PAGEID,
    OPERATION_LINEAR_HASH_REMOVE,
    &operateInsert
  };
  return o;
}
Operation getLinearHashRemove() {
  Operation o = {
    OPERATION_NOOP, 
    SIZEIS_PAGEID,
    OPERATION_LINEAR_HASH_INSERT,
    &operateRemove
  };
  return o;
}

int ThashInsert(int xid, recordid hashHeader, const byte* key, int keySize, const byte* value, int valueSize) {
  pthread_mutex_lock(&linear_hash_mutex);
  int argSize = sizeof(linearHash_insert_arg)+keySize;
  linearHash_insert_arg * arg = malloc(argSize);
  arg->hashHeader = hashHeader;
  arg->keySize = keySize;
  memcpy(arg+1, key, keySize);
  void * handle = TbeginNestedTopAction(xid, OPERATION_LINEAR_HASH_INSERT, (byte*)arg, argSize);
  free(arg);
  int ret = __ThashInsert(xid, hashHeader, key, keySize, value, valueSize);  
  TendNestedTopAction(xid, handle);
  pthread_mutex_unlock(&linear_hash_mutex);
  return ret;
}
static int __ThashInsert(int xid, recordid hashHeader, const byte* key, int keySize, const byte* value, int valueSize) {
  lladd_hash_header lhh;
  
  Tread(xid, hashHeader, &lhh);

  lhh.numEntries ++;
  
  if(lhh.numEntries > (int)((double)(lhh.nextSplit + twoToThe(lhh.bits-1)) * HASH_FILL_FACTOR)) {
  	ThashSplitBucket(xid, hashHeader, &lhh);
  }
  assert(lhh.keySize == keySize); assert(lhh.valueSize == valueSize);
  
  recordid bucket = lhh.buckets;
  bucket.slot = hash(key, keySize, lhh.bits, lhh.nextSplit);
  
  int ret = TlinkedListInsert(xid, bucket, key, keySize, value, valueSize);
  if(ret) { lhh.numEntries--; }
  Tset(xid, hashHeader, &lhh);
  
  return ret;
}
int ThashRemove(int xid, recordid hashHeader, const byte * key, int keySize) {

  pthread_mutex_lock(&linear_hash_mutex);
  byte * value;
  int valueSize = ThashLookup(xid, hashHeader, key, keySize, &value);
  if(valueSize == -1) {
    pthread_mutex_unlock(&linear_hash_mutex);
    return 0; 
  }
  int argSize = sizeof(linearHash_remove_arg) + keySize + valueSize;
  linearHash_remove_arg * arg = malloc(argSize);
  arg->hashHeader = hashHeader;
  arg->keySize = keySize;
  arg->valueSize = valueSize;
  memcpy(arg+1, key, keySize);
  memcpy((byte*)(arg+1)+keySize, value, valueSize);
  
  void * handle = TbeginNestedTopAction(xid, OPERATION_LINEAR_HASH_REMOVE, (byte*)arg, argSize);
  free(arg);
  free(value);
  int ret = __ThashRemove(xid, hashHeader, key, keySize);

  TendNestedTopAction(xid, handle);
  pthread_mutex_unlock(&linear_hash_mutex);
  return ret;
}

static int __ThashRemove(int xid, recordid hashHeader, const byte * key, int keySize) {
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
  pthread_mutex_lock(&linear_hash_mutex);
  Tread(xid, hashHeader, &lhh);
  
  assert(lhh.keySize == keySize);
  
  recordid bucket = lhh.buckets;
  bucket.slot = hash(key, keySize, lhh.bits, lhh.nextSplit);
  
  int ret = TlinkedListFind(xid, bucket, key, keySize, value);
  pthread_mutex_unlock(&linear_hash_mutex);
  return ret;
}
static void ThashSplitBucket(int xid, recordid hashHeader, lladd_hash_header * lhh) {
  long old_bucket = lhh->nextSplit;
  long new_bucket = old_bucket + twoToThe(lhh->bits-1);
  recordid old_bucket_rid = lhh->buckets;
  recordid new_bucket_rid = lhh->buckets;
  old_bucket_rid.slot = old_bucket;
  new_bucket_rid.slot = new_bucket;
 // void * handle = TbeginNestedTopAction(xid, OPERATION_NOOP, NULL, 0);
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
//  TendNestedTopAction(xid, handle);
  return;
}
lladd_hash_iterator * ThashIterator(int xid, recordid hashHeader, int keySize, int valueSize) {
  lladd_hash_iterator * it = malloc(sizeof(lladd_hash_iterator));
  it->hashHeader = hashHeader;
  lladd_hash_header lhh;
  Tread(xid, hashHeader, &lhh);
  it->bucket = lhh.buckets;
  it->numBuckets = lhh.nextSplit +twoToThe(lhh.bits-1);
  it->bucket.slot = 0;
  it->keySize = keySize;
  it->valueSize = valueSize;
  it->it = TlinkedListIterator(xid, it->bucket, it->keySize, it->valueSize);
  
  return it;
}
  
int ThashNext(int xid, lladd_hash_iterator * it, byte ** key, int * keySize, byte** value, int * valueSize) {
  while(!TlinkedListNext(xid, it->it, key, keySize, value, valueSize)) {
    it->bucket.slot++;
    if(it->bucket.slot < it->numBuckets) {
      it->it = TlinkedListIterator(xid, it->bucket, it->keySize, it->valueSize); 
    } else {
      free(it);
      return 0;
    }
  }
  return 1;
}
