#define __USE_GNU
#define _GNU_SOURCE
#include <pthread.h>
#include <lladd/transactional.h>
#include <lladd/hash.h>
#include "../page.h"
#include "../page/slotted.h"
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <lladd/operations/noop.h>



/**
   re-entrant implementation of a linear hash hable, using nensted top actions.
   
   @file
   
   @todo Improve concurrency of linearHashNTA and linkedListNTA.
*/

static pthread_mutex_t linear_hash_mutex;// = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;


typedef struct {
  recordid buckets;
  int keySize;
  int valueSize;
  long nextSplit;
  int bits;
  long numEntries;
} lladd_hash_header;


void LinearHashNTAInit() {
  // only need this function since PTHREAD_RECURSIVE_MUTEX_INITIALIZER is really broken...
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init(&linear_hash_mutex, &attr);
}


/* private methods... */
compensated_function static void ThashSplitBucket(int xid, recordid hashHeader, lladd_hash_header * lhh);


#define HASH_INIT_ARRAY_LIST_COUNT twoToThe(HASH_INIT_BITS)
#define HASH_INIT_ARRAY_LIST_MULT    2

compensated_function recordid ThashCreate(int xid, int keySize, int valueSize) {
  recordid hashHeader;
  lladd_hash_header lhh;

  try_ret(NULLRID) {
    hashHeader = Talloc(xid, sizeof(lladd_hash_header));
    if(keySize == VARIABLE_LENGTH || valueSize == VARIABLE_LENGTH) {
      lhh.buckets = TarrayListAlloc(xid, HASH_INIT_ARRAY_LIST_COUNT, HASH_INIT_ARRAY_LIST_MULT, sizeof(pagedListHeader));
    } else {
      lhh.buckets = TarrayListAlloc(xid, HASH_INIT_ARRAY_LIST_COUNT, HASH_INIT_ARRAY_LIST_MULT, sizeof(lladd_linkedList_entry) + keySize + valueSize);
    }
  } end_ret(NULLRID);
  try_ret(NULLRID) {
    TarrayListExtend(xid, lhh.buckets, HASH_INIT_ARRAY_LIST_COUNT);
  } end_ret(NULLRID);
  int i;
  recordid bucket = lhh.buckets;
  if(keySize == VARIABLE_LENGTH || valueSize == VARIABLE_LENGTH) {
    for(i = 0; i < HASH_INIT_ARRAY_LIST_COUNT; i++) {
      try_ret(NULLRID) {
	recordid rid = TpagedListAlloc(xid);
	bucket.slot = i;
	Tset(xid, bucket, &rid);
	//	printf("paged list alloced at rid {%d %d %d}\n", rid.page, rid.slot, rid.size);
      } end_ret(NULLRID);
    
    }

  } else {
    byte * entry = calloc(1, lhh.buckets.size);
    for(i = 0; i < HASH_INIT_ARRAY_LIST_COUNT; i++) {
      bucket.slot = i;
      begin_action_ret(free, entry, NULLRID) {
	Tset(xid, bucket, entry);
      } end_action_ret(NULLRID);
    }
    free (entry);
  }
  lhh.keySize = keySize;
  lhh.valueSize = valueSize;
  lhh.nextSplit = 0;
  lhh.bits = HASH_INIT_BITS;
  lhh.numEntries = 0;
  try_ret(NULLRID) {
    Tset(xid, hashHeader, &lhh);
  } end_ret(NULLRID);
  return hashHeader;
}
  
compensated_function void ThashDelete(int xid, recordid hash) {
  abort();
}

compensated_function static int __ThashInsert(int xid, recordid hashHeader, const byte* key, int keySize, const byte* value, int valueSize);
compensated_function static int __ThashRemove(int xid, recordid hashHeader, const byte * key, int keySize);

typedef struct {
  recordid hashHeader;
  int keySize;
} linearHash_insert_arg;

typedef struct {
  recordid hashHeader;
  int keySize;
  int valueSize;
} linearHash_remove_arg;

compensated_function static int operateInsert(int xid, Page *p,  lsn_t lsn, recordid rid, const void *dat) {
  const linearHash_remove_arg * args = dat;
  recordid hashHeader = args->hashHeader;
  int keySize = args->keySize;
  int valueSize = args->valueSize;
  
  assert(valueSize >= 0);

  byte * key = (byte*)(args+1);
  byte * value = ((byte*)(args+1))+ keySize;
  begin_action_ret(pthread_mutex_unlock, &linear_hash_mutex, compensation_error()) {
    pthread_mutex_lock(&linear_hash_mutex);
    __ThashInsert(xid, hashHeader, key, keySize, value, valueSize);
  } compensate_ret(compensation_error());
  return 0;  
}
compensated_function static int operateRemove(int xid, Page *p,  lsn_t lsn, recordid rid, const void *dat) {
  const linearHash_insert_arg * args = dat;
  recordid hashHeader = args->hashHeader;
  int keySize = args->keySize;  
  
  byte * key = (byte*)(args + 1);
  begin_action_ret(pthread_mutex_unlock, &linear_hash_mutex, compensation_error()) {
    pthread_mutex_lock(&linear_hash_mutex);
    __ThashRemove(xid, hashHeader, key, keySize);
  } compensate_ret(compensation_error());
  
  return 0;
}
Operation getLinearHashInsert() {
  Operation o = {
    //    OPERATION_LINEAR_HASH_INSERT, 
    OPERATION_NOOP,
    SIZEIS_PAGEID,
    OPERATION_LINEAR_HASH_REMOVE,
    &operateInsert
    //    &noop
  };
  return o;
}
Operation getLinearHashRemove() {
  Operation o = {
    //    OPERATION_LINEAR_HASH_REMOVE, 
    OPERATION_NOOP,
    SIZEIS_PAGEID,
    OPERATION_LINEAR_HASH_INSERT,
    &operateRemove
    //&noop
  };
  return o;
}

compensated_function int ThashInsert(int xid, recordid hashHeader, const byte* key, int keySize, const byte* value, int valueSize) {
  pthread_mutex_lock(&linear_hash_mutex);
  int argSize = sizeof(linearHash_insert_arg)+keySize;
  linearHash_insert_arg * arg = malloc(argSize);
  arg->hashHeader = hashHeader;
  arg->keySize = keySize;
  memcpy(arg+1, key, keySize);

  int ret;

  /** @todo MEMORY LEAK arg, handle on pthread_cancel.. */
  void * handle;
  begin_action_ret(pthread_mutex_unlock, &linear_hash_mutex, compensation_error()) {
    handle = TbeginNestedTopAction(xid, OPERATION_LINEAR_HASH_INSERT, (byte*)arg, argSize);
    free(arg);
    ret = __ThashInsert(xid, hashHeader, key, keySize, value, valueSize);  
  } end_action_ret(compensation_error());
  //  beg in_action_ret(pthread_mutex_unlock, &linear_hash_mutex, compensation_error()) {
    TendNestedTopAction(xid, handle);
    // } comp ensate_ret(compensation_error());
    pthread_mutex_unlock(&linear_hash_mutex);
  return ret;
}
compensated_function static int __ThashInsert(int xid, recordid hashHeader, const byte* key, int keySize, const byte* value, int valueSize) {
  lladd_hash_header lhh;
  try_ret(compensation_error()) {
    Tread(xid, hashHeader, &lhh);
  } end_ret(compensation_error());
  lhh.numEntries ++;
  try_ret(compensation_error()) {
    if(lhh.keySize == VARIABLE_LENGTH || lhh.valueSize == VARIABLE_LENGTH) {
      if(lhh.numEntries > (int)((double)(lhh.nextSplit + twoToThe(lhh.bits-1)) * (HASH_FILL_FACTOR))) {
	ThashSplitBucket(xid, hashHeader, &lhh);
      } 
    } else {
      if(lhh.numEntries > (int)((double)(lhh.nextSplit + twoToThe(lhh.bits-1)) * HASH_FILL_FACTOR)) {
	ThashSplitBucket(xid, hashHeader, &lhh);
      }
    }
  } end_ret(compensation_error());

  recordid bucket = lhh.buckets;
  bucket.slot = hash(key, keySize, lhh.bits, lhh.nextSplit);

  int ret;
  try_ret(compensation_error()) {  
    
    if(lhh.keySize == VARIABLE_LENGTH || lhh.valueSize == VARIABLE_LENGTH) {
      
      recordid bucketList;
      
      Tread(xid, bucket, &bucketList);
      
      //    int before = TpagedListSpansPages(xid, bucketList);

      ret = TpagedListInsert(xid, bucketList, key, keySize, value, valueSize);
      
      //    int after = TpagedListSpansPages(xid, bucketList);
      //    if(before != after) {  // Page overflowed...
      //      T hashSplitBucket(xid, hashHeader, &lhh);
      //      T hashSplitBucket(xid, hashHeader, &lhh);
      //    }

    } else {
      assert(lhh.keySize == keySize); assert(lhh.valueSize == valueSize);
      ret = TlinkedListInsert(xid, bucket, key, keySize, value, valueSize);
    }
    if(ret) { lhh.numEntries--; }
    Tset(xid, hashHeader, &lhh);

  } end_ret(compensation_error()); 
  
  return ret;
}
compensated_function int ThashRemove(int xid, recordid hashHeader, const byte * key, int keySize) {

  byte * value;
  int valueSize;
  int ret; 
  begin_action_ret(pthread_mutex_unlock, &linear_hash_mutex, compensation_error()) {
    pthread_mutex_lock(&linear_hash_mutex);
    valueSize = ThashLookup(xid, hashHeader, key, keySize, &value);
  } end_action_ret(compensation_error());

  if(valueSize == -1) {
    pthread_mutex_unlock(&linear_hash_mutex);
    return 0; 
  }

  begin_action_ret(pthread_mutex_unlock, &linear_hash_mutex, compensation_error()) {
    
    int argSize = sizeof(linearHash_remove_arg) + keySize + valueSize;
    linearHash_remove_arg * arg = malloc(argSize);
    arg->hashHeader = hashHeader;
    arg->keySize = keySize;
    arg->valueSize = valueSize;
    memcpy(arg+1, key, keySize);
    memcpy((byte*)(arg+1)+keySize, value, valueSize);
    void * handle;

    handle = TbeginNestedTopAction(xid, OPERATION_LINEAR_HASH_REMOVE, (byte*)arg, argSize);
    free(arg);
    free(value);

    ret = __ThashRemove(xid, hashHeader, key, keySize);
    TendNestedTopAction(xid, handle);

  } compensate_ret(compensation_error());

  return ret;
}

compensated_function static int __ThashRemove(int xid, recordid hashHeader, const byte * key, int keySize) {
  int ret;

  try_ret(compensation_error()) {
    lladd_hash_header lhh;
    Tread(xid, hashHeader, &lhh);
    lhh.numEntries--;
    Tset(xid, hashHeader, &lhh);
    
    recordid bucket = lhh.buckets;
    bucket.slot = hash(key, keySize, lhh.bits, lhh.nextSplit);
    
    if(lhh.keySize == VARIABLE_LENGTH || lhh.valueSize == VARIABLE_LENGTH) {
      recordid bucketList;
      Tread(xid, bucket, &bucketList);
      ret = TpagedListRemove(xid, bucketList, key, keySize);
    } else {
      if(lhh.keySize != keySize) { compensation_set_error(LLADD_INTERNAL_ERROR); }
      assert(lhh.keySize == keySize);
      ret = TlinkedListRemove(xid, bucket, key, keySize);
    }
  } end_ret(compensation_error());
    
  return ret;
}

compensated_function int ThashLookup(int xid, recordid hashHeader, const byte * key, int keySize, byte ** value) {
  lladd_hash_header lhh;
  int ret;

  // This whole thing is safe since the callee's do not modify global state... 
  
  begin_action_ret(pthread_mutex_unlock, &linear_hash_mutex, compensation_error()) {
    pthread_mutex_lock(&linear_hash_mutex);
    Tread(xid, hashHeader, &lhh);
  
    recordid bucket = lhh.buckets;
    bucket.slot = hash(key, keySize, lhh.bits, lhh.nextSplit);
    
    if(lhh.keySize == VARIABLE_LENGTH || lhh.valueSize == VARIABLE_LENGTH) {
      recordid bucketList;
      Tread(xid, bucket, &bucketList);
      ret = TpagedListFind(xid, bucketList, key, keySize, value);
    } else {
      assert(lhh.keySize == keySize);
      ret = TlinkedListFind(xid, bucket, key, keySize, value);
    }
  } compensate_ret(compensation_error());
  
  return ret;
}
compensated_function static void ThashSplitBucket(int xid, recordid hashHeader, lladd_hash_header * lhh) {
  // if(1) { return; }

  try {
    long old_bucket = lhh->nextSplit;
    long new_bucket = old_bucket + twoToThe(lhh->bits-1);
    recordid old_bucket_rid = lhh->buckets;
    recordid new_bucket_rid = lhh->buckets;
    old_bucket_rid.slot = old_bucket;
    new_bucket_rid.slot = new_bucket;
    // void * handle = TbeginNestedTopAction(xid, OPERATION_NOOP, NULL, 0);
    TarrayListExtend(xid, lhh->buckets, 1);
    recordid new_bucket_list; // will be uninitialized if we have fixed length entries.
    if(lhh->keySize == VARIABLE_LENGTH || lhh->valueSize == VARIABLE_LENGTH) {
      new_bucket_list = TpagedListAlloc(xid);
      Tset(xid, new_bucket_rid, &new_bucket_list);
    } else {
      byte * entry = calloc(1, lhh->buckets.size);
      Tset(xid, new_bucket_rid, entry);
      free(entry);  
    }
    if(lhh->nextSplit < twoToThe(lhh->bits-1)-1) {
      lhh->nextSplit++;
    } else {
      lhh->nextSplit = 0;
      lhh->bits++;    
    }
    
    /** @todo linearHashNTA's split bucket should use the 'move' function call. */
    if(lhh->keySize == VARIABLE_LENGTH || lhh->valueSize == VARIABLE_LENGTH) {
      recordid old_bucket_list;
      Tread(xid, old_bucket_rid, &old_bucket_list);
      
      lladd_pagedList_iterator * pit = TpagedListIterator(xid, old_bucket_list);
    
      byte *key, *value;
      int keySize, valueSize;
      while(TpagedListNext(xid, pit, &key, &keySize, &value, &valueSize)) {
	if(hash(key, keySize, lhh->bits, lhh->nextSplit) != old_bucket) {
	  TpagedListRemove(xid, old_bucket_list, key, keySize);
	  TpagedListInsert(xid, new_bucket_list, key, keySize, value, valueSize);
	}
	free(key);
	free(value);
      }
    } else {
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
    }
  } end;

  //  TendNestedTopAction(xid, handle);
  return;
}
lladd_hash_iterator * ThashIterator(int xid, recordid hashHeader, int keySize, int valueSize) {
  lladd_hash_iterator * it = malloc(sizeof(lladd_hash_iterator));
  begin_action_ret(free, it, NULL) {
    it->hashHeader = hashHeader;
    lladd_hash_header lhh;
    Tread(xid, hashHeader, &lhh);
    it->bucket = lhh.buckets;
    it->numBuckets = lhh.nextSplit +twoToThe(lhh.bits-1);
    it->bucket.slot = 0;
    it->keySize = keySize;
    it->valueSize = valueSize;
    assert(keySize == lhh.keySize);
    assert(valueSize == lhh.valueSize);
    if(keySize == VARIABLE_LENGTH || valueSize == VARIABLE_LENGTH) {
      it->it = NULL;
      recordid bucketList;
      Tread(xid, it->bucket, &bucketList);
      it->pit= TpagedListIterator(xid, bucketList);
    } else {
      it->pit = NULL;
      it->it = TlinkedListIterator(xid, it->bucket, it->keySize, it->valueSize);
    }
  } end_action_ret(NULL);
  return it;
}
  
int ThashNext(int xid, lladd_hash_iterator * it, byte ** key, int * keySize, byte** value, int * valueSize) {
  try_ret(0) { 
    if(it->it) {
      assert(!it->pit);
      while(!TlinkedListNext(xid, it->it, key, keySize, value, valueSize)) {
	if(compensation_error()) { return 0; }
	it->bucket.slot++;
	if(it->bucket.slot < it->numBuckets) {
	  it->it = TlinkedListIterator(xid, it->bucket, it->keySize, it->valueSize); 
	} else {
	  free(it);
	  return 0;
	}
      }
    } else {
      assert(it->pit);
      while(!TpagedListNext(xid, it->pit, key, keySize, value, valueSize)) {
	if(compensation_error()) { return 0; }
	it->bucket.slot++;
	if(it->bucket.slot < it->numBuckets) {
	  recordid bucketList;
	  Tread(xid, it->bucket, &bucketList);
	  it->pit = TpagedListIterator(xid, bucketList);
	} else {
	  free(it);
	  return 0;
	}
      }
    }
  } end_ret(0);
  return 1;
}

void ThashDone(int xid, lladd_hash_iterator * it) {
  if(it->it) {
    free(it->it);
  } 
  if(it->pit) {
    free(it->pit);
  }
  free(it);
}
