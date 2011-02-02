#include <config.h>
#define __USE_GNU
#define _GNU_SOURCE
#include <stasis/latches.h>
#include <stasis/transactional.h>
#include <stasis/hash.h>
#include <assert.h>
#include <string.h>
// The next two #includes are for deprecated code.
#include <stasis/fifo.h>
#include <stasis/multiplexer.h>
#include <stasis/logger/logMemory.h>
/**
   re-entrant implementation of a linear hash hable, using nested top actions.

   @file

   @todo Improve concurrency of linearHashNTA and linkedListNTA by leveraging Page.impl on the data structure header page?
*/


static void linearHashNTAIterator_close(int xid, void * it);
static int  linearHashNTAIterator_next (int xid, void * it);
static int  linearHashNTAIterator_key  (int xid, void * it, byte **key);
static int  linearHashNTAIterator_value(int xid, void * it, byte **value);

static pthread_mutex_t linear_hash_mutex;// = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;

typedef struct {
  recordid buckets;
  int keySize;
  int valueSize;
  pageid_t nextSplit;
  int bits;
  long numEntries;
} lladd_hash_header;

static void noopTupDone(int xid, void * foo) { }

void LinearHashNTAInit() {
  // only need this function since PTHREAD_RECURSIVE_MUTEX_INITIALIZER is really broken...
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init(&linear_hash_mutex, &attr);
  pthread_mutexattr_destroy(&attr);

  lladdIterator_def_t linearHashNTA_def = {
   linearHashNTAIterator_close,
   linearHashNTAIterator_next,
   linearHashNTAIterator_next,
   linearHashNTAIterator_key,
   linearHashNTAIterator_value,
   noopTupDone,
  };
  lladdIterator_register(LINEAR_HASH_NTA_ITERATOR, linearHashNTA_def);
}
void LinearHashNTADeinit() {
  pthread_mutex_destroy(&linear_hash_mutex);
}

/* private methods... */
compensated_function static void ThashSplitBucket(int xid, recordid hashHeader, lladd_hash_header * lhh);
/** @todo Remove defined HASH_INIT_ARRAY_LIST_COUNT */
#define HASH_INIT_ARRAY_LIST_COUNT (stasis_util_two_to_the(HASH_INIT_BITS))
#define HASH_INIT_ARRAY_LIST_MULT    2

compensated_function recordid ThashCreate(int xid, int keySize, int valueSize) {
  recordid hashHeader;
  lladd_hash_header lhh;

  memset(&lhh,0,sizeof(lhh));

  try_ret(NULLRID) {
    hashHeader = Talloc(xid, sizeof(lladd_hash_header));
    if(keySize == VARIABLE_LENGTH || valueSize == VARIABLE_LENGTH) {
      lhh.buckets = TarrayListAlloc(xid, HASH_INIT_ARRAY_LIST_COUNT, HASH_INIT_ARRAY_LIST_MULT, sizeof(recordid));
    } else {
      lhh.buckets = TarrayListAlloc(xid, HASH_INIT_ARRAY_LIST_COUNT, HASH_INIT_ARRAY_LIST_MULT, sizeof(stasis_linkedList_entry) + keySize + valueSize);
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
#ifdef ARRAY_LIST_OLD_ALLOC
    byte * entry = calloc(1, lhh.buckets.size);
    for(i = 0; i < HASH_INIT_ARRAY_LIST_COUNT; i++) {
      bucket.slot = i;
      begin_action_ret(free, entry, NULLRID) {
	Tset(xid, bucket, entry);
      } end_action_ret(NULLRID);
    }
    free (entry);
#endif
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

/*typedef struct {
  recordid hashHeader;
  int keySize;
} linearHash_insert_arg;

typedef struct {
  recordid hashHeader;
  int keySize;
  int valueSize;
  } linearHash_remove_arg;*/

//compensated_function static int operateInsert(int xid, Page *p,  lsn_t lsn, recordid rid, const void *dat) {
compensated_function static int op_linear_hash_insert(const LogEntry* e, Page* p) {
  const linearHash_remove_arg * args = stasis_log_entry_update_args_cptr(e);
  recordid hashHeader = args->hashHeader;
  int keySize = args->keySize;
  int valueSize = args->valueSize;

  assert(valueSize >= 0);

  byte * key = (byte*)(args+1);
  byte * value = ((byte*)(args+1))+ keySize;
  begin_action_ret(pthread_mutex_unlock, &linear_hash_mutex, compensation_error()) {
    pthread_mutex_lock(&linear_hash_mutex);
    __ThashInsert(e->xid, hashHeader, key, keySize, value, valueSize);
  } compensate_ret(compensation_error());
  return 0;
}
compensated_function static int op_linear_hash_remove(const LogEntry* e, Page* p) {
  const linearHash_insert_arg * args = stasis_log_entry_update_args_cptr(e);
  recordid hashHeader = args->hashHeader;
  int keySize = args->keySize;

  byte * key = (byte*)(args + 1);
  begin_action_ret(pthread_mutex_unlock, &linear_hash_mutex, compensation_error()) {
    pthread_mutex_lock(&linear_hash_mutex);
    __ThashRemove(e->xid, hashHeader, key, keySize);
  } compensate_ret(compensation_error());

  return 0;
}
stasis_operation_impl stasis_op_impl_linear_hash_insert() {
  stasis_operation_impl o = {
    OPERATION_LINEAR_HASH_INSERT,
    UNKNOWN_TYPE_PAGE,
    OPERATION_NOOP,
    OPERATION_LINEAR_HASH_REMOVE,
    &op_linear_hash_insert
  };
  return o;
}
stasis_operation_impl stasis_op_impl_linear_hash_remove() {
  stasis_operation_impl o = {
    OPERATION_LINEAR_HASH_REMOVE,
    UNKNOWN_TYPE_PAGE,
    OPERATION_NOOP,
    OPERATION_LINEAR_HASH_INSERT,
    &op_linear_hash_remove
  };
  return o;
}

compensated_function int ThashInsert(int xid, recordid hashHeader, const byte* key, int keySize, const byte* value, int valueSize) {
  /* XXX slow, but doesn't generate any log entries unless the key exists */
  int ret = ThashRemove(xid, hashHeader, key, keySize);
  hashHeader.size = sizeof(lladd_hash_header);
  pthread_mutex_lock(&linear_hash_mutex);
  int argSize = sizeof(linearHash_insert_arg)+keySize;
  linearHash_insert_arg * arg = calloc(1,argSize);
  arg->hashHeader = hashHeader;
  arg->keySize = keySize;
  memcpy(arg+1, key, keySize);

  /** @todo MEMORY LEAK arg, handle on pthread_cancel.. */
  void * handle;
  begin_action_ret(pthread_mutex_unlock, &linear_hash_mutex, compensation_error()) {
    handle = TbeginNestedTopAction(xid, OPERATION_LINEAR_HASH_INSERT, (byte*)arg, argSize);
    free(arg);
    __ThashInsert(xid, hashHeader, key, keySize, value, valueSize);
  } end_action_ret(compensation_error());

  TendNestedTopAction(xid, handle);

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
      if(lhh.numEntries > (int)((double)(lhh.nextSplit
                          + stasis_util_two_to_the(lhh.bits-1)) * (HASH_FILL_FACTOR))) {
	ThashSplitBucket(xid, hashHeader, &lhh);
      }
    } else {
      if(lhh.numEntries > (int)((double)(lhh.nextSplit
                          + stasis_util_two_to_the(lhh.bits-1)) * HASH_FILL_FACTOR)) {
	ThashSplitBucket(xid, hashHeader, &lhh);
      }
    }
  } end_ret(compensation_error());

  recordid bucket = lhh.buckets;
  bucket.slot = stasis_linear_hash(key, keySize, lhh.bits, lhh.nextSplit);

  int ret;
  try_ret(compensation_error()) {

    if(lhh.keySize == VARIABLE_LENGTH || lhh.valueSize == VARIABLE_LENGTH) {

      recordid bucketList;

      Tread(xid, bucket, &bucketList);

      //    int before = TpagedListSpansPages(xid, bucketList);
      ret = TpagedListRemove(xid, bucketList, key, keySize);
      TpagedListInsert(xid, bucketList, key, keySize, value, valueSize);

      //    int after = TpagedListSpansPages(xid, bucketList);
      //    if(before != after) {  // Page overflowed...
      //      T hashSplitBucket(xid, hashHeader, &lhh);
      //      T hashSplitBucket(xid, hashHeader, &lhh);
      //    }

    } else {
      assert(lhh.keySize == keySize); assert(lhh.valueSize == valueSize);
      ret = TlinkedListRemove(xid, bucket, key, keySize);
      TlinkedListInsert(xid, bucket, key, keySize, value, valueSize);
    }
    if(ret) { lhh.numEntries--; }
    Tset(xid, hashHeader, &lhh);

  } end_ret(compensation_error());

  return ret;
}
compensated_function int ThashRemove(int xid, recordid hashHeader, const byte * key, int keySize) {
  hashHeader.size = sizeof(lladd_hash_header);
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
    linearHash_remove_arg * arg = calloc(1,argSize);
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
    bucket.slot = stasis_linear_hash(key, keySize, lhh.bits, lhh.nextSplit);

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
  hashHeader.size = sizeof(lladd_hash_header);
  int ret;

  // This whole thing is safe since the callee's do not modify global state...

  begin_action_ret(pthread_mutex_unlock, &linear_hash_mutex, compensation_error()) {
    pthread_mutex_lock(&linear_hash_mutex);
    Tread(xid, hashHeader, &lhh);

    recordid bucket = lhh.buckets;
    bucket.slot = stasis_linear_hash(key, keySize, lhh.bits, lhh.nextSplit);

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

  try {
    long old_bucket = lhh->nextSplit;
    long new_bucket = old_bucket + stasis_util_two_to_the(lhh->bits-1);
    recordid old_bucket_rid = lhh->buckets;
    recordid new_bucket_rid = lhh->buckets;
    old_bucket_rid.slot = old_bucket;
    new_bucket_rid.slot = new_bucket;
    if(!(new_bucket % HASH_INIT_ARRAY_LIST_COUNT)) {
      TarrayListExtend(xid, lhh->buckets, HASH_INIT_ARRAY_LIST_COUNT);
    }
    recordid new_bucket_list; // will be uninitialized if we have fixed length entries.
    if(lhh->keySize == VARIABLE_LENGTH || lhh->valueSize == VARIABLE_LENGTH) {
      new_bucket_list = TpagedListAlloc(xid);
      Tset(xid, new_bucket_rid, &new_bucket_list);
    } else {
#ifdef ARRAY_LIST_OLD_ALLOC
      byte * entry = calloc(1, lhh->buckets.size);
      Tset(xid, new_bucket_rid, entry);
      free(entry);
#endif
    }
    if(lhh->nextSplit < stasis_util_two_to_the(lhh->bits-1)-1) {
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
	if(stasis_linear_hash(key, keySize, lhh->bits, lhh->nextSplit) != old_bucket) {
	  TpagedListRemove(xid, old_bucket_list, key, keySize);
	  TpagedListInsert(xid, new_bucket_list, key, keySize, value, valueSize);
	}
	free(key);
	free(value);
      }
      TpagedListClose(xid,pit);
    } else {
      stasis_linkedList_iterator * it = TlinkedListIterator(xid, old_bucket_rid, lhh->keySize, lhh->valueSize);
      byte * key, *value;
      int keySize, valueSize;
      while(TlinkedListNext(xid, it, &key, &keySize, &value, &valueSize)) {
	assert(valueSize == lhh->valueSize);
	assert(keySize == lhh->keySize);
	if(stasis_linear_hash(key, keySize, lhh->bits, lhh->nextSplit) != old_bucket) {
	  TlinkedListRemove(xid, old_bucket_rid, key, keySize);
	  TlinkedListInsert(xid, new_bucket_rid, key, keySize, value, valueSize);
	}
	free(key);
	free(value);
      }
      TlinkedListClose(xid, it);
    }
  } end;

  return;
}
lladd_hash_iterator * ThashIterator(int xid, recordid hashHeader, int keySize, int valueSize) {
  hashHeader.size = sizeof(lladd_hash_header);
  lladd_hash_iterator * it = calloc(1,sizeof(lladd_hash_iterator));
  begin_action_ret(free, it, NULL) {
    it->hashHeader = hashHeader;
    lladd_hash_header lhh;
    Tread(xid, hashHeader, &lhh);
    it->bucket = lhh.buckets;
    it->numBuckets = lhh.nextSplit + stasis_util_two_to_the(lhh.bits-1);
    it->bucket.slot = 0;
    /*    it->keySize = keySize;
    it->valueSize = valueSize;
    assert(keySize == lhh.keySize);
    assert(valueSize == lhh.valueSize); */
    keySize = lhh.keySize;
    it->keySize = lhh.keySize;
    valueSize = lhh.valueSize;
    it->valueSize = lhh.valueSize;
    if(keySize == VARIABLE_LENGTH || valueSize == VARIABLE_LENGTH) {
      it->it = NULL;
      recordid bucketList;
      assert(it->bucket.size == sizeof(bucketList));
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
	  TlinkedListClose(xid, it->it);
	  it->it = TlinkedListIterator(xid, it->bucket, it->keySize, it->valueSize);
	} else {
	  TlinkedListClose(xid, it->it);
	  it->it = 0;
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
	  TpagedListClose(xid,it->pit);
	  it->pit = TpagedListIterator(xid, bucketList);
	} else {
	  TpagedListClose(xid,it->pit);
	  it->pit = 0;
	  return 0;
	}
      }
    }
  } end_ret(0);
  return 1;
}

void ThashDone(int xid, lladd_hash_iterator * it) {
  if(it->it) {
    TlinkedListClose(xid, it->it);
  }
  if(it->pit) {
    TpagedListClose(xid, it->pit);
  }
  free(it);
}

typedef struct {
  lladd_hash_iterator* hit;
  byte * lastKey;
  int lastKeySize;
  byte * lastValue;
  int lastValueSize;
} lladd_linearHashNTA_generic_it;

lladdIterator_t * ThashGenericIterator(int xid, recordid hash) {
  lladdIterator_t * ret = malloc(sizeof(lladdIterator_t));
  ret->type = LINEAR_HASH_NTA_ITERATOR;
  ret->impl = malloc(sizeof(lladd_linearHashNTA_generic_it));

  ((lladd_linearHashNTA_generic_it*)(ret->impl))->hit = ThashIterator(xid, hash, -1, -1);
  ((lladd_linearHashNTA_generic_it*)(ret->impl))->lastKey = NULL;
  ((lladd_linearHashNTA_generic_it*)(ret->impl))->lastValue = NULL;

  return ret;

}

static void linearHashNTAIterator_close(int xid, void * impl) {
  lladd_linearHashNTA_generic_it * it = impl;

  ThashDone(xid, it->hit);

  if(it->lastKey) {
    free(it->lastKey);
  }
  if(it->lastValue) {
    free(it->lastValue);
  }
  free(it);
}

static int linearHashNTAIterator_next (int xid, void * impl) {
  lladd_linearHashNTA_generic_it * it = impl;

  if(it->lastKey) {
    free(it->lastKey);
    it->lastKey = NULL;
  }
  if(it->lastValue) {
    free(it->lastValue);
    it->lastValue = NULL;
  }
  return ThashNext(xid, it->hit, &(it->lastKey), &it->lastKeySize, &it->lastValue, &it->lastValueSize);
}

static int linearHashNTAIterator_key(int xid, void * impl, byte ** key) {
  lladd_linearHashNTA_generic_it * it = impl;

  *key = it->lastKey;

  return (it->lastKey   == NULL) ? 0 : it->lastKeySize;
}

static int linearHashNTAIterator_value(int xid, void * impl, byte ** value) {
  lladd_linearHashNTA_generic_it * it = impl;

  *value = it->lastValue;

  return (it->lastValue == NULL) ? 0 : it->lastValueSize;
}


//---------------------------------  async hash operations happen below here

typedef struct {
  int value_len;
  int key_len;
} asyncHashInsert_t;

void ThashInsertAsync(int xid, lladdConsumer_t * cons, recordid hash, byte * value, int value_len, byte * key, int key_len) {

  Tconsumer_push(xid, cons, key, key_len, value, value_len);

}

void ThashInsertConsume(int xid, recordid hash, lladdIterator_t * it) {

  while(Titerator_next(xid, it)) {

    byte * key;
    byte * value;

    int key_len = Titerator_key(xid, it, &key);
    int value_len = Titerator_value(xid, it, &value);

    ThashInsert(xid, hash, key, key_len, value, value_len);

    Titerator_tupleDone(xid, it);
  }
}

typedef struct {
  lladdIterator_t * it;
  recordid hash;
  int xid;
} hashAsyncWorker_arg;

void * ThashAsyncWorker(void * argp) {
  hashAsyncWorker_arg * arg = (hashAsyncWorker_arg*)argp;

  while(Titerator_next(arg->xid, arg->it)) {
    byte * fifo;
    int fifo_size = Titerator_value(arg->xid, arg->it, &fifo);
    assert(fifo_size == sizeof(lladdFifo_t));

    ThashInsertConsume(arg->xid, arg->hash,  ((lladdFifo_t*)fifo)->iterator);

    Titerator_tupleDone(arg->xid, arg->it);
  }
  return NULL;
}

/*lladdMultiplexer_t **/
lladdConsumer_t *  TasyncHashInit(int xid, recordid rid, int numWorkerThreads,
				  int mainFifoLen, int numFifos,
				  int subFifoLen, int dirtyFifoLen,
				  lladdIterator_t ** dirtyIterator) {

  lladdFifo_t * mainFifo   = logMemoryFifo(mainFifoLen, 0);
  lladdFifo_t * dirtyFifos = logMemoryFifo(dirtyFifoLen, 0);
  lladdFifoPool_t * fifoPool = lladdFifoPool_ringBufferInit(numFifos, subFifoLen, NULL, dirtyFifos);
  lladdMultiplexer_t * mux = lladdMultiplexer_alloc(xid, mainFifo->iterator, &multiplexHashLogByKey, fifoPool);


  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setstacksize (&attr, PTHREAD_STACK_MIN);

  lladdMultiplexer_start(mux, &attr);

  int i = 0;


  for(i = 0; i < numWorkerThreads; i++) {
    pthread_t thread;
    pthread_create(&thread, &attr, ThashAsyncWorker, mux->fifoPool->dirtyPoolFifo->iterator);
    pthread_detach(thread);
  }

  if(dirtyIterator) {
    *dirtyIterator = mux->fifoPool->dirtyPoolFifo->iterator;
  }

  return mainFifo->consumer;
}
