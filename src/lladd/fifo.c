#include <lladd/fifo.h>
#include <lladd/crc32.h>
#include <stdlib.h>
#include <stdio.h>
#include "logger/logMemory.h"

#include <string.h>
#include <assert.h>

/** 
    Obtain a member of a fifoPool based on the value of multiplexKey.  Use CRC32 to assign the key to a consumer. 
*/
lladdFifo_t * lladdFifoPool_getFifoCRC32( lladdFifoPool_t * pool, byte * multiplexKey, size_t multiplexKeySize) {
  int memberId =  crc32(multiplexKey, multiplexKeySize, (unsigned long)-1L) % pool->fifoCount;
  return pool->pool[memberId];
}
void lladdFifoPool_markDirty(int xid, lladdFifoPool_t * pool, lladdFifo_t * fifo) {
  if(pool->dirtyPoolFifo) {
    Tconsumer_push(xid, pool->dirtyPoolFifo->consumer, NULL, 0, (byte*)&fifo, sizeof(lladdFifo_t*));
  }
}

/**
   Create a new pool of ringBuffer based fifos

   @param consumerCount the number of consumers in the pool.
   @todo this function should be generalized to other consumer implementations.
*/
lladdFifoPool_t * lladdFifoPool_ringBufferInit (int consumerCount, int bufferSize, 
					   lladdFifoPool_getFifo_t * getFifo, lladdFifo_t * dirtyPoolFifo) {
  lladdFifoPool_t * pool = malloc(sizeof(lladdFifoPool_t));

  pool->getFifo = getFifo ? getFifo : lladdFifoPool_getFifoCRC32;
  pool->dirtyPoolFifo = dirtyPoolFifo;

  pool->pool = malloc(sizeof(lladdFifo_t*) * consumerCount);
  int i;
  for(i = 0; i < consumerCount; i++) {
    pool->pool[i] = logMemoryFifo(bufferSize, 0);
  }
  pool->fifoCount = consumerCount;
  return pool;
}

typedef struct { 
  int maxPtrs;
  int outPtrs;
  pthread_mutex_t mutex;
  pthread_cond_t  writeOK;
} lladdFifoPointerPool_t;

typedef struct pointerFifoEntry {
  struct pointerFifoEntry * prev;
  struct pointerFifoEntry * next;
  int keySize;
  int valSize;
} pointerFifoEntry;

typedef struct { 
  pointerFifoEntry * first;
  pointerFifoEntry * last;
  pointerFifoEntry * current;
  pthread_mutex_t mutex;
  int eof;
  pthread_cond_t  readOK;
  //pthread_cond_t  writeOK;
  lladdFifoPointerPool_t * pool;
} pointerFifoImpl;

static void doNext(int xid, pointerFifoImpl* impl) {

  impl->current = impl->last;
  if(impl->last->prev) {
    impl->last->prev->next = NULL;
    impl->last=impl->last->prev;
  } else {
    impl->first = NULL;
    impl->last  = NULL;
  }

}

int lladdFifoPool_iterator_next(int xid, void * it) {
  pointerFifoImpl * impl = (pointerFifoImpl *) it;

  pthread_mutex_lock(&(impl->mutex));

  while(impl->last == NULL && (!impl->eof)) {
    pthread_cond_wait(&(impl->readOK), &(impl->mutex));
  }

  if(impl->eof) {
    pthread_mutex_unlock(&(impl->mutex));
    return 0;
  }
  doNext(xid, impl);

  //  pthread_mutex_unlock(&(impl->mutex));

  return 1;

}
int lladdFifoPool_iterator_tryNext(int xid, void * it) {
  pointerFifoImpl * impl = (pointerFifoImpl *) it;
  
  pthread_mutex_lock(&(impl->mutex));

  if(impl->last == NULL) {
    pthread_mutex_unlock(&(impl->mutex));
    return 0;
  } else { 
    doNext(xid, impl);
  }

  //  pthread_mutex_unlock(&(impl->mutex));
  return 1;
}

void lladdFifoPool_iterator_releaseLock(int xid, void * it) {
  abort(); // didn't implement nextOrEmpty yet...
}

void lladdFifoPool_iterator_tupleDone(int xid, void * it) {
  pointerFifoImpl * impl = (pointerFifoImpl *) it;

  free(impl->current);

  pthread_mutex_unlock(&(impl->mutex));

  pthread_mutex_lock(&(impl->pool->mutex));
  impl->pool->outPtrs--;
  pthread_mutex_unlock(&(impl->pool->mutex));
  pthread_cond_broadcast(&(impl->pool->writeOK));
}

int lladdFifoPool_iterator_key (int xid, void * it, byte ** key) {
  pointerFifoImpl * impl = (pointerFifoImpl *) it;
  *key = (byte*)(impl->current+1);
  return impl->current->keySize;
}
int lladdFifoPool_iterator_value (int xid, void * it, byte ** val) {
  pointerFifoImpl * impl = (pointerFifoImpl *) it;
  *val = ((byte*)(impl->current+1))+impl->current->keySize;
  return impl->current->valSize;
}

void lladdFifoPool_iterator_close(int xid, void * it) {
  pointerFifoImpl * impl = (pointerFifoImpl *) it;
  pthread_mutex_lock(&(impl->mutex));
  assert(impl->eof);
  assert((!impl->first) && (!impl->last));
  printf("Leaking iterator in lladdFifoPool_iterator_close\n");
  pthread_mutex_unlock(&(impl->mutex));
}

void lladdFifoPool_consumer_close(int xid, void * it) {
  pointerFifoImpl * impl = (pointerFifoImpl *) it;
  pthread_mutex_lock(&(impl->mutex));
  impl->eof = 1;
  pthread_cond_broadcast(&(impl->readOK));
  pthread_mutex_unlock(&(impl->mutex));
}

int lladdFifoPool_consumer_push(int xid, void * it, byte * key, size_t keySize, byte * val, size_t valSize) {
  pointerFifoImpl * impl = (pointerFifoImpl *) it;

  pthread_mutex_lock(&(impl->pool->mutex));
  while(impl->pool->outPtrs == impl->pool->maxPtrs) {
    pthread_cond_wait(&(impl->pool->writeOK), &(impl->pool->mutex));
  }
  impl->pool->outPtrs++;
  pthread_mutex_unlock(&(impl->pool->mutex));

  pointerFifoEntry * entry = malloc(sizeof(pointerFifoEntry) + keySize + valSize);

  // do 'expensive stuff' w/o a mutex
  memcpy(entry+1, key, keySize);
  memcpy(((byte*)(entry+1))+keySize, val, valSize);
  entry->keySize = keySize;
  entry->valSize = valSize;
  
  pthread_mutex_lock(&(impl->mutex));
  
  entry->next = impl->first;
  if(impl->last == NULL) { 
    impl->last = entry; 
    assert(!impl->first);
  } else {
    assert(impl->first);
    entry->next->prev = entry;
  }
  entry->prev = NULL;
  impl->first = entry;

  pthread_cond_broadcast(&(impl->readOK));
  pthread_mutex_unlock(&(impl->mutex));

  
  return 0;
}


lladdFifoPool_t * lladdFifoPool_pointerPoolInit (int consumerCount, int pointerCount, 
						 lladdFifoPool_getFifo_t * getFifo, lladdFifo_t * dirtyPoolFifo) { 
  
  lladdFifoPool_t * pool = malloc(sizeof(lladdFifoPool_t));

  pool->pool          = malloc(sizeof(lladdFifo_t*) * consumerCount);
  int i;

  lladdFifoPointerPool_t * poolImpl = malloc(sizeof(lladdFifoPointerPool_t));
  poolImpl->maxPtrs = pointerCount;
  poolImpl->outPtrs = 0;
  pthread_mutex_init(&(poolImpl->mutex), NULL);
  pthread_cond_init(&(poolImpl->writeOK), NULL);

  for(i = 0; i < consumerCount; i++) {
    pool->pool[i] = malloc(sizeof(lladdFifo_t));
    pool->pool[i]->iterator = malloc(sizeof(lladdIterator_t));
    pool->pool[i]->consumer = malloc(sizeof(lladdConsumer_t));
    pool->pool[i]->iterator->type = POINTER_ITERATOR;
    pool->pool[i]->consumer->type = POINTER_CONSUMER;
    pointerFifoImpl * impl = 
      (pointerFifoImpl*) (pool->pool[i]->consumer->impl = pool->pool[i]->iterator->impl = malloc(sizeof(pointerFifoImpl)));
    impl->first = NULL;
    impl->last  = NULL;
    impl->eof = 0;
    pthread_mutex_init(&(impl->mutex), NULL);
    pthread_cond_init(&(impl->readOK), NULL);
    //    pthread_cond_init(&(impl->writeOK), NULL);

    impl->pool = poolImpl;
  }
  pool->getFifo       = getFifo ? getFifo : lladdFifoPool_getFifoCRC32;
  pool->fifoCount     = consumerCount;
  pool->dirtyPoolFifo = dirtyPoolFifo;

  return pool;
}
