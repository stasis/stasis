/**
  NOTE: Person who's using the consumer interface calls close first, (for now).
*/

#include <stasis/logger/logMemory.h>
#include <assert.h>

typedef struct {
  pthread_mutex_t mutex;
  pthread_mutex_t readerMutex;
  pthread_cond_t readReady;
  pthread_cond_t writeReady;
  ringBufferLog_t * ringBuffer;
  lsn_t cached_lsn;
  byte * cached_value;
  size_t cached_value_size;
  lsn_t eof;
} logMemory_fifo_t;



void logMemory_init() {
/* NO-OP */
}

lladdFifo_t * logMemoryFifo(size_t size, lsn_t initialOffset) {

 lladdFifo_t * fifo = (lladdFifo_t *) malloc(sizeof(lladdFifo_t));

 lladdIterator_t * iterator = (lladdIterator_t *) malloc(sizeof(lladdIterator_t));
 iterator->type = LOG_MEMORY_ITERATOR;
 iterator->impl = malloc(sizeof(logMemory_fifo_t));
 ((logMemory_fifo_t *)iterator->impl)->ringBuffer = openLogRingBuffer(size, initialOffset);
 pthread_mutex_init(&(((logMemory_fifo_t *)iterator->impl)->mutex), NULL);
 pthread_mutex_init(&(((logMemory_fifo_t *)iterator->impl)->readerMutex), NULL);
 pthread_cond_init (&(((logMemory_fifo_t *)iterator->impl)->readReady), NULL);
 pthread_cond_init (&(((logMemory_fifo_t *)iterator->impl)->writeReady), NULL);
 ((logMemory_fifo_t *)iterator->impl)->cached_value = NULL;
 ((logMemory_fifo_t *)iterator->impl)->eof = -1;

 lladdConsumer_t * consumer = (lladdConsumer_t *) malloc(sizeof(lladdConsumer_t));
 consumer->type = LOG_MEMORY_CONSUMER;
 consumer->impl = iterator->impl;

 fifo->iterator = iterator;
 fifo->consumer = consumer;


 return fifo;
}




/*------------- iterator interface implementation --------------------*/

/** This function should not be called until next() or one of its
    variants indicates that the entire fifo has been consumed, since
    this function assumes currently that the consumer interface is
    done so that it can deallocate resources.
*/
void logMemory_Iterator_close(int xid, void * impl) {
  closeLogRingBuffer( ((logMemory_fifo_t *) impl)->ringBuffer );
  free(impl);
}

int logMemory_Iterator_next (int xid, void * impl) {
  logMemory_fifo_t *fifo = (logMemory_fifo_t *) impl;
  pthread_mutex_lock(&(fifo->readerMutex));
  pthread_mutex_lock(&(fifo->mutex));
  size_t size;
  lsn_t lsn;
  int ret;

  if(fifo->eof != -1 && fifo->eof == ringBufferReadPosition(fifo->ringBuffer)) {
    pthread_mutex_unlock(&(fifo->mutex));
    pthread_mutex_unlock(&(fifo->readerMutex));
    return 0;
  }

  // TODO Check to see if we're done reading...

  while(-2 == (ret = ringBufferTruncateRead((byte *)&size, fifo->ringBuffer,  sizeof(size_t)))) {
    pthread_cond_wait(&(fifo->readReady), &(fifo->mutex));
    if(fifo->eof != -1 && fifo->eof == ringBufferReadPosition(fifo->ringBuffer)) {
      pthread_mutex_unlock(&(fifo->mutex));
      pthread_mutex_unlock(&(fifo->readerMutex));
      return 0;
    }
  }
  if (ret == -1) {
    pthread_mutex_unlock(&(fifo->mutex));
    pthread_mutex_unlock(&(fifo->readerMutex));
    return LLADD_INTERNAL_ERROR;

  }
  assert(!ret);

  byte * tmp;

  tmp = realloc(fifo->cached_value, size);
  if(tmp == NULL) {
    pthread_mutex_unlock(&(fifo->mutex));
    pthread_mutex_unlock(&(fifo->readerMutex));
    return LLADD_INTERNAL_ERROR;
  }

  fifo->cached_value = tmp;
  fifo->cached_value_size = size;

  while(-2 == (lsn = ringBufferTruncateRead( fifo->cached_value, fifo->ringBuffer, size))) {
    pthread_cond_wait(&(fifo->readReady), &(fifo->mutex));
  }
  if (ret == -1) {
    pthread_mutex_unlock(&(fifo->mutex));
    pthread_mutex_unlock(&(fifo->readerMutex));
    return LLADD_INTERNAL_ERROR;

  }

  assert(!ret);

  fifo->cached_lsn = (lsn_t)lsn;

  pthread_cond_broadcast(&(fifo->writeReady));
  pthread_mutex_unlock(&(fifo->mutex));
  return 1;

}

/** @todo logMemory_Iterator_tryNext is a cut and pasted version of
    .._next.  The functionality should be broken into modules and
    reused... */

int logMemory_Iterator_tryNext (int xid, void * impl) {
  logMemory_fifo_t *fifo = (logMemory_fifo_t *) impl;
  if(EBUSY == pthread_mutex_trylock(&(fifo->readerMutex))) {
    return 0;
  }
  pthread_mutex_lock(&(fifo->mutex));
  size_t size;
  lsn_t lsn;
  int ret;

  if(fifo->eof != -1 && fifo->eof == ringBufferReadPosition(fifo->ringBuffer)) {
    pthread_mutex_unlock(&(fifo->mutex));
    pthread_mutex_unlock(&(fifo->readerMutex));
    return 0;
  }

  // TODO Check to see if we're done reading...

  //From here on, we need to continue as normal since we consumed data from the ringbuffer...
  if(-2 == (ret = ringBufferTruncateRead((byte *)&size, fifo->ringBuffer,  sizeof(size_t)))) {
    pthread_mutex_unlock(&(fifo->mutex));
    pthread_mutex_unlock(&(fifo->readerMutex));
    return 0;
  }

  if (ret == -1) {
    pthread_mutex_unlock(&(fifo->mutex));
    pthread_mutex_unlock(&(fifo->readerMutex));
    return LLADD_INTERNAL_ERROR;

  }
  assert(!ret);

  byte * tmp;

  tmp = realloc(fifo->cached_value, size);
  if(tmp == NULL) {
    pthread_mutex_unlock(&(fifo->mutex));
    pthread_mutex_unlock(&(fifo->readerMutex));
    return LLADD_INTERNAL_ERROR;
  }

  fifo->cached_value = tmp;
  fifo->cached_value_size = size;

  while(-2 == (lsn = ringBufferTruncateRead( fifo->cached_value, fifo->ringBuffer, size))) {
    pthread_cond_wait(&(fifo->readReady), &(fifo->mutex));
  }
  if (ret == -1) {
    pthread_mutex_unlock(&(fifo->mutex));
    pthread_mutex_unlock(&(fifo->readerMutex));
    return LLADD_INTERNAL_ERROR;

  }

  assert(!ret);

  fifo->cached_lsn = (lsn_t)lsn;

  pthread_cond_broadcast(&(fifo->writeReady));
  pthread_mutex_unlock(&(fifo->mutex));
  return 1;

}

void logMemory_Iterator_releaseLock (int xid, void * impl) {
  logMemory_fifo_t * fifo = (logMemory_fifo_t *) impl;

  pthread_mutex_unlock(&(fifo->mutex));
  pthread_mutex_unlock(&(fifo->readerMutex));

}

/** Blocks until it can advance the iterator a single step or until
    the iterator is empty.  If this function returns 0 the caller can
    safely assume the iterator is currently empty (and any _push()
    requests are blocking).  Otherwise, this function works
    analagously to the normal _next() call

    @return 1 (and require tupleDone() to be called) if the iterator was advanced.
    @return 0 (and require releaseIteratorLock() to be called) if the iterator currently contains
               no more values, and is not waiting for another thread to call tupleDone())

    @todo logMemory_Iterator_nextOrEmpty is a cut and pasted version of
    .._next.  The functionality should be broken into modules and
    reused... */

int logMemory_Iterator_nextOrEmpty (int xid, void * impl) {
  logMemory_fifo_t *fifo = (logMemory_fifo_t *) impl;
  pthread_mutex_lock(&(fifo->readerMutex));
  pthread_mutex_lock(&(fifo->mutex));
  size_t size;
  lsn_t lsn;
  int ret;

  if(fifo->eof != -1 && fifo->eof == ringBufferReadPosition(fifo->ringBuffer)) {
    /*    pthread_mutex_unlock(&(fifo->mutex));
	  pthread_mutex_unlock(&(fifo->readerMutex)); */
    return 0;
  }

  // TODO Check to see if we're done reading...

  //From here on, we need to continue as normal since we consumed data from the ringbuffer...
  if(-2 == (ret = ringBufferTruncateRead((byte *)&size, fifo->ringBuffer,  sizeof(size_t)))) {
    /*    pthread_mutex_unlock(&(fifo->mutex));
	  pthread_mutex_unlock(&(fifo->readerMutex)); */
    // At this point, just assume the ring buffer is empty, since
    // anything in the process of doing an append is blocked.  (under
    // normal circumstances, there really won't be anything in the
    // ringbuffer anyway..
    return 0;
  }

  if (ret == -1) {
    pthread_mutex_unlock(&(fifo->mutex));
    pthread_mutex_unlock(&(fifo->readerMutex));
    return LLADD_INTERNAL_ERROR;

  }
  assert(!ret);

  byte * tmp;

  tmp = realloc(fifo->cached_value, size);
  if(tmp == NULL) {
    pthread_mutex_unlock(&(fifo->mutex));
    pthread_mutex_unlock(&(fifo->readerMutex));
    return LLADD_INTERNAL_ERROR;
  }

  fifo->cached_value = tmp;
  fifo->cached_value_size = size;

  while(-2 == (lsn = ringBufferTruncateRead( fifo->cached_value, fifo->ringBuffer, size))) {
    pthread_cond_wait(&(fifo->readReady), &(fifo->mutex));
  }
  if (ret == -1) {
    pthread_mutex_unlock(&(fifo->mutex));
    pthread_mutex_unlock(&(fifo->readerMutex));
    return LLADD_INTERNAL_ERROR;

  }

  assert(!ret);

  fifo->cached_lsn = (lsn_t)lsn;

  pthread_cond_broadcast(&(fifo->writeReady));
  pthread_mutex_unlock(&(fifo->mutex));
  return 1;

}

/* return the lsn */
int logMemory_Iterator_key (int xid, void * impl, byte ** key) {
  logMemory_fifo_t * fifo = (logMemory_fifo_t *) impl;
  *key = (byte *)&(fifo->cached_lsn);
  return sizeof(lsn_t);
}

int logMemory_Iterator_value (int xid, void * impl, byte ** value) {
  logMemory_fifo_t * fifo = (logMemory_fifo_t *) impl;
  *value = fifo->cached_value;
  return fifo->cached_value_size;
}

void logMemory_Iterator_releaseTuple(int xid, void *it) {
  logMemory_fifo_t * fifo = (logMemory_fifo_t *) it;
  pthread_mutex_unlock(&(fifo->readerMutex));
}

/* ------------------- consumer implementation ------------------------------*/

void logMemory_consumer_close(int xid, void *it){
  /* This needs to tell the iterator where the end of the ring buffer is. */
  logMemory_fifo_t * fifo = (logMemory_fifo_t *) (it);
  pthread_mutex_lock(&(fifo->mutex));
  fifo->eof = ringBufferAppendPosition(fifo->ringBuffer);
  assert(fifo->eof != -1);
  pthread_cond_broadcast(&(fifo->readReady)); // There may have been threads waiting on the next tuple before close was called.
  pthread_mutex_unlock(&(fifo->mutex));
}

/*compensated_function void Tconsumer_close(int xid, lladdConsumer_t * cons) {
  logMemory_consumer_close(xid, cons);
  }*/

int logMemory_consumer_push(int xid, /*lladdConsumer_t * cons*/ void * it, byte * key, size_t keySize, byte * val, size_t valSize) {
  int ret;
  logMemory_fifo_t * fifo = (logMemory_fifo_t *) (it);
  pthread_mutex_lock(&(fifo->mutex));

  while(-2 == (ret = ringBufferAppend(fifo->ringBuffer,
				      (byte *)&valSize,
				      sizeof(size_t) ))) {
    pthread_cond_wait(&(fifo->writeReady), &(fifo->mutex));
  }
  if(ret == -1) {   // asked to append something longer than the buffer!
    return LLADD_INTERNAL_ERROR;
  }
  while(-2 == ringBufferAppend( (fifo)->ringBuffer, val, valSize)) {
    pthread_cond_wait(&(fifo->writeReady), &(fifo->mutex));
  }

  if(ret == -1) {   // asked to append something longer than the buffer!
    return LLADD_INTERNAL_ERROR;
  }

  pthread_cond_broadcast(&(fifo->readReady));
  pthread_mutex_unlock(&(fifo->mutex));

  return ret;

}
/*  if(it->type == LOG_MEMORY_CONSUMER) {
    return logMemory_consumer_push(xid, it, key, keySize, val, valSize);
  }
  if(it->type == POINTER_CONSUMER) {
    return pointer_consumer_push(xid, it, key, keySize, val, valSize);
  }

  // always succeeds.
  }*/
