/**
  NOTE: Person who's using the consumer interface calls close first, (for now).
*/


#include <stdlib.h>
#include <assert.h>
#include "logMemory.h"

#include <lladd/compensations.h>

typedef struct {
  pthread_mutex_t mutex;
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




/* iterator interface implementation */

/* NOTE: assumes currently that the consumer interface is done so we can
         safely deallocate resources
*/
void logMemory_Iterator_close(int xid, void * impl) {
  closeLogRingBuffer( ((logMemory_fifo_t *) impl)->ringBuffer );
  free(impl);
}

compensated_function int logMemory_Iterator_next (int xid, void * impl) {
  logMemory_fifo_t *fifo = (logMemory_fifo_t *) impl;
  pthread_mutex_lock(&(fifo->mutex));
  size_t size;
  int lsn; 
  int ret;

  if(fifo->eof != -1 && fifo->eof == ringBufferReadPosition(fifo->ringBuffer)) {
    pthread_mutex_unlock(&(fifo->mutex));
    return 0;
  }

  // TODO Check to see if we're done reading...

  while(-2 == (ret = ringBufferTruncateRead((byte *)&size, fifo->ringBuffer,  sizeof(size_t)))) {
    pthread_cond_wait(&(fifo->readReady), &(fifo->mutex));
    if(fifo->eof != -1 && fifo->eof == ringBufferReadPosition(fifo->ringBuffer)) {
      pthread_mutex_unlock(&(fifo->mutex));
      return 0;
    }
  } 
  if (ret == -1) { 
    compensation_set_error(LLADD_INTERNAL_ERROR);
    pthread_mutex_unlock(&(fifo->mutex));
    return LLADD_INTERNAL_ERROR;

  }
  assert(!ret);
      
  byte * tmp;

  tmp = realloc(fifo->cached_value, size); 
  if(tmp == NULL) {
    compensation_set_error(LLADD_INTERNAL_ERROR);
    pthread_mutex_unlock(&(fifo->mutex));
    return LLADD_INTERNAL_ERROR;
  }

  fifo->cached_value = tmp;
  fifo->cached_value_size = size;
    
  while(-2 == (lsn = ringBufferTruncateRead( fifo->cached_value, fifo->ringBuffer, size))) {
    pthread_cond_wait(&(fifo->readReady), &(fifo->mutex));
  }
  if (ret == -1) { 
    compensation_set_error(LLADD_INTERNAL_ERROR);
    pthread_mutex_unlock(&(fifo->mutex));
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

int logMemory_Iterator_releaseTuple(int xid, void *it) {
 /* NO-OP */
 return 0;
}




/* consumer implementation */


void logMemory_Tconsumer_close(int xid, lladdConsumer_t *it){
  /* This needs to tell the iterator where the end of the ring buffer is. */
  logMemory_fifo_t * fifo = (logMemory_fifo_t *) (it->impl);
  pthread_mutex_lock(&(fifo->mutex));
  fifo->eof = ringBufferAppendPosition(fifo->ringBuffer);
  assert(fifo->eof != -1);
  pthread_cond_broadcast(&(fifo->readReady)); // There may have been threads waiting on the next tuple before close was called. 
  pthread_mutex_unlock(&(fifo->mutex));
}

compensated_function void Tconsumer_close(int xid, lladdConsumer_t * cons) {
  logMemory_Tconsumer_close(xid, cons);
}

compensated_function int Tconsumer_push(int xid, lladdConsumer_t *it, byte *key, size_t keySize, byte *val, size_t Valsize) {
  int ret;
  logMemory_fifo_t * fifo = (logMemory_fifo_t *) (it->impl);
  pthread_mutex_lock(&(fifo->mutex));

  while(-2 == (ret = ringBufferAppend(fifo->ringBuffer, 
				      (byte *)&Valsize, 
				      sizeof(size_t) ))) {
    pthread_cond_wait(&(fifo->writeReady), &(fifo->mutex));
  }
  if(ret == -1) {   // asked to append something longer than the buffer!
    compensation_set_error(LLADD_INTERNAL_ERROR);
    return LLADD_INTERNAL_ERROR;
  }
  while(-2 == ringBufferAppend( ((logMemory_fifo_t *) it->impl)->ringBuffer, val, Valsize)) {
    pthread_cond_wait(&(fifo->writeReady), &(fifo->mutex));
  }

  if(ret == -1) {   // asked to append something longer than the buffer!
    compensation_set_error(LLADD_INTERNAL_ERROR);
    return LLADD_INTERNAL_ERROR;
  }
  
  pthread_cond_broadcast(&(fifo->readReady));
  pthread_mutex_unlock(&(fifo->mutex));

  return ret;
  // always succeeds.
}
