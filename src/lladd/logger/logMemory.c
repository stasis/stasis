/**
  NOTE: Person who's using the consumer interface calls close first, (for now).
*/


#include <stdlib.h>
#include <lladd/ringbuffer.h>
#include <lladd/consumer.h>
#include <lladd/iterator.h>

 
typedef struct {
  //mutex?
  ringBufferLog_t * ringBuffer;
  lsn_t cached_lsn;
  byte * cached_value;
  size_t cached_value_size;
  
} logMemory_fifo_t;


typedef struct {
  lladdIterator_t *iterator;
  lladdConsumer_t *consumer;
} lladdFifo_t;


void logMemory_init() {
/* NO-OP */
}

lladdFifo_t * logMemoryFifo(size_t size, lsn_t initialOffset) {
                                    
 lladdFifo_t * fifo = (lladdFifo_t *) malloc(sizeof(lladdFifo_t));

 lladdIterator_t * iterator = (lladdIterator_t *) malloc(sizeof(lladdIterator_t));
 iterator->type = LOG_MEMORY_ITERATOR;
 iterator->impl = malloc(sizeof(logMemory_fifo_t)); 
 ((logMemory_fifo_t *)iterator->impl)->ringBuffer = openLogRingBuffer(size, initialOffset);

 lladdConsumer_t * consumer = (lladdConsumer_t *) malloc(sizeof(lladdConsumer_t));
 consumer->type = LOG_MEMORY_CONSUMER;
 consumer->impl = iterator->impl; /* FIXME: same logMemory_iterator_t as iterator?*/
 
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

int logMemory_Iterator_next (int xid, void * impl) {

  logMemory_fifo_t *fifo = (logMemory_fifo_t *) impl;
  size_t size;
  int lsn; 
  int ret;
  ret = ringBufferTruncateRead((byte *)&size, fifo->ringBuffer,  sizeof(size_t) );

  if (ret == 0) { /* NOTE: I assume that ringBufferTruncateRead returns 0 when a read is successfull. */
	 
	  /* TODO: the following might return null, in which case we should ... ? */
	  fifo->cached_value = realloc(fifo->cached_value, size); 
	  
	  lsn = ringBufferTruncateRead( fifo->cached_value, fifo->ringBuffer, size);
	  fifo->cached_lsn = (lsn_t)lsn;
          return 1;  /* FIXME: is this the right return value if there is a next value? */
  } else {
    return 0;        /* FIXME: is this the right return value when there is no next value? */
  }

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
  /* Currently this doesn't have to do anything */
}

int Tconsumer_push(int xid, lladdConsumer_t *it, byte *key, size_t keySize, byte *val, size_t Valsize) {
  ringBufferAppend( ((logMemory_fifo_t *) it->impl)->ringBuffer, (byte *)&Valsize, sizeof(size_t) );
  ringBufferAppend( ((logMemory_fifo_t *) it->impl)->ringBuffer, val, Valsize);
}
