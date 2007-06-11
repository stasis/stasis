
#include <stasis/iterator.h>
#include <stasis/consumer.h>
#ifndef __FIFO_H
#define __FIFO_H

typedef struct {
  lladdIterator_t *iterator;
  lladdConsumer_t *consumer;
} lladdFifo_t;

typedef struct lladdFifoPool_t { 
  lladdFifo_t ** pool;
  lladdFifo_t * (*getFifo)(struct lladdFifoPool_t * pool, 
			      byte * multiplexKey, 
			      size_t multiplexKeySize); 
  int fifoCount;
  lladdFifo_t * dirtyPoolFifo;
} lladdFifoPool_t;


typedef lladdFifo_t * (lladdFifoPool_getFifo_t)(lladdFifoPool_t * pool, byte * multiplexKey, size_t multiplexKeySize);

lladdFifoPool_getFifo_t lladdFifoPool_getFifoCRC32;

lladdFifoPool_t * lladdFifoPool_ringBufferInit (int consumerCount, int bufferSize, 
						lladdFifoPool_getFifo_t * getFifo, lladdFifo_t * dirtyPoolFifo);

void lladdFifoPool_markDirty(int xid, lladdFifoPool_t * pool, lladdFifo_t * fifo) ;


int lladdFifoPool_consumer_push(int xid, void * it, byte * key, size_t keySize, byte * val, size_t valSize);
void lladdFifoPool_consumer_close(int xid, void * it);
lladdFifoPool_t * lladdFifoPool_pointerPoolInit (int consumerCount, int pointerCount, 
						 lladdFifoPool_getFifo_t * getFifo, lladdFifo_t * dirtyPoolFifo);

void lladdFifoPool_iterator_close(int xid, void * it);
int lladdFifoPool_iterator_next(int xid, void * it);
int lladdFifoPool_iterator_tryNext(int xid, void * it);
int lladdFifoPool_iterator_key (int xid, void * it, byte ** key);
int lladdFifoPool_iterator_value (int xid, void * it, byte ** val);
void lladdFifoPool_iterator_tupleDone(int xid, void * it);
void lladdFifoPool_iterator_releaseLock(int xid, void * it);
#endif // __FIFO_H
