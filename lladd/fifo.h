
#include <lladd/iterator.h>
#include <lladd/consumer.h>
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


#endif // __FIFO_H
