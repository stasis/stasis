#include <lladd/fifo.h>
#include <lladd/crc32.h>
#include <stdlib.h>
#include "logger/logMemory.h"

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
