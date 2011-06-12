#include <stasis/transactional.h>
#include <stasis/ringbuffer.h>
#include <stasis/consumer.h>
#include <stasis/iterator.h>
#include <stasis/fifo.h>

#ifndef __LOGMEMORY_H
#define __LOGMEMORY_H

/**
   @file

   A poorly named in-memory fifo based on ringbuffer.h

   @todo Move this all to some reasonably named interface. :)

   $Id$
*/

lladdFifo_t * logMemoryFifo(size_t size, lsn_t initialOffset);

void logMemory_consumer_close(int xid, void *it);
int logMemory_consumer_push  (int xid, void * it, byte * key, size_t keySize, byte * val, size_t valSize);
void logMemory_Iterator_close(int xid, void * impl);
int logMemory_Iterator_next(int xid, void * impl);
int logMemory_Iterator_tryNext(int xid, void * impl);
int logMemory_Iterator_key (int xid, void * impl, byte ** key);
int logMemory_Iterator_value (int xid, void * impl, byte ** value);
void logMemory_Iterator_releaseTuple(int xid, void *it);
void logMemory_Iterator_releaseLock (int xid, void * impl);
#endif
