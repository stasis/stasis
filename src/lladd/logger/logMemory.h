#include <lladd/transactional.h>
#include <lladd/ringbuffer.h>
#include <lladd/consumer.h>
#include <lladd/iterator.h>
#include <lladd/fifo.h>

#ifndef __LOGMEMORY_H
#define __LOGMEMORY_H

/*typedef struct ringBufferLog_s ringBufferLog_t;

ringBufferLog_t * openLogRingBuffer(size_t size, lsn_t initialOffset);
void closeLogRingBuffer(ringBufferLog_t * log);
int ringBufferAppend(ringBufferLog_t * log, byte * dat, size_t size);
int ringBufferTruncateRead(byte * buf, ringBufferLog_t * log,size_t size); */

// int writeLogEntry(LogEntry * e); (as implemented by ring buffer, but need to remember size of entry too

// void syncLog() (block writers until log is empty / call sync log on consumers?)

// lsn_t flushedLSN  return lsn as of last syncLog?

// int truncateLog(lsn_t) (no-op?)

// lsn_t firstLogEntry()  Unimplemented?  Or return first thing in current ring buffer?

// void closeLogWriter() (syncLog + closeRingBuffer)

// deleteLogWriter no-op

lladdFifo_t * logMemoryFifo(size_t size, lsn_t initialOffset);

// LogEntry* readLSNEntry(lsn_t) if LSN is the next available log entry, return it, else error.

void logMemory_Tconsumer_close(int xid, lladdConsumer_t *it);
compensated_function int Tconsumer_push(int xid, lladdConsumer_t *it, byte *key, size_t keySize, byte *val, size_t Valsize);

void logMemory_Iterator_close(int xid, void * impl);
compensated_function int logMemory_Iterator_next (int xid, void * impl);
int logMemory_Iterator_key (int xid, void * impl, byte ** key);
int logMemory_Iterator_value (int xid, void * impl, byte ** value);
int logMemory_Iterator_releaseTuple(int xid, void *it);

#endif
