#include <lladd/transactional.h>

typedef struct ringBufferLog_s ringBufferLog_t;

ringBufferLog_t * openLogRingBuffer(size_t size, lsn_t initialOffset);
void closeLogRingBuffer(ringBufferLog_t * log);
int ringBufferAppend(ringBufferLog_t * log, byte * dat, size_t size);
int ringBufferTruncateRead(byte * buf, ringBufferLog_t * log,size_t size);

// int writeLogEntry(LogEntry * e); (as implemented by ring buffer, but need to remember size of entry too

// void syncLog() (block writers until log is empty / call sync log on consumers?)

// lsn_t flushedLSN  return lsn as of last syncLog?

// int truncateLog(lsn_t) (no-op?)

// lsn_t firstLogEntry()  Unimplemented?  Or return first thing in current ring buffer?

// void closeLogWriter() (syncLog + closeRingBuffer)

// deleteLogWriter no-op

// LogEntry* readLSNEntry(lsn_t) if LSN is the next available log entry, return it, else error.
