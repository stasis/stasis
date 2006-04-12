#include <lladd/logger/logger2.h>

#ifndef __INMEMORYLOG 
#define __INMEMORYLOG 1

int open_InMemoryLog();
int writeLogEntry_InMemoryLog(LogEntry * e);
lsn_t flushedLSN_InMemoryLog();
int truncateLog_InMemoryLog(lsn_t lsn);
lsn_t firstLogEntry_InMemoryLog();
void close_InMemoryLog();
LogEntry * readLSNEntry_InMemoryLog();
#endif
