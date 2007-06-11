#include "inMemoryLog.h"
#include "../latches.h"
#include <string.h>
#include <assert.h>
static rwl * flushedLSN_lock;
static lsn_t nextAvailableLSN;
static lsn_t globalOffset;
static rwl * globalOffset_lock;
static LogEntry ** buffer;
static lsn_t bufferLen;
int open_InMemoryLog() { 
  flushedLSN_lock   = initlock();
  globalOffset_lock = initlock();
  globalOffset = 0;
  nextAvailableLSN = 0;
  buffer = malloc(4096 * 1024 * sizeof (LogEntry *));
  bufferLen =4096 * 1024;
  return 0;
}

int writeLogEntry_InMemoryLog(LogEntry *e) {
  writelock(flushedLSN_lock, 0);
  lsn_t bufferOffset;

  int done = 0;
  do{ 
    writelock(globalOffset_lock,0);
    bufferOffset = nextAvailableLSN - globalOffset;
    if(bufferOffset > bufferLen) { 
      bufferLen *= 2;
      buffer = realloc(buffer, bufferLen);
    } else {
      done = 1;
    }
  } while (!done);


  e->LSN = nextAvailableLSN;

  LogEntry * cpy = malloc(sizeofLogEntry(e));
  memcpy(cpy, e, sizeofLogEntry(e));

  //  printf ("lsn: %ld\n", e->LSN);
  buffer[bufferOffset] = cpy;

  //  printf("lsn: %ld type: %d\n", e->LSN, e->type);
  nextAvailableLSN++;

  unlock(globalOffset_lock);
  unlock(flushedLSN_lock);
  return 0;
}

lsn_t flushedLSN_InMemoryLog() { 
  return nextAvailableLSN;
}

void syncLog_InMemoryLog() {
  // no-op
}

lsn_t nextEntry_InMemoryLog(const LogEntry * e) { 
  return e->LSN + 1;
}

int truncateLog_InMemoryLog(lsn_t lsn) {
  writelock(flushedLSN_lock,1);
  writelock(globalOffset_lock,1);
 
  assert(lsn <= nextAvailableLSN);


  if(lsn > globalOffset) { 
    for(int i = globalOffset; i < lsn; i++) { 
      free(buffer[i - globalOffset]);
    }
    assert((lsn-globalOffset) + (nextAvailableLSN -lsn) < bufferLen);
    memmove(&(buffer[0]), &(buffer[lsn - globalOffset]), sizeof(LogEntry*) * (nextAvailableLSN - lsn));
    globalOffset = lsn;
  }

  writeunlock(globalOffset_lock);
  writeunlock(flushedLSN_lock);

  return 0;
}

lsn_t firstLogEntry_InMemoryLog() {
  return globalOffset;
}

void close_InMemoryLog() {
  if(buffer) { 
    lsn_t firstEmptyOffset = nextAvailableLSN-globalOffset;
    for(lsn_t i = 0; i < firstEmptyOffset; i++) { 
      assert(buffer[i]->LSN == i+globalOffset);
      free(buffer[i]);
    }
    free(buffer);
    nextAvailableLSN = 0;
    globalOffset = 0;
    bufferLen = 0;
    buffer = 0;

  }

}

LogEntry * readLSNEntry_InMemoryLog(lsn_t lsn) { 
  // printf("lsn: %ld\n", lsn);
  if(lsn >= nextAvailableLSN) { return 0; } 
  assert(lsn-globalOffset >= 0 && lsn-globalOffset< bufferLen);
  readlock(globalOffset_lock, 0);
  LogEntry * ptr = buffer[lsn - globalOffset];
  unlock(globalOffset_lock);
  assert(ptr);
  assert(ptr->LSN == lsn);
  
  LogEntry * ret = malloc(sizeofLogEntry(ptr));

  memcpy(ret, ptr, sizeofLogEntry(ptr));
  
  //printf("lsn: %ld prevlsn: %ld\n", ptr->LSN, ptr->prevLSN);
  return ret;
}
long sizeofInternalLogEntry_InMemoryLog(const LogEntry * e) {
  abort();
}
