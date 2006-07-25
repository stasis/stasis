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
    readlock(globalOffset_lock,0);
    bufferOffset = nextAvailableLSN - globalOffset;
    if(bufferOffset > bufferLen) { 
      unlock(globalOffset_lock);
      writelock(globalOffset_lock,0);
      abort(); // really, need to extend log.
    } else {
      done = 1;
    }
  } while (!done);


  e->LSN = nextAvailableLSN;

  //  printf ("lsn: %ld\n", e->LSN);
  buffer[bufferOffset] = e;

  //  printf("lsn: %ld type: %d\n", e->LSN, e->type);
  nextAvailableLSN++;

  unlock(globalOffset_lock);
  unlock(flushedLSN_lock);
  return 0;
}

lsn_t flushedLSN_InMemoryLog() { 
  return nextAvailableLSN;
}

int truncateLog_InMemoryLog(lsn_t lsn) {
  abort();
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

LogEntry * readLSNEntry_InMemoryLog(lsn_t LSN) { 
  // printf("lsn: %ld\n", LSN);
  if(LSN >= nextAvailableLSN) { return 0; } 
  assert(LSN-globalOffset >= 0 && LSN-globalOffset< bufferLen);
  readlock(globalOffset_lock, 0);
  LogEntry * ptr = buffer[LSN - globalOffset];
  unlock(globalOffset_lock);
  assert(ptr);
  assert(ptr->LSN == LSN);
  //printf("lsn: %ld prevlsn: %ld\n", ptr->LSN, ptr->prevLSN);
  return ptr;
}
