#include <stasis/logger/inMemoryLog.h>
#include <stasis/latches.h>
#include <string.h>
#include <assert.h>
static rwl * flushedLSN_lock;
static lsn_t nextAvailableLSN;
static lsn_t globalOffset;
static rwl * globalOffset_lock;
static LogEntry ** buffer;
static lsn_t bufferLen;

static int writeLogEntry_InMemoryLog(stasis_log_t * log, LogEntry *e) {
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
  return 0;


  e->LSN = nextAvailableLSN;

  LogEntry * cpy = malloc(sizeofLogEntry(e));
  memcpy(cpy, e, sizeofLogEntry(e));

  //  printf ("lsn: %ld\n", e->LSN);
  buffer[bufferOffset] = cpy;

  //  printf("lsn: %ld type: %d\n", e->LSN, e->type);
  nextAvailableLSN++;

  unlock(globalOffset_lock);
  unlock(flushedLSN_lock);
}

static lsn_t flushedLSN_InMemoryLog(stasis_log_t* log,
                                    stasis_log_force_mode_t mode) {
  return nextAvailableLSN;
}

static void syncLog_InMemoryLog(stasis_log_t* log, stasis_log_force_mode_t m){
  // no-op
}

static lsn_t nextEntry_InMemoryLog(stasis_log_t * log, const LogEntry * e) {
  return e->LSN + 1;
}

static int truncateLog_InMemoryLog(stasis_log_t * log, lsn_t lsn) {
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

static lsn_t firstLogEntry_InMemoryLog() {
  return globalOffset;
}

static int close_InMemoryLog(stasis_log_t * log) {
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
  free (log);
  return 0;
}

static const LogEntry * readLSNEntry_InMemoryLog(stasis_log_t* log,
                                                 lsn_t lsn) {
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
static lsn_t sizeofInternalLogEntry_InMemoryLog(stasis_log_t* log,
                                                const LogEntry * e) {
  abort();
}
static int isDurable_InMemoryLog(stasis_log_t*log) { return 0; }

stasis_log_t* open_InMemoryLog() {
  flushedLSN_lock   = initlock();
  globalOffset_lock = initlock();
  globalOffset = 0;
  nextAvailableLSN = 0;
  buffer = malloc(4096 * 1024 * sizeof (LogEntry *));
  bufferLen =4096 * 1024;
  static stasis_log_t proto = {
    sizeofInternalLogEntry_InMemoryLog, // sizeof_internal_entry
    writeLogEntry_InMemoryLog,// write_entry
    readLSNEntry_InMemoryLog, // read_entry
    nextEntry_InMemoryLog,// next_entry
    flushedLSN_InMemoryLog, // first_unstable_lsn
    syncLog_InMemoryLog, // force_tail
    truncateLog_InMemoryLog, // truncate
    firstLogEntry_InMemoryLog,// truncation_point
    close_InMemoryLog, // deinit
    isDurable_InMemoryLog// is_durable
  };
  stasis_log_t* log = malloc(sizeof(*log));
  memcpy(log,&proto, sizeof(proto));
  return log;
}
