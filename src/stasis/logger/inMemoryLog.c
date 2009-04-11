#include <stasis/logger/inMemoryLog.h>
#include <stasis/latches.h>
#include <string.h>
#include <assert.h>

typedef struct {
	rwl * flushedLSN_lock;
	lsn_t nextAvailableLSN;
	lsn_t globalOffset;
	rwl * globalOffset_lock;
	LogEntry ** buffer;
	lsn_t bufferLen;
} stasis_log_impl_in_memory;

static lsn_t stasis_log_impl_in_memory_next_available_lsn(stasis_log_t * log) {
  stasis_log_impl_in_memory * impl = log->impl;
  writelock(impl->flushedLSN_lock,0);
  writelock(impl->globalOffset_lock,0);
  lsn_t ret = impl->nextAvailableLSN;
  unlock(impl->globalOffset_lock);
  unlock(impl->flushedLSN_lock);
  return ret;
}

static int stasis_log_impl_in_memory_write_entry(stasis_log_t * log, LogEntry *e) {
  stasis_log_impl_in_memory * impl = log->impl;
  writelock(impl->flushedLSN_lock, 0);
  lsn_t bufferOffset;

  int done = 0;
  do{
    writelock(impl->globalOffset_lock,0);
    bufferOffset = impl->nextAvailableLSN - impl->globalOffset;
    if(bufferOffset > impl->bufferLen) {
      impl->bufferLen *= 2;
      impl->buffer = realloc(impl->buffer, impl->bufferLen);
    } else {
      done = 1;
    }
  } while (!done);

  e->LSN = impl->nextAvailableLSN;

  LogEntry * cpy = malloc(sizeofLogEntry(e));
  memcpy(cpy, e, sizeofLogEntry(e));

  DEBUG("lsn: %ld\n", e->LSN);
  impl->buffer[bufferOffset] = cpy;

  DEBUG("lsn: %ld type: %d\n", e->LSN, e->type);
  impl->nextAvailableLSN++;

  unlock(impl->globalOffset_lock);
  unlock(impl->flushedLSN_lock);
  return 0;
}

static lsn_t stasis_log_impl_in_memory_first_unstable_lsn(stasis_log_t* log,
                                    stasis_log_force_mode_t mode) {
  stasis_log_impl_in_memory * impl = log->impl;
  return impl->nextAvailableLSN;
}

static void stasis_log_impl_in_memory_force_tail(stasis_log_t* log, stasis_log_force_mode_t m){
  // no-op
}

static lsn_t stasis_log_impl_in_memory_next_entry(stasis_log_t * log, const LogEntry * e) {
  return e->LSN + 1;
}

static int stasis_log_impl_in_memory_truncate(stasis_log_t * log, lsn_t lsn) {
  stasis_log_impl_in_memory * impl = log->impl;
  writelock(impl->flushedLSN_lock,1);
  writelock(impl->globalOffset_lock,1);

  assert(lsn <= impl->nextAvailableLSN);


  if(lsn > impl->globalOffset) {
    for(int i = impl->globalOffset; i < lsn; i++) {
      free(impl->buffer[i - impl->globalOffset]);
    }
    assert((lsn-impl->globalOffset) + (impl->nextAvailableLSN -lsn) < impl->bufferLen);
    memmove(&(impl->buffer[0]), &(impl->buffer[lsn - impl->globalOffset]),
			sizeof(LogEntry*) * (impl->nextAvailableLSN - lsn));
    impl->globalOffset = lsn;
  }

  writeunlock(impl->globalOffset_lock);
  writeunlock(impl->flushedLSN_lock);

  return 0;
}

static lsn_t stasis_log_impl_in_memory_truncation_point(stasis_log_t * log) {
  stasis_log_impl_in_memory * impl = log->impl;
  return impl->globalOffset;
}

static int stasis_log_impl_in_memory_close(stasis_log_t * log) {
  stasis_log_impl_in_memory * impl = log->impl;
  if(impl->buffer) {
    lsn_t firstEmptyOffset = impl->nextAvailableLSN-impl->globalOffset;
    for(lsn_t i = 0; i < firstEmptyOffset; i++) {
      assert(impl->buffer[i]->LSN == i+impl->globalOffset);
      free(impl->buffer[i]);
    }
    free(impl->buffer);
    impl->nextAvailableLSN = 0;
    impl->globalOffset = 0;
    impl->bufferLen = 0;
    impl->buffer = 0;
    free(impl);
  }
  free (log);
  return 0;
}

static const LogEntry * stasis_log_impl_in_memory_read_entry(stasis_log_t* log,
                                                 lsn_t lsn) {
  stasis_log_impl_in_memory * impl = log->impl;
  DEBUG("lsn: %ld\n", lsn);
  if(lsn >= impl->nextAvailableLSN) { return 0; }
  assert(lsn-impl->globalOffset >= 0 && lsn-impl->globalOffset< impl->bufferLen);
  readlock(impl->globalOffset_lock, 0);
  LogEntry * ptr = impl->buffer[lsn - impl->globalOffset];
  unlock(impl->globalOffset_lock);
  assert(ptr);
  assert(ptr->LSN == lsn);

  LogEntry * ret = malloc(sizeofLogEntry(ptr));

  memcpy(ret, ptr, sizeofLogEntry(ptr));

  DEBUG("lsn: %ld prevlsn: %ld\n", ptr->LSN, ptr->prevLSN);
  return ret;
}
static lsn_t stasis_log_impl_in_memory_sizeof_internal_entry(stasis_log_t* log,
                                                const LogEntry * e) {
  abort();
}
static int stasis_log_impl_in_memory_is_durable(stasis_log_t*log) { return 0; }

stasis_log_t* stasis_log_impl_in_memory_open() {
  stasis_log_impl_in_memory * impl = malloc(sizeof(*impl));
  impl->flushedLSN_lock   = initlock();
  impl->globalOffset_lock = initlock();
  impl->globalOffset = 0;
  impl->nextAvailableLSN = 0;
  impl->buffer = malloc(4096 * 1024 * sizeof (LogEntry *));
  impl->bufferLen =4096 * 1024;
  static stasis_log_t proto = {
    stasis_log_impl_in_memory_sizeof_internal_entry,
    stasis_log_impl_in_memory_write_entry,
    stasis_log_impl_in_memory_read_entry,
    stasis_log_impl_in_memory_next_entry,
    stasis_log_impl_in_memory_first_unstable_lsn,
    stasis_log_impl_in_memory_next_available_lsn,
    stasis_log_impl_in_memory_force_tail,
    stasis_log_impl_in_memory_truncate,
    stasis_log_impl_in_memory_truncation_point,
    stasis_log_impl_in_memory_close,
    stasis_log_impl_in_memory_is_durable
  };
  stasis_log_t* log = malloc(sizeof(*log));
  memcpy(log,&proto, sizeof(proto));
  log->impl = impl;
  return log;
}
