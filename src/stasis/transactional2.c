#include <stasis/latches.h>
#include <stasis/transactional.h>
#include <stasis/recovery.h>
#include <stasis/bufferManager.h>
#include <stasis/consumer.h>
#include <stasis/lockManager.h>
#include <stasis/compensations.h>
#include <stasis/pageHandle.h>
#include <stasis/page.h>
#include <stasis/transactionTable.h>

#include <stasis/bufferManager/legacy/pageFile.h>

#include <stasis/bufferManager/pageArray.h>
#include <stasis/bufferManager/bufferHash.h>
#include <stasis/bufferManager/legacy/legacyBufferManager.h>

#include <stasis/logger/logger2.h>
#include <stasis/logger/safeWrites.h>
#include <stasis/logger/inMemoryLog.h>

#include <stasis/truncation.h>
#include <stasis/io/handle.h>
#include <stasis/blobManager.h> // XXX remove this, move Tread() to set.c

#include <assert.h>
#include <stdio.h>

static int stasis_initted = 0;

static stasis_log_t* stasis_log_file = 0;
static stasis_dirty_page_table_t * stasis_dirty_page_table = 0;
static stasis_transaction_table_t * stasis_transaction_table;
static stasis_truncation_t * stasis_truncation = 0;
static stasis_alloc_t * stasis_alloc = 0;
static stasis_allocation_policy_t * stasis_allocation_policy = 0;
static stasis_buffer_manager_t * stasis_buffer_manager = 0;

void * stasis_runtime_buffer_manager() {
  return stasis_buffer_manager;
}
void * stasis_runtime_dirty_page_table() {
  return stasis_dirty_page_table;
}
void * stasis_runtime_transaction_table() {
  return stasis_transaction_table;
}
void * stasis_runtime_alloc_state() {
  return stasis_alloc;
}

stasis_log_t* stasis_log_default_factory() {
  stasis_log_t *log_file = 0;
  if(LOG_TO_FILE == stasis_log_type) {
    log_file = stasis_log_safe_writes_open(stasis_log_file_name,
                                           stasis_log_file_mode,
                                           stasis_log_file_permissions,
                                           stasis_log_softcommit);
    log_file->group_force =
      stasis_log_group_force_init(log_file, 10 * 1000 * 1000); // timeout in nsec; want 10msec.
  } else if(LOG_TO_MEMORY == stasis_log_type) {
    log_file = stasis_log_impl_in_memory_open();
    log_file->group_force = 0;
  }
  return log_file;
}

int Tinit() {
  stasis_initted = 1;

  compensations_init();

  stasis_operation_table_init();

  stasis_transaction_table = stasis_transaction_table_init();
  stasis_dirty_page_table  = stasis_dirty_page_table_init();

  stasis_page_init(stasis_dirty_page_table);

  stasis_log_file = stasis_log_factory();
  assert(stasis_log_file != NULL);

  stasis_buffer_manager = stasis_buffer_manager_factory(stasis_log_file, stasis_dirty_page_table);

  stasis_dirty_page_table_set_buffer_manager(stasis_dirty_page_table, stasis_buffer_manager); // xxx circular dependency.
  pageOperationsInit(stasis_log_file);
  stasis_allocation_policy = stasis_allocation_policy_init();
  stasis_alloc = stasis_alloc_init(stasis_transaction_table, stasis_allocation_policy);

  TnaiveHashInit();
  LinearHashNTAInit();
  BtreeInit();
  TlinkedListNTAInit();
  iterator_init();
  consumer_init();
  setupLockManagerCallbacksNil();
  //setupLockManagerCallbacksPage();

  stasis_recovery_initiate(stasis_log_file, stasis_transaction_table, stasis_alloc);
  stasis_truncation = stasis_truncation_init(stasis_dirty_page_table, stasis_transaction_table,
                                             stasis_buffer_manager, stasis_log_file);
  if(stasis_truncation_automatic) {
    // should this be before InitiateRecovery?
    stasis_truncation_thread_start(stasis_truncation);
  }
  stasis_log_file->set_truncation(stasis_log_file, stasis_truncation);
  return 0;
}


int Tbegin() {

  assert(stasis_initted);

  int xid;

  stasis_transaction_table_entry_t* newXact = stasis_transaction_table_begin(stasis_transaction_table, &xid);
  if(newXact != 0) {
    stasis_log_begin_transaction(stasis_log_file, xid, newXact);

    if(globalLockManager.begin) { globalLockManager.begin(newXact->xid); }

    return newXact->xid;
  } else {
    assert(xid == LLADD_EXCEED_MAX_TRANSACTIONS);
    return xid;
  }
}

void TupdateWithPage(int xid, pageid_t page, Page *p, const void * dat, size_t datlen, int op) {
  assert(stasis_initted);
  assert(page != INVALID_PAGE);
  LogEntry * e;
  stasis_transaction_table_entry_t * xact = stasis_transaction_table_get(stasis_transaction_table, xid);
  assert(xact);

  if(globalLockManager.writeLockPage && p) {
    globalLockManager.writeLockPage(xid, page);
  }

  if(p) writelock(p->rwlatch,0);

  e = stasis_log_write_update(stasis_log_file, xact, page, op, dat, datlen);

  assert(xact->prevLSN == e->LSN);
  DEBUG("Tupdate() e->LSN: %ld\n", e->LSN);
  LogEntry * multiE = 0;
  if(page == MULTI_PAGEID) {
    size_t len = sizeofLogEntry(stasis_log_file, e);
    multiE = malloc(len);
    memcpy(multiE, e, len);
  } else {
    stasis_operation_do(e, p);
  }
  stasis_log_file->write_entry_done(stasis_log_file, e);

  if(page == MULTI_PAGEID) {
    // Note: This is not atomic with the log entry.  MULTI operations are a special case of segments.
    // We assume that any concurrent updates to the backing pages commute with this operation.
    // For this to be true, either:
    // (a) the pages must not have LSNs in their headers, or
    // (b) we must have an implicit latch (which should be the case for allocation requests).
    stasis_operation_do(multiE, 0);
    free(multiE);
  }

  if(p) unlock(p->rwlatch);
}

void Tupdate(int xid, pageid_t page, const void * dat, size_t datlen, int op) {
  Page * p = loadPageForOperation(xid, page, op, 0);
  TupdateWithPage(xid, page, p, dat, datlen, op);
  if(p) releasePage(p);
}

void TreorderableUpdate(int xid, void * hp, pageid_t page,
                        const void *dat, size_t datlen, int op) {
  stasis_log_reordering_handle_t * h = (typeof(h))hp;
  assert(stasis_transaction_table_is_active(stasis_transaction_table, xid));
  Page * p = loadPage(xid, page);
  assert(p);
  try {
    if(globalLockManager.writeLockPage) {
      globalLockManager.writeLockPage(xid, p->id);
    }
  } end;

  pthread_mutex_lock(&h->mut);

  LogEntry * e = mallocScratchUpdateLogEntry(INVALID_LSN, INVALID_LSN, h->l->xid, op,
                                     p->id, datlen);

  memcpy(stasis_log_entry_update_args_ptr(e), dat, datlen);

  stasis_log_reordering_handle_append(h, p, op, dat, datlen, sizeofLogEntry(0, e));

  e->LSN = 0;
  writelock(p->rwlatch,0);
  stasis_operation_do(e, p);
  unlock(p->rwlatch);
  pthread_mutex_unlock(&h->mut);
  // page will be released by the log handle...
  //stasis_log_file->write_entry_done(stasis_log_file, e);
  free(e);
}
lsn_t TwritebackUpdate(int xid, pageid_t page,
                      const void *dat, size_t datlen, int op) {
  LogEntry * e = allocUpdateLogEntry(stasis_log_file, INVALID_LSN, xid, op, page, datlen);
  memcpy(stasis_log_entry_update_args_ptr(e), dat, datlen);

  stasis_transaction_table_entry_t* l = stasis_transaction_table_get(stasis_transaction_table, xid);
  stasis_log_file->write_entry(stasis_log_file, e);

  if(l->prevLSN == -1) { l->recLSN = e->LSN; }
  l->prevLSN = e->LSN;

  stasis_log_file->write_entry_done(stasis_log_file, e);
  return l->prevLSN;
}
/** DANGER: you need to set the LSN's on the pages that you want to write back,
    this method doesn't let you do that, so the only option is to pin until
    commit, then set a conservative (too high) lsn */
void TreorderableWritebackUpdate(int xid, void* hp,
                                 pageid_t page, const void * dat,
                                 size_t datlen, int op) {
  stasis_log_reordering_handle_t* h = hp;
  assert(stasis_transaction_table_is_active(stasis_transaction_table, xid));
  pthread_mutex_lock(&h->mut);
  LogEntry * e = mallocScratchUpdateLogEntry(INVALID_LSN, INVALID_LSN, xid, op, page, datlen);
  memcpy(stasis_log_entry_update_args_ptr(e), dat, datlen);
  stasis_log_reordering_handle_append(h, 0, op, dat, datlen, sizeofLogEntry(0, e));
  pthread_mutex_unlock(&h->mut);
  free(e);
}
void TupdateStr(int xid, pageid_t page,
                                     const char *dat, size_t datlen, int op) {
  Tupdate(xid, page, dat, datlen, op);
}

void TreadStr(int xid, recordid rid, char * dat) {
  Tread(xid, rid, dat);
}

Page * TreadWithPage(int xid, recordid rid, Page *p, void * dat) {
  readlock(p->rwlatch,0);

  rid = stasis_record_dereference(xid, p, rid);
  if(rid.page != p->id) {
    unlock(p->rwlatch);
    releasePage(p);
    p = loadPage(xid, rid.page);
    readlock(p->rwlatch,0);
  }
  short type = stasis_record_type_read(xid,p,rid);
  if(type == BLOB_SLOT) {
    DEBUG("call readBlob %lld %lld %lld\n", (long long)rid.page, (long long)rid.slot, (long long)rid.size);
    stasis_blob_read(xid,p,rid,dat);
    assert(rid.page == p->id);
  } else {
    stasis_record_read(xid, p, rid, dat);
  }
  unlock(p->rwlatch);
  return p;
}

compensated_function void Tread(int xid, recordid rid, void * dat) {
  Page * p;
  try {
    p = loadPage(xid, rid.page);
  } end;

  releasePage( TreadWithPage(xid, rid, p, dat) );
}

compensated_function void TreadRaw(int xid, recordid rid, void * dat) {
  Page * p;
  try {
    p = loadPage(xid, rid.page);
  } end;
  readlock(p->rwlatch,0);
  stasis_record_read(xid, p, rid, dat);
  unlock(p->rwlatch);
  releasePage(p);
}

static inline int TcommitHelper(int xid, int force) {
  lsn_t lsn;
  assert(xid >= 0);

  stasis_transaction_table_entry_t * xact = stasis_transaction_table_get(stasis_transaction_table, xid);
  if(xact->prevLSN != INVALID_LSN) {
    lsn = stasis_log_commit_transaction(stasis_log_file, stasis_transaction_table, xact, force);
    if(globalLockManager.commit) { globalLockManager.commit(xid); }
  }

  stasis_transaction_table_commit(stasis_transaction_table, xid);

  return 0;
}

int Tcommit(int xid) {
  return TcommitHelper(xid, 1); // 1 -> force write log
}
int TsoftCommit(int xid) {
  return TcommitHelper(xid, 0); // 0 -> don't force write log.
}
void TforceCommits() {
  stasis_log_force(stasis_log_file, INVALID_LSN, LOG_FORCE_COMMIT);
}

int Tprepare(int xid) {
  assert(xid >= 0);
  stasis_transaction_table_entry_t * xact = stasis_transaction_table_get(stasis_transaction_table, xid);
  assert(xact);
  stasis_log_prepare_transaction(stasis_log_file, xact);
  return 0;
}

int Tabort(int xid) {
  lsn_t lsn;
  assert(xid >= 0);

  stasis_transaction_table_entry_t * t = stasis_transaction_table_get(stasis_transaction_table, xid);
  assert(t->xid == xid);

  if( t->prevLSN != INVALID_LSN ) {
      lsn = stasis_log_abort_transaction(stasis_log_file, stasis_transaction_table, t);

      /** @todo is the order of the next two calls important? */
      undoTrans(stasis_log_file, stasis_transaction_table, *t); // XXX don't really need to pass the whole table in...
      if(globalLockManager.abort) { globalLockManager.abort(xid); }
  } else {
      // This would normally be called by stasis_recovery_undo inside of undoTrans.
      // Since we skip the call to undoTrans, we call it here.  Note that this is
      // different than the API usage in TcommitHelper().  The reason is that
      // undoTrans needs to deal with Tprepare().

      // @todo pull up all calls to stasis_transaction_table_forget(),
      // and move this invocation outside of the if-then-else.
      stasis_transaction_table_forget(stasis_transaction_table, xid);
  }

  return 0;
}
int Tforget(int xid) {
  stasis_transaction_table_entry_t * t = stasis_transaction_table_get(stasis_transaction_table, xid);
  assert(t->xid == xid);
  stasis_log_end_aborted_transaction(stasis_log_file, stasis_transaction_table, t);
  stasis_transaction_table_forget(stasis_transaction_table, t->xid);
  return 0;
}
int Tdeinit() {
  int count;
  int * active = stasis_transaction_table_list_active(stasis_transaction_table, &count);

  for(int i = 0; i < count; i++) {
    if(stasis_transaction_table_get(stasis_transaction_table,
				    active[i])->prevLSN != INVALID_LSN) {
      if(!stasis_suppress_unclean_shutdown_warnings) {
	fprintf(stderr, "WARNING: Tdeinit() is aborting transaction %d\n", active[i]);
      }
    }
    Tabort(active[i]);
  }
  free(active);

  active = stasis_transaction_table_list_active(stasis_transaction_table, &count);
  assert( count == 0 );
  free(active);

  stasis_truncation_deinit(stasis_truncation);
  TnaiveHashDeinit();
  LinearHashNTADeinit();
  TlinkedListNTADeinit();
  stasis_alloc_deinit(stasis_alloc);
  stasis_allocation_policy_deinit(stasis_allocation_policy);
  stasis_buffer_manager->stasis_buffer_manager_close(stasis_buffer_manager);
  DEBUG("Closing page file tdeinit\n");
  stasis_page_deinit();
  stasis_log_group_force_t * group_force = stasis_log_file->group_force;
  stasis_log_file->close(stasis_log_file);
  if(group_force) { stasis_log_group_force_deinit(group_force); }
  stasis_transaction_table_deinit(stasis_transaction_table);
  stasis_dirty_page_table_deinit(stasis_dirty_page_table);

  stasis_initted = 0;

  return 0;
}

int TuncleanShutdown() {
  // We're simulating a crash; don't complain when writes get lost,
  // and active transactions get rolled back.
  stasis_suppress_unclean_shutdown_warnings = 1;
  stasis_truncation_deinit(stasis_truncation);
  TnaiveHashDeinit();
  LinearHashNTADeinit();
  TlinkedListNTADeinit();
  stasis_alloc_deinit(stasis_alloc);
  stasis_allocation_policy_deinit(stasis_allocation_policy);

  stasis_buffer_manager->stasis_buffer_manager_simulate_crash(stasis_buffer_manager);
  // XXX: close_file?
  stasis_page_deinit();
  stasis_log_file->close(stasis_log_file);
  stasis_transaction_table_deinit(stasis_transaction_table);
  stasis_dirty_page_table_deinit(stasis_dirty_page_table);

  // Reset it here so the warnings will appear if a new stasis
  // instance encounters problems during a clean shutdown.
  stasis_suppress_unclean_shutdown_warnings = 0;
  return 0;
}

int TdurabilityLevel() {
  if(stasis_buffer_manager_factory == stasis_buffer_manager_mem_array_factory) {
    return VOLATILE;
  } else if(stasis_log_type == LOG_TO_MEMORY) {
    return PERSISTENT;
  } else {
    return DURABLE;
  }
}

void TtruncateLog() {
  stasis_truncation_truncate(stasis_truncation, 1);
}
typedef struct {
  lsn_t prev_lsn;
  lsn_t compensated_lsn;
} stasis_nta_handle;

int TnestedTopAction(int xid, int op, const byte * dat, size_t datSize) {
  stasis_transaction_table_entry_t * xact = stasis_transaction_table_get(stasis_transaction_table, xid);
  assert(xid >= 0);
  void * e = stasis_log_begin_nta(stasis_log_file,
			   xact,
			   op, dat, datSize);
  // HACK: breaks encapsulation.
  stasis_operation_do(e, NULL);

  stasis_log_end_nta(stasis_log_file, xact, e);

  return 0;
}

void * TbeginNestedTopAction(int xid, int op, const byte * dat, int datSize) {
  assert(xid >= 0);

  void * ret = stasis_log_begin_nta(stasis_log_file, stasis_transaction_table_get(stasis_transaction_table, xid), op, dat, datSize);
  DEBUG("Begin Nested Top Action e->LSN: %ld\n", e->LSN);
  return ret;
}

/**
    Call this function at the end of a nested top action.
    @return the lsn of the CLR.  Most users (everyone?) will ignore this.
*/
lsn_t TendNestedTopAction(int xid, void * handle) {

  lsn_t ret = stasis_log_end_nta(stasis_log_file, stasis_transaction_table_get(stasis_transaction_table, xid), handle);

  DEBUG("NestedTopAction CLR %d, LSN: %ld type: %ld (undoing: %ld, next to undo: %ld)\n", e->xid,
	 clrLSN, undoneLSN, *prevLSN);

  return ret;
}

int TactiveThreadCount(void) {
  return stasis_transaction_table_num_active_threads(stasis_transaction_table);
}
int* TlistActiveTransactions(int *count) {
  return stasis_transaction_table_list_active(stasis_transaction_table, count);
}
int TgetTransactionFingerprint(int xid, stasis_transaction_fingerprint_t * fp) {
  if(stasis_transaction_table_is_active(stasis_transaction_table, xid)) {
    lsn_t rec_lsn = stasis_transaction_table_get(stasis_transaction_table, xid)->recLSN;
    if(rec_lsn == INVALID_LSN) {  // Generate a dummy entry.
      Tupdate(xid,0,0,0,OPERATION_NOOP);
    }
    fp->xid = xid;
    rec_lsn = stasis_transaction_table_get(stasis_transaction_table, xid)->recLSN;
    fp->rec_lsn = rec_lsn;
    return 0;
  } else if(xid == INVALID_XID) {
    fp->xid = INVALID_XID;
    fp->rec_lsn = INVALID_LSN;
    return 0;
  } else {
    return ENOENT;
  }
}
int TisActiveTransaction(stasis_transaction_fingerprint_t * fp) {
  // stasis_transaction_table_is_active returns false for INVALID_XID, which is what we want.
  return stasis_transaction_table_is_active(stasis_transaction_table, fp->xid)
         && stasis_transaction_table_get(stasis_transaction_table, fp->xid)->recLSN == fp->rec_lsn;
}


void * stasis_log() {
  return stasis_log_file;
}
