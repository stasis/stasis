#include <stasis/latches.h>
#include <stasis/transactional.h>
#include <stasis/recovery.h>
#include <stasis/bufferManager.h>
#include <stasis/consumer.h>
#include <stasis/lockManager.h>
#include <stasis/compensations.h>
#include <stasis/pageHandle.h>
#include <stasis/page.h>

#include <stasis/bufferManager/legacy/pageFile.h>


#include <stasis/logger/logger2.h>
#include <stasis/logger/safeWrites.h>
#include <stasis/logger/inMemoryLog.h>

#include <stasis/truncation.h>
#include <stasis/io/handle.h>
#include <stasis/blobManager.h> // XXX remove this, move Tread() to set.c

#include <assert.h>
#include <stdio.h>

static int stasis_initted = 0;

TransactionLog stasis_transaction_table[MAX_TRANSACTIONS];
static int stasis_transaction_table_num_active = 0;
static int stasis_transaction_table_xid_count = 0;

static stasis_log_t* stasis_log_file = 0;
stasis_dirty_page_table_t * stasis_dirty_page_table = 0;
static stasis_truncation_t * stasis_truncation = 0;
static stasis_alloc_t * stasis_alloc = 0;

/**
	This mutex protects stasis_transaction_table, numActiveXactions and
	xidCount.
*/
static pthread_mutex_t stasis_transaction_table_mutex;

typedef enum {
  INVALID_XTABLE_XID = INVALID_XID,
  PENDING_XTABLE_XID = -2
} stasis_transaction_table_status;

void stasis_transaction_table_init() {
  memset(stasis_transaction_table, INVALID_XTABLE_XID,
	 sizeof(TransactionLog)*MAX_TRANSACTIONS);
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    pthread_mutex_init(&stasis_transaction_table[i].mut,0);
  }
}

void * stasis_runtime_dirty_page_table() {
  return stasis_dirty_page_table;
}
void * stasis_runtime_alloc_state() {
  return stasis_alloc;
}

int Tinit() {
  pthread_mutex_init(&stasis_transaction_table_mutex, NULL);
  stasis_initted = 1;
  stasis_transaction_table_num_active = 0;

  compensations_init();

  stasis_transaction_table_init();
  stasis_operation_table_init();

  stasis_log_file = 0;

  if(LOG_TO_FILE == stasis_log_type) {
    stasis_log_file = stasis_log_safe_writes_open(stasis_log_file_name,
                                                  stasis_log_file_mode,
                                                  stasis_log_file_permissions);
    stasis_log_file->group_force =
      stasis_log_group_force_init(stasis_log_file, 10 * 1000 * 1000); // timeout in nsec; want 10msec.
  } else if(LOG_TO_MEMORY == stasis_log_type) {
    stasis_log_file = stasis_log_impl_in_memory_open();
    stasis_log_file->group_force = 0;
  } else {
    assert(stasis_log_file != NULL);
  }

  stasis_dirty_page_table = stasis_dirty_page_table_init();
  stasis_page_init(stasis_dirty_page_table);
  stasis_page_handle_t * page_handle;
  if(bufferManagerFileHandleType == BUFFER_MANAGER_FILE_HANDLE_DEPRECATED) {
    printf("\nWarning: Using old I/O routines (with known bugs).\n");
    page_handle = openPageFile(stasis_log_file, stasis_dirty_page_table);
  } else {
    stasis_handle_t * h = stasis_handle_open(stasis_store_file_name);
    // XXX should not be global.
    page_handle = stasis_page_handle_open(h, stasis_log_file, stasis_dirty_page_table);
  }

  stasis_buffer_manager_open(bufferManagerType, page_handle);
  DEBUG("Buffer manager type = %d\n", bufferManagerType);
  pageOperationsInit();
  stasis_alloc = TallocInit();
  TnaiveHashInit();
  LinearHashNTAInit();
  BtreeInit();
  TlinkedListNTAInit();
  iterator_init();
  consumer_init();
  setupLockManagerCallbacksNil();
  //setupLockManagerCallbacksPage();

  stasis_recovery_initiate(stasis_log_file, stasis_alloc);
  stasis_truncation = stasis_truncation_init(stasis_dirty_page_table, stasis_log_file);
  if(stasis_truncation_automatic) {
    // should this be before InitiateRecovery?
    stasis_truncation_thread_start(stasis_truncation);
  }
  return 0;
}


int Tbegin() {

	int i, index = 0;
	int xidCount_tmp;

    assert(stasis_initted);

	pthread_mutex_lock(&stasis_transaction_table_mutex);

	if( stasis_transaction_table_num_active == MAX_TRANSACTIONS ) {
	  pthread_mutex_unlock(&stasis_transaction_table_mutex);
	  return LLADD_EXCEED_MAX_TRANSACTIONS;
	}
	else {
          DEBUG("%s:%d activate in begin\n",__FILE__,__LINE__);
          stasis_transaction_table_num_active++;
        }
	for( i = 0; i < MAX_TRANSACTIONS; i++ ) {
		stasis_transaction_table_xid_count++;
		if( stasis_transaction_table[stasis_transaction_table_xid_count%MAX_TRANSACTIONS].xid == INVALID_XTABLE_XID ) {
			index = stasis_transaction_table_xid_count%MAX_TRANSACTIONS;
			break;
		}
	}

	xidCount_tmp = stasis_transaction_table_xid_count;

	stasis_transaction_table[index].xid = PENDING_XTABLE_XID;

	pthread_mutex_unlock(&stasis_transaction_table_mutex);

	stasis_log_begin_transaction(stasis_log_file, xidCount_tmp, &stasis_transaction_table[index]);

	if(globalLockManager.begin) { globalLockManager.begin(stasis_transaction_table[index].xid); }

	return stasis_transaction_table[index].xid;
}

compensated_function void Tupdate(int xid, pageid_t page,
					       const void * dat, size_t datlen, int op) {
  assert(stasis_initted);
  assert(page != INVALID_PAGE);
  LogEntry * e;
  assert(xid >= 0 && stasis_transaction_table[xid % MAX_TRANSACTIONS].xid == xid);

  Page * p = loadPageForOperation(xid, page, op);

  try {
    if(globalLockManager.writeLockPage && p) {
      globalLockManager.writeLockPage(xid, page);
    }
  } end;

  if(p) writelock(p->rwlatch,0);

  e = stasis_log_write_update(stasis_log_file, &stasis_transaction_table[xid % MAX_TRANSACTIONS],
                page, op, dat, datlen);

  assert(stasis_transaction_table[xid % MAX_TRANSACTIONS].prevLSN == e->LSN);
  DEBUG("Tupdate() e->LSN: %ld\n", e->LSN);
  stasis_operation_do(e, p);
  freeLogEntry(e);

  if(p) unlock(p->rwlatch);
  if(p) releasePage(p);
}

void TreorderableUpdate(int xid, void * hp, pageid_t page,
                        const void *dat, size_t datlen, int op) {
  stasis_log_reordering_handle_t * h = (typeof(h))hp;
  assert(xid >= 0 && stasis_transaction_table[xid % MAX_TRANSACTIONS].xid == xid);
  Page * p = loadPage(xid, page);
  assert(p);
  try {
    if(globalLockManager.writeLockPage) {
      globalLockManager.writeLockPage(xid, p->id);
    }
  } end;

  pthread_mutex_lock(&h->mut);

  LogEntry * e = allocUpdateLogEntry(-1, h->l->xid, op,
                                     p->id,
                                     dat, datlen);

  stasis_log_reordering_handle_append(h, p, op, dat, datlen, sizeofLogEntry(0, e));

  e->LSN = 0;
  writelock(p->rwlatch,0);
  stasis_operation_do(e, p);
  unlock(p->rwlatch);
  pthread_mutex_unlock(&h->mut);
  // page will be released by the log handle...
  freeLogEntry(e);
}
lsn_t TwritebackUpdate(int xid, pageid_t page,
                      const void *dat, size_t datlen, int op) {
  assert(xid >= 0 && stasis_transaction_table[xid % MAX_TRANSACTIONS].xid == xid);
  LogEntry * e = allocUpdateLogEntry(-1, xid, op, page, dat, datlen);
  TransactionLog* l = &stasis_transaction_table[xid % MAX_TRANSACTIONS];
  stasis_log_file->write_entry(stasis_log_file, e);

  if(l->prevLSN == -1) { l->recLSN = e->LSN; }
  l->prevLSN = e->LSN;

  freeLogEntry(e);
  return l->prevLSN;
}
/** DANGER: you need to set the LSN's on the pages that you want to write back,
    this method doesn't let you do that, so the only option is to pin until
    commit, then set a conservative (too high) lsn */
void TreorderableWritebackUpdate(int xid, void* hp,
                                 pageid_t page, const void * dat,
                                 size_t datlen, int op) {
  stasis_log_reordering_handle_t* h = hp;
  assert(xid >= 0 && stasis_transaction_table[xid % MAX_TRANSACTIONS].xid == xid);
  pthread_mutex_lock(&h->mut);
  LogEntry * e = allocUpdateLogEntry(-1, xid, op, page, dat, datlen);
  stasis_log_reordering_handle_append(h, 0, op, dat, datlen, sizeofLogEntry(0, e));
  pthread_mutex_unlock(&h->mut);
}
compensated_function void TupdateStr(int xid, pageid_t page,
                                     const char *dat, size_t datlen, int op) {
  Tupdate(xid, page, dat, datlen, op);
}

compensated_function void TreadStr(int xid, recordid rid, char * dat) {
  Tread(xid, rid, dat);
}

compensated_function void Tread(int xid, recordid rid, void * dat) {
  Page * p;
  try {
    p = loadPage(xid, rid.page);
  } end;

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
  releasePage(p);
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

int Tcommit(int xid) {
  lsn_t lsn;
  assert(xid >= 0);
#ifdef DEBUGGING
  pthread_mutex_lock(&stasis_transaction_table_mutex);
  assert(stasis_transaction_table_num_active <= MAX_TRANSACTIONS);
  pthread_mutex_unlock(&stasis_transaction_table_mutex);
#endif

  lsn = stasis_log_commit_transaction(stasis_log_file, &stasis_transaction_table[xid % MAX_TRANSACTIONS]);
  if(globalLockManager.commit) { globalLockManager.commit(xid); }

  stasis_alloc_committed(stasis_alloc, xid);

  pthread_mutex_lock(&stasis_transaction_table_mutex);

  stasis_transaction_table[xid%MAX_TRANSACTIONS].xid = INVALID_XTABLE_XID;
  DEBUG("%s:%d deactivate %d\n",__FILE__,__LINE__,xid);
  stasis_transaction_table_num_active--;
  assert( stasis_transaction_table_num_active >= 0 );
  pthread_mutex_unlock(&stasis_transaction_table_mutex);

  return 0;
}

int Tprepare(int xid) {
  assert(xid >= 0);
  off_t i = xid % MAX_TRANSACTIONS;
  assert(stasis_transaction_table[i].xid == xid);
  stasis_log_prepare_transaction(stasis_log_file, &stasis_transaction_table[i]);
  return 0;
}

int Tabort(int xid) {
  lsn_t lsn;
  assert(xid >= 0);

  TransactionLog * t =&stasis_transaction_table[xid%MAX_TRANSACTIONS];
  assert(t->xid == xid);

  lsn = stasis_log_abort_transaction(stasis_log_file, t);

  /** @todo is the order of the next two calls important? */
  undoTrans(stasis_log_file, *t);
  if(globalLockManager.abort) { globalLockManager.abort(xid); }

  stasis_alloc_aborted(stasis_alloc, xid);

  return 0;
}
int Tforget(int xid) {
  TransactionLog * t = &stasis_transaction_table[xid%MAX_TRANSACTIONS];
  assert(t->xid == xid);
  stasis_log_end_aborted_transaction(stasis_log_file, t);
  stasis_transaction_table_forget(t->xid);
  return 0;
}
int Tdeinit() {
  int i;

  for( i = 0; i < MAX_TRANSACTIONS; i++ ) {
    if( stasis_transaction_table[i].xid != INVALID_XTABLE_XID ) {
      if(!stasis_suppress_unclean_shutdown_warnings) {
	fprintf(stderr, "WARNING: Tdeinit() is aborting transaction %d\n",
		stasis_transaction_table[i].xid);
      }
      Tabort(stasis_transaction_table[i].xid);
    }
  }
  assert( stasis_transaction_table_num_active == 0 );
  stasis_truncation_deinit(stasis_truncation);
  TnaiveHashDeinit();
  TallocDeinit(stasis_alloc);
  stasis_buffer_manager_close();
  DEBUG("Closing page file tdeinit\n");
  stasis_page_deinit();
  stasis_log_group_force_t * group_force = stasis_log_file->group_force;
  stasis_log_file->close(stasis_log_file);
  if(group_force) { stasis_log_group_force_deinit(group_force); }
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
  stasis_buffer_manager_simulate_crash();
  // XXX: close_file?
  stasis_page_deinit();
  stasis_log_file->close(stasis_log_file);
  stasis_transaction_table_num_active = 0;
  stasis_dirty_page_table_deinit(stasis_dirty_page_table);

  // Reset it here so the warnings will appear if a new stasis
  // instance encounters problems during a clean shutdown.
  stasis_suppress_unclean_shutdown_warnings = 0;
  return 0;
}

void stasis_transaction_table_max_transaction_id_set(int xid) {
  pthread_mutex_lock(&stasis_transaction_table_mutex);
  stasis_transaction_table_xid_count = xid;
  pthread_mutex_unlock(&stasis_transaction_table_mutex);
}
void stasis_transaction_table_active_transaction_count_set(int xid) {
  pthread_mutex_lock(&stasis_transaction_table_mutex);
  stasis_transaction_table_num_active = xid;
  pthread_mutex_unlock(&stasis_transaction_table_mutex);
}

lsn_t stasis_transaction_table_minRecLSN() {
  lsn_t minRecLSN = LSN_T_MAX;
  pthread_mutex_lock(&stasis_transaction_table_mutex);
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    if(stasis_transaction_table[i].xid != INVALID_XTABLE_XID) {
      lsn_t recLSN = stasis_transaction_table[i].recLSN;
      if(recLSN != -1 && recLSN < minRecLSN) {
        minRecLSN = recLSN;
      }
    }
  }
  pthread_mutex_unlock(&stasis_transaction_table_mutex);
  return minRecLSN;
}

int TactiveTransactionCount() {
  return stasis_transaction_table_num_active;
}

int* TlistActiveTransactions() {
  pthread_mutex_lock(&stasis_transaction_table_mutex);
  int * ret = malloc(sizeof(*ret));
  ret[0] = 0;
  int retcount = 0;
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    if(stasis_transaction_table[i].xid != INVALID_XTABLE_XID) {
      ret[retcount] = stasis_transaction_table[i].xid;
      retcount++;
      ret = realloc(ret, (retcount+1) * sizeof(*ret));
      ret[retcount] = 0;
    }
  }
  pthread_mutex_unlock(&stasis_transaction_table_mutex);
  return ret;
}
int TisActiveTransaction(int xid) {
  if(xid < 0) { return 0; }
  pthread_mutex_lock(&stasis_transaction_table_mutex);
  int ret = xid != INVALID_XTABLE_XID && stasis_transaction_table[xid%MAX_TRANSACTIONS].xid == xid;
  pthread_mutex_unlock(&stasis_transaction_table_mutex);
  return ret;
}

int stasis_transaction_table_roll_forward(int xid, lsn_t lsn, lsn_t prevLSN) {
  TransactionLog * l = &stasis_transaction_table[xid%MAX_TRANSACTIONS];
  if(l->xid == xid) {
    // rolling forward CLRs / NTAs makes prevLSN decrease.
    assert(l->prevLSN >= prevLSN);
  } else {
    pthread_mutex_lock(&stasis_transaction_table_mutex);
    assert(l->xid == INVALID_XTABLE_XID);
    l->xid = xid;
    l->recLSN = lsn;
    stasis_transaction_table_num_active++;
    pthread_mutex_unlock(&stasis_transaction_table_mutex);
  }
  l->prevLSN = lsn;
  return 0;
}
int stasis_transaction_table_roll_forward_with_reclsn(int xid, lsn_t lsn,
                                                      lsn_t prevLSN,
                                                      lsn_t recLSN) {
  assert(stasis_transaction_table[xid%MAX_TRANSACTIONS].recLSN == recLSN);
  return stasis_transaction_table_roll_forward(xid, lsn, prevLSN);
}
int stasis_transaction_table_forget(int xid) {
  assert(xid != INVALID_XTABLE_XID);
  TransactionLog * l = &stasis_transaction_table[xid%MAX_TRANSACTIONS];
  if(l->xid == xid) {
    pthread_mutex_lock(&stasis_transaction_table_mutex);
    l->xid = INVALID_XTABLE_XID;
    l->prevLSN = -1;
    l->recLSN = -1;
    stasis_transaction_table_num_active--;
    assert(stasis_transaction_table_num_active >= 0);
    pthread_mutex_unlock(&stasis_transaction_table_mutex);
  } else {
    assert(l->xid == INVALID_XTABLE_XID);
  }
  return 0;
}

int TdurabilityLevel() {
  if(bufferManagerType == BUFFER_MANAGER_MEM_ARRAY) {
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
  assert(xid >= 0);
  void * e = stasis_log_begin_nta(stasis_log_file,
			   &stasis_transaction_table[xid % MAX_TRANSACTIONS],
			   op, dat, datSize);
  // HACK: breaks encapsulation.
  stasis_operation_do(e, NULL);

  stasis_log_end_nta(stasis_log_file, &stasis_transaction_table[xid % MAX_TRANSACTIONS], e);

  return 0;
}

void * TbeginNestedTopAction(int xid, int op, const byte * dat, int datSize) {
  assert(xid >= 0);

  void * ret = stasis_log_begin_nta(stasis_log_file, &stasis_transaction_table[xid % MAX_TRANSACTIONS], op, dat, datSize);
  DEBUG("Begin Nested Top Action e->LSN: %ld\n", e->LSN);
  return ret;
}

/**
    Call this function at the end of a nested top action.
    @return the lsn of the CLR.  Most users (everyone?) will ignore this.
*/
lsn_t TendNestedTopAction(int xid, void * handle) {

  lsn_t ret = stasis_log_end_nta(stasis_log_file, &stasis_transaction_table[xid % MAX_TRANSACTIONS], handle);

  DEBUG("NestedTopAction CLR %d, LSN: %ld type: %ld (undoing: %ld, next to undo: %ld)\n", e->xid,
	 clrLSN, undoneLSN, *prevLSN);

  return ret;
}
void * stasis_log() {
  return stasis_log_file;
}
