#include <stasis/transactional.h>
#include <stasis/bufferManager.h>


uint64_t stasis_timestamp = 0;

//// ------------------------------------------------
//// ----      Core API                           ---
//// ------------------------------------------------

int __real_Tinit(void);
int __real_Tbegin(void);
void __real_Tupdate(int xid, pageid_t page, const void *dat, size_t datlen, int op);
void __real_TupdateWithPage(int xid, pageid_t page, Page *p, const void *dat, size_t datlen, int op);
void __real_TupdateStr(int xid, pageid_t page,
                                     const char *dat, size_t datlen, int op);
void __real_TreorderableUpdate(int xid, void * h, pageid_t page,
                        const void * dat, size_t datlen, int op);
lsn_t __real_TwritebackUpdate(int xid, pageid_t page,
                       const void * dat, size_t datlen, int op);
void __real_TreorderableWritebackUpdate(int xid, void* h,
                                 pageid_t page, const void * dat,
                                 size_t datlen, int op);
void __real_Tread(int xid, recordid rid, void *dat);
Page * __real_TreadWithPage(int xid, recordid rid, Page *p, void *dat);
void __real_TreadRaw(int xid, recordid rid, void *dat);
void __real_TreadStr(int xid, recordid rid, char *dat);
int __real_Tcommit(int xid);
int __real_TsoftCommit(int xid);
void __real_TforceCommits(void);
int __real_Tabort(int xid);
int __real_Tdeinit(void);
int __real_TuncleanShutdown(void);
//void __real_Trevive(int xid, lsn_t prevlsn, lsn_t reclsn);
int __real_Tprepare(int xid);
int __real_TnestedTopAction(int xid, int op, const byte * arg, size_t arg_len);
void * __real_TbeginNestedTopAction(int xid, int op, const byte* log_arguments,
                             int log_arguments_length);
lsn_t __real_TendNestedTopAction(int xid, void * handle);
int __real_Tforget(int xid);

int __wrap_Tinit(void) {
  printf("call_Tinit(%lld)\n", (long long)stasis_timestamp);
  int ret = __real_Tinit();
  printf("ret_Tinit(%lld)\n", (long long)stasis_timestamp);
  return ret;
}
int __wrap_Tbegin(void) {
  printf("call_Tbegin(%lld)\n", (long long)stasis_timestamp);
  int ret = __real_Tbegin();
  printf("ret_Tbegin(%lld)\n", (long long)stasis_timestamp);
  return ret;
}
void __wrap_Tupdate(int xid, pageid_t page, const void *dat, size_t datlen, int op) {
  printf("call_Tupdate(%lld, %d, %lld, %d)\n", (long long)stasis_timestamp, xid, page, op);
  __real_Tupdate(xid, page, dat, datlen, op);
  printf("ret_Tupdate(%lld, %d, %lld, %d)\n", (long long)stasis_timestamp, xid, page, op);
}
void __wrap_TupdateWithPage(int xid, pageid_t page, Page *p, const void *dat, size_t datlen, int op) {
  printf("call_Tupdate(%lld, %d, %lld, %d)\n", (long long)stasis_timestamp, xid, page, op);
  __real_TupdateWithPage(xid, page, p, dat, datlen, op);
  printf("ret_Tupdate(%lld, %d, %lld, %d)\n", (long long)stasis_timestamp, xid, page, op);
}
void __wrap_TupdateStr(int xid, pageid_t page,
                                     const char *dat, size_t datlen, int op) {
  printf("call_Tupdate(%lld, %d, %lld, %d)\n", (long long)stasis_timestamp, xid, page, op);
  __real_TupdateStr(xid, page, dat, datlen, op);
  printf("ret_Tupdate(%lld, %d, %lld, %d)\n", (long long)stasis_timestamp, xid, page, op);
}
void __wrap_TreorderableUpdate(int xid, void * h, pageid_t page,
                        const void * dat, size_t datlen, int op) {
  printf("call_TreorderableUpdate(%lld)\n", (long long)stasis_timestamp);
  __real_TreorderableUpdate(xid, h, page, dat, datlen, op);
  printf("ret_TreorderableUpdate(%lld)\n", (long long)stasis_timestamp);
}
lsn_t __wrap_TwritebackUpdate(int xid, pageid_t page,
                       const void * dat, size_t datlen, int op) {
  printf("call_TwritebackUpdate(%lld)\n", (long long)stasis_timestamp);
  lsn_t ret = __real_TwritebackUpdate(xid, page, dat, datlen, op);
  printf("ret_TwritebackUpdate(%lld)\n", (long long)stasis_timestamp);
  return ret;
}
void __wrap_TreorderableWritebackUpdate(int xid, void* h,
                                 pageid_t page, const void * dat,
                                 size_t datlen, int op) {
  printf("call_TreorderableWritebackUpdate(%lld)\n", (long long)stasis_timestamp);
  __real_TreorderableWritebackUpdate(xid, h, page, dat, datlen, op);
  printf("ret_TreorderableWritebackUpdate(%lld)\n", (long long)stasis_timestamp);
}
void __wrap_Tread(int xid, recordid rid, void *dat) {
  printf("call_Tread(%lld, %d, %lld, %d, %lld)\n", (long long)stasis_timestamp, xid, rid.page, rid.slot, (long long)rid.size);
  __real_Tread(xid, rid, dat);
  printf("ret_Tread(%lld, %d, %lld, %d, %lld)\n", (long long)stasis_timestamp, xid, rid.page, rid.slot, (long long)rid.size);
}
Page * __wrap_TreadWithPage(int xid, recordid rid, Page *p, void *dat) {
  printf("call_Tread(%lld, %d, %lld, %d, %lld)\n", (long long)stasis_timestamp, xid, rid.page, rid.slot, (long long)rid.size);
  Page * ret = __real_TreadWithPage(xid, rid, p, dat);
  printf("ret_Tread(%lld, %d, %lld, %d, %lld)\n", (long long)stasis_timestamp, xid, rid.page, rid.slot, (long long)rid.size);
  return ret;
}
void __wrap_TreadRaw(int xid, recordid rid, void *dat) {
  printf("call_Tread(%lld, %d, %lld, %d, %lld)\n", (long long)stasis_timestamp, xid, rid.page, rid.slot, (long long)rid.size);  // XXX due to interposition artifacts, this printf will rarely be called.
  __real_TreadRaw(xid, rid, dat);
  printf("ret_Tread(%lld, %d, %lld, %d, %lld)\n", (long long)stasis_timestamp, xid, rid.page, rid.slot, (long long)rid.size);
}
void __wrap_TreadStr(int xid, recordid rid, char *dat) {
  printf("call_Tread(%lld, %d, %lld, %d, %lld)\n", (long long)stasis_timestamp, xid, rid.page, rid.slot, (long long)rid.size);
  __real_TreadStr(xid, rid, dat);
  printf("ret_Tread(%lld, %d, %lld, %d, %lld)\n", (long long)stasis_timestamp, xid, rid.page, rid.slot, (long long)rid.size);
}
int __wrap_Tcommit(int xid) {
  printf("call_Tcommit(%lld, %d, true)\n", (long long)stasis_timestamp, xid);
  int ret = __real_Tcommit(xid);
  printf("ret_Tcommit(%lld, %d, true)\n", (long long)stasis_timestamp, xid);
  return ret;
}
int __wrap_TsoftCommit(int xid) {
  printf("call_Tcommit(%lld, %d, false)\n", (long long)stasis_timestamp, xid);
  int ret = __real_TsoftCommit(xid);
  printf("ret_Tcommit(%lld, %d, false)\n", (long long)stasis_timestamp, xid);
  return ret;
}
void __wrap_TforceCommits(void) {
  printf("call_TforceCommits(%lld)\n", (long long)stasis_timestamp);
  __real_TforceCommits();
  printf("ret_TforceCommits(%lld)\n", (long long)stasis_timestamp);
}
int __wrap_Tabort(int xid) {
  printf("call_Tabort(%lld, %d)\n", (long long)stasis_timestamp, xid);
  int ret = __real_Tabort(xid);
  printf("ret_Tabort(%lld, %d)\n", (long long)stasis_timestamp, xid);
  return ret;
}
int __wrap_Tdeinit(void) {
  printf("call_Tdeinit(%lld)\n", (long long)stasis_timestamp);
  int ret = __real_Tdeinit();
  printf("ret_Tdeinit(%lld)\n", (long long)stasis_timestamp);
  return ret;
}
int __wrap_TuncleanShutdown(void) {
  printf("call_TuncleanShutdown(%lld)\n", (long long)stasis_timestamp);
  int ret = __real_TuncleanShutdown();
  printf("ret_TuncleanShutdown(%lld)\n", (long long)stasis_timestamp);
  return ret;
}
//void __wrap_Trevive(int xid, lsn_t prevlsn, lsn_t reclsn) {
//  printf("call_Trevive(%lld, %d, %lld, %lld)\n", xid, prevlsn, reclsn);
//  __real_Trevive(xid, prevlsn, reclsn);
//  printf("ret_Trevive(%lld, %d, %lld, %lld)\n", xid, prevlsn, reclsn);
//}
int __wrap_Tprepare(int xid) {
  printf("call_Tprepare(%lld, %d)\n", (long long)stasis_timestamp, xid);
  int ret = __real_Tprepare(xid);
  printf("ret_Tprepare(%lld, %d)\n", (long long)stasis_timestamp, xid);
  return ret;
}
int __wrap_TnestedTopAction(int xid, int op, const byte * arg, size_t arg_len) {
  printf("call_TnestedTopAction(%lld, %d, %d)\n", (long long)stasis_timestamp, xid, op);
  int ret = __real_TnestedTopAction(xid, op, arg, arg_len);
  printf("ret_TnestedTopAction(%lld, %d, %d)\n", (long long)stasis_timestamp, xid, op);
  return ret;
}
void * __wrap_TbeginNestedTopAction(int xid, int op, const byte* log_arguments,
                             int log_arguments_length) {
  printf("call_TbeginNestedTopAction(%lld, %d, %d)\n", (long long)stasis_timestamp, xid, op);
  void * ret = __real_TbeginNestedTopAction(xid, op, log_arguments, log_arguments_length);
  printf("ret_TbeginNestedTopAction(%lld, %ld, %d, %d)\n", (long long)stasis_timestamp, (intptr_t)ret, xid, op);
  return ret;
}
lsn_t __wrap_TendNestedTopAction(int xid, void * handle) {
  printf("call_TendNestedTopAction(%lld, %d, %ld)\n", (long long)stasis_timestamp, xid, (intptr_t)handle);
  lsn_t lsn = __real_TendNestedTopAction(xid, handle);
  printf("ret_TendNestedTopAction(%lld, %lld, %d)\n", (long long)stasis_timestamp, lsn, xid);
  return lsn;
}
int __wrap_Tforget(int xid) {
  int ret = __real_Tforget(xid);
  return ret;
}


//// ------------------------------------------------
//// ----      Buffer Manager                     ---
//// ------------------------------------------------

// TODO scrape page LSN, dirty bits, etc.

Page*  __real_loadPage(int xid, pageid_t pageid);
Page * __real_loadPageOfType(int xid, pageid_t pageid, pagetype_t type);
Page * __real_loadUninitializedPage(int xid, pageid_t pageid);
Page * __real_loadPageForOperation(int xid, pageid_t pageid, int op, int is_recovery);
void   __real_releasePage(Page* p);
Page * __real_getCachedPage(int xid, pageid_t pageid);

#define CALL_LOADPAGE printf("call_loadPage(%lld, %d, %lld)\n", (long long)stasis_timestamp, (int)xid, (long long)pageid)
#define RET_LOADPAGE  printf("ret_loadPage(%lld, %ld, %d, %lld)\n", (long long)stasis_timestamp, (intptr_t)ret, (int)xid, (long long)pageid)
Page* __wrap_loadPage(int xid, pageid_t pageid) {
  CALL_LOADPAGE;
  Page * ret = __real_loadPage(xid, pageid);
  RET_LOADPAGE;
  return ret;
}
Page * __wrap_loadPageOfType(int xid, pageid_t pageid, pagetype_t type) {
  CALL_LOADPAGE;
  Page * ret = __real_loadPageOfType(xid, pageid, type);
  RET_LOADPAGE;
  return ret;
}
Page * __wrap_loadUninitializedPage(int xid, pageid_t pageid) {
  CALL_LOADPAGE;
  Page * ret = __real_loadUninitializedPage(xid, pageid);
  RET_LOADPAGE;
  return ret;
}
Page * __wrap_loadPageForOperation(int xid, pageid_t pageid, int op, int is_recovery) {
  CALL_LOADPAGE;
  Page * ret = __real_loadPageForOperation(xid, pageid, op, is_recovery);
  RET_LOADPAGE;
  return ret;
}
void   __wrap_releasePage(Page* p) {
  printf("call_releasePage(%lld, %ld)\n", (long long)stasis_timestamp, (intptr_t)p);
  __real_releasePage(p);
  printf("ret_releasePage(%lld, %ld)\n", (long long)stasis_timestamp, (intptr_t)p);
}
Page * __wrap_getCachedPage(int xid, pageid_t pageid) {
  CALL_LOADPAGE;
  Page * ret = __real_getCachedPage(xid, pageid);
  RET_LOADPAGE;
  return ret;
}

//// ------------------------------------------------
//// ----      Log                                ---
//// ------------------------------------------------

void __real_stasis_log_force(stasis_log_t* log, lsn_t lsn, stasis_log_force_mode_t mode);
void __real_stasis_log_begin_transaction(stasis_log_t* log, int xid, stasis_transaction_table_entry_t* l);
lsn_t __real_stasis_log_prepare_transaction(stasis_log_t* log, stasis_transaction_table_entry_t * l);
lsn_t __real_stasis_log_commit_transaction(stasis_log_t* log, stasis_transaction_table_t * tbl, stasis_transaction_table_entry_t * l, int force);
lsn_t __real_stasis_log_abort_transaction(stasis_log_t* log, stasis_transaction_table_t * tbl, stasis_transaction_table_entry_t * l);
lsn_t __real_stasis_log_end_aborted_transaction (stasis_log_t* log, stasis_transaction_table_t *tbl, stasis_transaction_table_entry_t * l);
LogEntry * __real_stasis_log_write_update(stasis_log_t* log,
                     stasis_transaction_table_entry_t * l, pageid_t page, unsigned int operation,
                     const byte * arg, size_t arg_size);
lsn_t __real_stasis_log_write_clr(stasis_log_t* log, const LogEntry * e);
lsn_t __real_stasis_log_write_dummy_clr(stasis_log_t* log, int xid, lsn_t prev_lsn);
void * __real_stasis_log_begin_nta(stasis_log_t* log, stasis_transaction_table_entry_t * l, unsigned int op,
                                const byte * arg, size_t arg_size);
lsn_t __real_stasis_log_end_nta(stasis_log_t* log, stasis_transaction_table_entry_t * l, LogEntry * e);

#define CALL_LOG(type) printf("call_log_%s(%lld, %d)\n", type, l->prevLSN,xid)
#define RET_LOG(type)  printf("ret_log_%s(%lld, %d)\n", type, lsn, xid)

void __wrap_stasis_log_force(stasis_log_t* log, lsn_t lsn, stasis_log_force_mode_t mode) {
  printf("call_log_force(%lld, %s, %lld, %lld, %lld, %lld)\n", lsn, mode == LOG_FORCE_COMMIT ? "commit" : mode == LOG_FORCE_WAL ? "wal" : "???", log->truncation_point(log), log->first_unstable_lsn(log, LOG_FORCE_COMMIT), log->first_unstable_lsn(log, LOG_FORCE_WAL), log->next_available_lsn(log));
  __real_stasis_log_force(log, lsn, mode);
  printf("ret_log_force(%lld, %s, %lld, %lld, %lld, %lld)\n", lsn, mode == LOG_FORCE_COMMIT ? "commit" : mode == LOG_FORCE_WAL ? "wal" : "???", log->truncation_point(log), log->first_unstable_lsn(log, LOG_FORCE_COMMIT), log->first_unstable_lsn(log, LOG_FORCE_WAL), log->next_available_lsn(log));
}
void __wrap_stasis_log_begin_transaction(stasis_log_t* log, int xid, stasis_transaction_table_entry_t* l) {
  CALL_LOG("begin");
  __real_stasis_log_begin_transaction(log, xid, l);
  lsn_t lsn = l->prevLSN;
  RET_LOG("begin");
}
lsn_t __wrap_stasis_log_prepare_transaction(stasis_log_t* log, stasis_transaction_table_entry_t * l) {
  int xid = l->xid;
  CALL_LOG("prepare");
  lsn_t lsn = __real_stasis_log_prepare_transaction(log, l);
  RET_LOG("prepare");
  return lsn;
}
lsn_t __wrap_stasis_log_commit_transaction(stasis_log_t* log, stasis_transaction_table_t * tbl, stasis_transaction_table_entry_t * l, int force) {
  int xid = l->xid;
  printf("call_log_%s(%lld, %d, %s, %lld, %lld, %lld, %lld)\n", "commit", l->prevLSN,xid,force ? "true" : "false", log->truncation_point(log), log->first_unstable_lsn(log, LOG_FORCE_COMMIT), log->first_unstable_lsn(log, LOG_FORCE_WAL), log->next_available_lsn(log));
  lsn_t lsn = __real_stasis_log_commit_transaction(log, tbl, l, force);
  printf("ret_log_%s(%lld, %d, %s, %lld, %lld, %lld, %lld)\n",  "commit", lsn,       xid,force ? "true" : "false", log->truncation_point(log), log->first_unstable_lsn(log, LOG_FORCE_COMMIT), log->first_unstable_lsn(log, LOG_FORCE_WAL), log->next_available_lsn(log));
  return lsn;
}
lsn_t __wrap_stasis_log_abort_transaction(stasis_log_t* log, stasis_transaction_table_t * tbl, stasis_transaction_table_entry_t * l) {
  int xid = l->xid;
  CALL_LOG("abort");
  lsn_t lsn = __real_stasis_log_abort_transaction(log, tbl, l);
  RET_LOG("abort");
  return lsn;
}
lsn_t __wrap_stasis_log_end_aborted_transaction (stasis_log_t* log, stasis_transaction_table_t *tbl, stasis_transaction_table_entry_t * l) {
  int xid = l->xid;
  CALL_LOG("end_aborted");
  lsn_t lsn = __real_stasis_log_end_aborted_transaction(log, tbl, l);
  RET_LOG("end_aborted");
  return lsn;
}
LogEntry * __wrap_stasis_log_write_update(stasis_log_t* log,
                     stasis_transaction_table_entry_t * l, pageid_t page, unsigned int operation,
                     const byte * arg, size_t arg_size) {
  // TODO extract page, rid, etc...
  int xid = l->xid;
  CALL_LOG("update");
  LogEntry * e = __real_stasis_log_write_update(log, l, page, operation, arg, arg_size);
  lsn_t lsn = e->LSN;
  RET_LOG("update");
  return e;
}
lsn_t __wrap_stasis_log_write_clr(stasis_log_t* log, const LogEntry * e) {
  int xid = e->xid;
  // XXX unlike all other entries int this file, we don't know the prevLSN here.
  printf("call_log_%s(%d, %lld)\n", "clr", xid, e->prevLSN);  // Last parameter is the prevLSN of the compensated entry (which should be the new prevLSN of the transaction)
  lsn_t lsn = __real_stasis_log_write_clr(log, e);
  RET_LOG("clr");
  return lsn;
}
lsn_t __wrap_stasis_log_write_dummy_clr(stasis_log_t* log, int xid, lsn_t prev_lsn) {
  return __real_stasis_log_write_dummy_clr(log, xid, prev_lsn);
}
void * __wrap_stasis_log_begin_nta(stasis_log_t* log, stasis_transaction_table_entry_t * l, unsigned int op,
                                const byte * arg, size_t arg_size) {
  int xid = l->xid;
  CALL_LOG("begin_nta");
  void * ret = __real_stasis_log_begin_nta(log, l, op, arg, arg_size);
  lsn_t lsn = l->prevLSN;
  printf("ret_log_%s(%ld, %lld, %d)\n", "begin_nta", (intptr_t) ret, lsn, xid);
  return ret;
}
lsn_t __wrap_stasis_log_end_nta(stasis_log_t* log, stasis_transaction_table_entry_t * l, LogEntry * e) {
  int xid = l->xid;
  printf("call_log_%s(%lld, %d, %ld)\n", "end_nta", l->prevLSN, xid, (intptr_t)e);
  lsn_t lsn = __real_stasis_log_end_nta(log, l, e);
  RET_LOG("end_nta");
  return lsn;

}



