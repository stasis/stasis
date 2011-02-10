/**
 * This file is a hack that allows -Wl,--wrap to work without conditional compilation.
 *
 * It defines empty declarations of __real_foo() functions that will never be resolved
 * when the __wrap_foo()'s are called.  If this doesn't make any sense, read ld(1)'s
 * coverage of the --wrap argument.
 *
 *  Created on: Feb 1, 2011
 *      Author: sears
 */
#include <assert.h>
#include <stasis/bufferManager.h>
#include <stasis/logger/logger2.h>

static int not_called = 0;

#define LINKER_STUB { assert(not_called); abort(); }

int __real_Tinit(void)LINKER_STUB
int __real_Tbegin(void)LINKER_STUB
void __real_Tupdate(int xid, pageid_t page, const void *dat, size_t datlen, int op)LINKER_STUB
void __real_TupdateWithPage(int xid, pageid_t page, Page *p, const void *dat, size_t datlen, int op)LINKER_STUB
void __real_TupdateStr(int xid, pageid_t page,
                                     const char *dat, size_t datlen, int op)LINKER_STUB
void __real_TreorderableUpdate(int xid, void * h, pageid_t page,
                        const void * dat, size_t datlen, int op)LINKER_STUB
lsn_t __real_TwritebackUpdate(int xid, pageid_t page,
                       const void * dat, size_t datlen, int op)LINKER_STUB
void __real_TreorderableWritebackUpdate(int xid, void* h,
                                 pageid_t page, const void * dat,
                                 size_t datlen, int op)LINKER_STUB
compensated_function void __real_Tread(int xid, recordid rid, void *dat)LINKER_STUB
Page * __real_TreadWithPage(int xid, recordid rid, Page *p, void *dat)LINKER_STUB
compensated_function void __real_TreadRaw(int xid, recordid rid, void *dat)LINKER_STUB
compensated_function void __real_TreadStr(int xid, recordid rid, char *dat)LINKER_STUB
int __real_Tcommit(int xid)LINKER_STUB
int __real_TsoftCommit(int xid)LINKER_STUB
void __real_TforceCommits(void)LINKER_STUB
int __real_Tabort(int xid)LINKER_STUB
int __real_Tdeinit(void)LINKER_STUB
int __real_TuncleanShutdown(void)LINKER_STUB
//void __real_Trevive(int xid, lsn_t prevlsn, lsn_t reclsn)LINKER_STUB
int __real_Tprepare(int xid)LINKER_STUB
int __real_TnestedTopAction(int xid, int op, const byte * arg, size_t arg_len)LINKER_STUB
void * __real_TbeginNestedTopAction(int xid, int op, const byte* log_arguments,
                             int log_arguments_length)LINKER_STUB
lsn_t __real_TendNestedTopAction(int xid, void * handle)LINKER_STUB
int __real_Tforget(int xid)LINKER_STUB


Page*  __real_loadPage(int xid, pageid_t pageid)                     LINKER_STUB
Page * __real_loadPageOfType(int xid, pageid_t pageid, pagetype_t type)
                                                                     LINKER_STUB
Page * __real_loadUninitializedPage(int xid, pageid_t pageid)        LINKER_STUB
Page * __real_loadPageForOperation(int xid, pageid_t pageid, int op, int is_recovery)
                                                                     LINKER_STUB
void   __real_releasePage(Page* p)                                   LINKER_STUB
Page * __real_getCachedPage(int xid, pageid_t pageid)                LINKER_STUB

void __real_stasis_log_force(stasis_log_t* log, lsn_t lsn, stasis_log_force_mode_t mode) LINKER_STUB
void __real_stasis_log_begin_transaction(stasis_log_t* log, int xid, stasis_transaction_table_entry_t* l) LINKER_STUB
lsn_t __real_stasis_log_prepare_transaction(stasis_log_t* log, stasis_transaction_table_entry_t * l) LINKER_STUB
lsn_t __real_stasis_log_commit_transaction(stasis_log_t* log, stasis_transaction_table_t * tbl, stasis_transaction_table_entry_t * l, int force) LINKER_STUB
lsn_t __real_stasis_log_abort_transaction(stasis_log_t* log, stasis_transaction_table_t * tbl, stasis_transaction_table_entry_t * l) LINKER_STUB
lsn_t __real_stasis_log_end_aborted_transaction (stasis_log_t* log, stasis_transaction_table_t *tbl, stasis_transaction_table_entry_t * l) LINKER_STUB
LogEntry * __real_stasis_log_write_update(stasis_log_t* log,
                     stasis_transaction_table_entry_t * l, pageid_t page, unsigned int operation,
                     const byte * arg, size_t arg_size) LINKER_STUB
lsn_t __real_stasis_log_write_clr(stasis_log_t* log, const LogEntry * e) LINKER_STUB
lsn_t __real_stasis_log_write_dummy_clr(stasis_log_t* log, int xid, lsn_t prev_lsn) LINKER_STUB
void * __real_stasis_log_begin_nta(stasis_log_t* log, stasis_transaction_table_entry_t * l, unsigned int op,
                                const byte * arg, size_t arg_size) LINKER_STUB
lsn_t __real_stasis_log_end_nta(stasis_log_t* log, stasis_transaction_table_entry_t * l, LogEntry * e) LINKER_STUB
