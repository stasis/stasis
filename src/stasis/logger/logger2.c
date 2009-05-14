/*---
This software is copyrighted by the Regents of the University of
California, and other parties. The following terms apply to all files
associated with the software unless explicitly disclaimed in
individual files.

The authors hereby grant permission to use, copy, modify, distribute,
and license this software and its documentation for any purpose,
provided that existing copyright notices are retained in all copies
and that this notice is included verbatim in any distributions. No
written agreement, license, or royalty fee is required for any of the
authorized uses. Modifications to this software may be copyrighted by
their authors and need not follow the licensing terms described here,
provided that the new terms are clearly indicated on the first page of
each file where they apply.

IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY
FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES
ARISING OUT OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY
DERIVATIVES THEREOF, EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
NON-INFRINGEMENT. THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, AND
THE AUTHORS AND DISTRIBUTORS HAVE NO OBLIGATION TO PROVIDE
MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

GOVERNMENT USE: If you are acquiring this software on behalf of the
U.S. government, the Government shall have only "Restricted Rights" in
the software and related documentation as defined in the Federal
Acquisition Regulations (FARs) in Clause 52.227.19 (c) (2). If you are
acquiring the software on behalf of the Department of Defense, the
software shall be classified as "Commercial Computer Software" and the
Government shall have only "Restricted Rights" as defined in Clause
252.227-7013 (c) (1) of DFARs. Notwithstanding the foregoing, the
authors grant the U.S. Government and others acting in its behalf
permission to use and distribute the software in accordance with the
terms specified in this license.
---*/

/**
   @file

   Abstract log implementation.  Provides access to methods that
   directly read and write log entries, force the log to disk, etc.

   @todo Switch logger2 to use function pointers
*/

#include <config.h>
#include <stasis/common.h>

#include <stasis/logger/logger2.h>

#include <stasis/logger/safeWrites.h>
#include <stasis/logger/inMemoryLog.h>
#include <stasis/page.h>

static lsn_t stasis_log_write_common(stasis_log_t* log, TransactionLog * l, int type) {
  LogEntry * e = allocCommonLogEntry(l->prevLSN, l->xid, type);
  lsn_t ret;

  log->write_entry(log, e);

  pthread_mutex_lock(&l->mut);
  if(l->prevLSN == -1) { l->recLSN = e->LSN; }
  l->prevLSN = e->LSN;
  pthread_mutex_unlock(&l->mut);

  DEBUG("Log Common %d, LSN: %ld type: %ld (prevLSN %ld)\n", e->xid,
	 (long int)e->LSN, (long int)e->type, (long int)e->prevLSN);

  ret = e->LSN;

  freeLogEntry(e);

  return ret;
}

static lsn_t stasis_log_write_prepare(stasis_log_t* log, TransactionLog * l) {
  LogEntry * e = allocPrepareLogEntry(l->prevLSN, l->xid, l->recLSN);
  lsn_t ret;

  DEBUG("Log prepare xid = %d prevlsn = %lld reclsn = %lld, %lld\n",
        e->xid, e->prevLSN, l->recLSN, getPrepareRecLSN(e));
  log->write_entry(log, e);

  pthread_mutex_lock(&l->mut);
  if(l->prevLSN == -1) { l->recLSN = e->LSN; }
  l->prevLSN = e->LSN;
  pthread_mutex_unlock(&l->mut);
  DEBUG("Log Common prepare XXX %d, LSN: %ld type: %ld (prevLSN %ld)\n",
        e->xid, (long int)e->LSN, (long int)e->type, (long int)e->prevLSN);

  ret = e->LSN;

  freeLogEntry(e);

  return ret;

}

LogEntry * stasis_log_write_update(stasis_log_t* log, TransactionLog * l,
                     Page * p, unsigned int op,
		     const byte * arg, size_t arg_size) {

  LogEntry * e = allocUpdateLogEntry(l->prevLSN, l->xid, op,
                                     p ? p->id : INVALID_PAGE,
                                     arg, arg_size);

  log->write_entry(log, e);
  DEBUG("Log Update %d, LSN: %ld type: %ld (prevLSN %ld) (arg_size %ld)\n", e->xid,
	 (long int)e->LSN, (long int)e->type, (long int)e->prevLSN, (long int) arg_size);
  pthread_mutex_lock(&l->mut);
  if(l->prevLSN == -1) { l->recLSN = e->LSN; }
  l->prevLSN = e->LSN;
  pthread_mutex_unlock(&l->mut);
  return e;
}

LogEntry * stasis_log_begin_nta(stasis_log_t* log, TransactionLog * l, unsigned int op,
                                const byte * arg, size_t arg_size) {
  LogEntry * e = allocUpdateLogEntry(l->prevLSN, l->xid, op, INVALID_PAGE, arg, arg_size);
  return e;
}
lsn_t stasis_log_end_nta(stasis_log_t* log, TransactionLog * l, LogEntry * e) {
  log->write_entry(log, e);
  pthread_mutex_lock(&l->mut);
  if(l->prevLSN == -1) { l->recLSN = e->LSN; }
  lsn_t ret = l->prevLSN = e->LSN;
  pthread_mutex_unlock(&l->mut);
  freeLogEntry(e);
  return ret;
}

lsn_t stasis_log_write_clr(stasis_log_t* log, const LogEntry * old_e) {
  LogEntry * e = allocCLRLogEntry(old_e);
  log->write_entry(log, e);

  DEBUG("Log CLR %d, LSN: %ld (undoing: %ld, next to undo: %ld)\n", xid,
  	 e->LSN, LSN, prevLSN);
  lsn_t ret = e->LSN;
  freeLogEntry(e);

  return ret;
}

lsn_t stasis_log_write_dummy_clr(stasis_log_t* log, int xid, lsn_t prevLSN) {
  // XXX waste of log bandwidth.
  const LogEntry * e = allocUpdateLogEntry(prevLSN, xid, OPERATION_NOOP,
              INVALID_PAGE, NULL, 0);
  lsn_t ret = stasis_log_write_clr(log, e);
  freeLogEntry(e);
  return ret;
}

void stasis_log_begin_transaction(stasis_log_t* log, int xid, TransactionLog* tl) {
  tl->xid = xid;

  DEBUG("Log Begin %d\n", xid);
  tl->prevLSN = -1;
  tl->recLSN = -1;
}

lsn_t stasis_log_abort_transaction(stasis_log_t* log, TransactionLog * l) {
  return stasis_log_write_common(log, l, XABORT);
}
lsn_t stasis_log_end_aborted_transaction(stasis_log_t* log, TransactionLog * l) {
	return stasis_log_write_common(log, l, XEND);
}
lsn_t stasis_log_prepare_transaction(stasis_log_t* log, TransactionLog * l) {
  lsn_t lsn = stasis_log_write_prepare(log, l);
  stasis_log_force(log, lsn, LOG_FORCE_COMMIT);
  return lsn;
}


lsn_t stasis_log_commit_transaction(stasis_log_t* log, TransactionLog * l) {
  lsn_t lsn = stasis_log_write_common(log, l, XCOMMIT);
  stasis_log_force(log, lsn, LOG_FORCE_COMMIT);
  return lsn;
}

void stasis_log_force(stasis_log_t* log, lsn_t lsn,
              stasis_log_force_mode_t mode) {
  if((mode == LOG_FORCE_COMMIT) && log->group_force) {
    stasis_log_group_force(log->group_force, lsn);
  } else {
    if(log->first_unstable_lsn(log,mode) <= lsn) {
      log->force_tail(log,mode);
    }
  }
}
