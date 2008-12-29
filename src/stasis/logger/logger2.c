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

#include <stdio.h>
#include <assert.h>

#include <stasis/logger/logger2.h>

#include <stasis/logger/safeWrites.h>
#include <stasis/logger/inMemoryLog.h>
#include <stasis/page.h>

/**
   @todo loggerType should go away.
 */
#ifdef USE_LOGGER
int loggerType = USE_LOGGER;
#else
int loggerType = LOG_TO_FILE;
#endif

/**
   @todo stasis_log_file should be in transactional2.c, and not global
 */
stasis_log_t* stasis_log_file = 0;

static int pendingCommits;

TransactionLog LogTransBegin(stasis_log_t* log, int xid) {
  TransactionLog tl;
  tl.xid = xid;

  DEBUG("Log Begin %d\n", xid);
  tl.prevLSN = -1;
  tl.recLSN = -1;
  return tl;
}

static lsn_t LogTransCommon(stasis_log_t* log, TransactionLog * l, int type) {
  LogEntry * e = allocCommonLogEntry(l->prevLSN, l->xid, type);
  lsn_t ret;

  log->write_entry(log, e);

  if(l->prevLSN == -1) { l->recLSN = e->LSN; }
  l->prevLSN = e->LSN;
  DEBUG("Log Common %d, LSN: %ld type: %ld (prevLSN %ld)\n", e->xid, 
	 (long int)e->LSN, (long int)e->type, (long int)e->prevLSN);

  ret = e->LSN;

  freeLogEntry(e);

  return ret;

}
static lsn_t LogTransCommonPrepare(stasis_log_t* log, TransactionLog * l) {
  LogEntry * e = allocPrepareLogEntry(l->prevLSN, l->xid, l->recLSN);
  lsn_t ret;

  DEBUG("Log prepare xid = %d prevlsn = %lld reclsn = %lld, %lld\n",
        e->xid, e->prevLSN, l->recLSN, getPrepareRecLSN(e));
  log->write_entry(log, e);

  if(l->prevLSN == -1) { l->recLSN = e->LSN; }
  l->prevLSN = e->LSN;
  DEBUG("Log Common prepare XXX %d, LSN: %ld type: %ld (prevLSN %ld)\n",
        e->xid, (long int)e->LSN, (long int)e->type, (long int)e->prevLSN);

  ret = e->LSN;

  freeLogEntry(e);

  return ret;

}

LogEntry * LogUpdate(stasis_log_t* log, TransactionLog * l,
                     Page * p, unsigned int op,
		     const byte * arg, size_t arg_size) {

  LogEntry * e = allocUpdateLogEntry(l->prevLSN, l->xid, op,
                                     p ? p->id : INVALID_PAGE,
                                     arg, arg_size);

  log->write_entry(log, e);
  DEBUG("Log Update %d, LSN: %ld type: %ld (prevLSN %ld) (arg_size %ld)\n", e->xid, 
	 (long int)e->LSN, (long int)e->type, (long int)e->prevLSN, (long int) arg_size);

  if(l->prevLSN == -1) { l->recLSN = e->LSN; }
  l->prevLSN = e->LSN;
  return e;
}

lsn_t LogCLR(stasis_log_t* log, const LogEntry * old_e) { 
  LogEntry * e = allocCLRLogEntry(old_e);
  log->write_entry(log, e);

  DEBUG("Log CLR %d, LSN: %ld (undoing: %ld, next to undo: %ld)\n", xid, 
  	 e->LSN, LSN, prevLSN);
  lsn_t ret = e->LSN;
  freeLogEntry(e);

  return ret;
}

lsn_t LogDummyCLR(stasis_log_t* log, int xid, lsn_t prevLSN,
                  lsn_t compensatedLSN) {
  LogEntry * e = allocUpdateLogEntry(prevLSN, xid, OPERATION_NOOP,
                                     INVALID_PAGE, NULL, 0);
  e->LSN = compensatedLSN;
  lsn_t ret = LogCLR(log, e);
  freeLogEntry(e);
  return ret;
}

lsn_t LogTransCommit(stasis_log_t* log, TransactionLog * l) {
  lsn_t lsn = LogTransCommon(log, l, XCOMMIT);
  LogForce(log, lsn, LOG_FORCE_COMMIT);
  return lsn;
}

lsn_t LogTransAbort(stasis_log_t* log, TransactionLog * l) {
  return LogTransCommon(log, l, XABORT);
}
lsn_t LogTransPrepare(stasis_log_t* log, TransactionLog * l) {
  lsn_t lsn = LogTransCommonPrepare(log, l);
  LogForce(log, lsn, LOG_FORCE_COMMIT);
  return lsn;
}

static void groupCommit(stasis_log_t* log, lsn_t lsn);

void LogForce(stasis_log_t* log, lsn_t lsn,
              stasis_log_force_mode_t mode) {
  if(mode == LOG_FORCE_COMMIT) {
    groupCommit(log, lsn);
  } else {
    if(log->first_unstable_lsn(log,mode) <= lsn) {
      log->force_tail(log,mode);
    }
  }
}

static void groupCommit(stasis_log_t* log, lsn_t lsn) {
  static pthread_mutex_t check_commit = PTHREAD_MUTEX_INITIALIZER;
  static pthread_cond_t tooFewXacts = PTHREAD_COND_INITIALIZER;

  struct timeval now;
  struct timespec timeout;

  pthread_mutex_lock(&check_commit);
  if(log->first_unstable_lsn(log,LOG_FORCE_COMMIT) > lsn) {
    pthread_mutex_unlock(&check_commit);
    return;
  }
  gettimeofday(&now, NULL);
  timeout.tv_sec = now.tv_sec;
  timeout.tv_nsec = now.tv_usec * 1000;
  //                   0123456789  <- number of zeros on the next three lines...
  timeout.tv_nsec +=   100000000; // wait ten msec.
  if(timeout.tv_nsec > 1000000000) {
    timeout.tv_nsec -= 1000000000;
    timeout.tv_sec++;
  }

  pendingCommits++;
  int xactcount = TactiveTransactionCount();
  if((log->is_durable(log) && xactcount > 1 && pendingCommits < xactcount) ||
     (xactcount > 20 && pendingCommits < (int)((double)xactcount * 0.95))) {
    int retcode;
    while(ETIMEDOUT != (retcode = pthread_cond_timedwait(&tooFewXacts, &check_commit, &timeout))) { 
      if(retcode != 0) { 
	printf("Warning: %s:%d: pthread_cond_timedwait was interrupted by "
               "a signal in groupCommit().  Acting as though it timed out.\n",
               __FILE__, __LINE__);
	break;
      }
      if(log->first_unstable_lsn(log,LOG_FORCE_COMMIT) > lsn) {
	pendingCommits--;
	pthread_mutex_unlock(&check_commit);
	return;
      }
    }
  }
  if(log->first_unstable_lsn(log,LOG_FORCE_COMMIT) <= lsn) {
    log->force_tail(log, LOG_FORCE_COMMIT);
    assert(log->first_unstable_lsn(log,LOG_FORCE_COMMIT) > lsn);
    pthread_cond_broadcast(&tooFewXacts);
  }
  assert(log->first_unstable_lsn(log,LOG_FORCE_COMMIT) > lsn);
  pendingCommits--;
  pthread_mutex_unlock(&check_commit);
  return;
}
