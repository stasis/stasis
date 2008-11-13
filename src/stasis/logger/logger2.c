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

#include <stasis/logger/logWriter.h>
#include <stasis/logger/inMemoryLog.h>
#include <stasis/page.h>

#ifdef USE_LOGGER
int loggerType = USE_LOGGER;
#else
int loggerType = LOG_TO_FILE;
#endif

static int pendingCommits;
static int syncLogCount;

long LoggerSizeOfInternalLogEntry(const LogEntry * e) {
  if(loggerType == LOG_TO_FILE) { 
    return sizeofInternalLogEntry_LogWriter(e);
  } else if (loggerType == LOG_TO_MEMORY) {
    return sizeofInternalLogEntry_InMemoryLog(e);   
  } else {
    // we dont have an appropriate implementation, or weren't initialized...
    abort();
  }
}

void LogWrite(LogEntry * e) { 
  if(loggerType == LOG_TO_FILE) { 
    writeLogEntry(e);
  } else if (loggerType == LOG_TO_MEMORY) {
    writeLogEntry_InMemoryLog(e);
  } else { 
    abort();
  }
  return;
}

int LogInit(int logType) { 

  loggerType = logType;

  pendingCommits = 0;
  syncLogCount = 0;
  if(LOG_TO_FILE == logType) { 
    openLogWriter();
  } else if(LOG_TO_MEMORY == logType) { 
    open_InMemoryLog();
  } else { 
    return -1;
  }
  return 0;
}

int LogDeinit() { 
  if(LOG_TO_FILE == loggerType) { 
    closeLogWriter();
  } else if(LOG_TO_MEMORY == loggerType) { 
    close_InMemoryLog();
  } else { 
    abort();
  }
  return 0;
}

void LogForce(lsn_t lsn) { 
  lsn_t flushedLSN = LogFlushedLSN();
  if(flushedLSN < lsn) {
    if(LOG_TO_FILE == loggerType) { 
      syncLog_LogWriter();
    } else if (LOG_TO_MEMORY == loggerType) { 
      syncLog_InMemoryLog();
    } else { 
      abort();
    }
  }
  assert(LogFlushedLSN() >= lsn);
}
void LogTruncate(lsn_t lsn) { 
  if(LOG_TO_FILE == loggerType) { 
    truncateLog_LogWriter(lsn);
  } else if(LOG_TO_MEMORY == loggerType) { 
    truncateLog_InMemoryLog(lsn);
  } else { 
    abort();
  }
}

lsn_t LogFlushedLSN() { 
  lsn_t ret;
  if(LOG_TO_FILE == loggerType) { 
    ret = flushedLSN_LogWriter();
  } else if(LOG_TO_MEMORY == loggerType) { 
    ret = flushedLSN_InMemoryLog();
  } else {
    abort();
  }
  return ret;
}

lsn_t LogTruncationPoint() { 
  lsn_t ret;
  if(LOG_TO_FILE == loggerType) { 
    ret =  firstLogEntry();
  } else if(LOG_TO_MEMORY == loggerType) { 
    ret = firstLogEntry_InMemoryLog();
  } else { 
    abort();
  }
  return ret;
}
const LogEntry * LogReadLSN(lsn_t lsn) { 
  LogEntry * ret;
  if(LOG_TO_FILE == loggerType) { 
    ret = readLSNEntry_LogWriter(lsn);
  } else if(LOG_TO_MEMORY == loggerType) { 
    ret = readLSNEntry_InMemoryLog(lsn);
  } else {
    abort();
  }
  return ret;
}

lsn_t LogNextEntry(const LogEntry * e) { 
  lsn_t ret;
  if(LOG_TO_FILE == loggerType) { 
    ret = nextEntry_LogWriter(e);
  } else if(LOG_TO_MEMORY == loggerType) { 
    ret = nextEntry_InMemoryLog(e);
  } else {
    abort();
  }
  return ret;
}

void FreeLogEntry(const LogEntry * e) { 
  if(LOG_TO_FILE == loggerType) { 
    free((void*)e);
  } else if(LOG_TO_MEMORY == loggerType) {
    free((void*)e);
  } else { 
    abort();
  }

}

TransactionLog LogTransBegin(int xid) {
  TransactionLog tl;
  tl.xid = xid;
  
  DEBUG("Log Begin %d\n", xid);
  tl.prevLSN = -1;
  tl.recLSN = -1;
  return tl;
}

static lsn_t LogTransCommon(TransactionLog * l, int type) {
  LogEntry * e = allocCommonLogEntry(l->prevLSN, l->xid, type);
  lsn_t ret;

  LogWrite(e);

  if(l->prevLSN == -1) { l->recLSN = e->LSN; }
  l->prevLSN = e->LSN;
  DEBUG("Log Common %d, LSN: %ld type: %ld (prevLSN %ld)\n", e->xid, 
	 (long int)e->LSN, (long int)e->type, (long int)e->prevLSN);

  ret = e->LSN;

  FreeLogEntry(e);

  return ret;

}
static lsn_t LogTransCommonPrepare(TransactionLog * l) {
  LogEntry * e = allocPrepareLogEntry(l->prevLSN, l->xid, l->recLSN);
  lsn_t ret;

  DEBUG("Log prepare xid = %d prevlsn = %lld reclsn = %lld, %lld\n",e->xid,e->prevLSN,l->recLSN, getPrepareRecLSN(e));
  LogWrite(e);

  if(l->prevLSN == -1) { l->recLSN = e->LSN; }
  l->prevLSN = e->LSN;
  DEBUG("Log Common prepare XXX %d, LSN: %ld type: %ld (prevLSN %ld)\n", e->xid, 
	 (long int)e->LSN, (long int)e->type, (long int)e->prevLSN);

  ret = e->LSN;

  FreeLogEntry(e);

  return ret;

}

static void groupForce(lsn_t l) {
  static pthread_mutex_t check_commit = PTHREAD_MUTEX_INITIALIZER;
  static pthread_cond_t tooFewXacts = PTHREAD_COND_INITIALIZER;

  struct timeval now;
  struct timespec timeout;
  
  pthread_mutex_lock(&check_commit);
  if(LogFlushedLSN() >= l) {
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
  if((xactcount > 1 && pendingCommits < xactcount) ||
     (xactcount > 20 && pendingCommits < (int)((double)xactcount * 0.95))) {
    int retcode;
    while(ETIMEDOUT != (retcode = pthread_cond_timedwait(&tooFewXacts, &check_commit, &timeout))) { 
      if(retcode != 0) { 
	printf("Warning: %s:%d: pthread_cond_timedwait was interrupted by a signal in groupCommit().  Acting as though it timed out.\n", __FILE__, __LINE__);
	break;
      }
      if(LogFlushedLSN() >= l) {
	pendingCommits--;
	pthread_mutex_unlock(&check_commit);
	return;
      }
    }
  } 
  if(LogFlushedLSN() < l) {
    syncLog_LogWriter();
    syncLogCount++;
    pthread_cond_broadcast(&tooFewXacts);
  }
  assert(LogFlushedLSN() >= l);
  pendingCommits--;
  pthread_mutex_unlock(&check_commit);
  return;
}

static lsn_t groupCommit(TransactionLog * l) {
  lsn_t ret = LogTransCommon(l, XCOMMIT);
  groupForce(ret);
  return ret;
}
static lsn_t groupPrepare(TransactionLog * l) {
  lsn_t ret = LogTransCommonPrepare(l);
  groupForce(ret);
  return ret;
}

lsn_t LogTransCommit(TransactionLog * l) { 
  return groupCommit(l);
}

lsn_t LogTransAbort(TransactionLog * l) {
  return LogTransCommon(l, XABORT);
}
lsn_t LogTransPrepare(TransactionLog * l) {
  return groupPrepare(l);
}

LogEntry * LogUpdate(TransactionLog * l, Page * p, unsigned int op,
		     const byte * arg, size_t arg_size) {

  LogEntry * e = allocUpdateLogEntry(l->prevLSN, l->xid, op,
                                     p ? p->id : INVALID_PAGE,
                                     arg, arg_size);

  LogWrite(e);
  DEBUG("Log Update %d, LSN: %ld type: %ld (prevLSN %ld) (arg_size %ld)\n", e->xid, 
	 (long int)e->LSN, (long int)e->type, (long int)e->prevLSN, (long int) arg_size);

  if(l->prevLSN == -1) { l->recLSN = e->LSN; }
  l->prevLSN = e->LSN;
  return e;
}

lsn_t LogCLR(const LogEntry * old_e) { 
  LogEntry * e = allocCLRLogEntry(old_e);
  LogWrite(e);

  DEBUG("Log CLR %d, LSN: %ld (undoing: %ld, next to undo: %ld)\n", xid, 
  	 e->LSN, LSN, prevLSN);
  lsn_t ret = e->LSN;
  FreeLogEntry(e);

  return ret;
}

lsn_t LogDummyCLR(int xid, lsn_t prevLSN, lsn_t compensatedLSN) {
  LogEntry * e = allocUpdateLogEntry(prevLSN, xid, OPERATION_NOOP,
                                     INVALID_PAGE, NULL, 0);
  e->LSN = compensatedLSN;
  lsn_t ret = LogCLR(e);
  FreeLogEntry(e);
  return ret;
}
