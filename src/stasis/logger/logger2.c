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
   @file Abstract log implementation.  Provides access to methods that
   directly read and write log entries, force the log to disk, etc.

   @todo Switch logger2 to use function pointers
*/

#include <config.h>
#include <stasis/common.h>

#include <stdio.h>
#include <assert.h>

#include <stasis/logger/logger2.h>

#include "logWriter.h"
#include "inMemoryLog.h"
#include "page.h"  

#ifdef USE_LOGGER
int loggerType = USE_LOGGER;
#else
int loggerType = LOG_TO_FILE;
#endif

extern int numActiveXactions;
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

/**
   @todo This should be usable by all calls that sync the log; not just commit.
*/
static lsn_t groupCommit(TransactionLog * l) {
  static pthread_mutex_t check_commit = PTHREAD_MUTEX_INITIALIZER;
  static pthread_cond_t tooFewXacts = PTHREAD_COND_INITIALIZER;

  lsn_t ret = LogTransCommon(l, XCOMMIT);

  struct timeval now;
  struct timespec timeout;
  
  pthread_mutex_lock(&check_commit);
  if(LogFlushedLSN() >= ret) {
    pthread_mutex_unlock(&check_commit);
    return ret;
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
  if((numActiveXactions > 1 && pendingCommits < numActiveXactions) ||
     (numActiveXactions > 20 && pendingCommits < (int)((double)numActiveXactions * 0.95))) {
    int retcode;
    while(ETIMEDOUT != (retcode = pthread_cond_timedwait(&tooFewXacts, &check_commit, &timeout))) { 
      if(retcode != 0) { 
	printf("Warning: %s:%d: pthread_cond_timedwait was interrupted by a signal in groupCommit().  Acting as though it timed out.\n", __FILE__, __LINE__);
	break;
      }
      if(LogFlushedLSN() >= ret) {
	pendingCommits--;
	pthread_mutex_unlock(&check_commit);
	return ret;
      }
    }
  } 
  if(LogFlushedLSN() < ret) {
    syncLog_LogWriter();
    syncLogCount++;
    pthread_cond_broadcast(&tooFewXacts);
  }
  assert(LogFlushedLSN() >= ret);
  pendingCommits--;
  pthread_mutex_unlock(&check_commit);
  return ret;
}

lsn_t LogTransCommit(TransactionLog * l) { 
  return groupCommit(l);
}

lsn_t LogTransAbort(TransactionLog * l) {
  return LogTransCommon(l, XABORT);
}


/** 
    @todo Does the handling of operation types / argument sizes belong
    here?  Shouldn't it be in logEntry.c, or perhaps with other code
    that reasons about the various operation types?
*/
static LogEntry * LogAction(TransactionLog * l, Page * p, recordid rid, int operation,
		     const byte * args, int deferred) {
  void * preImage = NULL;
  long argSize  = 0;
  LogEntry * e;


  argSize = operationsTable[operation].sizeofData;

  if(argSize == SIZEOF_RECORD) argSize = physical_slot_length(rid.size);
  if(argSize == SIZEIS_PAGEID) argSize = rid.page;

  int undoType = operationsTable[operation].undo;
  
  if(undoType == NO_INVERSE) {
    DEBUG("Creating %ld byte physical pre-image.\n", physical_slot_length(rid.size));

    preImage = malloc(physical_slot_length(rid.size));
    recordRead(l->xid, p, rid, preImage);
  } else if (undoType == NO_INVERSE_WHOLE_PAGE) {
    DEBUG("Logging entire page\n");

    preImage = malloc(PAGE_SIZE);
    memcpy(preImage, p->memAddr, PAGE_SIZE);
  } else { 
    DEBUG("No pre-image");
  }
  
  if(!deferred) { 
    e = allocUpdateLogEntry(l->prevLSN, l->xid, operation, rid, args, argSize, 
			    preImage);
  } else { 
    e = allocDeferredLogEntry(l->prevLSN, l->xid, operation, rid, args, argSize,
			      preImage);
  }
  
  LogWrite(e);
  DEBUG("Log Update %d, LSN: %ld type: %ld (prevLSN %ld) (argSize %ld)\n", e->xid, 
	 (long int)e->LSN, (long int)e->type, (long int)e->prevLSN, (long int) argSize);

  if(preImage) {
    free(preImage);
  }
  if(l->prevLSN == -1) { l->recLSN = e->LSN; }
  l->prevLSN = e->LSN;
  return e;
}

LogEntry * LogUpdate(TransactionLog * l, Page * p, recordid rid, int operation,
		     const byte * args) {
  return LogAction(l, p, rid, operation, args, 0); // 0 -> not deferred
}
LogEntry * LogDeferred(TransactionLog * l, Page * p, recordid rid, int operation,
		     const byte * args) {
  return LogAction(l, p, rid, operation, args, 1); // 1 -> deferred
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

lsn_t LogDummyCLR(int xid, lsn_t prevLSN) { 
  LogEntry * e = allocUpdateLogEntry(prevLSN, xid, OPERATION_NOOP, 
				     NULLRID, NULL, 0, 0);
  lsn_t ret = LogCLR(e);
  FreeLogEntry(e);
  return ret;
}
