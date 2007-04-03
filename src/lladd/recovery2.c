/** 

   @file Implements three phase recovery

*/

#include <config.h>
#include <lladd/common.h>

#include <stdio.h>
#include <assert.h>

#include <pbl/pbl.h>

#include <lladd/recovery.h>
#include <lladd/bufferManager.h>
#include <lladd/lockManager.h>

/** @todo Add better log iterator guard support and remove this include.*/
#include <lladd/operations/prepare.h>

#include "logger/logHandle.h"
/** @todo Get rid of linkedlist.[ch] */
#include "linkedlist.h"
#include "page.h" // Needed for pageReadLSN. 


static pblHashTable_t * transactionLSN;
static LinkedList * rollbackLSNs = NULL;
/** @todo There is no real reason to have this mutex (which prevents
    concurrent aborts, except that we need to protect rollbackLSNs's
    from concurrent modifications. */
static pthread_mutex_t rollback_mutex = PTHREAD_MUTEX_INITIALIZER;

/** 
    Determines which transactions committed, and which need to be redone.

    In the original version, this function also:
     - Determined the point in the log at which to start the Redo pass.
     - Calculated a list of all dirty pages.

    It no longer does either of these things:
     - A checkpointing algorithm could figure out where the redo pass 
       should begin.  (It would then truncate the log at that point.)  This
       function could be called before analysis if efficiency is a concern.
     - We were using the list of dirty pages as an optimization to prevent
       the pages from being read later during recovery.  Since this function
       no longer reads the pages in, there's no longer any reason to build 
       the list of dirty pages.
*/
static void Analysis () {

  const LogEntry * e;

  LogHandle lh = getLogHandle();

  /** After recovery, we need to know what the highest XID in the
      log was so that we don't accidentally reuse XID's.  This keeps
      track of that value. */
  int highestXid = 0;
  
  /** @todo loadCheckPoint() - Jump forward in the log to the last
      checkpoint.  (getLogHandle should do this automatically,
      since the log will be truncated on checkpoint anyway.) */

  while((e = nextInLog(&lh))) {
    
    lsn_t * xactLSN = (lsn_t*)pblHtLookup(transactionLSN,    &(e->xid), sizeof(int));

    if(highestXid < e->xid) {
      highestXid = e->xid;
    }

    /** Track LSN's in two data structures:
         - map: xid -> max LSN
	 - sorted list of maxLSN's
    */
    
    if(xactLSN == NULL) {
      xactLSN = malloc(sizeof(lsn_t)); 
      pblHtInsert(transactionLSN, &(e->xid), sizeof(int), xactLSN);
      
    } else {
      /* We've seen this xact before, and must have put a value in
	 rollbackLSNs for it.  That value is now stale, so remove
	 it. */
      
      DEBUG("Removing %ld\n", *xactLSN);
      removeVal(&rollbackLSNs, *xactLSN);
    }

    /* Now, rollbackLSNs certainly does not contain an LSN for this
       transaction, and *xactLSN points to a value in the hash, so
       writing to it updates the hash.  This doesn't update the
       rollbackLSN data structure, so it doesn't hurt to update this
       value for all log entries.  */

    *xactLSN = e->LSN;

    switch(e->type) {
    case XCOMMIT:
      /* We've removed this XACT's last LSN from the list of LSN's to
	 be rolled back, so we're done. */
      break;
    case XEND:
      /* 
	 XEND means this transaction reached stable storage.
	 Therefore, we can skip redoing any of its operations.  (The
	 timestamps on each page guarantee that the redo phase will
	 not overwrite this transaction's work with stale data.)

	 The redo phase checks for a transaction's presence in
	 transactionLSN before redoing its actions.  Therefore, if we
	 remove this transaction from the hash, it will not be redone.
      */
      pblHtRemove(transactionLSN,    &(e->xid), sizeof(int));
      break;
    case UPDATELOG:
    case CLRLOG:
      /* 
	 If the last record we see for a transaction is an update or clr, 
	 then the transaction must not have committed, so it must need
	 to be rolled back. 

	 Add it to the list

      */
      DEBUG("Adding %ld\n", e->LSN);

      addSortedVal(&rollbackLSNs, e->LSN);
      break;
    case XABORT: 
      // If the last record we see for a transaction is an abort, then
      // the transaction didn't commit, and must be rolled back. 
      DEBUG("Adding %ld\n", e->LSN);
      addSortedVal(&rollbackLSNs, e->LSN);
      break;  
    case INTERNALLOG:
      // Created by the logger, just ignore it
      // Make sure the log entry doesn't interfere with real xacts.
      assert(e->xid == INVALID_XID); 
      break; 
    default:
      abort();
    }
    FreeLogEntry(e);
  }
  TsetXIDCount(highestXid);
}

static void Redo() {
  LogHandle lh = getLogHandle();
  const LogEntry  * e;
  
  while((e = nextInLog(&lh))) {
    // Is this log entry part of a transaction that needs to be redone?
    if(pblHtLookup(transactionLSN, &(e->xid), sizeof(int)) != NULL) {
      // Check to see if this entry's action needs to be redone
      switch(e->type) { 
      case UPDATELOG:
      case CLRLOG: 
	{
	  // redoUpdate checks the page that contains e->rid, so we
	  // don't need to check to see if the page is newer than this
	  // log entry.
	  redoUpdate(e);
	  FreeLogEntry(e);
	} break;
      case DEFERLOG: 
	{ 
	  // XXX deferred_push(e);
	} break;
      case XCOMMIT:
	{
	  if(globalLockManager.commit)
	    globalLockManager.commit(e->xid);
	  FreeLogEntry(e);
	} break;
      case XABORT: 
	{ 
	  // wait until undo is complete before informing the lock manager
	  FreeLogEntry(e);
	} break;
      case INTERNALLOG:
	{
	  FreeLogEntry(e);
	} break;
      default:
	abort();
      }
    } 
  }
}
/**
    @todo Guards shouldn't be hardcoded in Undo()
*/
static void Undo(int recovery) {
  LogHandle lh;
  void * prepare_guard_state;

  while(rollbackLSNs != NULL) {
    const LogEntry * e;
    lsn_t rollback = popMaxVal(&rollbackLSNs);

    prepare_guard_state = getPrepareGuardState();

    DEBUG("Undoing LSN %ld\n", (long int)rollback);

    if(recovery) {
      lh = getGuardedHandle(rollback, &prepareGuard, prepare_guard_state);
    } else {
      lh = getLSNHandle(rollback);
    } 

    int thisXid = -1;
    while((e = previousInTransaction(&lh))) {
      thisXid = e->xid;
      lsn_t this_lsn, clr_lsn;
      switch(e->type) {
      case UPDATELOG:
	{
	  // If the rid is valid, load the page for undoUpdate.
	  // undoUpdate checks the LSN before applying physical undos

	  Page * p = NULL;
	  if(e->update.rid.size != -1) {
	    p = loadPage(thisXid, e->update.rid.page);

	    // If this fails, something is wrong with redo or normal operation.
	    this_lsn= pageReadLSN(p);
	    assert(e->LSN <= this_lsn);  

	  } else { 
	    // The log entry is not associated with a particular page.
	    // (Therefore, it must be an idempotent logical log entry.)
	  }

	  clr_lsn = LogCLR(e);
	  undoUpdate(e, p, clr_lsn);

	  if(p) { 
	    releasePage(p);
	  } 

	  break;
	}
      case DEFERLOG:
	// The transaction is aborting, so it never committed.  Therefore
	// actions deferred to commit have never been applied; ignore this
	// log entry.
	break;
      case CLRLOG:
	// Don't undo CLRs; they were undone during Redo
	break;
      case XABORT:
	// Since XABORT is a no-op, we can silentlt ignore it.  XABORT
	// records may be passed in by undoTrans.
	break;
      case XCOMMIT:
	// Should never abort a transaction that contains a commit record
	abort();
      default:
	printf
	  ("Unknown log type to undo (TYPE=%d,XID= %d,LSN=%lld), skipping...\n",
	   e->type, e->xid, e->LSN); 
	fflush(NULL);
	abort();
      }
      FreeLogEntry(e);
    }
    int transactionWasPrepared = prepareAction(prepare_guard_state);
    free(prepare_guard_state);
    if(!transactionWasPrepared && globalLockManager.abort) {
      globalLockManager.abort(thisXid);
    }
  }
}

void InitiateRecovery() {

  transactionLSN = pblHtCreate();
  DEBUG("Analysis started\n");
  Analysis();
  DEBUG("Redo started\n");
  Redo();
  DEBUG("Undo started\n");
  Undo(1);
  DEBUG("Recovery complete.\n");

  pblHtDelete(transactionLSN);
  
  destroyList(&rollbackLSNs);
  assert(rollbackLSNs==0);
}


void undoTrans(TransactionLog transaction) { 

  pthread_mutex_lock(&rollback_mutex);
  assert(!rollbackLSNs);

  if(transaction.prevLSN > 0) {
    DEBUG("scheduling lsn %ld for undo.\n", transaction.prevLSN);
    addSortedVal(&rollbackLSNs, transaction.prevLSN);
  } else {
    /* Nothing to undo.  (Happens for read-only xacts.) */
  }

  Undo(0);
  if(rollbackLSNs) {
    destroyList(&rollbackLSNs);
  }
  assert(rollbackLSNs == 0);
  pthread_mutex_unlock(&rollback_mutex);

}
