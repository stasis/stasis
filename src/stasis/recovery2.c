/** 

   @file

   Implements three phase recovery

*/

#include <config.h>
#include <stasis/common.h>

#include <stdio.h>
#include <assert.h>

#include <pbl/pbl.h>

#include <stasis/recovery.h>
#include <stasis/bufferManager.h>
#include <stasis/lockManager.h>

/** @todo Add better log iterator guard support and remove this include.*/
//#include <stasis/operations/prepare.h>

#include <stasis/logger/logHandle.h>
/** @todo Get rid of linkedlist */
#include <stasis/linkedlist.h>
#include <stasis/page.h> // Needed for pageReadLSN. 

static pblHashTable_t * transactionLSN;
static LinkedList * rollbackLSNs = NULL;
/** @todo There is no real reason to have this mutex (which prevents
    concurrent aborts), except that we need to protect rollbackLSNs's
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
static void Analysis() {

  DEBUG("Recovery: Analysis\n");

  const LogEntry * e;

  LogHandle lh = getLogHandle();

  /** After recovery, we need to know what the highest XID in the
      log was so that we don't accidentally reuse XID's.  This keeps
      track of that value. */
  int highestXid = 0;

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
      
      DEBUG("Removing %lld\n", *xactLSN);
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
    case XEND: {
      /* 
	 XEND means this transaction reached stable storage.
	 Therefore, we can skip redoing any of its operations.  (The
	 timestamps on each page guarantee that the redo phase will
	 not overwrite this transaction's work with stale data.)

	 The redo phase checks for a transaction's presence in
	 transactionLSN before redoing its actions.  Therefore, if we
	 remove this transaction from the hash, it will not be redone.
      */
	lsn_t* free_lsn = pblHtLookup(transactionLSN, &(e->xid), sizeof(int));
	pblHtRemove(transactionLSN,    &(e->xid), sizeof(int));
	free(free_lsn);
      }
      break;
    case UPDATELOG:
    case CLRLOG:
      /* 
	 If the last record we see for a transaction is an update or clr, 
	 then the transaction must not have committed, so it must need
	 to be rolled back. 

	 Add it to the list

      */
      DEBUG("Adding %lld\n", e->LSN);

      addSortedVal(&rollbackLSNs, e->LSN);
      break;
    case XABORT: 
      // If the last record we see for a transaction is an abort, then
      // the transaction didn't commit, and must be rolled back. 
      DEBUG("Adding %lld\n", e->LSN);
      addSortedVal(&rollbackLSNs, e->LSN);
      break;  
    case XPREPARE:
      addSortedVal(&rollbackLSNs, e->LSN);
      break; // XXX check to see if the xact exists?
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

/**
   Who runs where (if encountered):
                        Redo       Undo
    Physical             Y          Y (generate CLR, 'redo' it)

    Logical              N (nops)   N (if encountered, already did phys undo;
                                       otherwise, CLR masks it)

                                      (clr for logical always gets generated by end NTA)

    CLR for Physical     Y          N (whole point of CLR's is to skup this undo)

    CLR for Logical      N (could be undone later in Redo; NTA, so xact could commit)

                                    Y  (NTA replaces physical undo)
 */

static void Redo() {
  LogHandle lh = getLogHandle();
  const LogEntry  * e;

  DEBUG("Recovery: Redo\n");

  while((e = nextInLog(&lh))) {
    // Is this log entry part of a transaction that needs to be redone?
    if(pblHtLookup(transactionLSN, &(e->xid), sizeof(int)) != NULL) {
      // Check to see if this entry's action needs to be redone
      switch(e->type) { 
      case UPDATELOG:
        {
          if(e->update.page == INVALID_PAGE) {
            // logical redo; ignore
          } else {
            redoUpdate(e);
          }
        } break;
      case CLRLOG: 
	{
          const LogEntry *ce = LogReadLSN(((CLRLogEntry*)e)->clr.compensated_lsn);
          if(ce->update.page == INVALID_PAGE) {
            // logical redo of end of NTA; no-op
          } else {
            // ughh; need to grab page here so that abort() can be atomic below...
            Page * p = loadPage(e->xid, ce->update.page);
            writelock(p->rwlatch,0);
            undoUpdate(ce, e->LSN, p);
            unlock(p->rwlatch);
            releasePage(p);
          }
          FreeLogEntry(ce);
	} break;
      case XCOMMIT:
	{
	  if(globalLockManager.commit)
	    globalLockManager.commit(e->xid);
	} break;
      case XABORT: 
	{ 
	  // wait until undo is complete before informing the lock manager
	} break;
      case INTERNALLOG:
	{
	} break;
      case XPREPARE:
	{
	} break;
      default:
	abort();
      }
    }
    FreeLogEntry(e);
  }
}
static void Undo(int recovery) {
  LogHandle lh;

  DEBUG("Recovery: Undo (in recovery = %d)\n", recovery);

  while(rollbackLSNs != NULL) {
    const LogEntry * e;
    lsn_t rollback = popMaxVal(&rollbackLSNs);

    DEBUG("Undoing LSN %ld\n", (long int)rollback);

    lh = getLSNHandle(rollback);

    int thisXid = -1;

    // Is this transaction just a loser, or was it aborted?
    int reallyAborted = 0;
    // Have we reached a XPREPARE that we should pay attention to?
    int prepared = 0;
    while((!prepared) && (e = previousInTransaction(&lh))) {
      thisXid = e->xid;
      switch(e->type) {
      case UPDATELOG:
	{
          if(e->update.page == INVALID_PAGE) { 
            DEBUG("logical update\n");

            // logical undo: no-op; then the NTA didn't complete, and
            // we've finished physical undo for this op
          } else {
            DEBUG("physical update\n");

            // atomically log (getting clr), and apply undo.
            // otherwise, there's a race where the page's LSN is
            // updated before we undo.
            Page* p = NULL;
            if(e->update.page != INVALID_PAGE) {
              p = loadPage(e->xid, e->update.page);
              writelock(p->rwlatch,0);
            }

            // Log a CLR for this entry
            lsn_t clr_lsn = LogCLR(e);
            DEBUG("logged clr\n");

            undoUpdate(e, clr_lsn, p);

            if(p) {
              unlock(p->rwlatch);
              releasePage(p);
            }

            DEBUG("rolled back clr's update\n");
          }
	  break;
	}
      case CLRLOG:
        {
          const LogEntry * ce = LogReadLSN(((CLRLogEntry*)e)->clr.compensated_lsn);
          if(ce->update.page == INVALID_PAGE) {
            DEBUG("logical clr\n");
            undoUpdate(ce, 0, 0); // logical undo; effective LSN doesn't matter
          } else {
            DEBUG("physical clr: op %d lsn %lld\n", ce->update.funcID, ce->LSN);
            // no-op.  Already undone during redo.  This would redo the original op.
          }
          FreeLogEntry(ce);
        }
	break;
      case XABORT:
	DEBUG("Found abort for %d\n", e->xid);
	reallyAborted = 1;
	// Since XABORT is a no-op, we can silently ignore it.  XABORT
	// records may be passed in by undoTrans.
	break;
      case XCOMMIT:
	// Should never abort a transaction that contains a commit record
	abort();
      case XPREPARE: {
        DEBUG("found prepared xact %d\n", e->xid);

	if(!reallyAborted) {
	  DEBUG("xact wasn't aborted\n");
	  prepared = 1;
	  Trevive(e->xid, e->LSN, getPrepareRecLSN(e));
	} else {
	  DEBUG("xact was aborted\n");
	}
      } break;
      default:
	DEBUG
	  ("Unknown log type to undo (TYPE=%d,XID= %d,LSN=%lld), skipping...\n",
	   e->type, e->xid, e->LSN); 
	abort();
      }
      FreeLogEntry(e);
    }
    if((!prepared) && globalLockManager.abort) {
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
  TallocPostInit();
  Undo(1);
  DEBUG("Recovery complete.\n");

  for(void * it = pblHtFirst(transactionLSN); it; it = pblHtNext(transactionLSN)) {
    free(pblHtCurrent(transactionLSN));
  }
  pblHtDelete(transactionLSN);

  destroyList(&rollbackLSNs);
  assert(rollbackLSNs==0);
}


void undoTrans(TransactionLog transaction) { 

  pthread_mutex_lock(&rollback_mutex);
  assert(!rollbackLSNs);

  if(transaction.prevLSN > 0) {
    DEBUG("scheduling xid %d (lsn %lld) for undo.\n", transaction.xid, transaction.prevLSN);
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
