/** 

    Replacement for recovery.c 

    A lot of refactoring has been done to simplify the contents of recovery.c

    Hopefully, this file will be nice and clean. :)

*/

#include <config.h>
#include <lladd/common.h>
#include <lladd/recovery.h>

#include <pbl/pbl.h>
#include "linkedlist.h"
#include "logger/logHandle.h"
#include "logger/logWriter.h"
#include <lladd/bufferManager.h>

#include <lladd/transactional.h>

#include <stdio.h>
#include <assert.h>


/** @todo This include is an artifact of our lack of infrastructure to support log iterator guards.  */
#include <lladd/operations/prepare.h>

static pblHashTable_t * transactionLSN;
static LinkedListPtr rollbackLSNs = NULL;
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

  LogEntry * e;

  LogHandle lh = getLogHandle();

  /** After recovery, we need to know what the highest XID in the
      log was so that we don't accidentally reuse XID's.  This keeps
      track of that value. */
  int highestXid = 0;
  
  /** @todo loadCheckPoint() - Jump forward in the log to the last
      checkpoint.  (Maybe getLogHandle should do this automatically,
      since the log will be truncated on checkpoint anyway.) */

  while((e = nextInLog(&lh))) {
    
    lsn_t * xactLSN = (lsn_t*)pblHtLookup(transactionLSN,    &(e->xid), sizeof(int));
    /*    recordid rid = e->contents.update.rid; */

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

	 Add it to the appropriate list

      */
      DEBUG("Adding %ld\n", e->LSN);

      addSortedVal(&rollbackLSNs, e->LSN);
      break;
    case XABORT: 
      /* Don't want this XID in the list of rolled back lsn's since
	 this XACT will be rolled back during redo. */
      break;      
    default:
      assert (0);
    }
    free (e);
  }
  TsetXIDCount(highestXid);
}

static void Redo() {
  LogHandle lh = getLogHandle();
  LogEntry  * e;
  
  while((e = nextInLog(&lh))) {

    /* Check to see if this log entry is part of a transaction that needs to be redone. */
    if(pblHtLookup(transactionLSN, &(e->xid), sizeof(int)) != NULL) {
      /* Check to see if this log entry contains an action that needs to be redone. */
      if(e->type == UPDATELOG || 
	 e->type == CLRLOG) {
	/* redoUpdate checks the page that contains e->rid, so we
	   don't need to check to see if the page is newer than this
	   log entry. */
	if(e->type == UPDATELOG) { 
	  addPendingEvent(e->contents.update.rid.page); 
	} else {
	  addPendingEvent(e->contents.clr.rid.page);
	}
	redoUpdate(e);
      }
    }
  }
}

static void Undo(int recovery) {
  LogHandle lh;
  void * prepare_guard_state;


  /*  printf("!"); fflush(NULL); */

  prepare_guard_state = getPrepareGuardState();
  
  while(rollbackLSNs != NULL) {
    LogEntry * e;
    lsn_t rollback = popMaxVal(&rollbackLSNs);

    DEBUG("Undoing LSN %ld\n", (long int)rollback);
    if(recovery) {
      /** @todo shouldn't be hardcoded here! */
      lh = getGuardedHandle(rollback, &prepareGuard, prepare_guard_state);
    } else {
      /** @todo probably want guards that are run during normal operation. */
      lh = getLSNHandle(rollback);
    } 


    /* e = readLSNEntry(rollback);  (THIS STATEMENT HAD NO EFFECT)*/
    /*    printf("#"); fflush(NULL); */
    

    /*    printf("e->prev_offset: %ld\n", e->prevLSN);
	  printf("prev_offset: %ld\n", lh.prev_offset); */

    while((e = previousInTransaction(&lh))) {
      lsn_t this_lsn, clr_lsn;
      /*      printf("."); fflush(NULL); */
      switch(e->type) {
      case UPDATELOG:
	

      /* Sanity check.  If this fails, we've already undone this
	 update, or something is wrong with the redo phase or normal operation. */
	this_lsn= readLSN(e->contents.update.rid.page); 

	
	/*	printf("1"); fflush(NULL); */

	assert(e->LSN <= this_lsn);  

	/* printf("1a"); fflush(NULL); */
	
	/* Need to log a clr here. */

	clr_lsn = LogCLR(e);

	/* Undo update is a no-op if the page does not reflect this
	   update, but it will write the new clr_lsn if necessary.  */

	undoUpdate(e, clr_lsn);

	/*	printf("1b"); fflush(NULL); */
      break;
      case CLRLOG:  
	/* Don't need to do anything special to handle CLR's.  
	   Iterator will correctly jump to clr's previous undo record. */

	/*	printf("2"); fflush(NULL); */
      break;
      case XABORT:
	/* Since XABORT is a no-op, we can silentlt ignore it. (XABORT
	   records may be passed in by undoTrans.)*/
      break;
      default:
	printf ("Unknown log type to undo (TYPE=%d, XID= %d, LSN=%ld), skipping...\n", e->type, e->xid, e->LSN); 
      break;
      }
    }
    /*    printf("$"); fflush(NULL); */
  }
  free(prepare_guard_state);
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

  /** @todo Should we manually empty the hash table? */
  pblHtDelete(transactionLSN);
  
  /** @todo CleanUp(); */
}


void undoTrans(TransactionLog transaction) { 
  if(rollbackLSNs) {
    destroyList(rollbackLSNs);
  }
  rollbackLSNs = 0;
  if(transaction.prevLSN > 0) {
    DEBUG("scheduling lsn %ld for undo.\n", transaction.prevLSN);
    addSortedVal(&rollbackLSNs, transaction.prevLSN);
  } else {
    /* Nothing to undo.  (Happens for read-only xacts.) */
  }

  Undo(0);
}
