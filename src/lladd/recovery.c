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

/*****************
 * @file 
 * $Id$
 * 
 * Implements recovery for a NO STEAL system -- we won't need any undo functions, CLRs, or to redo any aborted/uncommitted transactions
 * 
 * @deprecated 
 * @see recovery2.c
 * *************/

#include <stdlib.h>

#include "recovery.h"
#include <lladd/transactional.h>
#include <lladd/common.h>
#include "linkedlist.h"
#include <lladd/logger.h>
#include "logger/logparser.h"
#include "logger/logstreamer.h"
#include <pbl/pbl.h>
#include <lladd/page.h>
#include <lladd/bufferManager.h>
#include <stdio.h>
/* just set an arbitrary recovery xid */
#define RECOVERY_XID 69
/* moved from recovery.h, sice this is the only file that uses it. */

static void undoUpdateRec(CommonLog cl, UpdateLog ul);



/* map from page number to the recLSN that dirtied it */
static pblHashTable_t *dirtyPages;
/* sorted list of recLSNs of active transactiosn */

static LinkedListPtr transRecLSNs=NULL;

/* map from transID to status of the transaction: U (to be undone), or not in
 * there, or committed but not ended (C)
 * */
static pblHashTable_t *transTable;

/* the LSN of the dirty page with the smallest corresponding LSN */
static long minDirtyLSN;


void undoUpdateRec(CommonLog cl, UpdateLog ul) {
	int undoIndex = operationsTable[ul.funcID].undo;
	long CLRLSN;
	CLRLSN = LogCLR(-1, cl.xid, cl.LSN, ul.rid, cl.prevLSN);
	/*printf ("Undoing LSN=%ld (and writing new pageLSN=%ld\n", cl.LSN, CLRLSN); */
	if (undoIndex==NO_INVERSE)
	  writeRecord(RECOVERY_XID, ul.rid, ul.preImage); /*if there's no inverse we directly write the preimage to the record*/
	else operationsTable[undoIndex].run(RECOVERY_XID, ul.rid, ul.args);
	writeLSN(CLRLSN, ul.rid.page);
}
/**
 * Do this CLR action, and return the undoNextLSN listed in this clrlog
 */
/*static long DoCLRAction(CLRLog clrlog) {
	UpdateLog ul;
	CommonLog commonLog = readCommonLogFromLSN(clrlog.thisUpdateLSN);
	if (commonLog.type!=UPDATELOG) {
		printf ("Don't know how to undo a non-update log (LSN=%ld)! Aborting undo...\n", commonLog.LSN);
		return -1;
	}
	ul = updateStringToParts(commonLog.extraData);
	undoUpdate(commonLog, ul);
	return clrlog.undoNextLSN;	
	}*/

/**
 * Redo this clrlog, and it doesn't write out any CLR for the CLR. For now copy
 * and pasted form undoUpdate, make it cleaner in the future.
 */
static void RedoCLRAction(CLRLog clrlog) {
	int undoIndex;
	UpdateLog ul;       
	CommonLog commonLog = readCommonLogFromLSN(clrlog.thisUpdateLSN);
	if (commonLog.type!=UPDATELOG) {
		printf ("Don't know how to undo a non-update log (LSN=%ld)! Aborting undo...\n", commonLog.LSN);		
		return;		
	}
	ul = updateStringToParts(commonLog.extraData);
	undoIndex = operationsTable[ul.funcID].undo;
	printf ("ReUndoing LSN=%ld\n", commonLog.LSN);
	if (undoIndex==NO_INVERSE)
	  writeRecord(RECOVERY_XID, ul.rid, ul.preImage); /*if there's no inverse we directly write the preimage to the record*/
	else operationsTable[undoIndex].run(RECOVERY_XID, ul.rid, ul.args);
	writeLSN(commonLog.LSN, clrlog.rid.page);
}
static void recovInit() {
  /*	startLogStream(); */  /* This is now called after Tinit() opens the log (to support Tprepare, and to fix the reused XID bug) */
	dirtyPages = pblHtCreate();
	transTable = pblHtCreate();

}
/*
   loadCheckPoint scans the log from the end to find the most recent checkpoint
   loads the dirty page table from the checkpoint
   set the lsn to start to be one after the end_checkpoint log **check this-i think it should be one after begin_checkpoint
   if there are no checkpoints, LSN starts is 0
   */
static void loadCheckPoint (long *endCheckptLSN) {
  /*for now just assume no checkpoints
    therefore committedtrans table and DPT are empty
    start recovery at LSN 0 */
  *endCheckptLSN = 1;
}

/*
   At end of recovery, wrap up --
   delete the committed transaction list

   possibly in the future: delete the log file
   */
static void CleanUp() {
  /*	void *trans; */
	destroyList(transRecLSNs);
	transRecLSNs = 0;
	/*any pages that were dirty should be flushed to disk and then the
	  corresponding transaction ended */
	/*flushAllPages(); */
	/*trans = pblHtFirst(transTable);
		while (trans!=NULL) {
		LogEnd(-1, *(int *)pblHtCurrentKey(transTable));	
		trans = pblHtNext(transTable);
		}*/
	pblHtDelete(dirtyPages);
	pblHtDelete(transTable);
	flushLog();
	/*	closeLogStream(); */
	bufTransCommit(RECOVERY_XID);  /*What is this? */
}

/*
   Returns true ifa page must be redone
   loads the page into loadedPage if it needs to be redone
   */
static int MustBeRedone(long entryLSN, int page, Page *loadedPage) {
  /* if this log entry's page is dirty */
	long tmp;
	long *recLSN = pblHtLookup(dirtyPages, &page, sizeof(int));
	if (recLSN!=NULL) {
	  /*unmalloc this entry in the table, don't need it anymore */
		tmp = *recLSN;
		/* if the log entry's LSN is less than the recLSN of the dirty page */
		if (entryLSN <= tmp) {
		  /*load page into memory, if the log's LSN is greater than (more recent) the LSN of that actual page (as it was on disk) */
			*loadedPage = loadPage(page);
			if (entryLSN > readLSN(page)) 
				return 1;
		}
	}
	return 0;
}

/*
   Given a page number and the log's LSN, analyze the page, aka
   look it up in the dirty page table and add it if it's not there, and 
   have the table reflect the most recent LSN

   also keeps track of the smallest dirty LSN
   */
static void analyzePage(int page, long curLSN) {
	long *LSNCopy;
	/*add page to the dirty page table if not there already with recLSN equal to this LSN */
	long *oldLSN = pblHtLookup(dirtyPages, &page, sizeof(int));
	if (oldLSN==NULL) { 
	  /*make a new entry */
		LSNCopy = (long *)malloc(sizeof(long));
		*LSNCopy = curLSN;
		pblHtInsert( dirtyPages, &page, sizeof(int), LSNCopy);
	} else {
	  /*update the old entry to have the most recent LSN */
		*oldLSN = curLSN;
	}
	/*if it's the smallest dirty LSN, make note of it */
	if (minDirtyLSN==-1 || curLSN < minDirtyLSN)
		minDirtyLSN = curLSN;
}

/**
 * Analysis is the first phase:
 *  1) determines the point in the log at which to start the Redo pass
 *  2) determines (a conservative superset  of the) pages in the buffer pool
 *  that were dirty at the time of the crash
 * 
 * At the end of Analysis, transaction table contains accurate list of all
 * transactions that were active at the time of the crash, and dirty page table
 * includes all pages that were dirty ta time of crash (plus maybe some that
 * were written to disk)
 */
static void RecovAnalysis () {
	long curLSN, *thisTLSN;
	int highestXid = -1;
	CommonLog logEntry;
	UpdateLog ul;
	recordid thisRid;
	CLRLog clrlog;
	char *status;
	pblHashTable_t *transMaxLSN = pblHtCreate();
	minDirtyLSN = -1;
	loadCheckPoint(&curLSN); /*sets DPT, AT and the current LSN to the  last checkpoint if there is on */
	/*scan through the log until the end*/
	logEntry = readCommonLogFromLSN(curLSN);
	while (logEntry.valid==1) {
	  if(highestXid < logEntry.xid) {
	    highestXid = logEntry.xid;
	  }
		if ((status = (char *)pblHtLookup(transTable, &logEntry.xid, sizeof(int)))==NULL) { 
			status = malloc(sizeof(char));
			*status = 'U'; /*default is to be undone */
			pblHtInsert(transTable, &logEntry.xid, sizeof(int), status);
		}
		if ((thisTLSN = (long *)pblHtLookup(transMaxLSN, &logEntry.xid, sizeof(int))) != NULL) {
		  removeVal(&transRecLSNs, *thisTLSN); /*it's not the max anymore -- will add another val in later */
	}
		else {
			thisTLSN = (long *)malloc(sizeof(long));
			*thisTLSN = -1;
			pblHtInsert(transMaxLSN, &logEntry.xid, sizeof(int), thisTLSN);
		}
		curLSN = logEntry.LSN;
		switch(logEntry.type) {
			case XCOMMIT:
				*status = 'C';
				break;
			case XEND:
				pblHtRemove(transTable, &logEntry.xid, sizeof(int)); /*not a transaction anymore, remove it from the transaction table */
				pblHtRemove(transMaxLSN, &logEntry.xid, sizeof(int));
				break;
			case UPDATELOG:
			        /*determine what page it modified*/
				ul = updateStringToParts(logEntry.extraData);
				analyzePage(ul.rid.page, logEntry.LSN);
				free(logEntry.extraData);
				/*removeVal(&transRecLSNs, *thisTLSN);*/
				addSortedVal(&transRecLSNs, logEntry.LSN);
				*thisTLSN = logEntry.LSN;
				break;
			case XALLOC:
				thisRid = allocStringToRID(logEntry.extraData);
				analyzePage(thisRid.page, logEntry.LSN);
				free(logEntry.extraData);
				/*removeVal(&transRecLSNs, *thisTLSN); */
				addSortedVal(&transRecLSNs, logEntry.LSN);
				*thisTLSN = logEntry.LSN;
				break;
			case CLRLOG:
				clrlog = CLRStringToParts(logEntry.extraData);
				analyzePage(clrlog.rid.page, logEntry.LSN);
				addSortedVal(&transRecLSNs, logEntry.LSN);
				*thisTLSN = logEntry.LSN;
				break;
		}
		/*printList(transRecLSNs); */
		logEntry = readNextCommonLog();
	}
	TsetXIDCount(highestXid);
	pblHtDelete(transMaxLSN);
}

/**
 * Redo applies updates of all committed transactions if they haven't been flushed to disk yet
 *
 */
static void RecovRedo () {
  long curLSN = minDirtyLSN;  /* start with smallest recLSN in dirty page table */
	UpdateLog ul;
	/*	long *recLSN,  */
	long nextLSN;
	Page loadedPage;
	CLRLog clrlog;
	recordid rec; 
	int i;
	CommonLog logEntry;
	/* starting from there, scan forward until end of log */
	logEntry = readCommonLogFromLSN(curLSN);
	while (logEntry.valid!=0) {
	  /*for each redoable log record, check whether logged action must be redone - redoable actions are update logs/alloc logs and must be a committed transaction*/
		if (pblHtLookup(transTable, &logEntry.xid, sizeof(int))!=NULL) {
		  switch (logEntry.type) { /*only look at redoable log entries */
				case UPDATELOG:
					ul = updateStringToParts(logEntry.extraData);

					if (MustBeRedone(logEntry.LSN, ul.rid.page, &loadedPage)==1) {
					  /*reapply logged action, AND set LSN on page to the LSN of the redone log record, flush to disk(?)
					    note that this is going around logging (which is done in transactional) so this will be unlogged*/
						printf ("redoing op %d at LSN=%ld (>%ld), rid (page, slot, size)=%d,%d,%d. args (length %d)=", ul.funcID, logEntry.LSN, loadedPage.LSN, ul.rid.page, ul.rid.slot, ul.rid.size, ul.argSize);
						for (i=0; i<ul.argSize; i++)
							printf ("%c", *(char*)(ul.args+i));	
						printf ("\n");  
						operationsTable[ul.funcID].run(RECOVERY_XID, ul.rid, ul.args);
						/*loadedPage.LSN = logEntry.LSN;*/
						writeLSN(logEntry.LSN, ul.rid.page);
					}
					/** @todo KLUDGE:  updateStringToParts mallocs ul.args, while assembleUpdateLog does not.  Therefore, we remove the const qualifier from ul.args here, 
					    and free it.  Elsewhere, we do not have to do this. */
					free((byte*)ul.args);
					break;
				case XALLOC:
					rec = allocStringToRID(logEntry.extraData);
	 				if (MustBeRedone(logEntry.LSN, rec.page, &loadedPage)==1) {
					  /*reallocate the record in the page just like how it was before*/
						printf ("redoing record allocation at LSN=%ld: rid=%d,%d,%ld where pageid=%d.\n", logEntry.LSN, rec.page, rec.slot, (long int)rec.size, (int)loadedPage.id);
						pageSlotRalloc(loadedPage, rec);
						writeLSN(logEntry.LSN, rec.page);
					}

					break;
				case CLRLOG:
					clrlog = CLRStringToParts(logEntry.extraData);
					if (MustBeRedone(logEntry.LSN, clrlog.rid.page, &loadedPage)==1) {
						printf("redoing CLR at LSN=%ld (>%ld), rid=%d,%d,%ld...", logEntry.LSN, loadedPage.LSN, clrlog.rid.page, clrlog.rid.slot, (long int)clrlog.rid.size);
						nextLSN = streamPos();
						RedoCLRAction(clrlog);
						seekInLog(nextLSN);
						writeLSN(logEntry.LSN, clrlog.rid.page);
					}
					break;
		        } /* switch*/
		} /* committed trans IF */
		if (logEntry.extraData) 
			free(logEntry.extraData);
		logEntry = readNextCommonLog();
	} /* while */
}

/**
 * Looking at the set of lastLSN values for all 'loser' (non ending)
 * transactions, pick the most recent LSN, undo it (if it is an update log) and
 * add in prevLSN to the set of to be looked at LSNs, or add undoNextLSN if it
 * is a CLR log
 */
static void RecovUndo(int recovering) {
	long LSNToLookAt;
	CommonLog comLog;
	UpdateLog ul;
	CLRLog clrlog;
	recordid rec;
	Page loadedPage;
	while (transRecLSNs!=NULL) {
		LSNToLookAt = popMaxVal(&transRecLSNs);
		comLog = readCommonLogFromLSN(LSNToLookAt);
		switch (comLog.type) {
			case UPDATELOG:
				ul = updateStringToParts(comLog.extraData);
				undoUpdateRec(comLog, ul);
				if ((ul.funcID != OPERATION_PREPARE) || 
				    (!recovering)) {
				  if (comLog.prevLSN>0) /*0 or -1 are invalid LSNs*/
				    addSortedVal(&transRecLSNs, comLog.prevLSN);
				  /*else LogEnd(-1, comLog.xid);*/
				} else if (ul.funcID == OPERATION_PREPARE) {
				  /* TODO: Ugly! */
				  printf("Reviving XID: %d\n", comLog.xid);
				  Trevive(comLog.xid, LSNToLookAt);
				} else {
				  printf("EEK!\n");
				}
				/*else LogEnd(-1, comLog.xid);*/
				break;
			case CLRLOG:
				clrlog = CLRStringToParts(comLog.extraData);
				if (clrlog.undoNextLSN>0)
					addSortedVal(&transRecLSNs, clrlog.undoNextLSN);
				/*else LogEnd(-1, comLog.xid);*/
				break;
			case XALLOC:
				rec = allocStringToRID(comLog.extraData);

				loadedPage = loadPage(rec.page);
				printf ("NOT undoing record allocation at LSN=%ld: rid=%d,%d,%ld where pageid=%d.\n", comLog.LSN, rec.page, rec.slot, (long int)rec.size, loadedPage.id);
				/* If page is already in memory (likely), then this just gets a pointer to it. */
				/* The next line is correct, but commented because it causes trouble with LLADD hash. */
				/*	pageDeRalloc(loadedPage, rec);  */
				if (comLog.prevLSN>0) /*0 or -1 are invalid LSNs*/
				    addSortedVal(&transRecLSNs, comLog.prevLSN);
				break;
			default: printf ("Unknown log type to undo (TYPE=%d, XID= %d, LSN=%ld), skipping...\n", comLog.type, comLog.xid, comLog.LSN);
					 if (comLog.prevLSN>0)
						 addSortedVal(&transRecLSNs, comLog.prevLSN);
					 /*else LogEnd(-1, comLog.xid);*/
					 break;
		}

	}
}

/**
 * because this messes with transRecLSNs,
 * should NOT be called by recovery. Call it from
 * non-recovery code!
 */
void undoTrans(Transaction t) {
        if (transRecLSNs) {
            destroyList(transRecLSNs);
        }
	transRecLSNs = 0;
	if(t.LSN > 0) {
	  addSortedVal(&transRecLSNs, t.LSN);
	} else {
	  /* printf ("Nothing to undo for xid %d\n", t.xid); I think this is normal for the sequence Tbegin, Tread, Tabort -- Rusty */
	}
	RecovUndo(0);
}

void InitiateRecovery () {
	recovInit(); 
	printf ("Analysis\n");
	RecovAnalysis();
	printf ("Redo\n");
	RecovRedo();
	printf ("Undo\n");
	RecovUndo(1);
	CleanUp(); 
}
