#include <config.h>
#include <lladd/common.h>
#include "latches.h"
#include <lladd/transactional.h>

#include <lladd/recovery.h>
#include "logger/logWriter.h"
#include <lladd/bufferManager.h>
#include "page.h"
#include <lladd/logger/logger2.h>

#include <stdio.h>
#include <assert.h>
#include "page/indirect.h"

TransactionLog XactionTable[MAX_TRANSACTIONS];
int numActiveXactions = 0;
int xidCount = 0;


/** 
    Locking for transactional2.c works as follows:
    
    numActiveXactions, xidCount are protected, XactionTable is not.
    This implies that we do not support multi-threaded transactions,
    at least for now.
*/
pthread_mutex_t transactional_2_mutex;

#define INVALID_XTABLE_XID -1
#define PENDING_XTABLE_XID -2
/** Needed for debugging -- sometimes we don't want to run all of Tinit() */

void setupOperationsTable() {
	memset(XactionTable, INVALID_XTABLE_XID, sizeof(TransactionLog)*MAX_TRANSACTIONS);
	operationsTable[OPERATION_SET]       = getSet();
	operationsTable[OPERATION_INCREMENT] = getIncrement();
	operationsTable[OPERATION_DECREMENT] = getDecrement();
	operationsTable[OPERATION_ALLOC]     = getAlloc();
	operationsTable[OPERATION_PREPARE]   = getPrepare();
	/*	operationsTable[OPERATION_LHINSERT]  = getLHInsert(); 
		operationsTable[OPERATION_LHREMOVE]  = getLHRemove(); */
	operationsTable[OPERATION_DEALLOC]     = getDealloc();
	operationsTable[OPERATION_PAGE_ALLOC] = getPageAlloc();
	operationsTable[OPERATION_PAGE_DEALLOC] = getPageDealloc();
	operationsTable[OPERATION_PAGE_SET] = getPageSet();


}

int Tinit() {
         
        pthread_mutex_init(&transactional_2_mutex, NULL);

        setupOperationsTable();
	
	bufInit();

	openLogWriter();

	InitiateRecovery();


	return 0;
}


int Tbegin() {

	int i, index = 0;
	int xidCount_tmp;

	pthread_mutex_lock(&transactional_2_mutex);

	if( numActiveXactions == MAX_TRANSACTIONS ) {
	  pthread_mutex_unlock(&transactional_2_mutex);
	  return EXCEED_MAX_TRANSACTIONS;
	}
	else
		numActiveXactions++;

	for( i = 0; i < MAX_TRANSACTIONS; i++ ) {
		xidCount++;
		if( XactionTable[xidCount%MAX_TRANSACTIONS].xid == INVALID_XTABLE_XID ) {
			index = xidCount%MAX_TRANSACTIONS;
			break;
		}
	}

	xidCount_tmp = xidCount;

	assert( i < MAX_TRANSACTIONS );
	
	XactionTable[index].xid = PENDING_XTABLE_XID;

	pthread_mutex_unlock(&transactional_2_mutex);	

	XactionTable[index] = LogTransBegin(xidCount_tmp);

	return XactionTable[index].xid;
}

void Tupdate(int xid, recordid rid, const void *dat, int op) {
  LogEntry * e;
  Page * p;  
#ifdef DEBUGGING
  pthread_mutex_lock(&transactional_2_mutex);
  assert(numActiveXactions <= MAX_TRANSACTIONS);
  pthread_mutex_unlock(&transactional_2_mutex);
#endif

  p = loadPage(rid.page);

  if(*page_type_ptr(p) == INDIRECT_PAGE) {
    releasePage(p);
    rid = dereferenceRID(rid);
    p = loadPage(rid.page); 
  }

  e = LogUpdate(&XactionTable[xid % MAX_TRANSACTIONS], p, rid, op, dat);
  
  assert(XactionTable[xid % MAX_TRANSACTIONS].prevLSN == e->LSN);

  DEBUG("Tupdate() e->LSN: %ld\n", e->LSN);

  doUpdate(e, p);
  releasePage(p);

  free(e);

}

void Tread(int xid, recordid rid, void * dat) {
  Page * p = loadPage(rid.page);
  if(*page_type_ptr(p) == SLOTTED_PAGE) {
    readRecord(xid, p, rid, dat);
  } else if(*page_type_ptr(p) == INDIRECT_PAGE) {
    releasePage(p);
    rid = dereferenceRID(rid);
    p = loadPage(rid.page);
    readRecord(xid, p, rid, dat);
  } else {
    abort();
  }
  releasePage(p);
}

int Tcommit(int xid) {
  lsn_t lsn;
#ifdef DEBUGGING 
  pthread_mutex_lock(&transactional_2_mutex);
  assert(numActiveXactions <= MAX_TRANSACTIONS);
  pthread_mutex_unlock(&transactional_2_mutex);
#endif

  lsn = LogTransCommit(&XactionTable[xid % MAX_TRANSACTIONS]);
  bufTransCommit(xid, lsn); /* unlocks pages */

  pthread_mutex_lock(&transactional_2_mutex);
  XactionTable[xid%MAX_TRANSACTIONS].xid = INVALID_XTABLE_XID;
  numActiveXactions--;
  assert( numActiveXactions >= 0 );
  pthread_mutex_unlock(&transactional_2_mutex);



  return 0;
}

int Tabort(int xid) {
  lsn_t lsn;
  
  TransactionLog * t =&XactionTable[xid%MAX_TRANSACTIONS];

  lsn = LogTransAbort(t /*&XactionTable[xid%MAX_TRANSACTIONS]*/);

  /** @todo is the order of the next two calls important? */
  undoTrans(*t/*XactionTable[xid%MAX_TRANSACTIONS]*/); 
  bufTransAbort(xid, lsn); 

  pthread_mutex_lock(&transactional_2_mutex);
  
  XactionTable[xid%MAX_TRANSACTIONS].xid = INVALID_XTABLE_XID;
  numActiveXactions--;
  assert( numActiveXactions >= 0 );
  pthread_mutex_unlock(&transactional_2_mutex);
  return 0;
}

int Tdeinit() {
	int i;

	for( i = 0; i < MAX_TRANSACTIONS; i++ ) {
		if( XactionTable[i].xid != INVALID_XTABLE_XID ) {
			Tabort(XactionTable[i].xid);
			printf("WARNING: Tdeinit() is aborting transaction %d\n", XactionTable[i].xid);
		}
	}
	assert( numActiveXactions == 0 );

	bufDeinit();
	closeLogWriter();

	return 0;
}

void Trevive(int xid, long lsn) {
  int index = xid % MAX_TRANSACTIONS;
  pthread_mutex_lock(&transactional_2_mutex);
  if(XactionTable[index].xid != INVALID_XTABLE_XID) {
    if(xid != XactionTable[index].xid) {
      printf("Clashing Tprepare()'ed XID's encountered on recovery!!\n");
      assert(0);
    }
    assert(XactionTable[index].xid == xid);
    assert(XactionTable[index].prevLSN == lsn);
  } else {
    XactionTable[index].xid = xid;
    XactionTable[index].prevLSN = lsn;

    numActiveXactions++;

  }
  pthread_mutex_unlock(&transactional_2_mutex);
}

void TsetXIDCount(int xid) {
  pthread_mutex_lock(&transactional_2_mutex);
  xidCount = xid;
  pthread_mutex_unlock(&transactional_2_mutex);
}
