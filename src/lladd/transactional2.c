#include <config.h>
#include <lladd/common.h>
#include "latches.h"
#include <lladd/transactional.h>

#include <lladd/recovery.h>
#include <lladd/bufferManager.h>
#include <lladd/consumer.h>
#include <lladd/lockManager.h>
#include <lladd/compensations.h>
#include "page.h"
#include <lladd/logger/logger2.h>
#include <lladd/truncation.h>
#include <stdio.h>
#include <assert.h>
#include "page/indirect.h"
#include <limits.h>

TransactionLog XactionTable[MAX_TRANSACTIONS];
int numActiveXactions = 0;
int xidCount = 0;

const recordid ROOT_RECORD = {1, 0, -1};
const recordid NULLRID = {0,0,-1};
const short SLOT_TYPE_LENGTHS[] = { 0, 0, sizeof(blob_record_t), -1};
/** 
    Locking for transactional2.c works as follows:
    
    numActiveXactions, xidCount are protected, XactionTable is not.
    This implies that we do not support multi-threaded transactions,
    at least for now.
*/
pthread_mutex_t transactional_2_mutex;

#define INVALID_XTABLE_XID INVALID_XID
#define PENDING_XTABLE_XID (-2)
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
	operationsTable[OPERATION_REALLOC]     = getRealloc();
	/*	operationsTable[OPERATION_PAGE_ALLOC] = getPageAlloc();
		operationsTable[OPERATION_PAGE_DEALLOC] = getPageDealloc(); */
	operationsTable[OPERATION_PAGE_SET] = getPageSet();

	/*	operationsTable[OPERATION_UPDATE_FREESPACE]         = getUpdateFreespace();
	operationsTable[OPERATION_UPDATE_FREESPACE_INVERSE] = getUpdateFreespaceInverse();
	operationsTable[OPERATION_UPDATE_FREELIST]          = getUpdateFreelist();
	operationsTable[OPERATION_UPDATE_FREELIST_INVERSE] = getUpdateFreelistInverse();
	
	operationsTable[OPERATION_FREE_PAGE] = getFreePageOperation();
	operationsTable[OPERATION_ALLOC_FREED] = getAllocFreedPage();
	operationsTable[OPERATION_UNALLOC_FREED] = getUnallocFreedPage(); */
	operationsTable[OPERATION_NOOP] = getNoop();
	operationsTable[OPERATION_INSTANT_SET] = getInstantSet();
	operationsTable[OPERATION_ARRAY_LIST_ALLOC]  = getArrayListAlloc();
	//	operationsTable[OPERATION_INITIALIZE_FIXED_PAGE] = getInitFixed();
	operationsTable[OPERATION_INITIALIZE_PAGE] = getInitializePage();
	//	operationsTable[OPERATION_UNINITIALIZE_PAGE] = getUnInitPage();

	operationsTable[OPERATION_LINEAR_INSERT] = getLinearInsert();
	operationsTable[OPERATION_UNDO_LINEAR_INSERT] = getUndoLinearInsert();
	operationsTable[OPERATION_LINEAR_DELETE] = getLinearDelete();
	operationsTable[OPERATION_UNDO_LINEAR_DELETE] = getUndoLinearDelete();
	
	operationsTable[OPERATION_SET_RANGE] = getSetRange();
	operationsTable[OPERATION_SET_RANGE_INVERSE] = getSetRangeInverse();
	
	operationsTable[OPERATION_LINKED_LIST_INSERT] = getLinkedListInsert();
	operationsTable[OPERATION_LINKED_LIST_REMOVE] = getLinkedListRemove();

	operationsTable[OPERATION_LINEAR_HASH_INSERT] = getLinearHashInsert();
	operationsTable[OPERATION_LINEAR_HASH_REMOVE] = getLinearHashRemove();
	
	operationsTable[OPERATION_SET_RAW] = getSetRaw();
	operationsTable[OPERATION_INSTANT_SET_RAW] = getInstantSetRaw();

	operationsTable[OPERATION_ALLOC_BOUNDARY_TAG] = getAllocBoundaryTag();

	operationsTable[OPERATION_FIXED_PAGE_ALLOC] = getFixedPageAlloc();

	operationsTable[OPERATION_ALLOC_REGION] = getAllocRegion();
	operationsTable[OPERATION_DEALLOC_REGION] = getDeallocRegion();

	/* 
	   int i;

		for(i = 0; i <= OPERATION_LINEAR_HASH_REMOVE; i++) {
	  if(operationsTable[i].id != i) {
	    printf("mismatch %d -> %d\n", i, operationsTable[i].id);
	  }
	}
	*/
}


int Tinit() {
	setBufferManager(BUFFER_MANAGER_HASH);

        pthread_mutex_init(&transactional_2_mutex, NULL);
	numActiveXactions = 0;

	compensations_init();

        setupOperationsTable();
	dirtyPagesInit();
	
	pageInit();
	bufInit();

	LogInit(loggerType);

	try_ret(compensation_error()) { 
	  pageOperationsInit();
	} end_ret(compensation_error());

	initNestedTopActions();

	TallocInit();

	ThashInit();
	LinearHashNTAInit();
	LinkedListNTAInit();
	iterator_init();
	consumer_init();
	setupLockManagerCallbacksNil();
	//setupLockManagerCallbacksPage();
	
	InitiateRecovery();
	
	truncationInit();
	if(lladd_enableAutoTruncation) { 
	  autoTruncate(); // should this be before InitiateRecovery?
	}
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

	XactionTable[index].xid = PENDING_XTABLE_XID;

	pthread_mutex_unlock(&transactional_2_mutex);	

	XactionTable[index] = LogTransBegin(xidCount_tmp);

	if(globalLockManager.begin) { globalLockManager.begin(XactionTable[index].xid); }

	return XactionTable[index].xid;
}

static compensated_function void TupdateHelper(int xid, recordid rid, const void * dat, int op, Page * p) {
  LogEntry * e;
  assert(xid >= 0);
  try { 
    if(globalLockManager.writeLockPage) {
      globalLockManager.writeLockPage(xid, rid.page);
    }
  } end;

    
  e = LogUpdate(&XactionTable[xid % MAX_TRANSACTIONS], p, rid, op, dat);
  
  assert(XactionTable[xid % MAX_TRANSACTIONS].prevLSN == e->LSN);
  
  DEBUG("T update() e->LSN: %ld\n", e->LSN);
  
  doUpdate(e, p);

  FreeLogEntry(e);
}

compensated_function void Tupdate(int xid, recordid rid, const void *dat, int op) {
  Page * p;  
  assert(xid >= 0);
#ifdef DEBUGGING
  pthread_mutex_lock(&transactional_2_mutex);
  assert(numActiveXactions <= MAX_TRANSACTIONS);
  pthread_mutex_unlock(&transactional_2_mutex);
#endif
  try { 
    p = loadPage(xid, rid.page);
  } end;
  if(op != OPERATION_SET_RAW && op != OPERATION_INSTANT_SET_RAW) { 
    if(*page_type_ptr(p) == INDIRECT_PAGE) {
      releasePage(p);
      try { 
	rid = dereferenceRID(xid, rid);
	p = loadPage(xid, rid.page); 
      } end;
      // @todo Kludge! Shouldn't special case operations in transactional2. 
    } else if(*page_type_ptr(p) == ARRAY_LIST_PAGE && 
	      op != OPERATION_LINEAR_INSERT && 
	      op != OPERATION_UNDO_LINEAR_INSERT &&
	      op != OPERATION_LINEAR_DELETE && 
	      op != OPERATION_UNDO_LINEAR_DELETE  ) {
      rid = dereferenceArrayListRid(p, rid.slot);
      releasePage(p);
      try { 
	p = loadPage(xid, rid.page); 
      } end;
    } 
  }

  /** @todo For logical undo logs, grabbing a lock makes no sense! */
  begin_action(releasePage, p) { 
    TupdateHelper(xid, rid, dat, op, p);
  } compensate;

}

void TreadUnlocked(int xid, recordid rid, void * dat) {
  Page * p;
  try { 
    p = loadPage(xid, rid.page);
  } end;

  rid = interpretRidUnlocked(xid, rid, p);
  if(rid.page != p->id) { 
    releasePage(p);
    p = loadPage(xid, rid.page);
  }
  readRecordUnlocked(xid, p, rid, dat);
  releasePage(p);
}

compensated_function void Tread(int xid, recordid rid, void * dat) {
  Page * p;
  try { 
    p = loadPage(xid, rid.page);
  } end;

  rid = interpretRid(xid, rid, p);
  if(rid.page != p->id) { 
    releasePage(p);
    p = loadPage(xid, rid.page);
  }
  readRecord(xid, p, rid, dat);
  releasePage(p);
}

int Tcommit(int xid) {
  lsn_t lsn;
  assert(xid >= 0);
#ifdef DEBUGGING 
  pthread_mutex_lock(&transactional_2_mutex);
  assert(numActiveXactions <= MAX_TRANSACTIONS);
  pthread_mutex_unlock(&transactional_2_mutex);
#endif

  lsn = LogTransCommit(&XactionTable[xid % MAX_TRANSACTIONS]);
  if(globalLockManager.commit) { globalLockManager.commit(xid); }

  allocTransactionCommit(xid);

  pthread_mutex_lock(&transactional_2_mutex);

  XactionTable[xid%MAX_TRANSACTIONS].xid = INVALID_XTABLE_XID;
  numActiveXactions--;
  assert( numActiveXactions >= 0 );
  pthread_mutex_unlock(&transactional_2_mutex);

  return 0;
}

int Tabort(int xid) {
  lsn_t lsn;
  assert(xid >= 0);

  TransactionLog * t =&XactionTable[xid%MAX_TRANSACTIONS];

  lsn = LogTransAbort(t /*&XactionTable[xid%MAX_TRANSACTIONS]*/);

  /** @todo is the order of the next two calls important? */
  undoTrans(*t/*XactionTable[xid%MAX_TRANSACTIONS]*/); 
  if(globalLockManager.abort) { globalLockManager.abort(xid); }

  allocTransactionAbort(xid);

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
			fprintf(stderr, "WARNING: Tdeinit() is aborting transaction %d\n", XactionTable[i].xid);
			Tabort(XactionTable[i].xid);
		}
	}
	assert( numActiveXactions == 0 );
	truncationDeinit();
	ThashDeinit();
	bufDeinit();
	pageDeinit();
	LogDeinit();
	dirtyPagesDeinit();
	return 0;
}

void Trevive(int xid, long lsn) {
  assert(xid >= 0);
  int index = xid % MAX_TRANSACTIONS;
  pthread_mutex_lock(&transactional_2_mutex);

  DEBUG("Reviving xid %d at lsn %ld\n", xid, lsn);
  
  if(XactionTable[index].xid != INVALID_XTABLE_XID) {
    if(xid != XactionTable[index].xid) {
      fprintf(stderr, "Clashing Tprepare()'ed XID's encountered on recovery!!\n");
      abort();
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

lsn_t transactions_minRecLSN() { 
  lsn_t minRecLSN = LSN_T_MAX; // LogFlushedLSN()
  pthread_mutex_lock(&transactional_2_mutex);
  for(int i = 0; i < MAX_TRANSACTIONS; i++) { 
    if(XactionTable[i].xid != INVALID_XTABLE_XID) { 
      lsn_t recLSN = XactionTable[i].recLSN;
      if(recLSN != -1 && recLSN < minRecLSN) { 
	minRecLSN = recLSN;
      }
    }
  }
  pthread_mutex_unlock(&transactional_2_mutex);
  return minRecLSN;
}

int TisActiveTransaction(int xid) { 
  if(xid > 0) { return 0; }
  pthread_mutex_lock(&transactional_2_mutex);
  int ret = xid != INVALID_XTABLE_XID && XactionTable[xid%MAX_TRANSACTIONS].xid == xid;
  pthread_mutex_unlock(&transactional_2_mutex);
  return ret;
}

