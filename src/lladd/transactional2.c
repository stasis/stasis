#include <config.h>
#include <lladd/common.h>
#include "latches.h"
#include <lladd/transactional.h>

#include <lladd/recovery.h>
#include "logger/logWriter.h"
#include <lladd/bufferManager.h>
#include <lladd/consumer.h>
#include <lladd/lockManager.h>
#include <lladd/compensations.h>

#include "page.h"
#include <lladd/logger/logger2.h>

#include <stdio.h>
#include <assert.h>
#include "page/indirect.h"

TransactionLog XactionTable[MAX_TRANSACTIONS];
int numActiveXactions = 0;
int xidCount = 0;

const recordid ROOT_RECORD = {1, 0, -1};
const recordid NULLRID = {0,0,-1};

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
	operationsTable[OPERATION_REALLOC]     = getRealloc();
	/*	operationsTable[OPERATION_PAGE_ALLOC] = getPageAlloc();
		operationsTable[OPERATION_PAGE_DEALLOC] = getPageDealloc(); */
	operationsTable[OPERATION_PAGE_SET] = getPageSet();

	operationsTable[OPERATION_UPDATE_FREESPACE]         = getUpdateFreespace();
	operationsTable[OPERATION_UPDATE_FREESPACE_INVERSE] = getUpdateFreespaceInverse();
	operationsTable[OPERATION_UPDATE_FREELIST]          = getUpdateFreelist();
	operationsTable[OPERATION_UPDATE_FREELIST_INVERSE] = getUpdateFreelistInverse();
	
	operationsTable[OPERATION_FREE_PAGE] = getFreePageOperation();
	operationsTable[OPERATION_ALLOC_FREED] = getAllocFreedPage();
	operationsTable[OPERATION_UNALLOC_FREED] = getUnallocFreedPage();
	operationsTable[OPERATION_NOOP] = getNoop();
	operationsTable[OPERATION_INSTANT_SET] = getInstantSet();
	operationsTable[OPERATION_ARRAY_LIST_ALLOC]  = getArrayListAlloc();
	operationsTable[OPERATION_INITIALIZE_FIXED_PAGE] = getInitFixed();
	operationsTable[OPERATION_UNINITIALIZE_PAGE] = getUnInitPage();

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
         
        pthread_mutex_init(&transactional_2_mutex, NULL);

        setupOperationsTable();
	
	bufInit();

	openLogWriter();

	//	try_ret(compensation_error()) { 
	  pageOperationsInit();
	  //	} end_ret(compensation_error());
	initNestedTopActions();
	ThashInit();
	LinearHashNTAInit();
	LinkedListNTAInit();
	compensations_init();
	iterator_init();
	consumer_init();
	setupLockManagerCallbacksNil();
	//setupLockManagerCallbacksPage();
	
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

	if(globalLockManager.begin) { globalLockManager.begin(XactionTable[index].xid); }

	return XactionTable[index].xid;
}

static compensated_function void TupdateHelper(int xid, recordid rid, const void * dat, int op, Page * p) {
  LogEntry * e;

  //  try { 
    if(globalLockManager.writeLockPage) {
      globalLockManager.writeLockPage(xid, rid.page);
    }
    //  } end;

    
  e = LogUpdate(&XactionTable[xid % MAX_TRANSACTIONS], p, rid, op, dat);
  
  assert(XactionTable[xid % MAX_TRANSACTIONS].prevLSN == e->LSN);
  
  DEBUG("T update() e->LSN: %ld\n", e->LSN);
  
  doUpdate(e, p);

  free(e);
}

compensated_function void Tupdate(int xid, recordid rid, const void *dat, int op) {
  Page * p = 0;  
#ifdef DEBUGGING
  pthread_mutex_lock(&transactional_2_mutex);
  assert(numActiveXactions <= MAX_TRANSACTIONS);
  pthread_mutex_unlock(&transactional_2_mutex);
#endif
  //  try { 
    p = loadPage(xid, rid.page);
    //  } end;
  if(*page_type_ptr(p) == INDIRECT_PAGE) {
    releasePage(p);
    //    try { 
      rid = dereferenceRID(xid, rid);
      p = loadPage(xid, rid.page); 
      //    } end;
    /** @todo Kludge! Shouldn't special case operations in transactional2. */
  } else if(*page_type_ptr(p) == ARRAY_LIST_PAGE && 
	    op != OPERATION_LINEAR_INSERT && 
	    op != OPERATION_UNDO_LINEAR_INSERT &&
	    op != OPERATION_LINEAR_DELETE && 
	    op != OPERATION_UNDO_LINEAR_DELETE  ) {
    rid = dereferenceArrayListRid(p, rid.slot);
    releasePage(p);
    //    try { 
      p = loadPage(xid, rid.page); 
      //    } end;
  } 

  /** @todo For logical undo logs, grabbing a lock makes no sense! */
  //  begin_action(releasePage, p) { 
    TupdateHelper(xid, rid, dat, op, p);
    /*    if(globalLockManager.writeLockPage) {
      globalLockManager.writeLockPage(xid, rid.page);
      }
      
      e = LogUpdate(&XactionTable[xid % MAX_TRANSACTIONS], p, rid, op, dat);
      
      } en d_action;
      
      assert(XactionTable[xid % MAX_TRANSACTIONS].prevLSN == e->LSN);
      
      DEBUG("Tupdate() e->LSN: %ld\n", e->LSN);
      
      doUpdate(e, p); 
      releasePage(p);*/
    //  } compensate;
    releasePage(p);

}

compensated_function void alTupdate(int xid, recordid rid, const void *dat, int op) {
  Page * p = 0;
  //  try {
    p = loadPage(xid, rid.page);
    //  } end;
  
  //  begin_action(releasePage, p) {
  TupdateHelper(xid, rid, dat, op, p);
    //  } compensate;
  releasePage(p);

}


void TreadUnlocked(int xid, recordid rid, void * dat) {
  Page * p = 0;
  //  try { 
    p = loadPage(xid, rid.page);
    //  } end;
  int page_type = *page_type_ptr(p);
  if(page_type == SLOTTED_PAGE  || page_type == FIXED_PAGE || !page_type ) {

  } else if(page_type == INDIRECT_PAGE) {
    releasePage(p);

    //    try {
      rid = dereferenceRIDUnlocked(xid, rid);
      p = loadPage(xid, rid.page);
      //    } end;
  } else if(page_type == ARRAY_LIST_PAGE) {
    rid = dereferenceArrayListRidUnlocked(p, rid.slot);
    releasePage(p);
    //    try { 
      p = loadPage(xid, rid.page);
      //    } end;

  } else {
    abort();
  }
  readRecordUnlocked(xid, p, rid, dat);
  releasePage(p);
}

compensated_function void Tread(int xid, recordid rid, void * dat) {
  Page * p = 0;

  p = loadPage(xid, rid.page);
  int page_type = *page_type_ptr(p);
  if(page_type == SLOTTED_PAGE  || page_type == FIXED_PAGE || !page_type ) {

  } else if(page_type == INDIRECT_PAGE) {
    releasePage(p);
    rid = dereferenceRID(xid, rid);
    p = loadPage(xid, rid.page);


  } else if(page_type == ARRAY_LIST_PAGE) {
   
    rid = dereferenceArrayListRid(p, rid.slot);
    releasePage(p);
    p = loadPage(xid, rid.page);

  } else {
    abort();
  }
  readRecord(xid, p, rid, dat);
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
	ThashDeinit();
	bufDeinit();
	closeLogWriter();

	return 0;
}

void Trevive(int xid, long lsn) {
  int index = xid % MAX_TRANSACTIONS;
  pthread_mutex_lock(&transactional_2_mutex);

  DEBUG("Reviving xid %d at lsn %ld\n", xid, lsn);
  
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
