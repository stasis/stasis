#include <config.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <lladd/common.h>
#include "latches.h"
#include <lladd/transactional.h>
#include <lladd/recovery.h>
#include <lladd/bufferManager.h>
#include <lladd/consumer.h>
#include <lladd/lockManager.h>
#include <lladd/compensations.h>
#ifdef USE_PAGEFILE
#include "pageFile.h"
#endif
#include <lladd/pageHandle.h>
#include "page.h"
#include <lladd/logger/logger2.h>
#include <lladd/truncation.h>
#include <lladd/io/handle.h>
#include <stdio.h>
#include <assert.h>
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
	// @todo clean out unused constants...
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
	
	//operationsTable[OPERATION_SET_RAW] = getSetRaw();
	//operationsTable[OPERATION_INSTANT_SET_RAW] = getInstantSetRaw();

	operationsTable[OPERATION_ALLOC_BOUNDARY_TAG] = getAllocBoundaryTag();

	operationsTable[OPERATION_FIXED_PAGE_ALLOC] = getFixedPageAlloc();

	operationsTable[OPERATION_ALLOC_REGION] = getAllocRegion();
	operationsTable[OPERATION_DEALLOC_REGION] = getDeallocRegion();

}

// @todo this factory stuff doesn't really belong here...
static stasis_handle_t * fast_factory(lsn_t off, lsn_t len, void * ignored) { 
  stasis_handle_t * h = stasis_handle(open_memory)(off);
  //h = stasis_handle(open_debug)(h);
  stasis_write_buffer_t * w = h->append_buffer(h, len);
  w->h->release_write_buffer(w);
  return h;
}
typedef struct sf_args {
  char * filename;
  int    openMode;
  int    filePerm;
} sf_args;
static stasis_handle_t * slow_factory(void * argsP) { 
  sf_args * args = (sf_args*) argsP;
  stasis_handle_t * h =  stasis_handle(open_file)(0, args->filename, args->openMode, args->filePerm);
  //h = stasis_handle(open_debug)(h);
  return h;
}
int Tinit() {
        pthread_mutex_init(&transactional_2_mutex, NULL);
	numActiveXactions = 0;

	compensations_init();

        setupOperationsTable();
	dirtyPagesInit();
	LogInit(loggerType);
	pageInit();
#ifndef USE_PAGEFILE
	struct sf_args * slow_arg = malloc(sizeof(sf_args));
	slow_arg->filename = STORE_FILE;
#ifdef PAGE_FILE_O_DIRECT
	slow_arg->openMode = O_CREAT | O_RDWR | O_DIRECT;
#else
	slow_arg->openMode = O_CREAT | O_RDWR;
#endif
	slow_arg->filePerm = FILE_PERM;
	// Allow 4MB of outstanding writes.
        // @todo Where / how should we open storefile?
        stasis_handle_t * pageFile = 
         stasis_handle(open_non_blocking)(slow_factory, slow_arg, fast_factory,
                                          NULL, 20, PAGE_SIZE * 1024, 1024);
        pageHandleOpen(pageFile);
#else
        printf("\nWarning: Using old I/O routines.\n");
        openPageFile();
#endif // USE_PAGEFILE
	bufInit(bufferManagerType);
        DEBUG("Buffer manager type = %d\n", bufferManagerType);
	pageOperationsInit();
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

static compensated_function void TactionHelper(int xid, recordid rid, 
					       const void * dat, int op, 
					       Page * p, int deferred) {
  LogEntry * e;
  assert(xid >= 0);
  try { 
    if(globalLockManager.writeLockPage) {
      globalLockManager.writeLockPage(xid, rid.page);
    }
  } end;
  if(! deferred) { 
    e = LogUpdate(&XactionTable[xid % MAX_TRANSACTIONS], p, rid, op, dat);
    assert(XactionTable[xid % MAX_TRANSACTIONS].prevLSN == e->LSN);
    DEBUG("Tupdate() e->LSN: %ld\n", e->LSN);
    doUpdate(e, p);
    FreeLogEntry(e);
  } else { 
    e = LogDeferred(&XactionTable[xid % MAX_TRANSACTIONS], p, rid, op, dat);
    assert(XactionTable[xid % MAX_TRANSACTIONS].prevLSN == e->LSN);    
    DEBUG("Deferring e->LSN: %ld\n", e->LSN);
    // XXX update XactionTable...
    //XXX deferred_push(e);
  }


}

static recordid resolveForUpdate(int xid, Page * p, recordid rid) { 
  
  if(*page_type_ptr(p) == INDIRECT_PAGE) { 
    rid = dereferenceRID(xid, rid);
  } else if(*page_type_ptr(p) == ARRAY_LIST_PAGE) { 
    rid = dereferenceArrayListRid(xid, p, rid.slot);
  }
  return rid;
}

compensated_function void TupdateRaw(int xid, recordid rid, 
				     const void * dat, int op) { 
  assert(xid >= 0);
  Page * p = loadPage(xid, rid.page);
  TactionHelper(xid, rid, dat, op, p, 0); // 0 -> not deferred
  releasePage(p);
}

compensated_function void TupdateStr(int xid, recordid rid, 
                                     const char *dat, int op) {
  Tupdate(xid, rid, dat, op);
}

compensated_function void Tupdate(int xid, recordid rid, 
				  const void *dat, int op) { 
  Page * p = loadPage(xid, rid.page);
  rid = resolveForUpdate(xid, p, rid);
  
  if(p->id != rid.page) { 
    releasePage(p);
    p = loadPage(xid, rid.page);
  }
  
  TactionHelper(xid, rid, dat, op, p, 0); // 0 -> not deferred
  releasePage(p);
}

compensated_function void Tdefer(int xid, recordid rid, 
				 const void * dat, int op) {

  Page * p = loadPage(xid, rid.page);
  recordid newrid = resolveForUpdate(xid, p, rid);
  // Caller cannot rely on late or early binding of rid.
  assert(rid.page == newrid.page && 
	 rid.slot == newrid.slot && 
	 rid.size == newrid.size);  
  TactionHelper(xid, rid, dat, op, p, 1); // 1 -> deferred.
  releasePage(p);
}

compensated_function void TreadStr(int xid, recordid rid, char * dat) {
  Tread(xid, rid, dat);
}

compensated_function void Tread(int xid, recordid rid, void * dat) {
  Page * p;
  try { 
    p = loadPage(xid, rid.page);
  } end;

  rid = recordDereference(xid, p, rid);
  if(rid.page != p->id) { 
    releasePage(p);
    p = loadPage(xid, rid.page);
  }
  recordRead(xid, p, rid, dat);
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

  lsn = LogTransAbort(t);

  /** @todo is the order of the next two calls important? */
  undoTrans(*t);
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
        DEBUG("Closing page file tdeinit\n");
	closePageFile();
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
  lsn_t minRecLSN = LSN_T_MAX;
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
  if(xid < 0) { return 0; }
  pthread_mutex_lock(&transactional_2_mutex);
  int ret = xid != INVALID_XTABLE_XID && XactionTable[xid%MAX_TRANSACTIONS].xid == xid;
  pthread_mutex_unlock(&transactional_2_mutex);
  return ret;
}

int TdurabilityLevel() {
  if(bufferManagerType == BUFFER_MANAGER_MEM_ARRAY) { 
    return VOLATILE;
  } else if(loggerType == LOG_TO_MEMORY) { 
    return PERSISTENT;
  } else { 
    return DURABLE;
  }
}
