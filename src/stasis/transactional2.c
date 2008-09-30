#include <config.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <stasis/common.h>
#include <stasis/latches.h>
#include <stasis/transactional.h>
#include <stasis/recovery.h>
#include <stasis/bufferManager.h>
#include <stasis/consumer.h>
#include <stasis/lockManager.h>
#include <stasis/compensations.h>
#include <stasis/pageFile.h>
#include <stasis/pageHandle.h>
#include <stasis/page.h>
#include <stasis/logger/logger2.h>
#include <stasis/truncation.h>
#include <stasis/io/handle.h>
#include <stasis/blobManager.h> // XXX remove this, move Tread() to set.c
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
	operationsTable[OPERATION_SET_INVERSE] = getSetInverse();
	operationsTable[OPERATION_INCREMENT] = getIncrement();
	operationsTable[OPERATION_DECREMENT] = getDecrement();
	operationsTable[OPERATION_ALLOC]     = getAlloc();
	//	operationsTable[OPERATION_PREPARE]   = getPrepare();
	/*	operationsTable[OPERATION_LHINSERT]  = getLHInsert(); 
		operationsTable[OPERATION_LHREMOVE]  = getLHRemove(); */
	operationsTable[OPERATION_DEALLOC]     = getDealloc();
	operationsTable[OPERATION_REALLOC]     = getRealloc();

	operationsTable[OPERATION_PAGE_SET_RANGE] = getPageSetRange();
	operationsTable[OPERATION_PAGE_SET_RANGE_INVERSE] = getPageSetRangeInverse();

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
	operationsTable[OPERATION_INITIALIZE_PAGE] = getInitializePage();

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
	operationsTable[OPERATION_ALLOC_REGION_INVERSE] = getAllocRegionInverse();

	operationsTable[OPERATION_DEALLOC_REGION] = getDeallocRegion();
	operationsTable[OPERATION_DEALLOC_REGION_INVERSE] = getDeallocRegionInverse();

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
static stasis_handle_t * slow_file_factory(void * argsP) { 
  sf_args * args = (sf_args*) argsP;
  stasis_handle_t * h =  stasis_handle(open_file)(0, args->filename, args->openMode, args->filePerm);
  //h = stasis_handle(open_debug)(h);
  return h;
}
static stasis_handle_t * slow_pfile_factory(void * argsP) {
  stasis_handle_t * h = argsP;
  return h;
}
static int (*slow_close)(stasis_handle_t * h) = 0;
static stasis_handle_t * slow_pfile = 0;
static int nop_close(stasis_handle_t*h) { return 0; }

int Tinit() {
        pthread_mutex_init(&transactional_2_mutex, NULL);
	numActiveXactions = 0;

	compensations_init();

        setupOperationsTable();
	dirtyPagesInit();
	LogInit(loggerType);
	stasis_page_init();

#ifndef HAVE_O_DIRECT
	if(bufferManagerO_DIRECT) {
	  printf("O_DIRECT not supported by this build; switching to conventional buffered I/O.\n");
	  bufferManagerO_DIRECT = 0;
	}
#endif
	int openMode;
	if(bufferManagerO_DIRECT) {
#ifdef HAVE_O_DIRECT
	  openMode = O_CREAT | O_RDWR | O_DIRECT;
#else
              printf("Can't happen\n");
              abort();
#endif
	} else {
	  openMode = O_CREAT | O_RDWR;
	}
        switch(bufferManagerFileHandleType) {
          case BUFFER_MANAGER_FILE_HANDLE_NON_BLOCKING: {
            struct sf_args * slow_arg = malloc(sizeof(sf_args));
            slow_arg->filename = STORE_FILE;

	    slow_arg->openMode = openMode;

            slow_arg->filePerm = FILE_PERM;
            // Allow 4MB of outstanding writes.
            // @todo Where / how should we open storefile?
            stasis_handle_t * pageFile;
            int worker_thread_count = 4;
            if(bufferManagerNonBlockingSlowHandleType == IO_HANDLE_PFILE) {
              //              printf("\nusing pread()/pwrite()\n");
              slow_pfile = stasis_handle_open_pfile(0, slow_arg->filename, slow_arg->openMode, slow_arg->filePerm);
              slow_close = slow_pfile->close;
              slow_pfile->close = nop_close;
              pageFile =
		stasis_handle(open_non_blocking)(slow_pfile_factory, slow_pfile, 1, fast_factory,
						 NULL, worker_thread_count, PAGE_SIZE * 1024 , 1024);

            } else if(bufferManagerNonBlockingSlowHandleType == IO_HANDLE_FILE) {
              pageFile =
		stasis_handle(open_non_blocking)(slow_file_factory, slow_arg, 0, fast_factory,
						 NULL, worker_thread_count, PAGE_SIZE * 1024, 1024);
            } else {
              printf("Unknown value for config option bufferManagerNonBlockingSlowHandleType\n");
              abort();
            }
            //pageFile = stasis_handle(open_debug)(pageFile);
            pageHandleOpen(pageFile);
          } break;
	  case BUFFER_MANAGER_FILE_HANDLE_FILE: {
	    stasis_handle_t * pageFile =
	      stasis_handle_open_file(0, STORE_FILE, openMode, FILE_PERM);
	    pageHandleOpen(pageFile);
	  } break;
	  case BUFFER_MANAGER_FILE_HANDLE_PFILE: {
	    stasis_handle_t * pageFile =
	      stasis_handle_open_pfile(0, STORE_FILE, openMode, FILE_PERM);
	    pageHandleOpen(pageFile);
	  } break;
	  case BUFFER_MANAGER_FILE_HANDLE_DEPRECATED: { 
            printf("\nWarning: Using old I/O routines (with known bugs).\n");
            openPageFile();
          } break;
          default: {
            printf("\nUnknown buffer manager filehandle type: %d\n",
                   bufferManagerFileHandleType);
            abort();
          }
        }
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
	if(stasis_truncation_automatic) {
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
					       const void * dat, size_t datlen, int op,
					       Page * p) {
  LogEntry * e;
  assert(xid >= 0);
  try { 
    if(globalLockManager.writeLockPage) {
      globalLockManager.writeLockPage(xid, rid.page);
    }
  } end;

  writelock(p->rwlatch,0);

  e = LogUpdate(&XactionTable[xid % MAX_TRANSACTIONS], p, op, dat, datlen);
  assert(XactionTable[xid % MAX_TRANSACTIONS].prevLSN == e->LSN);
  DEBUG("Tupdate() e->LSN: %ld\n", e->LSN);
  doUpdate(e, p);
  FreeLogEntry(e);

  unlock(p->rwlatch);
}

// XXX remove this function once it's clear that nobody is failing the assert in Tupdate()
compensated_function void TupdateRaw(int xid, recordid rid, const void * dat, size_t datlen,
                                     int op) {
  assert(xid >= 0);
  Page * p = loadPage(xid, rid.page);
  TactionHelper(xid, rid, dat, datlen, op, p);
  releasePage(p);
}

compensated_function void TupdateStr(int xid, recordid rid,
                                     const char *dat, size_t datlen, int op) {
  Tupdate(xid, rid, dat, datlen, op);
}

compensated_function void Tupdate(int xid, recordid rid, 
				  const void *dat, size_t datlen, int op) { 
  Page * p = loadPage(xid, rid.page);
  readlock(p->rwlatch,0);
  recordid rid2 = stasis_record_dereference(xid, p, rid);
  assert(rid2.page == rid.page);
  unlock(p->rwlatch);

  TactionHelper(xid, rid, dat, datlen, op, p);
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

  readlock(p->rwlatch,0);

  rid = stasis_record_dereference(xid, p, rid);
  if(rid.page != p->id) { 
    unlock(p->rwlatch);
    releasePage(p);
    p = loadPage(xid, rid.page);
    readlock(p->rwlatch,0);
  }
  if(rid.size > BLOB_THRESHOLD_SIZE) {
    DEBUG("call readBlob %lld %lld %lld\n", (long long)rid.page, (long long)rid.slot, (long long)rid.size);
    readBlob(xid,p,rid,dat);
    assert(rid.page == p->id);
  } else {
    stasis_record_read(xid, p, rid, dat);
  }
  unlock(p->rwlatch);
  releasePage(p);
}

compensated_function void TreadRaw(int xid, recordid rid, void * dat) {
  Page * p;
  try { 
    p = loadPage(xid, rid.page);
  } end;
  readlock(p->rwlatch,0);
  stasis_record_read(xid, p, rid, dat);
  unlock(p->rwlatch);
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

int Tprepare(int xid) {
  assert(xid >= 0);
  off_t i = xid % MAX_TRANSACTIONS;
  assert(XactionTable[i].xid == xid);
  LogTransPrepare(&XactionTable[i]);
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
      if(!stasis_suppress_unclean_shutdown_warnings) {
	fprintf(stderr, "WARNING: Tdeinit() is aborting transaction %d\n",
		XactionTable[i].xid);
      }
      Tabort(XactionTable[i].xid);
    }
  }
  assert( numActiveXactions == 0 );
  truncationDeinit();
  ThashDeinit();
  TallocDeinit();
  deinitNestedTopActions();
  bufDeinit();
  DEBUG("Closing page file tdeinit\n");
  closePageFile();
  if(slow_pfile) {
    slow_close(slow_pfile);
    slow_pfile = 0;
    slow_close = 0;
  }
  stasis_page_deinit();
  LogDeinit();
  dirtyPagesDeinit();
  return 0;
}

int TuncleanShutdown() {
  // We're simulating a crash; don't complain when writes get lost,
  // and active transactions get rolled back.
  stasis_suppress_unclean_shutdown_warnings = 1;
  truncationDeinit();
  ThashDeinit();
  simulateBufferManagerCrash();
  if(slow_pfile) {
    slow_close(slow_pfile);
    slow_pfile = 0;
    slow_close = 0;
  }
  stasis_page_deinit();
  LogDeinit();
  numActiveXactions = 0;
  dirtyPagesDeinit();

  // Reset it here so the warnings will appear if a new stasis
  // instance encounters problems during a clean shutdown.
  stasis_suppress_unclean_shutdown_warnings = 0;
  return 0;
}

void Trevive(int xid, lsn_t prevlsn, lsn_t reclsn) {
  assert(xid >= 0);
  assert(reclsn != -1);
  int index = xid % MAX_TRANSACTIONS;
  pthread_mutex_lock(&transactional_2_mutex);

  DEBUG("Reviving xid %d at lsn %ld\n", xid, lsn);
  
  if(XactionTable[index].xid != INVALID_XTABLE_XID) {
    abort();
    /*    if(xid != XactionTable[index].xid) {
      fprintf(stderr, "Clashing Tprepare()'ed XID's encountered on recovery!!\n");
      abort();
    }
    assert(XactionTable[index].xid == xid);
    assert(XactionTable[index].prevLSN == lsn); */
  } else {
    XactionTable[index].xid = xid;
    XactionTable[index].prevLSN = prevlsn;
    XactionTable[index].recLSN = reclsn;
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

int* TlistActiveTransactions() {
  pthread_mutex_lock(&transactional_2_mutex);
  int * ret = malloc(sizeof(*ret));
  ret[0] = 0;
  int retcount = 0;
  for(int i = 0; i < MAX_TRANSACTIONS; i++) {
    if(XactionTable[i].xid != INVALID_XTABLE_XID) {
      ret[retcount] = XactionTable[i].xid;
      retcount++;
      ret = realloc(ret, (retcount+1) * sizeof(*ret));
      ret[retcount] = 0;
    }
  }
  pthread_mutex_unlock(&transactional_2_mutex);
  return ret;
}
int TisActiveTransaction(int xid) { 
  if(xid < 0) { return 0; }
  pthread_mutex_lock(&transactional_2_mutex);
  int ret = xid != INVALID_XTABLE_XID && XactionTable[xid%MAX_TRANSACTIONS].xid == xid;
  pthread_mutex_unlock(&transactional_2_mutex);
  return ret;
}

int stasis_transaction_table_set_prev_lsn(int xid, lsn_t prevLSN) {
  assert(XactionTable[xid%xidCount%MAX_TRANSACTIONS].xid == xid);
  XactionTable[xid%MAX_TRANSACTIONS].prevLSN = prevLSN;
  return 0;
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
