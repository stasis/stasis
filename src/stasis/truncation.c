#include <limits.h>
#include <stasis/truncation.h>
#include <pbl/pbl.h>
#include <stasis/logger/logger2.h>
#include "page.h"
#include <assert.h>

static int initialized = 0;
static int automaticallyTuncating = 0;
static pthread_t truncationThread;

static pthread_mutex_t shutdown_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  shutdown_cond  = PTHREAD_COND_INITIALIZER;

static pblHashTable_t * dirtyPages = 0;
static pthread_mutex_t dirtyPages_mutex = PTHREAD_MUTEX_INITIALIZER;

#ifdef LONG_TEST
#define TARGET_LOG_SIZE (1024 * 1024 * 5)
#define TRUNCATE_INTERVAL 1
#define MIN_INCREMENTAL_TRUNCATION (1024 * 1024 * 1)
#else 
#define TARGET_LOG_SIZE (1024 * 1024 * 50)
#define TRUNCATE_INTERVAL 1
#define MIN_INCREMENTAL_TRUNCATION (1024 * 1024 * 25)
#endif
void dirtyPages_add(Page * p) {
  pthread_mutex_lock(&dirtyPages_mutex);
  if(!p->dirty) { 
    p->dirty = 1;
    //assert(p->LSN);
    void* ret = pblHtLookup(dirtyPages, &(p->id), sizeof(int));
    assert(!ret);
    lsn_t * insert = malloc(sizeof(lsn_t));
    *insert = p->LSN;
    pblHtInsert(dirtyPages, &(p->id), sizeof(int), insert); //(void*)p->LSN);
  }
  pthread_mutex_unlock(&dirtyPages_mutex);
}
 
void dirtyPages_remove(Page * p) { 
  pthread_mutex_lock(&dirtyPages_mutex);
  //  printf("Removing page %d\n", p->id);
  //assert(pblHtLookup(dirtyPages, &(p->id), sizeof(int)));
  //  printf("With lsn = %d\n", (lsn_t)pblHtCurrent(dirtyPages));
  p->dirty = 0;
  pblHtRemove(dirtyPages, &(p->id), sizeof(int));
  //assert(!ret); <--- Due to a bug in the PBL compatibility mode,
  //there is no way to tell whether the value didn't exist, or if it
  //was null.
  pthread_mutex_unlock(&dirtyPages_mutex);
}

int dirtyPages_isDirty(Page * p) { 
  int ret;
  pthread_mutex_lock(&dirtyPages_mutex);
  ret = p->dirty;
  pthread_mutex_unlock(&dirtyPages_mutex);
  return ret;
}

static lsn_t dirtyPages_minRecLSN() { 
  lsn_t lsn = LSN_T_MAX; // LogFlushedLSN ();
  int* pageid;
  pthread_mutex_lock(&dirtyPages_mutex);

  for( pageid = (int*)pblHtFirst (dirtyPages); pageid; pageid = (int*)pblHtNext(dirtyPages)) { 
    lsn_t * thisLSN = (lsn_t*) pblHtCurrent(dirtyPages);
    //    printf("lsn = %d\n", thisLSN);
    if(*thisLSN < lsn) { 
      lsn = *thisLSN;
    }
  }
  pthread_mutex_unlock(&dirtyPages_mutex);

  return lsn;
}

static void dirtyPages_flush() { 
  // XXX Why was this MAX_BUFFER_SIZE+1?!?
  int * staleDirtyPages = malloc(sizeof(int) * (MAX_BUFFER_SIZE));
  int i;
  for(i = 0; i < MAX_BUFFER_SIZE; i++) { 
    staleDirtyPages[i] = -1;
  }
  Page* p = 0;
  pthread_mutex_lock(&dirtyPages_mutex);
  void* tmp;
  i = 0;
  
  for(tmp = pblHtFirst(dirtyPages); tmp; tmp = pblHtNext(dirtyPages)) { 
    staleDirtyPages[i] = *((int*) pblHtCurrentKey(dirtyPages));
    i++;
  }
  assert(i < MAX_BUFFER_SIZE);
  pthread_mutex_unlock(&dirtyPages_mutex);

  for(i = 0; i < MAX_BUFFER_SIZE && staleDirtyPages[i] != -1; i++) {
    p = loadPage(-1, staleDirtyPages[i]);
    writeBackPage(p);
    releasePage(p);
  }
  free(staleDirtyPages);
}

void dirtyPagesInit() { 
  dirtyPages = pblHtCreate();
}


void dirtyPagesDeinit() { 
  void * tmp;
  int areDirty = 0;
  for(tmp = pblHtFirst(dirtyPages); tmp; tmp = pblHtNext(dirtyPages)) {
    free(pblHtCurrent(dirtyPages));
    if((!areDirty) &&
       (!stasis_suppress_unclean_shutdown_warnings)) {
      printf("Warning:  dirtyPagesDeinit detected dirty, unwritten pages.  "
	     "Updates lost?\n");
      areDirty = 1;
    }
  }
  pblHtDelete(dirtyPages);
  dirtyPages = 0;
}
void truncationInit() { 
  initialized = 1;
}

void truncationDeinit() { 
  pthread_mutex_lock(&shutdown_mutex);
  initialized = 0;
  if(automaticallyTuncating) {
    void * ret = 0;
    pthread_mutex_unlock(&shutdown_mutex);
    pthread_cond_broadcast(&shutdown_cond);
    pthread_join(truncationThread, &ret);
  } else { 
    pthread_mutex_unlock(&shutdown_mutex);
  }
  automaticallyTuncating = 0;
}

static void* periodicTruncation(void * ignored) { 
  pthread_mutex_lock(&shutdown_mutex);
  while(initialized) { 
    if(LogFlushedLSN() - LogTruncationPoint() > TARGET_LOG_SIZE) {
      truncateNow();
    }
    struct timeval now;
    struct timespec timeout;
    int timeret = gettimeofday(&now, 0);
    assert(0 == timeret);
    
    timeout.tv_sec = now.tv_sec;
    timeout.tv_nsec = now.tv_usec;
    timeout.tv_sec += TRUNCATE_INTERVAL;
    
    pthread_cond_timedwait(&shutdown_cond, &shutdown_mutex, &timeout);
  }
  pthread_mutex_unlock(&shutdown_mutex);
  return (void*)0;
}

void autoTruncate() { 
  assert(!automaticallyTuncating);
  automaticallyTuncating = 1;
  pthread_create(&truncationThread, 0, &periodicTruncation, 0);
}


int truncateNow() { 
  

  // *_minRecLSN() used to return the same value as flushed if
  //there were no outstanding transactions, but flushed might
  //not point to the front of a log entry...  now, both return
  //LSN_T_MAX if there are no outstanding transactions / no
  //dirty pages.
  
  lsn_t page_rec_lsn = dirtyPages_minRecLSN();
  lsn_t xact_rec_lsn = transactions_minRecLSN();
  lsn_t flushed_lsn  = LogFlushedLSN();

  lsn_t rec_lsn = page_rec_lsn < xact_rec_lsn ? page_rec_lsn : xact_rec_lsn;
  rec_lsn = (rec_lsn < flushed_lsn) ? rec_lsn : flushed_lsn;

  lsn_t log_trunc = LogTruncationPoint();
  if((xact_rec_lsn - log_trunc) > MIN_INCREMENTAL_TRUNCATION) { 
    //fprintf(stderr, "xact = %ld \t log = %ld\n", xact_rec_lsn, log_trunc);
    if((rec_lsn - log_trunc) > MIN_INCREMENTAL_TRUNCATION) { 
      //      fprintf(stderr, "Truncating now. rec_lsn = %ld, log_trunc = %ld\n", rec_lsn, log_trunc);
      //      fprintf(stderr, "Truncating to rec_lsn = %ld\n", rec_lsn);
      forcePages();
      LogTruncate(rec_lsn);
      return 1;
    } else { 
      lsn_t flushed = LogFlushedLSN();
      if(flushed - log_trunc > 2 * TARGET_LOG_SIZE) { 
	//fprintf(stderr, "Flushing dirty buffers: rec_lsn = %ld log_trunc = %ld flushed = %ld\n", rec_lsn, log_trunc, flushed);
	dirtyPages_flush();
	
	page_rec_lsn = dirtyPages_minRecLSN();
	rec_lsn = page_rec_lsn < xact_rec_lsn ? page_rec_lsn : xact_rec_lsn;
	rec_lsn = (rec_lsn < flushed_lsn) ? rec_lsn : flushed_lsn;
	
	//fprintf(stderr, "Flushed Dirty Buffers.  Truncating to rec_lsn = %ld\n", rec_lsn);

	forcePages();
	LogTruncate(rec_lsn);
	return 1;

      } else { 
	return 0;
      }
    }
  } else {
    return 0;
  }
}
