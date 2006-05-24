#include <lladd/truncation.h>
#include <pbl/pbl.h>
#include <lladd/logger/logger2.h>
#include "page.h"
#include <assert.h>
#include "pageFile.h"

volatile static int initialized = 0;
static int automaticallyTuncating = 0;
static pthread_t truncationThread;

static pblHashTable_t * dirtyPages = 0;
static pthread_mutex_t dirtyPages_mutex = PTHREAD_MUTEX_INITIALIZER;

int lladd_enableAutoTruncation = 1;
#define TARGET_LOG_SIZE (1024 * 1024 * 50)
#define TRUNCATE_INTERVAL 1
#define MIN_INCREMENTAL_TRUNCATION (1024 * 1024 * 10)
void dirtyPages_add(Page * p) {
  pthread_mutex_lock(&dirtyPages_mutex);
  if(!p->dirty) { 
    p->dirty = 1;
    //assert(p->LSN);
    void* ret = pblHtLookup(dirtyPages, &(p->id), sizeof(int));
    assert(!ret);
    pblHtInsert(dirtyPages, &(p->id), sizeof(int), (void*)p->LSN);
  }
  pthread_mutex_unlock(&dirtyPages_mutex);
}
 
void dirtyPages_remove(Page * p) { 
  pthread_mutex_lock(&dirtyPages_mutex);
  //  printf("Removing page %d\n", p->id);
  //assert(pblHtLookup(dirtyPages, &(p->id), sizeof(int)));
  //  printf("With lsn = %d\n", (lsn_t)pblHtCurrent(dirtyPages));
  int ret = pblHtRemove(dirtyPages, &(p->id), sizeof(int));
  //assert(!ret); <--- Due to a bug in the PBL compatibility mode,
  //there is no way to tell whether the value didn't exist, or if it
  //was null.
  pthread_mutex_unlock(&dirtyPages_mutex);
}

static lsn_t dirtyPages_minRecLSN() { 
  lsn_t lsn = LogFlushedLSN ();
  int* pageid;
  pthread_mutex_lock(&dirtyPages_mutex);

  for( pageid = (int*)pblHtFirst (dirtyPages); pageid; pageid = (int*)pblHtNext(dirtyPages)) { 
    lsn_t thisLSN = (lsn_t) pblHtCurrent(dirtyPages);
    //    printf("lsn = %d\n", thisLSN);
    if(thisLSN < lsn) { 
      lsn = thisLSN;
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
    //if(p->dirty) { 
    pageWrite(p);
      //      dirtyPages_remove(p);
      //}
    releasePage(p);
  }
  free(staleDirtyPages);
}

void dirtyPagesInit() { 
  dirtyPages = pblHtCreate();
}


void dirtyPagesDeinit() { 
  pblHtDelete(dirtyPages);
  dirtyPages = 0;
}
void truncationInit() { 
  initialized = 1;
}

void truncationDeinit() { 
  initialized = 0;
  if(automaticallyTuncating) {
    void * ret = 0;
    pthread_join(truncationThread, &ret);
  }
  automaticallyTuncating = 0;
}

static void* periodicTruncation(void * ignored) { 
  while(initialized) { 
    if(LogFlushedLSN() - LogTruncationPoint() > TARGET_LOG_SIZE) {
      truncateNow();
    }
    // @todo TRUNCATE_INTERVAL should be dynamically set...
    sleep(TRUNCATE_INTERVAL);
  }
  return (void*)0;
}

void autoTruncate() { 
  assert(!automaticallyTuncating);
  automaticallyTuncating = 1;
  pthread_create(&truncationThread, 0, &periodicTruncation, 0);
}


int truncateNow() { 
  lsn_t page_rec_lsn = dirtyPages_minRecLSN();
  lsn_t xact_rec_lsn = transactions_minRecLSN();
  lsn_t rec_lsn = page_rec_lsn < xact_rec_lsn ? page_rec_lsn : xact_rec_lsn;
  lsn_t log_trunc = LogTruncationPoint();
  if((rec_lsn - log_trunc) > MIN_INCREMENTAL_TRUNCATION) { 
    printf("Truncating now. rec_lsn = %ld, log_trunc = %ld\n", rec_lsn, log_trunc);
    LogTruncate(rec_lsn);
    return 1;
  } else { 
    lsn_t flushed = LogFlushedLSN();
    if(flushed - log_trunc > 2 * TARGET_LOG_SIZE) { 
      printf("Flushing dirty buffers: rec_lsn = %ld log_trunc = %ld flushed = %ld\n", rec_lsn, log_trunc, flushed);
      fflush(stdout);
      dirtyPages_flush();

      page_rec_lsn = dirtyPages_minRecLSN();
      rec_lsn = page_rec_lsn < xact_rec_lsn ? page_rec_lsn : xact_rec_lsn;

      printf("Truncating to rec_lsn = %ld\n", rec_lsn);
      fflush(stdout);
      if(rec_lsn != flushed) {
	LogTruncate(rec_lsn);
	return 1;
      } else {
	return 0;
      }

    } else { 
      return 0;
    }
  }
}
