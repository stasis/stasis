#include <stdlib.h>
#include <config.h>
#include <lladd/transactional.h>
#include <lladd/bufferManager.h>
#include <lladd/bufferPool.h>
#include <lladd/truncation.h>
#include "latches.h"
#include "bufferManager/pageArray.h"

static Page ** pageMap;
static int pageCount;
static pthread_mutex_t pageArray_mut = PTHREAD_MUTEX_INITIALIZER;

static Page * paLoadPage(int xid, int pageid) {
  pthread_mutex_lock(&pageArray_mut);
  if(pageid >= pageCount) { 
    pageMap = realloc(pageMap, (1+pageid) * sizeof(Page*));
    for(int i = pageCount; i <= pageid; i++) { 
      pageMap[i] = 0;
    }
    pageCount = pageid + 1;
  }

  if(!pageMap[pageid]) { 
    pageMap[pageid] = malloc(sizeof(Page));
    pageMap[pageid]->id = pageid;
    pageMap[pageid]->LSN = 0;
    pageMap[pageid]->dirty = 0;
    pageMap[pageid]->next = 0;
    pageMap[pageid]->prev = 0;
    pageMap[pageid]->queue = 0;
    pageMap[pageid]->inCache = 1;
    pageMap[pageid]->rwlatch = initlock();
    pageMap[pageid]->loadlatch = initlock();
    pageMap[pageid]->memAddr= calloc(PAGE_SIZE, sizeof(byte));
  }
  pthread_mutex_unlock(&pageArray_mut);
  return pageMap[pageid];
}

static void paReleasePage(Page * p) { 
  /* no-op */
}

static void paWriteBackPage(Page * p) { 
  dirtyPages_remove(p);
}
static void paForcePages() { /* no-op */ }

static void paBufDeinit() { 
  for(int i =0; i < pageCount; i++) { 
    if(pageMap[i]) { 
      deletelock(pageMap[i]->rwlatch);
      deletelock(pageMap[i]->loadlatch);
      free(pageMap[i]);
    }
  }
}
void paBufInit () { 

  releasePage = paReleasePage;
  loadPage = paLoadPage;
  writeBackPage = paWriteBackPage;
  forcePages = paForcePages;
  bufDeinit = paBufDeinit; 

  pageCount = 0;
  pageMap = 0;
}
