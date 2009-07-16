#include <stdlib.h>
#include <config.h>
#include <stasis/transactional.h>
#include <stasis/bufferManager.h>
#include <stasis/bufferPool.h>
#include <stasis/truncation.h>
#include <stasis/latches.h>
#include <stasis/bufferManager/pageArray.h>
#include <stasis/page.h>

static Page ** pageMap;
static pageid_t pageCount;
static pthread_mutex_t pageArray_mut = PTHREAD_MUTEX_INITIALIZER;

static Page * paLoadPage(int xid, pageid_t pageid, pagetype_t type) {

  pthread_mutex_lock(&pageArray_mut);
  if(pageid >= pageCount) {
    pageMap = realloc(pageMap, (1+pageid) * sizeof(Page*));
    for(pageid_t i = pageCount; i <= pageid; i++) {
      pageMap[i] = 0;
    }
    pageCount = pageid + 1;
  }

  if(!pageMap[pageid]) {
    pageMap[pageid] = malloc(sizeof(Page));
    pageMap[pageid]->id = pageid;
    pageMap[pageid]->pageType = type;
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
static Page* paGetCachedPage(int xid, pageid_t page) {
  return paLoadPage(xid, page, UNKNOWN_TYPE_PAGE);
}
static void paReleasePage(Page * p) {
  stasis_dirty_page_table_set_clean(stasis_dirty_page_table, p);
}

static void paWriteBackPage(Page * p) {  /* no-op */ }
static void paForcePages() { /* no-op */ }

static void paBufDeinit() {
  for(pageid_t i =0; i < pageCount; i++) {
    if(pageMap[i]) {
      deletelock(pageMap[i]->rwlatch);
      deletelock(pageMap[i]->loadlatch);
      free(pageMap[i]);
    }
  }
}

void stasis_buffer_manager_mem_array_open () {

  releasePageImpl = paReleasePage;
  loadPageImpl = paLoadPage;
  getCachedPageImpl = paGetCachedPage;
  writeBackPage = paWriteBackPage;
  forcePages = paForcePages;
  stasis_buffer_manager_close = paBufDeinit;
  stasis_buffer_manager_simulate_crash = paBufDeinit;

  pageCount = 0;
  pageMap = 0;
}
