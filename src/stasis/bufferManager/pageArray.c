#include <stasis/transactional.h>
#include <stasis/bufferPool.h>
#include <stasis/truncation.h>
#include <stasis/latches.h>
#include <stasis/bufferManager/pageArray.h>
#include <stasis/page.h>

typedef struct {
	Page ** pageMap;
	pageid_t pageCount;
	pthread_mutex_t mut;
} stasis_buffer_manager_page_array_t;

static Page * paLoadPage(stasis_buffer_manager_t *bm, int xid, pageid_t pageid, pagetype_t type) {
  stasis_buffer_manager_page_array_t *pa = bm->impl;
  pthread_mutex_lock(&pa->mut);
  if(pageid >= pa->pageCount) {
    pa->pageMap = realloc(pa->pageMap, (1+pageid) * sizeof(Page*));
    for(pageid_t i = pa->pageCount; i <= pageid; i++) {
      pa->pageMap[i] = 0;
    }
    pa->pageCount = pageid + 1;
  }

  if(!pa->pageMap[pageid]) {
    pa->pageMap[pageid] = malloc(sizeof(Page));
    pa->pageMap[pageid]->id = pageid;
    pa->pageMap[pageid]->pageType = type == UNKNOWN_TYPE_PAGE ? 0 : type;
    pa->pageMap[pageid]->LSN = 0;
    pa->pageMap[pageid]->dirty = 0;
    pa->pageMap[pageid]->next = 0;
    pa->pageMap[pageid]->prev = 0;
    pa->pageMap[pageid]->queue = 0;
    pa->pageMap[pageid]->inCache = 1;
    pa->pageMap[pageid]->rwlatch = initlock();
    pa->pageMap[pageid]->loadlatch = initlock();
    pa->pageMap[pageid]->memAddr= calloc(PAGE_SIZE, sizeof(byte));
  } else{
    if(type != UNKNOWN_TYPE_PAGE) { assert(type == pa->pageMap[pageid]->pageType); }
  }
  pthread_mutex_unlock(&pa->mut);
  return pa->pageMap[pageid];
}
static Page* paLoadUninitPage(stasis_buffer_manager_t *bm, int xid, pageid_t page) {
  return paLoadPage(bm, xid, page, UNKNOWN_TYPE_PAGE);
}
static Page* paGetCachedPage(stasis_buffer_manager_t *bm, int xid, pageid_t page) {
  return paLoadPage(bm, xid, page, UNKNOWN_TYPE_PAGE);
}
static void paReleasePage(stasis_buffer_manager_t *bm, Page * p) {
  writelock(p->rwlatch,0);
  stasis_dirty_page_table_set_clean(stasis_runtime_dirty_page_table(), p);
  unlock(p->rwlatch);
}

static int paWriteBackPage(stasis_buffer_manager_t *bm, pageid_t p) { return 0;  /* no-op */ }
static void paForcePages(stasis_buffer_manager_t * bm) { /* no-op */ }
static void paForcePageRange(stasis_buffer_manager_t *bm, pageid_t start, pageid_t stop) { /* no-op */ }

static void paBufDeinit(stasis_buffer_manager_t * bm) {
  stasis_buffer_manager_page_array_t *pa = bm->impl;

  for(pageid_t i =0; i < pa->pageCount; i++) {
    if(pa->pageMap[i]) {
      deletelock(pa->pageMap[i]->rwlatch);
      deletelock(pa->pageMap[i]->loadlatch);
      free(pa->pageMap[i]);
    }
  }
  pthread_mutex_destroy(&pa->mut);
  free(pa);
}

stasis_buffer_manager_t * stasis_buffer_manager_mem_array_open () {

  stasis_buffer_manager_t * bm = malloc(sizeof(*bm));
  stasis_buffer_manager_page_array_t * pa = malloc(sizeof(*pa));

  bm->releasePageImpl = paReleasePage;
  bm->loadPageImpl = paLoadPage;
  bm->loadUninitPageImpl = paLoadUninitPage;
  bm->prefetchPages = NULL;
  bm->getCachedPageImpl = paGetCachedPage;
  bm->writeBackPage = paWriteBackPage;
  bm->forcePages = paForcePages;
  bm->forcePageRange = paForcePageRange;
  bm->stasis_buffer_manager_close = paBufDeinit;
  bm->stasis_buffer_manager_simulate_crash = paBufDeinit;
  bm->impl = pa;
  pa->pageCount = 0;
  pa->pageMap = 0;
  pthread_mutex_init(&pa->mut,0);
  return bm;
}
stasis_buffer_manager_t* stasis_buffer_manager_mem_array_factory(stasis_log_t * log, stasis_dirty_page_table_t *dpt) {
  return stasis_buffer_manager_mem_array_open();
}
