/*
 * dirtyPageTable.c
 *
 *  Created on: May 18, 2009
 *      Author: sears
 */

#include <pbl/pbl.h>
#include <stasis/common.h>
#include <stasis/dirtyPageTable.h>
#include <stasis/page.h>
#include <stasis/bufferManager.h>

struct stasis_dirty_page_table_t {
  pblHashTable_t * table;
  pthread_mutex_t mutex;
};

void stasis_dirty_page_table_set_dirty(stasis_dirty_page_table_t * dirtyPages, Page * p) {
  pthread_mutex_lock(&dirtyPages->mutex);
  if(!p->dirty) {
    p->dirty = 1;
    //assert(p->LSN);
    void* ret = pblHtLookup(dirtyPages->table, &(p->id), sizeof(p->id));
    assert(!ret);
    lsn_t * insert = malloc(sizeof(lsn_t));
    *insert = p->LSN;
    pblHtInsert(dirtyPages->table, &(p->id), sizeof(p->id), insert); //(void*)p->LSN);
  }
  pthread_mutex_unlock(&dirtyPages->mutex);
}

void stasis_dirty_page_table_set_clean(stasis_dirty_page_table_t * dirtyPages, Page * p) {
  pthread_mutex_lock(&dirtyPages->mutex);
  //  printf("Removing page %d\n", p->id);
  //assert(pblHtLookup(dirtyPages, &(p->id), sizeof(int)));
  //  printf("With lsn = %d\n", (lsn_t)pblHtCurrent(dirtyPages));
  p->dirty = 0;
  lsn_t * old = pblHtLookup(dirtyPages->table, &(p->id),sizeof(p->id));
  pblHtRemove(dirtyPages->table, &(p->id), sizeof(p->id));
  if(old) {
    free(old);
  }
  //assert(!ret); <--- Due to a bug in the PBL compatibility mode,
  //there is no way to tell whether the value didn't exist, or if it
  //was null.
  pthread_mutex_unlock(&dirtyPages->mutex);
}

int stasis_dirty_page_table_is_dirty(stasis_dirty_page_table_t * dirtyPages, Page * p) {
  int ret;
  pthread_mutex_lock(&dirtyPages->mutex);
  ret = p->dirty;
  pthread_mutex_unlock(&dirtyPages->mutex);
  return ret;
}

lsn_t stasis_dirty_page_table_minRecLSN(stasis_dirty_page_table_t * dirtyPages) {
  lsn_t lsn = LSN_T_MAX; // LogFlushedLSN ();
  pageid_t* pageid;
  pthread_mutex_lock(&dirtyPages->mutex);

  for( pageid = (pageid_t*)pblHtFirst (dirtyPages->table); pageid; pageid = (pageid_t*)pblHtNext(dirtyPages->table)) {
    lsn_t * thisLSN = (lsn_t*) pblHtCurrent(dirtyPages->table);
    //    printf("lsn = %d\n", thisLSN);
    if(*thisLSN < lsn) {
      lsn = *thisLSN;
    }
  }
  pthread_mutex_unlock(&dirtyPages->mutex);

  return lsn;
}

void stasis_dirty_page_table_flush(stasis_dirty_page_table_t * dirtyPages) {
  pageid_t * staleDirtyPages = malloc(sizeof(pageid_t) * (MAX_BUFFER_SIZE));
  int i;
  for(i = 0; i < MAX_BUFFER_SIZE; i++) {
    staleDirtyPages[i] = -1;
  }
  Page* p = 0;
  pthread_mutex_lock(&dirtyPages->mutex);
  void* tmp;
  i = 0;

  for(tmp = pblHtFirst(dirtyPages->table); tmp; tmp = pblHtNext(dirtyPages->table)) {
    staleDirtyPages[i] = *((pageid_t*) pblHtCurrentKey(dirtyPages->table));
    i++;
  }
  assert(i < MAX_BUFFER_SIZE);
  pthread_mutex_unlock(&dirtyPages->mutex);

  for(i = 0; i < MAX_BUFFER_SIZE && staleDirtyPages[i] != -1; i++) {
    p = getCachedPage(-1, staleDirtyPages[i]);
    if(p) {
      writeBackPage(p);
      releasePage(p);
    }
  }
  free(staleDirtyPages);
}
void stasis_dirty_page_table_flush_range(stasis_dirty_page_table_t * dirtyPages, pageid_t start, pageid_t stop) {
  pageid_t * staleDirtyPages = malloc(sizeof(pageid_t) * (MAX_BUFFER_SIZE));
  int i;
  Page * p = 0;

  pthread_mutex_lock(&dirtyPages->mutex);

  void *tmp;
  i = 0;
  for(tmp = pblHtFirst(dirtyPages->table); tmp; tmp = pblHtNext(dirtyPages->table)) {
    pageid_t num = *((pageid_t*) pblHtCurrentKey(dirtyPages->table));
    if(num <= start && num < stop) {
      staleDirtyPages[i] = num;
      i++;
    }
  }
  staleDirtyPages[i] = -1;
  pthread_mutex_unlock(&dirtyPages->mutex);

  for(i = 0; i < MAX_BUFFER_SIZE && staleDirtyPages[i] != -1; i++) {
    p = getCachedPage(-1, staleDirtyPages[i]);
    if(p) {
      writeBackPage(p);
      releasePage(p);
    }
  }
  free(staleDirtyPages);
  forcePageRange(start*PAGE_SIZE,stop*PAGE_SIZE);

}
stasis_dirty_page_table_t * stasis_dirty_page_table_init() {
  stasis_dirty_page_table_t * ret = malloc(sizeof(*ret));
  ret->table = pblHtCreate();
  pthread_mutex_init(&ret->mutex, 0);
  return ret;
}


void stasis_dirty_page_table_deinit(stasis_dirty_page_table_t * dirtyPages) {
  void * tmp;
  int areDirty = 0;
  for(tmp = pblHtFirst(dirtyPages->table); tmp; tmp = pblHtNext(dirtyPages->table)) {
    free(pblHtCurrent(dirtyPages->table));
    if((!areDirty) &&
       (!stasis_suppress_unclean_shutdown_warnings)) {
      printf("Warning:  dirtyPagesDeinit detected dirty, unwritten pages.  "
         "Updates lost?\n");
      areDirty = 1;
    }
  }
  pblHtDelete(dirtyPages->table);
  pthread_mutex_destroy(&dirtyPages->mutex);
  free(dirtyPages);
}
