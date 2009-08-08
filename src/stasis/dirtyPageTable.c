/*
 * dirtyPageTable.c
 *
 *  Created on: May 18, 2009
 *      Author: sears
 */

#include <stasis/redblack.h>
#include <stasis/common.h>
#include <stasis/dirtyPageTable.h>
#include <stasis/page.h>
#include <stasis/bufferManager.h>

#include <stdio.h>

typedef struct {
  pageid_t p;
  lsn_t lsn;
} dpt_entry;

static int dpt_cmp(const void *ap, const void * bp, const void * ignored) {
  const dpt_entry * a = ap;
  const dpt_entry * b = bp;

  return a->p < b->p ? -1 : (a->p == b->p ? 0 : 1);
}

struct stasis_dirty_page_table_t {
  struct rbtree * table;
  pthread_mutex_t mutex;
};

void stasis_dirty_page_table_set_dirty(stasis_dirty_page_table_t * dirtyPages, Page * p) {
  pthread_mutex_lock(&dirtyPages->mutex);
  assertlocked(p->rwlatch);
  if(!p->dirty) {
    p->dirty = 1;
    dpt_entry * e = malloc(sizeof(*e));
    e->p = p->id;
    e->lsn = p->LSN;
    const void * ret = rbsearch(e, dirtyPages->table);
    assert(ret == e); // otherwise, the entry was already in the table.
  } else {
    dpt_entry e = { p->id, 0};
    assert(rbfind(&e, dirtyPages->table));
  }
  pthread_mutex_unlock(&dirtyPages->mutex);
}

void stasis_dirty_page_table_set_clean(stasis_dirty_page_table_t * dirtyPages, Page * p) {
  pthread_mutex_lock(&dirtyPages->mutex);
  dpt_entry dummy = {p->id, 0};
  const dpt_entry * e = rbdelete(&dummy, dirtyPages->table);

  if(e) {
    assert(e->p == p->id);
    assert(p->dirty);
    p->dirty = 0;
    free((void*)e);
  } else {
    assert(!p->dirty);
  }
  pthread_mutex_unlock(&dirtyPages->mutex);
}

int stasis_dirty_page_table_is_dirty(stasis_dirty_page_table_t * dirtyPages, Page * p) {
  int ret;
  pthread_mutex_lock(&dirtyPages->mutex);
  ret = p->dirty;
  dpt_entry e = { p->id, 0};
  const void* found = rbfind(&e, dirtyPages->table);
  assert((found && ret) || !(found||ret));
  pthread_mutex_unlock(&dirtyPages->mutex);
  return ret;
}

lsn_t stasis_dirty_page_table_minRecLSN(stasis_dirty_page_table_t * dirtyPages) {
  lsn_t lsn = LSN_T_MAX;
  pthread_mutex_lock(&dirtyPages->mutex);
  for(const dpt_entry * e = rbmin(dirtyPages->table);
          e;
          e = rblookup(RB_LUGREAT, e, dirtyPages->table)) {
    if(e->lsn < lsn) {
      lsn = e->lsn;
    }
  }
  pthread_mutex_unlock(&dirtyPages->mutex);
  return lsn;
}

void stasis_dirty_page_table_flush(stasis_dirty_page_table_t * dirtyPages) {
  stasis_dirty_page_table_flush_range(dirtyPages, 0, 0); // pageid_t = 0 means flush to EOF.
}
void stasis_dirty_page_table_flush_range(stasis_dirty_page_table_t * dirtyPages, pageid_t start, pageid_t stop) {

  pthread_mutex_lock(&dirtyPages->mutex);
  pageid_t * staleDirtyPages = 0;
  pageid_t n = 0;
  dpt_entry dummy = { start, 0 };
  for(const dpt_entry * e = rblookup(RB_LUGTEQ, &dummy, dirtyPages->table);
         e && (stop == 0 || e->p < stop);
         e = rblookup(RB_LUGREAT, e, dirtyPages->table)) {
    n++;
    staleDirtyPages = realloc(staleDirtyPages, sizeof(pageid_t) * n);
    staleDirtyPages[n-1] = e->p;
  }
  pthread_mutex_unlock(&dirtyPages->mutex);

  for(pageid_t i = 0; i < n; i++) {
      int err = writeBackPage(staleDirtyPages[i]);
      if(stop && (err == EBUSY)) { abort(); /*api violation!*/ }
  }
  free(staleDirtyPages);
  forcePageRange(start*PAGE_SIZE,stop*PAGE_SIZE);
}

stasis_dirty_page_table_t * stasis_dirty_page_table_init() {
  stasis_dirty_page_table_t * ret = malloc(sizeof(*ret));
  ret->table = rbinit(dpt_cmp, 0);
  pthread_mutex_init(&ret->mutex, 0);
  return ret;
}

void stasis_dirty_page_table_deinit(stasis_dirty_page_table_t * dirtyPages) {
  int areDirty = 0;
  dpt_entry dummy = {0, 0};
  for(const dpt_entry * e = rblookup(RB_LUGTEQ, &dummy, dirtyPages->table);
         e;
         e = rblookup(RB_LUGREAT, &dummy, dirtyPages->table)) {

    if((!areDirty) &&
       (!stasis_suppress_unclean_shutdown_warnings)) {
      printf("Warning:  dirtyPagesDeinit detected dirty, unwritten pages.  "
         "Updates lost?\n");
      areDirty = 1;
    }
    dummy = *e;
    rbdelete(e, dirtyPages->table);
    free((void*)e);
  }

  rbdestroy(dirtyPages->table);
  pthread_mutex_destroy(&dirtyPages->mutex);
  free(dirtyPages);
}
