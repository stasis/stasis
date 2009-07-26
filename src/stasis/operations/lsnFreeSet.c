#include <stasis/operations.h>
#include <stasis/page.h>
#include <stasis/logger/reorderingHandle.h>

#include <stdio.h>

static int op_lsn_free_set(const LogEntry *e, Page *p) {
  if(p->pageType != SLOTTED_LSN_FREE_PAGE) {  abort() ; }
  assert(e->update.arg_size >= (sizeof(pageoff_t) * 2));
  int size = e->update.arg_size;
  size -= (2*sizeof(pageoff_t));
  const pageoff_t * a = (const pageoff_t*)getUpdateArgs(e);
  const byte* b = (const byte*)&(a[2]);
  assertlocked(p->rwlatch);
  memcpy(p->memAddr + a[0], b, a[1]);
  return 0;
}
static int op_lsn_free_unset(const LogEntry *e, Page *p) {
  if(p->pageType != SLOTTED_LSN_FREE_PAGE) { return 0; }
  assert(e->update.arg_size >= (sizeof(pageoff_t) * 2));
  int size = e->update.arg_size;
  size -= (2*sizeof(pageoff_t));
  const pageoff_t * a = (const pageoff_t*)getUpdateArgs(e);
  const byte* b = (const byte*)&(a[2]);
  assertlocked(p->rwlatch);
  memcpy(p->memAddr + a[0], b+a[1], a[1]);
  return 0;
}
int TsetReorderable(int xid, stasis_log_reordering_handle_t * h,
                                  recordid rid, const void * dat) {
  Page * p = loadPage(xid, rid.page);
  readlock(p->rwlatch,0);
  rid = stasis_record_dereference(xid,p,rid);
  short type = stasis_record_type_read(xid,p,rid);
  if(type == BLOB_SLOT) {
    fprintf(stderr, "LSN-Free blobs are not implemented!\n");
    fflush(stderr);
    abort();
    unlock(p->rwlatch);
    releasePage(p);
    return 1;
  } else {
    rid.size = stasis_record_type_to_size(rid.size);
    intptr_t sz = 2 * (sizeof(pageoff_t) + rid.size);
    byte * buf = calloc(sz, 1);
    pageoff_t * a = (pageoff_t*)buf;
    // XXX hack!
    byte * writeBuf = stasis_record_write_begin(xid, p, rid);
    a[0] = writeBuf - p->memAddr;
    stasis_record_write_done(xid, p, rid, writeBuf);
    a[1] = rid.size;
    byte * b = (byte*)&(a[2]);
    // postimage
    memcpy(b,dat,rid.size);
    // preimage
    stasis_record_read(xid, p, rid, b+rid.size);

    unlock(p->rwlatch);
    releasePage(p);
    if(!h) {
      Tupdate(xid,rid.page,buf,sz,OPERATION_SET_LSN_FREE);
    } else {
      TreorderableUpdate(xid,h,rid.page,buf,sz,OPERATION_SET_LSN_FREE);
    }
    free(buf);
  }
  return 0;
}
int TsetLsnFree(int xid, recordid rid, const void * dat) {
  return TsetReorderable(xid, 0, rid, dat);
}
int TsetReorderableWriteBack(int xid, stasis_log_reordering_handle_t * h,
                             pageid_t page, pageoff_t off, pageoff_t len,
                             const void * dat, const void * olddat) {
  intptr_t sz = 2 * (sizeof(pageoff_t) + len);
  byte * buf = calloc(sz,1);
  pageoff_t * a = (pageoff_t*)buf;
  a[0] = off;
  a[1] = len;
  byte * b = (byte*)&a[2];
  memcpy(b,dat,len);
  memcpy(b+len,dat,len);
  lsn_t ret = 0;
  if(!h) {
    ret = TwritebackUpdate(xid,page,buf,sz,OPERATION_SET_LSN_FREE);
  } else {
    TreorderableWritebackUpdate(xid,h,page,buf,sz,OPERATION_SET_LSN_FREE);
  }
  free(buf);
  return ret;
}
int TsetWriteBack(int xid, pageid_t page, pageoff_t off, pageoff_t len, const void * dat, const void * olddat) {
  return TsetReorderableWriteBack(xid,0,page,off,len,dat,olddat);
}

stasis_operation_impl stasis_op_impl_lsn_free_set() {
  stasis_operation_impl o = {
    OPERATION_SET_LSN_FREE,
    SEGMENT_PAGE,
    OPERATION_SET_LSN_FREE,
    OPERATION_SET_LSN_FREE_INVERSE,
    op_lsn_free_set
  };
  return o;
}

stasis_operation_impl stasis_op_impl_lsn_free_set_inverse() {
  stasis_operation_impl o = {
    OPERATION_SET_LSN_FREE_INVERSE,
    SEGMENT_PAGE,
    OPERATION_SET_LSN_FREE_INVERSE,
    OPERATION_SET_LSN_FREE,
    op_lsn_free_unset
  };
  return o;
}
