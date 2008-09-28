#include <string.h>
#include <stasis/transactional.h>
#include <stasis/page/raw.h>
#include <assert.h>


void allocBlob(int xid, recordid rid) {
  assert(rid.size>0);
  int pageCount = (rid.size / USABLE_SIZE_OF_PAGE) + ((rid.size % USABLE_SIZE_OF_PAGE) ? 1 : 0);
  long startPage = TpageAllocMany(xid, pageCount);
  blob_record_t rec;
  rec.offset = startPage;
  rec.size = rid.size;
  recordid rid2 = rid;
  rid2.size = BLOB_SLOT;
  Tset(xid, rid2, (byte*)&rec);
  //  printf("Page start = %d, count = %d, rid.size=%d\n", rec.offset, pageCount, rid.size);
  //  printf("rid = {%d %d %d}\n", rid.page, rid.slot, rid.size);
}

void readBlob(int xid, Page * p2, recordid rid, byte * buf) {
  int chunk;
  recordid rawRid = rid;
  rawRid.size = BLOB_SLOT;
  byte * pbuf = alloca(PAGE_SIZE);
  blob_record_t rec;
  stasis_record_read(xid, p2, rawRid, (byte*)&rec);

  for(chunk = 0; (chunk+1) * USABLE_SIZE_OF_PAGE < rid.size; chunk++) {
    //printf("Chunk = %d->%lld\n", chunk, (long long)rec.offset+chunk);
    TpageGet(xid, rec.offset+chunk, pbuf);
    memcpy(buf + (chunk * USABLE_SIZE_OF_PAGE), pbuf, USABLE_SIZE_OF_PAGE);
  }

  TpageGet(xid, rec.offset+chunk, pbuf);
  memcpy(buf + (chunk * USABLE_SIZE_OF_PAGE), pbuf, rid.size % USABLE_SIZE_OF_PAGE);
  //printf("Chunk = %d->%lld\n", chunk, (long long)rec.offset+chunk);

}

void writeBlob(int xid, Page * p, recordid rid, const void* dat) {
    blob_record_t rec;
    recordid r = rid;
    r.size = sizeof(blob_record_t);
    stasis_record_read(xid, p, r, (byte*)&rec);

    assert(rec.offset);
    int64_t chunk = 0;
    for(; (chunk+1) * USABLE_SIZE_OF_PAGE < rid.size; chunk++) {
      Page * cnk = loadPage(xid, rec.offset+chunk);
      writelock(cnk->rwlatch,0);
      if(*stasis_page_type_ptr(cnk) != BLOB_PAGE) {
        stasis_blob_initialize_page(cnk);
      }
      unlock(cnk->rwlatch);
      // Don't care about race; writes in race have undefined semantics...
      TpageSetRange(xid,rec.offset+chunk,0,((const byte*)dat)+(chunk*USABLE_SIZE_OF_PAGE),USABLE_SIZE_OF_PAGE);
    }
    Page * cnk = loadPage(xid, rec.offset+chunk);
    writelock(cnk->rwlatch,0);
    if(*stasis_page_type_ptr(cnk) != BLOB_PAGE) {
      stasis_blob_initialize_page(cnk);
    }
    unlock(cnk->rwlatch);
    byte * buf = calloc(1,USABLE_SIZE_OF_PAGE);
    memcpy(buf, ((const byte*)dat)+(chunk*USABLE_SIZE_OF_PAGE), rid.size % USABLE_SIZE_OF_PAGE);
    TpageSetRange(xid,rec.offset+chunk,0,buf,USABLE_SIZE_OF_PAGE);
    free(buf);
}
static int notSupported(int xid, Page * p) { return 0; }

static void blobLoaded(Page *p) {
  p->LSN = *stasis_page_lsn_ptr(p);
  DEBUG("load lsn: %lld\n", (long long)p->LSN);
}
static void blobFlushed(Page *p) {
  *stasis_page_lsn_ptr(p) = p->LSN;
  DEBUG("flush lsn: %lld\n", (long long)p->LSN);
}
static void blobCleanup(Page *p) { }

static page_impl pi = {
    BLOB_PAGE,
    0, //read,
    0, //write,
    0, //readDone
    0, //writeDone
    0, //getType,
    0, //setType,
    0, //getLength,
    0, //recordFirst,
    0, //recordNext,
    notSupported, // is block supported
    0, //pageGenericBlockFirst,
    0, //pageGenericBlockNext,
    0, //pageGenericBlockDone,
    0, //freespace,
    0, //compact,
    0, //preRalloc,
    0, //postRalloc,
    0, //Free,
    0, //XXX page_impl_dereference_identity,
    blobLoaded,
    blobFlushed,
    blobCleanup
};
page_impl blobImpl() {
  return pi;
}
void stasis_blob_initialize_page(Page * p) {
  assertlocked(p->rwlatch);
  DEBUG("lsn: %lld\n",(long long)p->LSN);
  stasis_page_cleanup(p);
  *stasis_page_type_ptr(p) = BLOB_PAGE;
}
