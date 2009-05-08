#include <string.h>
#include <stasis/transactional.h>
#include <stasis/page/raw.h>
#include <assert.h>


void stasis_blob_alloc(int xid, recordid rid) {
  assert(rid.size>0);
  pageid_t pageCount = (rid.size / USABLE_SIZE_OF_PAGE) + ((rid.size % USABLE_SIZE_OF_PAGE) ? 1 : 0);
  long startPage = TpageAllocMany(xid, pageCount);
  blob_record_t rec;
  rec.offset = startPage;
  rec.size = rid.size;
  rid.size = sizeof(rec);
  TsetRaw(xid, rid, (byte*)&rec);
  DEBUG("Page start = %d, count = %d, rid.size=%d\n", rec.offset, pageCount, rid.size);
  DEBUG("rid = {%d %d %d}\n", rid.page, rid.slot, rid.size);
}

void stasis_blob_dealloc(int xid, blob_record_t *r) {
  TregionDealloc(xid, r->offset);
}

void stasis_blob_read(int xid, Page * p, recordid rid, byte * buf) {
  pageid_t chunk;
  recordid rawRid = rid;
  rawRid.size = BLOB_SLOT;
  byte * pbuf = alloca(PAGE_SIZE);
  blob_record_t rec;
  stasis_record_read(xid, p, rawRid, (byte*)&rec);

  for(chunk = 0; (chunk+1) * USABLE_SIZE_OF_PAGE < rid.size; chunk++) {
    DEBUG("Chunk = %d->%lld\n", chunk, (long long)rec.offset+chunk);
    TpageGet(xid, rec.offset+chunk, pbuf);
    memcpy(buf + (chunk * USABLE_SIZE_OF_PAGE), pbuf, USABLE_SIZE_OF_PAGE);
  }

  TpageGet(xid, rec.offset+chunk, pbuf);
  memcpy(buf + (chunk * USABLE_SIZE_OF_PAGE), pbuf, rid.size % USABLE_SIZE_OF_PAGE);
  DEBUG("Chunk = %d->%lld\n", chunk, (long long)rec.offset+chunk);
}

void stasis_blob_write(int xid, Page * p, recordid rid, const void* dat) {
    blob_record_t rec;
    recordid r = rid;
    r.size = sizeof(blob_record_t);
    stasis_record_read(xid, p, r, (byte*)&rec);

    assert(rec.offset);
    pageid_t chunk = 0;
    for(; (chunk+1) * USABLE_SIZE_OF_PAGE < rid.size; chunk++) {
      Page * cnk = loadPage(xid, rec.offset+chunk);
      writelock(cnk->rwlatch,0);
      if(*stasis_page_type_ptr(cnk) != BLOB_PAGE) {
        stasis_page_blob_initialize(cnk);
      }
      unlock(cnk->rwlatch);
      // Don't care about race; writes in race have undefined semantics...
      TpageSetRange(xid,rec.offset+chunk,0,((const byte*)dat)+(chunk*USABLE_SIZE_OF_PAGE),USABLE_SIZE_OF_PAGE);
    }
    Page * cnk = loadPage(xid, rec.offset+chunk);
    writelock(cnk->rwlatch,0);
    if(*stasis_page_type_ptr(cnk) != BLOB_PAGE) {
      stasis_page_blob_initialize(cnk);
    }
    unlock(cnk->rwlatch);
    byte * buf = calloc(1,USABLE_SIZE_OF_PAGE);
    memcpy(buf, ((const byte*)dat)+(chunk*USABLE_SIZE_OF_PAGE), rid.size % USABLE_SIZE_OF_PAGE);
    TpageSetRange(xid,rec.offset+chunk,0,buf,USABLE_SIZE_OF_PAGE);
    free(buf);
}
static int stasis_page_not_supported(int xid, Page * p) { return 0; }

static void stasis_page_blob_loaded(Page *p) {
  p->LSN = *stasis_page_lsn_ptr(p);
  DEBUG("load lsn: %lld\n", (long long)p->LSN);
}
static void stasis_page_blob_flushed(Page *p) {
  *stasis_page_lsn_ptr(p) = p->LSN;
  DEBUG("flush lsn: %lld\n", (long long)p->LSN);
}
static void stasis_page_blob_cleanup(Page *p) { }

page_impl stasis_page_blob_impl() {
  page_impl pi = {
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
      stasis_page_not_supported, // is block supported
      0, //pageGenericBlockFirst,
      0, //pageGenericBlockNext,
      0, //pageGenericBlockDone,
      0, //freespace,
      0, //compact,
      0, //preRalloc,
      0, //postRalloc,
      0, //Free,
      0, //XXX page_impl_dereference_identity,
      stasis_page_blob_loaded,
      stasis_page_blob_flushed,
      stasis_page_blob_cleanup
  };
  return pi;
}
void stasis_page_blob_initialize(Page * p) {
  assertlocked(p->rwlatch);
  DEBUG("lsn: %lld\n",(long long)p->LSN);
  stasis_page_cleanup(p);
  *stasis_page_type_ptr(p) = BLOB_PAGE;
}
