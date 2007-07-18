#include <string.h>
#include <stasis/transactional.h>
#include "page/raw.h"
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
  Tset(xid, rid2, &rec);
  //  printf("Page start = %d, count = %d, rid.size=%d\n", rec.offset, pageCount, rid.size);
  //  printf("rid = {%d %d %d}\n", rid.page, rid.slot, rid.size);
}

void readBlob(int xid, Page * p2, recordid rid, byte * buf) {
  int chunk;
  recordid rawRid = rid;
  rawRid.size = BLOB_SLOT;
  byte * pbuf = alloca(PAGE_SIZE);
  blob_record_t rec;
  recordRead(xid, p2, rawRid, (byte*)&rec);

  for(chunk = 0; (chunk+1) * USABLE_SIZE_OF_PAGE < rid.size; chunk++) {
    TpageGet(xid, rec.offset+chunk, pbuf);
    memcpy(buf + (chunk * USABLE_SIZE_OF_PAGE), pbuf, USABLE_SIZE_OF_PAGE);
  }

  TpageGet(xid, rec.offset+chunk, pbuf);
  memcpy(buf + (chunk * USABLE_SIZE_OF_PAGE), pbuf, rid.size % USABLE_SIZE_OF_PAGE);
  //  printf("Chunk = %d\n", chunk);

}

void writeBlob(int xid, Page * p2, lsn_t lsn, recordid rid, const byte * buf) {
  int chunk;
  recordid rawRid = rid;
  rawRid.size = BLOB_SLOT;
  byte * pbuf = alloca(PAGE_SIZE);
  blob_record_t rec;
  recordRead(xid, p2, rawRid, (byte*)&rec);
  Page tmp;
  tmp.memAddr=pbuf;

  assert(rec.offset);
  for(chunk = 0; (chunk+1) * USABLE_SIZE_OF_PAGE < rid.size; chunk++) {
    TpageGet(xid, rec.offset+chunk, pbuf);

    *page_type_ptr(&tmp) = BLOB_PAGE;
    memcpy(pbuf, buf + (chunk * USABLE_SIZE_OF_PAGE), USABLE_SIZE_OF_PAGE);
    TpageSet(xid, rec.offset+chunk, pbuf);
  }
  TpageGet(xid, rec.offset+chunk, pbuf);
  memcpy(pbuf, buf + (chunk * USABLE_SIZE_OF_PAGE), rid.size % USABLE_SIZE_OF_PAGE);
  TpageSet(xid, rec.offset+chunk, pbuf);
  //  printf("Write Chunk = %d (%d)\n", chunk, rec.offset+chunk);
}

static int notSupported(int xid, Page * p) { return 0; }

void blobLoaded(Page *p) {
  p->LSN = *lsn_ptr(p);
}
void blobFlushed(Page *p) {
  *lsn_ptr(p) = p->LSN;
}

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
};
page_impl blobImpl() {
  return pi;
}
