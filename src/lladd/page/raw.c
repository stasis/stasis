#include "page/raw.h"
#include <lladd/logger/logger2.h>
/**
   @todo Should rawPageInferMetadata set a page type in the Page
   struct?
*/
void rawPageInferMetadata(Page * p) { 
  p->LSN = LogFlushedLSN();
}

byte* rawPageGetData(int xid, Page * p) {
  return units_from_start_raw(byte, p, 0);
}

void  rawPageSetData(int xid, lsn_t lsn, Page * p) { 
  writelock(p->rwlatch, 255);
  rawPageWriteLSN(xid, p, lsn);
  p->dirty = 1;
  unlock(p->rwlatch);
  return;
}

lsn_t rawPageReadLSN(const Page * p) { 
  // There are some potential optimizations here since the page
  // doesn't "really" have an LSN at all, but we need to be careful
  // about log truncation...
  return p->LSN; 
}

void rawPageWriteLSN(int xid, Page * p, lsn_t lsn) { 
  if(p->LSN < lsn) { p->LSN = lsn; }
}

void rawPageCommit(int xid) { 
  // no-op
}

void rawPageAbort(int xid) { 
  // no-op
}
