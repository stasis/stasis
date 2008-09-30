#include <stasis/page/raw.h>
#include <stasis/logger/logger2.h>
#include <stasis/truncation.h>
/**
   @todo Should rawPageInferMetadata set a page type in the Page
   struct?

   XXX rawPageInferMetadata is wrong; setting lsn to LogFlushedLSN() breaks
   recovery.
*/
void rawPageInferMetadata(Page * p) { 
  p->LSN = LogFlushedLSN();
}

byte* rawPageGetData(int xid, Page * p) {
  assertlocked(p->rwlatch);
  return units_from_start_raw(byte, p, 0);
}

void  rawPageSetData(int xid, lsn_t lsn, Page * p) { 
  assertlocked(p->rwlatch);
  //  writelock(p->rwlatch, 255);
  rawPageWriteLSN(xid, p, lsn);
  dirtyPages_add(p);
  //  unlock(p->rwlatch);
  return;
}

lsn_t rawPageReadLSN(const Page * p) { 
  assertlocked(p->rwlatch);
  // There are some potential optimizations here since the page
  // doesn't "really" have an LSN at all, but we need to be careful
  // about log truncation...
  return p->LSN; 
}

void rawPageWriteLSN(int xid, Page * p, lsn_t lsn) { 
  assertlocked(p->rwlatch);
  if(p->LSN < lsn) { p->LSN = lsn; }
}

void rawPageCommit(int xid) { 
  // no-op
}

void rawPageAbort(int xid) { 
  // no-op
}
