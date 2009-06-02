#include "lsn_bench_common.h"

int main (int argc, char ** argv) {
  unlink("storefile.txt");
  unlink("logfile.txt");
  char * mode = argv[1];
  long long num_rids = atoll(argv[2]);
  long long num_xacts = atoll(argv[3]);
  long long writes_per_xact = atoll(argv[4]);
  recordid * rids;
  recordid * fast;
  cached_addr * cache;
  int writeback = !(strcmp(mode, "writeback")&&strcmp(mode,"writeback-net"));
  //  stasis_truncation_automatic = 0;
  Tinit();

  alloc_rids(num_rids,&rids,&fast);

  if(!(strcmp(mode, "normal-net")&&strcmp(mode,"writeback-net"))) {
    emulate_remote_pages();
  }

  if(writeback) {
    build_cache(rids,&cache,num_rids);
  }

  lsn_t last_lsn = 0;
  for(long long i = 0; i < num_xacts; i++) {
    int xid = Tbegin();

    for(long long j = 0; j < writes_per_xact; j++) {
      long long idx = ((i*writes_per_xact)+j)%num_rids;

      if(!(strcmp(mode, "normal")&&strcmp(mode, "normal-net"))) {
        TsetLsnFree(xid, rids[idx], &j);
      } else {
        assert(writeback);
        int old = cache[idx].val;
        cache[idx].val = j;
        last_lsn = TsetWriteBack(xid, cache[idx].pid,cache[idx].off,
                                 cache[idx].len,&j,&old);
      }
    }
    Tcommit(xid);
  }
  // XXX hack; really, want to register upcall in buffermanager...
  if(writeback) {
    for(long long i = 0; i < num_rids; i++) {
      Page *p = loadPage(-1, rids[i].page);
      writelock(p->rwlatch,0);
      stasis_record_write(-1, p, rids[i], (byte*)&cache[i].val);
      stasis_page_lsn_write(-1, p, last_lsn);
      unlock(p->rwlatch);
      releasePage(p);
      releasePage(p);
    }
  }
  Tdeinit();
}
