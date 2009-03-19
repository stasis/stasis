#include <stasis/transactional.h>
#include <string.h>
#include <unistd.h>
void alloc_rids(long long num_rids, recordid ** slow, recordid ** fast) {
  *slow = malloc(num_rids * sizeof(**slow));
  *fast = malloc((num_rids / 10) * sizeof(**fast));

  int xid = Tbegin();

  byte * old = malloc(PAGE_SIZE);
  byte * new = malloc(PAGE_SIZE);

  for(long long i = 0; i < num_rids; ) {
    pageid_t pid = TpageAlloc(xid);
    Page * p = loadPage(xid, pid);
    writelock(p->rwlatch,0);
    memcpy(old, p->memAddr, PAGE_SIZE);
    stasis_slotted_lsn_free_initialize_page(p);
    while(i < num_rids &&
          (
           ((*slow)[i] = stasis_record_alloc_begin(xid, p, sizeof(int))).size
           == sizeof(int)
           )
          ) {
      stasis_record_alloc_done(xid, p, (*slow)[i]);
      if(!(i%10)) {
        (*fast)[i/10] = stasis_record_alloc_begin(xid, p, sizeof(int));
        if((*fast)[i/10].size!=sizeof(int)) {
          break; // don't increment i
        }
        stasis_record_alloc_done(xid, p, (*fast)[i/10]);
      }
      assert((*slow)[i].size != -1);
      i++;
    }
    memcpy(new, p->memAddr, PAGE_SIZE);
    memcpy(p->memAddr, old, PAGE_SIZE);
    unlock(p->rwlatch);
    releasePage(p);
    TpageSet(xid, pid, new);
  }
  free(old);
  free(new);

  Tcommit(xid);
}

typedef struct {
  pageid_t pid;
  pageoff_t off;
  pageoff_t len;
  int val;
} cached_addr;

void build_cache(recordid * rids, cached_addr** cache, long long count) {
  *cache = malloc (sizeof(**cache) * count);
  lsn_t log_trunc = stasis_log_file->truncation_point(stasis_log_file);
  for(long long i = 0; i < count; i++) {
    (*cache)[i].pid = rids[i].page;

    Page * p = loadPage(-1, (*cache)[i].pid);
    readlock(p->rwlatch,0);
    byte * b = stasis_record_write_begin(-1, p, rids[i]);
    (*cache)[i].off = b - p->memAddr;
    stasis_record_write_done(-1, p, rids[i], b);
    stasis_page_lsn_write(-1, p, log_trunc);
    (*cache)[i].len = stasis_record_type_to_size(rids[i].size);
    (*cache)[i].val = 0;
    unlock(p->rwlatch);
    //    releasePage(p);
  }
}
int net_latency = 2;
static byte * (*origWrite)(int xid, Page *p, recordid rid);
static byte * slowWrite(int xid, Page *p, recordid rid) {
  usleep(net_latency * 1000);
  return origWrite(xid,p,rid);
}
static const byte * (*origRead)(int xid, Page *p, recordid rid);
static const byte * slowRead(int xid, Page *p, recordid rid) {
  usleep(net_latency * 1000);
  return origRead(xid,p,rid);
}

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
  if(writeback) {
    build_cache(rids,&cache,num_rids);
  }

  if(!(strcmp(mode, "normal-net")&&strcmp(mode,"writeback-net"))) {
    origWrite = stasis_page_impl_get(SLOTTED_LSN_FREE_PAGE)->recordWrite;
    origRead = stasis_page_impl_get(SLOTTED_LSN_FREE_PAGE)->recordRead;

    // xxx a bit of cheating; don't pay latency for lsn write
    //     (could amortize w/ recordWrite)

    stasis_page_impl_get(SLOTTED_LSN_FREE_PAGE)->recordWrite = slowWrite;
    stasis_page_impl_get(SLOTTED_LSN_FREE_PAGE)->recordRead = slowRead;
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
      stasis_record_write(-1, p, last_lsn, rids[i], (byte*)&cache[i].val);
      unlock(p->rwlatch);
      releasePage(p);
      releasePage(p);
    }
  }
  Tdeinit();
}
