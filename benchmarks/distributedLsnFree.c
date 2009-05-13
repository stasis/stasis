#include "lsn_bench_common.h"

int main(int argc, char ** argv) {
  unlink("storefile.txt");
  unlink("logfile.txt");
  char * mode = argv[1];
  long long num_rids = atoll(argv[2]);
  long long num_xacts = atoll(argv[3]);
  long long writes_per_xact = atoll(argv[4]);
  recordid * rids;
  recordid * fast;
  cached_addr * cache;
  int writeback = !(strcmp(mode, "writeback")&&strcmp(mode,"writeback-net")&&strcmp(mode,"writeback-pipeline"));
  //  stasis_truncation_automatic = 0;

  /*if(!(strcmp(mode, "writeback-pipeline"))) {
    // pipelining likes big queues
    //  stasis_log_file_write_buffer_size = 50 * 1024 * 1024;
  } else {
  }*/
  stasis_log_file_write_buffer_size = 50 * 1024 * 1024;
  Tinit();

  alloc_rids(num_rids,&rids,&fast);
  int net = 0;

  if(writeback) {
    build_cache(rids,&cache,num_rids);
  }
  // XXX move above build_cache!
  if(!(strcmp(mode, "normal-net")&&strcmp(mode,"writeback-net"))) {
    net = 1;
    emulate_remote_pages();
    emulate_remote_log();
  }
  int num_workers = 100;
  if(!strcmp(mode, "writeback-pipeline")) {
    emulate_remote_pages();
    net = 1;
    num_workers = 10;
    stasis_log_reordering_usleep_after_flush = net_latency * 1000;
  }

  //  stasis_log_reordering_handle_t* handles[num_workers];


  lsn_t last_lsn = 0;
  for(long long x = 0; x < num_xacts; x++) {
    int xid = Tbegin();
    if(net && writeback) {
      writeback_unit_of_work_arg a[num_workers];
      pthread_t workers[num_workers];
      for(int i =0 ; i < num_workers; i++) {
        a[i].num_rids = num_rids;
        a[i].rid_per_xact = writes_per_xact;
        a[i].cache = cache;
        a[i].done = 0;
        a[i].xid = xid;
        a[i].workerid = i;
        a[i].iterationid = x;
        a[i].divisor = num_workers;
        pthread_mutex_init(&a[i].mut,0);

        pthread_create(&workers[i], 0, writeback_unit_of_work, &a[i]);
      }
      for(int i =0; i < num_workers; i++) {
        pthread_mutex_lock(&a[i].mut);
        a[i].done = 1;
        pthread_mutex_unlock(&a[i].mut);
      }
      for(int i =0 ; i < num_workers; i++) {
        pthread_join(workers[i],0);
      }
    } else {
    /*    if(writeback && net) {
      for(int i = 0; i < num_workers; i++) {
        handles[i] = stasis_log_reordering_handle_open(
                    &stasis_transaction_table[xid%MAX_TRANSACTIONS],
                    stasis_log_file,
                    (0.9*stasis_log_file_write_buffer_size)/num_workers,
                    //512*1024/ua->divisor, // 0.5 mb in log tail at once
                    1000000/num_workers, // max num outstanding requests
                    (50 * 1024 * 1024)/num_workers // max backlog in bytes
                                                       );
      }
      } */

      for(long long j = 0; j < writes_per_xact; j++) {
        // long long idx = ((x*writes_per_xact)+j)%num_rids;
        long long idx = j % num_rids;
        //        if(!(j % 100)) { printf("."); fflush(stdout); }
        if(!(strcmp(mode, "normal")&&strcmp(mode, "normal-net"))) {
          TsetLsnFree(xid, rids[idx], &j);
        } else {
          assert(writeback);
          int old = cache[idx].val;
          cache[idx].val = j;
          if(net) {
          /*          TsetReorderableWriteBack(xid, handles[j%num_workers], cache[idx].pid,
                                   cache[idx].off, cache[idx].len,&j,&old);
          */
          } else {
            last_lsn = TsetWriteBack(xid, cache[idx].pid,cache[idx].off,
                                     cache[idx].len,&j,&old);
            assert(last_lsn);
          }
        }
      }
      /*    if(writeback && net) {
            for(int j = 0; j < num_workers; j++) {
            stasis_log_reordering_handle_close(handles[j]);
            }
            } */
    }
    if(net) {
      last_lsn = stasis_transaction_table[xid%MAX_TRANSACTIONS].prevLSN;
    }
    Tcommit(xid);
  }
  // XXX hack; really, want to register upcall in buffermanager...
  if(writeback) {
    printf("starting writeback"); fflush(stdout);
    assert(last_lsn);
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
