#include "lsn_bench_common.h"

int main(int argc, char** argv) {
  char * mode = argv[1];
  bulk_worker_args a;
  a.num_rids = atoll(argv[2]);
  a.rid_per_xact = atoll(argv[3]);

  a.done = 0;
  pthread_mutex_init(&a.mut,0);
  unlink("storefile.txt");
  unlink("logfile.txt");

  // disable truncation, as it interferes w/ the benchmark.

  stasis_truncation_automatic = 0;
  // XXX instead of overriding this, set tail of priority log to 80%
  // stasis log buf or something...

  //  stasis_log_write_buffer_size = 50 * 1024 * 1024;

  printf("%s %s %s %s %lld\n", argv[0], argv[1], argv[2], argv[3],
         stasis_log_write_buffer_size);

  Tinit();

  // 10% as big as slow rids; interspersed
  recordid * fast_rids;

  alloc_rids(a.num_rids, &a.rids, &fast_rids);
  pthread_t worker;
  if(!strcmp(mode, "none")) {
    // nop
  } else if (!strcmp(mode, "normal")) {
    pthread_create(&worker, 0, normal_worker, &a);
  } else if (!strcmp(mode, "normal-net")) {
    emulate_remote_log();
    pthread_create(&worker, 0, normal_worker, &a);
  } else if (!strcmp(mode, "bg-net")) {
    emulate_remote_log();
    pthread_create(&worker, 0, bg_worker, &a);
  } else {
    assert(!strcmp(mode, "bg"));
    pthread_create(&worker, 0, bg_worker, &a);
  }

  sleep(10);

  // run benchmark here
  for(int i = 0; i < 60; i++) {
    struct timeval tv;
    gettimeofday(&tv, 0);
    long long start = tv.tv_usec + tv.tv_sec * 1000000;
    int xid = Tbegin();
    TsetLsnFree(xid, fast_rids[i % (a.num_rids/10)], &i);
    Tcommit(xid);
    gettimeofday(&tv, 0);
    long long stop = tv.tv_usec + tv.tv_sec * 1000000;

    printf("high(ms),%lld\n", stop-start);
    fflush(stdout);
    sleep(1);

  }

  pthread_mutex_lock(&a.mut);
  a.done = 1;
  pthread_mutex_unlock(&a.mut);

  pthread_join(worker, 0);
  Tdeinit();

}
