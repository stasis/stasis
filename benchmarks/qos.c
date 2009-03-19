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
  long long num_rids;
  long long rid_per_xact;
  recordid * rids;
  int done;
  pthread_mutex_t mut;
} bulk_worker_args;

static int (*original_write_entry) (struct stasis_log_t* log, LogEntry * e);

static int net_latency = 2;


int my_write_entry(struct stasis_log_t* log, LogEntry *e) {
  usleep(net_latency * 1000);
  return original_write_entry(log,e);
}

void* normal_worker(void * ap) {
  bulk_worker_args * a = ap;
  pthread_mutex_lock(&a->mut);
  for(int i = 0; !a->done; i++) {
    pthread_mutex_unlock(&a->mut);

    struct timeval tv;
    gettimeofday(&tv, 0);
    long long start = tv.tv_usec + tv.tv_sec * 1000000;

    int xid = Tbegin();
    for(int j = 0; j < a->rid_per_xact; j++) {
      int val = i * a->rid_per_xact + j;
      Tset(xid, a->rids[j%a->num_rids], &val);
    }
    Tcommit(xid);

    gettimeofday(&tv, 0);
    long long stop = tv.tv_usec + tv.tv_sec * 1000000;
    printf("low(ms),%lld\n", stop-start);
    fflush(stdout);

    pthread_mutex_lock(&a->mut);
  }
  pthread_mutex_unlock(&a->mut);
  return 0;

}
typedef struct {
  int xid;
  int n; // which worker am i?
  int i; // which iteration is this?
  int divisor;
  bulk_worker_args * a;
} unit_of_work_arg;
void * bg_unit_of_work(void * ap) {
  unit_of_work_arg * ua = ap;
  bulk_worker_args * a = ua->a;

  stasis_log_reordering_handle_t * rh
    = stasis_log_reordering_handle_open(&XactionTable[ua->xid%MAX_TRANSACTIONS],
                                        stasis_log_file,
                                        (stasis_log_write_buffer_size * 0.25)/ua->divisor,
                                        //512*1024/ua->divisor, // 0.5 mb in log tail at once
                                        1000000/ua->divisor, // max num outstanding requests
                                        (50 * 1024 * 1024)/ua->divisor // max backlog in bytes
                                        );
  for(int j = 0; j < a->rid_per_xact/ua->divisor; j++) {
    int val = ua->i * (a->rid_per_xact/ua->divisor) + j;
    TsetReorderable(ua->xid, rh, a->rids[(j*ua->divisor+ua->n)%a->num_rids], &val);
  }
  stasis_log_reordering_handle_close(rh);
  return 0;
}

void* bg_worker(void * ap) {
  bulk_worker_args * a = ap;
  pthread_mutex_lock(&a->mut);
  for(int i = 0; !a->done; i++) {
    pthread_mutex_unlock(&a->mut);

    struct timeval tv;
    gettimeofday(&tv, 0);
    long long start = tv.tv_usec + tv.tv_sec * 1000000;

    int xid = Tbegin();
    if(stasis_log_file->write_entry == my_write_entry) {
      // based on tweaking; also, normal-net is ~ 100x slower than nromal
      int num_worker = 100;
      pthread_t workers[num_worker];
      unit_of_work_arg args[num_worker];
      for(int w = 0; w < num_worker; w++) {
        args[i].xid = xid;
        args[i].n = w;
        args[i].i = i;
        args[i].divisor = num_worker;
        args[i].a = a;
        pthread_create(&workers[w], 0, bg_unit_of_work, &(args[i]));
      }
      for(int w = 0; w < num_worker; w++) {
        pthread_join(workers[w], 0);
      }

    } else {
      unit_of_work_arg unit_arg = {
        xid,
        0,
        i,
        1,
        ap
      };
      bg_unit_of_work(&unit_arg);
    }
    Tcommit(xid);

    gettimeofday(&tv, 0);
    long long stop = tv.tv_usec + tv.tv_sec * 1000000;
    printf("low(ms),%lld\n", stop-start);
    fflush(stdout);

    pthread_mutex_lock(&a->mut);
  }
  pthread_mutex_unlock(&a->mut);
  return 0;
}

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

  stasis_log_write_buffer_size = 50 * 1024 * 1024;

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
    original_write_entry = stasis_log_file->write_entry;
    stasis_log_file->write_entry = my_write_entry;
    pthread_create(&worker, 0, normal_worker, &a);
  } else if (!strcmp(mode, "bg-net")) {
    original_write_entry = stasis_log_file->write_entry;
    stasis_log_file->write_entry = my_write_entry;
    pthread_create(&worker, 0, bg_worker, &a);
  } else {
    assert(!strcmp(mode, "bg"));
    pthread_create(&worker, 0, bg_worker, &a);
  }

  sleep(10);
  // sleep 10 (reach steady state)

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
