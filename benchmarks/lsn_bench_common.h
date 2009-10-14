#include <stasis/transactional.h>
#include <stasis/bufferManager.h>

#include <stdio.h>

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
  lsn_t log_trunc = ((stasis_log_t*)stasis_log())->truncation_point(stasis_log());
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
static int net_latency = 2;
static byte * (*origWrite)(int xid, Page *p, recordid rid);
byte * slowWrite(int xid, Page *p, recordid rid) {
  usleep(net_latency * 1000);
  return origWrite(xid,p,rid);
}

static const byte * (*origRead)(int xid, Page *p, recordid rid);
const byte * slowRead(int xid, Page *p, recordid rid) {
  usleep(net_latency * 1000);
  return origRead(xid,p,rid);
}

static int (*original_write_entry) (struct stasis_log_t* log, LogEntry * e);
int my_write_entry(struct stasis_log_t* log, LogEntry *e) {
  usleep(net_latency * 1000);
  return original_write_entry(log,e);
}

void emulate_remote_log() {
    original_write_entry = ((stasis_log_t*)stasis_log())->write_entry;
    ((stasis_log_t*)stasis_log())->write_entry = my_write_entry;
}
void emulate_remote_pages() {
    origWrite = stasis_page_impl_get(SLOTTED_LSN_FREE_PAGE)->recordWrite;
    origRead = stasis_page_impl_get(SLOTTED_LSN_FREE_PAGE)->recordRead;
    // xxx a bit of cheating; don't pay latency for lsn write
    //     (could amortize w/ recordWrite)
    stasis_page_impl_get(SLOTTED_LSN_FREE_PAGE)->recordWrite = slowWrite;
    stasis_page_impl_get(SLOTTED_LSN_FREE_PAGE)->recordRead = slowRead;
}

///////////  Background workers for parallelizing enqueues to slow logs
typedef struct {
  long long num_rids;
  long long rid_per_xact;
  recordid * rids;
  int done;
  pthread_mutex_t mut;
} bulk_worker_args;

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
  long long num_rids;
  long long rid_per_xact;
  cached_addr * cache;
  int done;
  int xid;
  int workerid;
  int iterationid;
  int divisor;
  pthread_mutex_t mut;
} writeback_unit_of_work_arg;
void * writeback_unit_of_work(void * ap) {
  writeback_unit_of_work_arg * ua = ap;

  stasis_log_reordering_handle_t * rh
    = stasis_log_reordering_handle_open(
                    stasis_transaction_table_get(stasis_runtime_transaction_table(), ua->xid),
                    stasis_log(),
                    (0.9*stasis_log_file_write_buffer_size)/ua->divisor,
                    //512*1024/ua->divisor, // 0.5 mb in log tail at once
                    1000000/ua->divisor, // max num outstanding requests
                    (50 * 1024 * 1024)/ua->divisor // max backlog in bytes
                                                       );
  /*
stasis_log_reordering_handle_open(&stasis_transaction_table[ua->xid%MAX_TRANSACTIONS],
                                        stasis_log_file,
                                        (stasis_log_file_write_buffer_size * 0.25)/ua->divisor,
                                        //512*1024/ua->divisor, // 0.5 mb in log tail at once
                                        1000000/ua->divisor, // max num outstanding requests
                                        (50 * 1024 * 1024)/ua->divisor // max backlog in bytes
                                        );*/
  for(int j = 0; j < ua->rid_per_xact/ua->divisor; j++) {
    long long idx = (ua->workerid+(ua->divisor*j)) % ua->num_rids;
    int old = ua->cache[idx].val;
    ua->cache[idx].val = (ua->workerid+(ua->divisor*j));
    TsetReorderableWriteBack(ua->xid, rh, ua->cache[idx].pid,
                             ua->cache[idx].off, ua->cache[idx].len,&ua->cache[idx].val,&old);
    //    TsetReorderable(ua->xid, rh, a->rids[(j*ua->divisor+ua->n)%a->num_rids], &val);

  }
  stasis_log_reordering_handle_close(rh);
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
    = stasis_log_reordering_handle_open(stasis_transaction_table_get(stasis_runtime_transaction_table(), ua->xid),
                                        stasis_log(),
                                        (stasis_log_file_write_buffer_size * 0.25)/ua->divisor,
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
    if(((stasis_log_t*)stasis_log())->write_entry == my_write_entry) {
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
