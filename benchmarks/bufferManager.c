  /*
   * bufferManager.c
   *
   *  Created on: Sep 23, 2010
   *      Author: sears
   */
#include <stasis/flags.h>
#include <stasis/io/handle.h>
#include <stasis/transactional.h>
#include <stasis/util/histogram.h>
#include <stasis/page/fixed.h>

#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <math.h>
#include <stdlib.h>

DECLARE_HISTOGRAM_64(load_hist)
DECLARE_HISTOGRAM_64(release_hist)

struct thread_arg {
  unsigned int seed;
  unsigned long long num_ops;
  double write_frac;
  stasis_histogram_64_t * load_hist;
  stasis_histogram_64_t * release_hist;
  pageid_t page_count;
  unsigned long long target_ops;
};
int do_load(pageid_t page_count) {
    for(int i = 1; i < page_count; i++) {
      Page * p = loadUninitializedPage(-1, i);
      stasis_page_fixed_initialize_page(p, sizeof(i), 1);
      recordid rid = {i, 0, sizeof(i)};
      stasis_record_write(-1, p, rid,(byte*) &i);
      stasis_page_lsn_write(-1, p, p->LSN + 1);
      releasePage(p);
    }
    return 0;
  }

int do_scan(pageid_t page_count) {
  for(int i = 1; i < page_count; i++) {
    Page * p = loadPage(-1, i);
    recordid rid = {i, 0, sizeof(i)};
    stasis_record_write(-1, p, rid,(byte*) &i);
    assert(i == rid.page);
    releasePage(p);
  }
  return 0;
}
void * random_op_thread(void * argp) {
  struct thread_arg * a = argp;
  struct timeval start_time;
  gettimeofday(&start_time,0);
  int xid = Tbegin();

  stasis_log_t * l = stasis_log();
  lsn_t lsn = l->next_available_lsn(l);

  for(int i = 0; i < a->num_ops; i++) {
    pageid_t pid = rand_r(&a->seed) % a->page_count;
    int op = (rand_r(&a->seed) < (int)(a->write_frac * (double)RAND_MAX));

    struct timeval start;
    gettimeofday(&start,0);
    double elapsed = stasis_timeval_to_double(stasis_subtract_timeval(start, start_time));


    while(a->target_ops &&
	  elapsed < ((double)i) / (double)a->target_ops) {
      struct timespec ts = stasis_double_to_timespec(0.001);
      nanosleep(&ts, 0);
      gettimeofday(&start,0);
      elapsed = stasis_timeval_to_double(stasis_subtract_timeval(start, start_time));
    }
    stasis_histogram_tick(a->load_hist);
    Page * p = loadPage(xid, pid);
    stasis_histogram_tock(a->load_hist);
    if(op) { // Do write
      stasis_page_lsn_write(xid, p, lsn);
    }
    stasis_histogram_tick(a->release_hist);
    releasePage(p);
    stasis_histogram_tock(a->release_hist);
  }
  Tcommit(xid);
  free(argp);
  return 0;
}
int do_operations(pageid_t page_count, int num_threads, unsigned long long num_ops, double write_frac, int target_ops) {
  unsigned long long ops_per_thread = ceil(((double)num_ops) / (double)num_threads);
  unsigned long long ops_remaining = num_ops;
  pthread_t * threads = malloc(sizeof(threads[0]) * num_threads);

  struct timeval tv;
  gettimeofday(&tv,0);
  uint64_t base_seed = tv.tv_usec;

  for(int i = 0; i < num_threads ; i++) {
    if(ops_remaining <= 0) { num_threads = i; break; }
    struct thread_arg *a = malloc(sizeof(*a));
    a->seed = base_seed + i;
    a->num_ops = ops_remaining < ops_per_thread ? ops_remaining : ops_per_thread;
    a->write_frac = write_frac;
    a->load_hist = &load_hist;
    a->release_hist = &release_hist;
    a->page_count = page_count;
    a->target_ops = target_ops / num_threads;
    if(i == num_threads - 1) {
      a->target_ops = target_ops - (a->target_ops * (num_threads-1));
    }
    pthread_create(&threads[i], 0, random_op_thread, a);
    ops_remaining -= ops_per_thread;
  }
  for(int i = 0 ; i < num_threads; i++) {
    pthread_join(threads[i], 0);
  }
  return 0;
}
int main(int argc, char * argv[]) {
  pageid_t file_size = 1024 * 128;
  int load = 0;
  int scan = 0;
  int threads = 1;
  unsigned long long num_ops = 0;
  int target_ops = 0;
  double write_frac = 0.5;
  stasis_buffer_manager_hint_writes_are_sequential = 1;
  for(int i = 1; i < argc; i++) {
    if(!strcmp(argv[i], "--mem-size")) {
      i++;
      stasis_buffer_manager_size = atoll(argv[i]) * 1024 * 1024 / PAGE_SIZE;
    } else if(!strcmp(argv[i], "--file-size")) {
      i++;
      file_size = (atoll(argv[i]) * 1024 * 1024) / PAGE_SIZE;
    } else if(!strcmp(argv[i], "--load")) {
      load = 1;
    } else if(!strcmp(argv[i], "--threads")) {
      i++;
      threads = atoi(argv[i]);
    } else if(!strcmp(argv[i], "--scan")) {
      scan = 1;
    } else if(!strcmp(argv[i], "--numops")) {
      i++;
      num_ops = atoll(argv[i]);
    } else if(!strcmp(argv[i], "--write-frac")) {
      i++;
      write_frac = atof(argv[i]);
    } else if(!strcmp(argv[i], "--target-ops")) {
      i++;
      target_ops = atoi(argv[i]);
    } else if(!strcmp(argv[i], "--raid1")) {
      stasis_handle_factory = stasis_handle_raid1_factory;
    } else if(!strcmp(argv[i], "--raid0")) {
      stasis_handle_factory = stasis_handle_raid0_factory;
    } else {
      fprintf(stderr, "unknown argument: %s\n", argv[i]);
      abort();
    }
   }

  printf("Calling Tinit().\n");
  Tinit();
  printf("Tinit() done.\n");
  if(load) {
    printf("Loading %lld pages (%lld megabytes)\n", file_size, (file_size * PAGE_SIZE)/(1024*1024));
    do_load(file_size);
  }
  if(scan) {
    printf("Scanning %lld pages (%lld megabytes)\n", file_size, (file_size * PAGE_SIZE)/(1024*1024));
    do_scan(file_size);
  }
  if(num_ops) {
    printf("Performing %lld uniform random operations\n", num_ops);
    do_operations(file_size, threads, num_ops, write_frac, target_ops);
  }
  printf("Calling Tdeinit().\n");
  Tdeinit();
  printf("Tdeinit() done\n");
  stasis_histograms_auto_dump();
  return 0;
}
