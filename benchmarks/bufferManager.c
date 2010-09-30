  /*
   * bufferManager.c
   *
   *  Created on: Sep 23, 2010
   *      Author: sears
   */
#include <stasis/transactional.h>
#include <stasis/util/histogram.h>
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
  stasis_histogram_64_t * load_hist;
  stasis_histogram_64_t * release_hist;
  pageid_t page_count;
};
int do_load(pageid_t page_count) {
    for(int i = 1; i < page_count; i++) {
      Page * p = loadUninitializedPage(-1, i);
      stasis_fixed_initialize_page(p, sizeof(i), 1);
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

  for(int i = 0; i < a->num_ops; i++) {
    pageid_t pid = rand_r(&a->seed) % a->page_count;

    struct timeval start, stop;
    gettimeofday(&start,0);
    Page * p = loadPage(-1, pid);
    gettimeofday(&stop,0);
    stasis_histogram_insert_log_timeval(a->load_hist, stasis_subtract_timeval(stop, start));

    gettimeofday(&start,0);
    releasePage(p);
    gettimeofday(&stop,0);
    stasis_histogram_insert_log_timeval(a->release_hist, stasis_subtract_timeval(stop, start));
  }
  free(argp);
  return 0;
}
int do_operations(pageid_t page_count, int num_threads, unsigned long long num_ops) {
  unsigned long long ops_per_thread = ceil(((double)num_ops) / (double)num_threads);
  unsigned long long ops_remaining = num_ops;
  pthread_t * threads = malloc(sizeof(threads[0]) * num_threads);

  for(int i = 0; i < num_threads ; i++) {
    if(ops_remaining <= 0) { num_threads = i; break; }
    struct thread_arg *a = malloc(sizeof(*a));
    a->seed = i;
    a->num_ops = ops_remaining < ops_per_thread ? ops_remaining : ops_per_thread;
    a->load_hist = &load_hist;
    a->release_hist = &release_hist;
    a->page_count = page_count;
    pthread_create(&threads[i], 0, random_op_thread, a);
    ops_remaining -= ops_per_thread;
  }
  for(int i = 0 ; i < num_threads; i++) {
    pthread_join(threads[i], 0);
  }
  return 0;
}
int main(int argc, char * argv[]) {
  pageid_t file_size;
  int load = 0;
  int scan = 0;
  int threads = 1;
  unsigned long long num_ops = 0;
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
    } else {
      fprintf(stderr, "unknown argument: %s\n)", argv[i]);
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
    do_operations(file_size, threads, num_ops);
  }
  printf("Calling Tdeinit().\n");
  Tdeinit();
  printf("Tdeinit() done\n");
  stasis_histograms_auto_dump();
  return 0;
}
