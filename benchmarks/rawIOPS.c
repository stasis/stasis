/*
 * rawIOPS.c
 *
 *  Created on: Aug 26, 2011
 *      Author: sears
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE // for O_DIRECT
#endif
#include <config.h>
#include <stasis/common.h>
#include <stasis/util/random.h>
#include <stasis/util/time.h>
#include <stasis/util/histogram.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>

DECLARE_HISTOGRAM_64(iop_hist)

static const long MB = (1024 * 1024);

typedef struct {
  int fd;
  int page_size;
  uint64_t start_off;
  uint64_t end_off;
  uint64_t opcount;
  double elapsed;
} thread_arg;

uint64_t completed_ops;
uint64_t op_count;

void * status_worker(void * ignored) {
  uint64_t last_ops = 0;
  int iter = 0;
  while(1) {
    struct timespec ts = stasis_double_to_timespec(1.0);
    nanosleep(&ts,0);
    printf("current ops/sec %lld\n", (long long) (completed_ops - last_ops));
    last_ops = completed_ops;
    iter ++;
    if((! (iter % 10)) && (op_count == 0)) {
      stasis_histograms_auto_dump();
      stasis_histogram_64_clear(&iop_hist);
    }
  }
}

void * worker(void * argp) {
  thread_arg * arg = argp;

  void * buf = 0;
  int err;
#ifdef HAVE_POSIX_MEMALIGN
  err = posix_memalign(&buf, 512, arg->page_size);
  if(err) {
    printf("Couldn't allocate memory with posix_memalign: %s\n", strerror(err));
    fflush(stdout);
    abort();
  }
#else
  buf = malloc(arg->page_size * 2);
  buf = (void*)((intptr_t)buf & ~(arg->page_size-1));
#endif
  struct timeval start, stop;
  for(uint64_t i = 0; (!arg->opcount) || i <  arg->opcount; i++) {
    gettimeofday(&start, 0);
    uint64_t offset
      = arg->start_off + stasis_util_random64(arg->end_off
                                   - (arg->start_off+arg->page_size));
    offset &= ~(arg->page_size-1);
    DEBUG("pread(%d %x %d %lld)\n", arg->fd,
          (unsigned int)buf, (int)arg->page_size, (long long)offset);
    stasis_histogram_tick(&iop_hist);
    err = pread(arg->fd, buf, arg->page_size, offset);
    stasis_histogram_tock(&iop_hist);
    __sync_fetch_and_add(&completed_ops, 1);
    if(err == -1) {
      perror("Could not read from file"); fflush(stderr); fflush(stdout); abort();
    }
    gettimeofday(&stop, 0);
    arg->elapsed += stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
  }
#ifdef HAVE_POSIX_MEMALIGN
  free(buf);
#endif
  return 0;
}

int main(int argc, char * argv[]) {
  if(argc != 7) {
    printf("Usage %s filename page_size num_threads op_count start_off end_off\n", argv[0]);
    printf(" Note:  If you get errors about invalid arguments during read, make sure\n"
           "        page size is a power of two, and >= 512.");
    abort();
  }
  char * filename    = argv[1];
  int page_size      = atoi(argv[2]);
  int num_threads    = atoi(argv[3]);
  op_count  = atoll(argv[4]);
  uint64_t start_off = atoll(argv[5]);
  uint64_t end_off   = atoll(argv[6]) * MB;

  completed_ops = 0;

#ifdef HAVE_O_DIRECT
  int fd = open(filename, O_RDONLY|O_DIRECT);
#else
  printf("Warning: not using O_DIRECT; file system cache will be used.\n");
  int fd = open(filename, O_RDONLY);
#endif
  if(fd == -1) {
    perror("Couldn't open file");
    abort();
  }
  struct timeval start, stop;
  pthread_t status;
  pthread_t * threads = stasis_malloc(num_threads, pthread_t);
  thread_arg * arg = stasis_malloc(num_threads, thread_arg);

  gettimeofday(&start,0);
  pthread_create(&status, 0, status_worker, 0);
  for(int i = 0; i < num_threads; i++) {
    arg[i].fd = fd;
    arg[i].page_size = page_size;
    arg[i].start_off = start_off;
    arg[i].end_off = end_off;
    arg[i].opcount = op_count / num_threads;
    arg[i].elapsed = 0.0;
    pthread_create(&threads[i], 0, worker, &arg[i]);
  }

  double sum_elapsed = 0;
  for(int i = 0; i < num_threads; i++) {
    pthread_join(threads[i], 0);
    sum_elapsed += arg[i].elapsed;
  }
  gettimeofday(&stop,0);
  for(int i = 0; i < num_threads; i++) {
    if(arg[i].elapsed < (sum_elapsed / (1.5 * (double)num_threads))) {
      fprintf(stderr, "Warning: skew detected.  Thread %d took %f seconds, mean was %f\n",
             i, arg[i].elapsed, sum_elapsed/num_threads);
    }
  }
  double wallclock_elapsed = stasis_timeval_to_double(
                              stasis_subtract_timeval(stop, start));
  printf("%d threads %lld mb %lld ops / %f seconds = %f IOPS.\n", num_threads, (long long)(end_off / MB), (long long)op_count, wallclock_elapsed, ((double)op_count) / wallclock_elapsed);

  close(fd);
  stasis_histograms_auto_dump();
  return 0;
}
