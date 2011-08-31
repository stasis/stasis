/*
 * rawIOPS.c
 *
 *  Created on: Aug 26, 2011
 *      Author: sears
 */
#include <config.h>
#include <stasis/common.h>
#include <stasis/util/random.h>
#include <stasis/util/time.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>

static const long MB = (1024 * 1024);

typedef struct {
  int fd;
  int page_size;
  uint64_t start_off;
  uint64_t end_off;
  uint64_t opcount;
  double elapsed;
} thread_arg;

void * worker(void * argp) {
  thread_arg * arg = argp;

  void * buf = 0;
  int err = posix_memalign(&buf, 512, arg->page_size);
  if(err) {
    printf("Couldn't allocate memory with posix_memalign: %s\n", strerror(err));
    fflush(stdout);
    abort();
  }

  struct timeval start, stop;
  for(uint64_t i = 0; i <  arg->opcount; i++) {
    gettimeofday(&start, 0);
    uint64_t offset
      = arg->start_off + stasis_util_random64(arg->end_off
                                   - (arg->start_off+arg->page_size));
    offset &= ~(arg->page_size-1);
    DEBUG("pread(%d %x %d %lld)\n", arg->fd,
          (unsigned int)buf, (int)arg->page_size, (long long)offset);
    err = pread(arg->fd, buf, arg->page_size, offset);
    if(err == -1) {
      perror("Could not read from file"); fflush(stderr); fflush(stdout); abort();
    }
    gettimeofday(&stop, 0);
    arg->elapsed += stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
  }
  free(buf);
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
  uint64_t op_count  = atoll(argv[4]);
  uint64_t start_off = atoll(argv[5]);
  uint64_t end_off   = atoll(argv[6]) * MB;

  int fd = open(filename, O_RDONLY|O_DIRECT);

  if(fd == -1) {
    perror("Couldn't open file");
    abort();
  }
  struct timeval start, stop;

  pthread_t * threads = malloc(sizeof(threads[0]) * num_threads);
  thread_arg * arg = malloc(sizeof(arg[0]) * num_threads);

  gettimeofday(&start,0);
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
      printf("Warning: skew detected.  Thread %d took %f seconds, mean was %f\n",
             i, arg[i].elapsed, sum_elapsed/num_threads);
    }
  }
  double wallclock_elapsed = stasis_timeval_to_double(
                              stasis_subtract_timeval(stop, start));
  printf("%d threads %lld mb %lld ops / %f seconds = %f IOPS.", num_threads, end_off / MB, (long long)op_count, wallclock_elapsed, ((double)op_count) / wallclock_elapsed);

  close(fd);


}
