/**
 * stride.c
 *
 *  Created on: Sep 1, 2011
 *      Author: sears
 */
#include <config.h>
#include <stasis/common.h>
#include <stasis/util/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

int main(int argc, char * argv[]) {
  if(argc != 6) {
    printf("usage: %s filename num_workers num_ops write_size stride", argv[0]); fflush(stdout); abort();
  }
  int NUM_WORKERS = atoi(argv[2]);
  uint64_t ops = atoll(argv[3]);
  int write_size = atoi(argv[4]);
  uint64_t stride = atoll(argv[5]);
  int fd = -1;
#ifdef HAVE_O_DSYNC
  fd = open(argv[1], O_WRONLY|O_DSYNC); //|O_DIRECT);
#else
  fd = open(argv[1], O_WRONLY|O_SYNC); //|O_DIRECT);
#endif
  struct timeval start, stop;
  void * buf;
#ifdef HAVE_POSIX_MEMALIGN
  posix_memalign(&buf, 512, write_size);
#else
  buf = malloc(2 * write_size);
  buf = (void*)(((intptr_t)buf) & ~(write_size-1));
#endif
  memset(buf, 0, write_size);

  gettimeofday(&start, 0);

  for(uint64_t i = 0; i < ops; i++) {
    int err = pwrite(fd, buf, write_size, stride * i * write_size);
    if(err == -1) {
      perror("Couldn't write");
      abort();
    }
    assert(err && err == write_size);
  }

  gettimeofday(&stop, 0);

  double elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop,start));
  double ops_per_sec = ((double)ops) / elapsed;
  printf("%lld ops in %f seconds = %f ops/second, %f speedup, %f mb/sec\n", (long long)ops, elapsed, ops_per_sec, ops_per_sec/ /*166.666*/(/*7200.0*/5400.0/60.0), ((double)ops*write_size)/(1024.0*1024.0*elapsed));

  close(fd);
}
