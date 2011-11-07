/*
 * butterfly.c
 *
 *  Created on: Sep 9, 2011
 *      Author: sears
 */
#include <config.h>
#include <stasis/common.h>
#include <stasis/util/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

int main(int argc, char * argv[]) {
  if(argc != 4) {
    printf("usage: %s filename file_size sector_size\n", argv[0]); fflush(stdout); abort();
  }
  const char * filename = argv[1];
  uint64_t file_size = atoll(argv[2]);
  int sector_size = atoi(argv[3]);

  int fd = -1;
  fd = open(filename, O_WRONLY|O_DSYNC); //|O_DIRECT);

  struct timeval start, stop;
  void * buf;
  posix_memalign(&buf, sector_size, sector_size);
  memset(buf, 0, sector_size);


  gettimeofday(&stop,0);
  for(uint64_t i = 0; i < file_size/sector_size; i++) {
    int err = pwrite(fd, buf, sector_size, 0);
    if(err == -1) {
      perror("Couldn't write");
      abort();
    }
    assert(err && err == sector_size);
    gettimeofday(&start, 0);
    double reset_elapsed = stasis_timeval_to_double(stasis_subtract_timeval(start,stop));
    err = pwrite(fd, buf, sector_size, i * sector_size);
    if(err == -1) {
      perror("Couldn't write");
      abort();
    }
    assert(err && err == sector_size);

    gettimeofday(&stop, 0);
    double elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop,start));
    printf("%lld\t%0.5f\t%0.5f\n", (long long) i, elapsed, reset_elapsed);
  }

  close(fd);
}

