/*
 * hashPerformance.c
 *
 *  Created on: Aug 24, 2011
 *      Author: sears
 */
#include <stasis/common.h>
#include <stasis/util/hash.h>
#include <stasis/util/time.h>
#include <stdio.h>

int main(int argc, char * argv[]) {
  char * foo = calloc(1024*1024*1024,1);
  struct timeval start, stop;
  gettimeofday(&start, 0);
  for(long i = 0; i < (1024*1024*1024/sizeof(long)); i++) {
    ((long*)foo)[i] = i;
  }
  gettimeofday(&stop, 0);

  double elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
  printf("Took %f seconds to write to 1GB (%f mb/sec)\n", elapsed, (1024.0)/elapsed);

  long len = 1;
  for(long i = 0; i < 31; i++) {
    gettimeofday(&start, 0);
    stasis_crc32(foo, len, 0);
    gettimeofday(&stop, 0);

    elapsed = stasis_timeval_to_double(stasis_subtract_timeval(stop, start));
    printf("Took %f seconds to checksum %ld bytes (%f mb/sec)\n", elapsed, len, ((double)len)/((1024.0*1024.0)*elapsed));
    len *=2;
  }
}
