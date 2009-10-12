/*
 * pinDifferentPages.c
 *
 *  Created on: Oct 12, 2009
 *      Author: sears
 */

#include <stasis/transactional.h>

#include <pthread.h>
#include <stdio.h>


char * usage = "%s numthreads numops\n";

unsigned long numops;

static void* worker(void* arg) {
  pageid_t pid = *(pageid_t*)arg;
  for(unsigned long i = 0; i < numops; i++) {
    Page * p = loadPage(-1, pid);
    releasePage(p);
  }

  return 0;
}

int main(int argc, char * argv[]) {
  if(argc != 3) { printf(usage, argv[0]); abort(); }
  char * endptr;
  unsigned long numthreads = strtoul(argv[1], &endptr, 10);
  if(*endptr != 0) { printf(usage, argv[0]); abort(); }
  numops= strtoul(argv[2], &endptr, 10) / numthreads;
  if(*endptr != 0) { printf(usage, argv[0]); abort(); }

  pthread_t workers[numthreads];
  pageid_t pids[numthreads];

  Tinit();

  for(int i = 0; i < numthreads; i++) {
    pids[i] = i;
    pthread_create(&workers[i], 0, worker, &pids[i]);
  }
  for(int i = 0; i < numthreads; i++) {
    pthread_join(workers[i], 0);
  }

  Tdeinit();

}
