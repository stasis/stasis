/*
 * writeLatches.c
 *
 *  Created on: Oct 12, 2009
 *      Author: sears
 */
#include <stasis/util/rw.h>

#include <pthread.h>
#include <stdio.h>

char * usage = "%s numthreads numops\n";

unsigned long numops;

static void* worker(void* arg) {
  rwl * l = arg;

  for(unsigned long i = 0; i < numops; i++) {
    writelock(l, 0);
    unlock(l);
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
  rwl * l[numthreads];

  for(int i = 0; i < numthreads; i++) {
    l[i] = initlock();
    pthread_create(&workers[i], 0, worker, l[i]);
  }
  for(int i = 0; i < numthreads; i++) {
    pthread_join(workers[i], 0);
  }
}
