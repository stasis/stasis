/*
 * dirtyPages.c
 *
 *  Created on: Oct 12, 2009
 *      Author: sears
 */

#include <stasis/transactional.h>
#include <pthread.h>
#include <stdio.h>

char * usage = "%s numthreads numops\n";

stasis_dirty_page_table_t * dpt;

unsigned long numops;
unsigned long numthreads;
Page ** p;

static void* worker(void* arg) {
  Page * p = arg;
  for(unsigned long i = 0; i < numops; i++) {
    stasis_dirty_page_table_set_dirty(dpt, p);
  }
  return 0;
}

int main(int argc, char * argv[]) {
  if(argc != 3) { printf(usage, argv[0]); abort(); }
  char * endptr;
  numthreads = strtoul(argv[1], &endptr, 10);
  if(*endptr != 0) { printf(usage, argv[0]); abort(); }
  numops= strtoul(argv[2], &endptr, 10) / numthreads;
  if(*endptr != 0) { printf(usage, argv[0]); abort(); }

  pthread_t workers[numthreads];

  p = malloc(sizeof(Page *) * numthreads);

  Tinit();

  dpt = stasis_runtime_dirty_page_table();

  for(int i = 0; i < numthreads; i++) {
    p[i] = loadPage(-1, i);
  }

  for(int i = 0; i < numthreads; i++) {
    pthread_create(&workers[i], 0, worker, p[i]);
  }
  for(int i = 0; i < numthreads; i++) {
    pthread_join(workers[i], 0);
  }

  for(int i = 0; i < numthreads; i++) {
    releasePage(p[i]);
  }

  Tdeinit();
}

